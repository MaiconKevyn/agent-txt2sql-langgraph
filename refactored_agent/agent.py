import logging
import re
from typing import Dict, Any, List, Optional

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

from .config import AgentConfig
from .db import Database
from .llm import LLMClient
from .sql_utils import is_select_only, sanitize_sql
from .state import ChatState, make_turn_input

logger = logging.getLogger("refactored_agent.agent")

SCHEMA_KEYWORDS = ["tabela", "coluna", "schema", "estrutura", "dicionario"]
CONVERSATIONAL_KEYWORDS = ["o que e", "oque e", "significa", "defina", "explique", "como funciona"]
KEYWORD_TABLE_MAP = {
    "morte": "mortes",
    "morreu": "mortes",
    "morreram": "mortes",
    "obito": "mortes",
    "óbito": "mortes",
    "mortalidade": "mortes",
    "falec": "mortes",
    "uti": "uti_detalhes",
    "terapia intensiva": "uti_detalhes",
    "obstet": "obstetricos",
    "gestante": "obstetricos",
    "prenatal": "obstetricos",
    "parto": "obstetricos",
    "cid": "cid10",
    "diagnost": "cid10",
    "diagnóstico": "cid10",
    "doenca": "cid10",
    "doença": "cid10",
    "proced": "procedimentos",
    "cirurgia": "procedimentos",
    "hospital": "hospital",
    "municipio": "municipios",
    "município": "municipios",
    "cidade": "municipios",
    "ibge": "dado_ibge",
    "população": "dado_ibge",
    "instrucao": "instrucao",
}


class LangGraphChatAgent:
    def __init__(self, config: AgentConfig):
        self.config = config
        self.db = Database(config.database_url)
        self.llm = LLMClient(
            provider=config.llm_provider,
            model=config.llm_model,
            temperature=config.llm_temperature,
            timeout=config.llm_timeout,
        )
        self.workflow = self._build_workflow()

    def ask(self, user_query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        turn_input = make_turn_input(user_query, max_retries=2)
        config = {}
        if session_id:
            config = {"configurable": {"thread_id": session_id}}
        final_state = self.workflow.invoke(turn_input, config=config)
        return self._state_to_result(final_state)

    def _state_to_result(self, state: ChatState) -> Dict[str, Any]:
        return {
            "success": not bool(state.get("error")),
            "route": state.get("route"),
            "sql": state.get("validated_sql") or state.get("generated_sql"),
            "rows": state.get("results", []),
            "response": state.get("response", ""),
            "error": state.get("error", ""),
        }

    def _build_workflow(self):
        graph = StateGraph(ChatState)

        graph.add_node("init_turn", self._init_turn)
        graph.add_node("classify", self._classify)
        graph.add_node("list_tables", self._list_tables)
        graph.add_node("get_schema", self._get_schema)
        graph.add_node("generate_sql", self._generate_sql)
        graph.add_node("validate_sql", self._validate_sql)
        graph.add_node("repair_sql", self._repair_sql)
        graph.add_node("execute_sql", self._execute_sql)
        graph.add_node("respond", self._respond)

        graph.add_edge(START, "init_turn")
        graph.add_edge("init_turn", "classify")

        graph.add_conditional_edges(
            "classify",
            self._route_after_classify,
            {
                "database": "list_tables",
                "schema": "list_tables",
                "conversational": "respond",
            },
        )

        graph.add_edge("list_tables", "get_schema")

        graph.add_conditional_edges(
            "get_schema",
            self._route_after_schema,
            {
                "database": "generate_sql",
                "schema": "respond",
            },
        )

        graph.add_edge("generate_sql", "validate_sql")

        graph.add_conditional_edges(
            "validate_sql",
            self._route_after_validate,
            {
                "execute": "execute_sql",
                "repair": "repair_sql",
                "respond": "respond",
            },
        )

        graph.add_edge("repair_sql", "validate_sql")

        graph.add_conditional_edges(
            "execute_sql",
            self._route_after_execute,
            {
                "respond": "respond",
                "repair": "repair_sql",
            },
        )

        graph.add_edge("respond", END)

        return graph.compile(checkpointer=MemorySaver())

    def _init_turn(self, state: ChatState) -> Dict[str, Any]:
        user_query = self._last_user_message(state.get("messages", []))
        return {
            "user_query": user_query,
            "route": "",
            "available_tables": [],
            "selected_tables": [],
            "schema_context": "",
            "generated_sql": "",
            "validated_sql": "",
            "results": [],
            "response": "",
            "error": "",
            "retry_count": 0,
            "sql_provided": False,
            "max_retries": state.get("max_retries", 2) or 2,
        }

    def _classify(self, state: ChatState) -> Dict[str, Any]:
        raw_query = (state.get("user_query") or "").strip()
        q = raw_query.lower()
        if self._looks_like_sql(raw_query):
            return {
                "route": "database",
                "generated_sql": self._clean_sql(raw_query),
                "sql_provided": True,
            }

        if any(k in q for k in SCHEMA_KEYWORDS):
            return {"route": "schema"}
        if any(k in q for k in CONVERSATIONAL_KEYWORDS):
            return {"route": "conversational"}
        return {"route": "database"}

    def _route_after_classify(self, state: ChatState) -> str:
        return state.get("route", "database")

    def _list_tables(self, state: ChatState) -> Dict[str, Any]:
        tables = self.db.list_tables()
        selected = self._select_tables(state.get("user_query", ""), tables)
        return {
            "available_tables": tables,
            "selected_tables": selected,
        }

    def _get_schema(self, state: ChatState) -> Dict[str, Any]:
        tables = state.get("selected_tables", [])
        if not tables:
            return {"schema_context": ""}
        schema_text = self.db.get_schema_text(tables)
        return {"schema_context": self._truncate(schema_text, self.config.max_schema_chars)}

    def _route_after_schema(self, state: ChatState) -> str:
        return "database" if state.get("route") == "database" else "schema"

    def _generate_sql(self, state: ChatState) -> Dict[str, Any]:
        if state.get("sql_provided") and state.get("generated_sql"):
            return {}

        join_hints = self._build_join_hints(state.get("selected_tables", []))
        system_prompt = """PostgreSQL expert for SUS data. Return ONLY SQL, no text.

MANDATORY RULES:
1. Column names MUST have double quotes: "SEXO", "N_AIH", "IDADE"
2. SEXO values: 1=male/homem, 3=female/mulher (NEVER use 2!)
3. Deaths use mortes table joined with internacoes ON mortes."N_AIH" = internacoes."N_AIH"

COPY THESE PATTERNS EXACTLY:
- Count women: SELECT COUNT(*) FROM internacoes WHERE "SEXO" = 3;
- Count men: SELECT COUNT(*) FROM internacoes WHERE "SEXO" = 1;
- Male deaths: SELECT COUNT(*) FROM mortes m JOIN internacoes i ON m."N_AIH" = i."N_AIH" WHERE i."SEXO" = 1;
- Female deaths: SELECT COUNT(*) FROM mortes m JOIN internacoes i ON m."N_AIH" = i."N_AIH" WHERE i."SEXO" = 3;
- Total deaths: SELECT COUNT(*) FROM mortes;"""
        user_content = (
            f"User question: {state.get('user_query', '')}\n\n"
            f"Schema:\n{state.get('schema_context', '')}\n\n"
            f"Join hints:\n{join_hints or 'none'}\n\n"
            "SQL:" 
        )
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_content)]
        sql = self.llm.invoke(messages)
        return {"generated_sql": self._clean_sql(sql)}

    def _validate_sql(self, state: ChatState) -> Dict[str, Any]:
        sql = state.get("generated_sql") or ""
        retry_count = state.get("retry_count", 0)
        if not sql:
            return {"error": "SQL vazia", "retry_count": retry_count + 1}

        ok, reason = is_select_only(sql)
        if not ok:
            return {"error": reason, "retry_count": retry_count + 1}

        valid, err = self.db.explain(sanitize_sql(sql))
        if not valid:
            return {"error": err, "retry_count": retry_count + 1}

        return {"validated_sql": sql, "error": ""}

    def _route_after_validate(self, state: ChatState) -> str:
        if state.get("error"):
            return "repair" if state.get("retry_count", 0) <= state.get("max_retries", 2) else "respond"
        return "execute"

    def _repair_sql(self, state: ChatState) -> Dict[str, Any]:
        system_prompt = (
            "You are a PostgreSQL expert. Fix the SQL using only the schema. "
            "Return ONLY the fixed SQL, no markdown."
        )
        user_content = (
            f"User question: {state.get('user_query', '')}\n\n"
            f"Schema:\n{state.get('schema_context', '')}\n\n"
            f"SQL:\n{state.get('generated_sql', '')}\n\n"
            f"Error:\n{state.get('error', '')}\n\n"
            "Fixed SQL:" 
        )
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_content)]
        repaired = self.llm.invoke(messages)
        repaired = self._clean_sql(repaired)
        if repaired and sanitize_sql(repaired) != sanitize_sql(state.get("generated_sql", "")):
            return {"generated_sql": repaired}
        return {}

    def _execute_sql(self, state: ChatState) -> Dict[str, Any]:
        sql = state.get("validated_sql") or ""
        retry_count = state.get("retry_count", 0)
        if not sql:
            return {"error": "SQL nao validada", "retry_count": retry_count + 1}

        try:
            rows = self.db.execute(sanitize_sql(sql), max_rows=self.config.max_rows)
            return {"results": rows, "error": ""}
        except Exception as exc:
            return {"error": str(exc), "retry_count": retry_count + 1}

    def _route_after_execute(self, state: ChatState) -> str:
        if state.get("error") and state.get("retry_count", 0) <= state.get("max_retries", 2):
            return "repair"
        return "respond"

    def _respond(self, state: ChatState) -> Dict[str, Any]:
        route = state.get("route")
        if route == "conversational":
            response = self._respond_conversational(state)
        elif route == "schema":
            response = self._respond_schema(state)
        else:
            response = self._respond_database(state)

        return {
            "response": response,
            "messages": [AIMessage(content=response)],
        }

    def _respond_conversational(self, state: ChatState) -> str:
        system_prompt = (
            "Voce e um assistente de dados de saude. Responda em portugues. "
            "Se nao souber, diga que nao sabe."
        )
        history = self._recent_messages(state.get("messages", []), limit=8)
        messages = [SystemMessage(content=system_prompt)] + history
        return self.llm.invoke(messages).strip()

    def _respond_schema(self, state: ChatState) -> str:
        tables = state.get("available_tables", [])
        selected = state.get("selected_tables", [])
        if not tables:
            return "Nenhuma tabela encontrada."
        if not selected:
            selected = tables[:3]
        schema = state.get("schema_context", "")
        if schema:
            return f"Tabelas selecionadas: {', '.join(selected)}\n\n{schema}"
        return f"Tabelas disponiveis: {', '.join(tables)}"

    def _respond_database(self, state: ChatState) -> str:
        if state.get("error"):
            return f"Desculpe, não foi possível executar a consulta: {state.get('error')}"

        results = state.get("results", [])
        if not results:
            return "A consulta foi executada, mas não retornou nenhum resultado."

        # Usa LLM para gerar resposta natural
        return self._synthesize_natural_response(state)

    def _synthesize_natural_response(self, state: ChatState) -> str:
        """Usa o LLM para transformar os resultados em linguagem natural."""
        user_query = state.get("user_query", "")
        results = state.get("results", [])
        sql = state.get("validated_sql", "")

        # Formata os resultados para o prompt
        if len(results) == 1 and len(results[0]) == 1:
            # Resultado único (ex: COUNT)
            value = list(results[0].values())[0]
            results_text = f"Valor: {self._format_number_br(value)}"
        else:
            # Múltiplos resultados
            rows_text = []
            for row in results[:10]:  # Limita a 10 para o prompt
                row_str = ", ".join(f"{k}: {self._format_number_br(v)}" for k, v in row.items())
                rows_text.append(row_str)
            results_text = "\n".join(rows_text)
            if len(results) > 10:
                results_text += f"\n... (total: {len(results)} registros)"

        system_prompt = """Você é um assistente de dados de saúde do SUS brasileiro.
Sua tarefa é transformar resultados de consultas SQL em respostas naturais e amigáveis em português.

REGRAS:
1. Responda de forma clara e direta
2. Use números formatados (ex: 6.167.320 em vez de 6167320)
3. Contextualize o resultado com base na pergunta do usuário
4. Seja conciso mas informativo
5. NÃO inclua o código SQL na resposta
6. Use linguagem acessível para leigos"""

        user_content = f"""Pergunta do usuário: {user_query}

Resultado da consulta:
{results_text}

Gere uma resposta natural e amigável em português:"""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_content)
        ]

        return self.llm.invoke(messages).strip()

    def _format_number_br(self, value: Any) -> str:
        """Formata números no padrão brasileiro."""
        if value is None:
            return "N/A"
        if isinstance(value, bool):
            return "sim" if value else "não"
        if isinstance(value, int):
            return f"{value:,}".replace(",", ".")
        if isinstance(value, float):
            return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return str(value)

    def _select_tables(self, user_query: str, tables: List[str]) -> List[str]:
        q = user_query.lower()
        selected: List[str] = []
        for key, table in KEYWORD_TABLE_MAP.items():
            if key in q and table in tables and table not in selected:
                selected.append(table)

        if not selected:
            if "internacoes" in tables:
                selected.append("internacoes")
            elif tables:
                selected.append(tables[0])

        if "mortes" in selected and "internacoes" in tables and "internacoes" not in selected:
            selected.append("internacoes")

        if any(k in q for k in ["cidade", "municipio"]):
            if "municipios" in tables and "municipios" not in selected:
                selected.append("municipios")
            if "internacoes" in tables and "internacoes" not in selected:
                selected.append("internacoes")

        return selected[:4]

    def _build_join_hints(self, selected: List[str]) -> str:
        hints = []
        if "mortes" in selected and "internacoes" in selected:
            hints.append('Join mortes mo to internacoes i on mo."N_AIH" = i."N_AIH"')
        if "municipios" in selected and "internacoes" in selected:
            hints.append('Join municipios mu on i."MUNIC_RES" = mu.codigo_6d')
        if "cid10" in selected and "internacoes" in selected:
            hints.append('Join cid10 c on i."DIAG_PRINC" = c."CID"')
        return "\n".join(f"- {h}" for h in hints)

    def _format_response(self, rows: List[Dict[str, Any]]) -> str:
        if not rows:
            return "Nenhum resultado encontrado."

        if len(rows) == 1:
            row = rows[0]
            if len(row) == 1:
                value = list(row.values())[0]
                return f"Resultado: {self._format_value(value)}"
            return "Resultado: " + self._format_row(row)

        lines = []
        for row in rows[: self.config.max_results_display]:
            lines.append(self._format_row(row))

        suffix = ""
        if len(rows) >= self.config.max_rows:
            suffix = "\nMostrando resultados limitados."

        return "Resultados:\n" + "\n".join(lines) + suffix

    def _format_row(self, row: Dict[str, Any]) -> str:
        parts = []
        for key, value in row.items():
            parts.append(f"{key}={self._format_value(value)}")
        return ", ".join(parts)

    def _format_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return "sim" if value else "nao"
        if isinstance(value, int):
            return self._format_number(value)
        if isinstance(value, float):
            return self._format_number(value, decimals=2)
        return str(value)

    def _format_number(self, value: float, decimals: int = 0) -> str:
        if decimals == 0:
            s = f"{int(value):,}"
        else:
            s = f"{value:,.{decimals}f}"
        return s.replace(",", ".")

    def _clean_sql(self, sql: str) -> str:
        if not sql:
            return ""
        cleaned = sql.replace("```sql", "").replace("```", "").strip()
        cleaned = sanitize_sql(cleaned)
        if cleaned and not cleaned.endswith(";"):
            cleaned += ";"
        return cleaned

    def _truncate(self, text: str, max_chars: int) -> str:
        if len(text) <= max_chars:
            return text
        return text[: max_chars - 20] + "\n... (truncated)"

    def _looks_like_sql(self, text: str) -> bool:
        return bool(re.match(r"\s*(select|with)\b", text, flags=re.I))

    def _last_user_message(self, messages: List[Any]) -> str:
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                return msg.content
        return ""

    def _recent_messages(self, messages: List[Any], limit: int = 6) -> List[Any]:
        if not messages:
            return []
        return list(messages)[-limit:]
