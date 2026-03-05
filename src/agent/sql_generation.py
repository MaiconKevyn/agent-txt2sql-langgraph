"""SQL generation node, structured output schema, and pre-generation hints."""

import concurrent.futures
import os
import time
from typing import Dict, List, Optional

from pydantic import BaseModel, Field
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from .llm_manager import get_llm_manager
from .schema_node import _enhance_sus_schema_context
from .state import (
    MessagesStateTXT2SQL,
    ExecutionPhase,
    add_ai_message,
    update_phase,
    add_error,
)
from ..utils.logging_config import get_nodes_logger
from ..application.config.table_templates import build_table_specific_prompt, build_multi_table_prompt

logger = get_nodes_logger()


# ---------------------------------------------------------------------------
# Structured output schema
# ---------------------------------------------------------------------------

class SQLOutput(BaseModel):
    """Structured output for SQL generation — eliminates markdown-fence parsing."""
    sql: str = Field(description="Valid PostgreSQL SELECT query answering the user question")
    reasoning: str = Field(description="Brief explanation of table/filter choices (1-2 sentences)")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score 0-1; use <0.6 for uncertain queries")


# ---------------------------------------------------------------------------
# Self-consistency: N-candidate generation
# ---------------------------------------------------------------------------

N_SQL_CANDIDATES = 1
TEMPERATURE_CANDIDATES = 0.8


def _generate_sql_candidates(
    formatted_messages: list,
    llm_manager,
    primary_sql: str,
    primary_confidence: float,
    n: int = N_SQL_CANDIDATES,
) -> List[Dict]:
    """Generate N SQL candidates in parallel at high temperature for majority voting.

    Returns list of dicts [{"sql": str, "confidence": float}, ...].
    Element 0 is always the primary (temperature=0) candidate.
    Additional n-1 candidates are generated concurrently at TEMPERATURE_CANDIDATES.
    """
    candidates: List[Dict] = [{"sql": primary_sql, "confidence": primary_confidence}]

    if n <= 1:
        return candidates

    api_key = os.getenv("OPENAI_API_KEY")
    diverse_llm = ChatOpenAI(
        model=llm_manager.config.llm_model,
        temperature=TEMPERATURE_CANDIDATES,
        api_key=api_key,
    ).with_structured_output(SQLOutput)

    def _one(_) -> Optional[Dict]:
        try:
            result = diverse_llm.invoke(formatted_messages)
            sql = llm_manager._clean_sql_query(result.sql)
            return {"sql": sql, "confidence": result.confidence} if sql else None
        except Exception as exc:
            logger.debug("Candidate generation failed", extra={"error": str(exc)})
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=n - 1) as pool:
        futures = [pool.submit(_one, i) for i in range(n - 1)]
        for fut in concurrent.futures.as_completed(futures):
            r = fut.result()
            if r and r.get("sql"):
                candidates.append(r)

    logger.info(
        "SQL candidates generated",
        extra={"n_requested": n, "n_generated": len(candidates)},
    )
    return candidates


# ---------------------------------------------------------------------------
# Pre-generation hints
# ---------------------------------------------------------------------------

def _build_pregeneration_hints(selected_tables: List[str], user_query: str) -> str:
    """
    Generate targeted warnings based on selected tables.
    Injected into generate_sql_node BEFORE calling the LLM.
    Prevents predictable failure patterns (generate→fail→repair cycles).
    """
    hints = []

    if "socioeconomico" in selected_tables:
        hints.append(
            "🚨 SOCIOECONOMICO ALERT: long-format table. "
            "MANDATORY: add WHERE metrica = '<metric>' in EVERY query. "
            "Options: 'populacao_total', 'idhm', 'mortalidade_infantil_1ano', "
            "'bolsa_familia_total', 'esgotamento_sanitario_domicilio', 'taxa_envelhecimento'. "
            "Choosing wrong metric OR omitting it produces WRONG aggregations."
        )

    if "tempo" in selected_tables:
        hints.append(
            "🚨 TEMPO ALERT: NEVER join tempo table on computed expression. "
            "Use EXTRACT(YEAR/MONTH FROM DT_INTER) directly — NO JOIN. "
            "✅ WHERE EXTRACT(YEAR FROM \"DT_INTER\") = 2020  "
            "❌ JOIN tempo t ON EXTRACT(YEAR FROM i.\"DT_INTER\") = t.ano → CARTESIAN PRODUCT"
        )

    if "atendimentos" in selected_tables:
        hints.append(
            "🚨 ATENDIMENTOS ALERT: junction table pattern MANDATORY. "
            "internacoes i → JOIN atendimentos a ON i.\"N_AIH\" = a.\"N_AIH\" "
            "→ JOIN procedimentos p ON a.\"PROC_REA\" = p.\"PROC_REA\". "
            "NEVER reference a.\"NOME_PROC\" — that column is in procedimentos, not atendimentos."
        )

    if "especialidade" in selected_tables:
        hints.append(
            "🚨 ESPECIALIDADE ALERT: join on ESPEC code. "
            "JOIN especialidade e ON i.\"ESPEC\" = e.\"ESPEC\" → SELECT e.\"DESCRICAO\". "
            "For UTI: use WHERE \"VAL_UTI\" > 0 (NOT ESPEC BETWEEN 74 AND 83)."
        )

    if "hospital" in selected_tables:
        hints.append(
            "🚨 HOSPITAL ALERT: hospital table has only CNES, MUNIC_MOV, GESTAO, NATUREZA, NAT_JUR — NO patient counts. "
            "To count PATIENTS by municipality: COUNT(i.\"N_AIH\") from internacoes (NOT COUNT(DISTINCT h.\"CNES\") which counts hospitals). "
            "✅ 'municípios que atendem mais pacientes' → "
            "SELECT mu.nome, COUNT(i.\"N_AIH\") FROM internacoes i "
            "JOIN hospital h ON i.\"CNES\" = h.\"CNES\" "
            "JOIN municipios mu ON h.\"MUNIC_MOV\" = mu.codigo_6d "
            "GROUP BY mu.nome ORDER BY COUNT(i.\"N_AIH\") DESC. "
            "❌ COUNT(DISTINCT h.\"CNES\") → conta hospitais, NÃO pacientes."
        )

    # Query-keyword-based hints (trigger on question phrasing, not just tables)
    q_lower = user_query.lower()
    per_group_triggers = ["de cada", "por cada", "por grupo", "por faixa", "por nacionalidade"]
    # Also detect explicit multi-age-group queries: "menos de X anos, entre X e Y anos..."
    import re as _re
    # Per-group hint ONLY when asking for TOP-N (not for simple aggregates like AVG/SUM per group)
    top_n_context = (
        any(t in q_lower for t in ["principais", "top", "mais comuns", "mais frequentes", "mais realizados"])
        or bool(_re.search(r"\b\d+\s+principais\b", q_lower))
    )
    has_multi_age = (
        bool(_re.search(r"menos de\s+\d+\s+anos", q_lower))
        and ("entre" in q_lower or "acima de" in q_lower or "mais de" in q_lower)
        and top_n_context  # only trigger per-group for TOP-N questions, not simple AVG/SUM per age group
    )
    if has_multi_age:
        # Detect age boundaries from the query text and derive CASE labels
        age_bounds = _re.findall(r"(\d+)\s+anos", q_lower)
        limit_n = _re.search(r"\b(\d+)\s+principais", q_lower)
        n_str = limit_n.group(1) if limit_n else "10"
        cutoffs = sorted(set(int(x) for x in age_bounds)) if age_bounds else [18, 64]
        if len(cutoffs) >= 2:
            c1, c2 = cutoffs[0], cutoffs[1]
            case_expr = (
                f"CASE WHEN i.\"IDADE\" < {c1} THEN 'Menor de {c1}' "
                f"WHEN i.\"IDADE\" BETWEEN {c1} AND {c2} THEN '{c1} a {c2}' "
                f"ELSE '{c2 + 1} ou mais' END"
            )
        else:
            c1 = cutoffs[0] if cutoffs else 18
            case_expr = f"CASE WHEN i.\"IDADE\" < {c1} THEN 'Menor de {c1}' ELSE '{c1} ou mais' END"
        hints.append(
            f"🚨 MULTI-AGE-GROUP TOP-N ALERT: OBRIGATÓRIO usar ROW_NUMBER OVER PARTITION BY faixa etária. "
            f"NUNCA usar LIMIT global — LIMIT limita o TOTAL de linhas, não por grupo. "
            f"Rótulos OBRIGATÓRIOS para esta pergunta: {case_expr}. "
            f"PADRÃO ESTRUTURAL OBRIGATÓRIO (adapte as colunas à pergunta):\n"
            f"  SELECT faixa_etaria, <col1>, <col2>, cnt FROM (\n"
            f"    SELECT {case_expr} AS faixa_etaria,\n"
            f"           <col1>, <col2>, COUNT(*) AS cnt,\n"
            f"           ROW_NUMBER() OVER (PARTITION BY {case_expr} ORDER BY COUNT(*) DESC) AS rn\n"
            f"    FROM internacoes i [JOIN lookup_table ON ...]\n"
            f"    GROUP BY faixa_etaria, <col1>, <col2>\n"
            f"  ) sub WHERE rn <= {n_str} ORDER BY faixa_etaria, rn;"
        )
    elif top_n_context and any(t in q_lower for t in per_group_triggers):
        hints.append(
            "🚨 PER-GROUP TOP-N ALERT: pergunta pede top-N POR GRUPO ('de cada', 'por grupo', 'por faixa'). "
            "OBRIGATÓRIO: usar ROW_NUMBER() OVER (PARTITION BY group_col ORDER BY COUNT(*) DESC, tiebreaker_col ASC) AS rn, "
            "depois WHERE rn <= N (ou rn = 1 para o principal). "
            "NUNCA usar LIMIT global — LIMIT limita o total de linhas, NÃO por grupo. "
            "TIEBREAKER OBRIGATÓRIO: sempre adicionar segunda coluna no ORDER BY do ROW_NUMBER para resultado determinístico: "
            "ex: ORDER BY COUNT(*) DESC, i.\"VALUE_COL\" ASC. "
            "PADRÃO CORRETO: SELECT ... FROM (SELECT ..., ROW_NUMBER() OVER (PARTITION BY group_col ORDER BY COUNT(*) DESC, value_col ASC) AS rn "
            "FROM internacoes WHERE value_col IS NOT NULL GROUP BY group_col, value_col) sub WHERE sub.rn = 1 ORDER BY group_col;"
        )

    if not hints:
        return ""

    return (
        "\n\n⚠️ TABLE-SPECIFIC WARNINGS FOR THIS QUERY:\n"
        + "\n".join(f"  {h}" for h in hints)
        + "\n"
    )


# ---------------------------------------------------------------------------
# Node function
# ---------------------------------------------------------------------------

def generate_sql_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Generate SQL Node - Using ChatPromptTemplate with Table-Specific Rules

    Generates SQL queries using ChatPromptTemplate with dynamic table-specific rules
    Following official LangGraph SQL agent patterns with enhanced prompt templates
    """
    start_time = time.time()

    logger.info("SQL generation node started", extra={
        "user_query": state['user_query'][:100]
    })

    try:
        llm_manager = get_llm_manager()
        user_query = state["user_query"]
        schema_context = state.get("schema_context", "")
        selected_tables = state.get("selected_tables", [])

        # Enrich schema with value mappings (single source of truth)
        schema_context = _enhance_sus_schema_context(schema_context)

        logger.info("Tables selected for SQL generation", extra={"tables": selected_tables})

        if len(selected_tables) > 1:
            table_rules = build_multi_table_prompt(selected_tables)
            logger.debug("Multi-table rules applied", extra={"tables": selected_tables})
        else:
            table_rules = build_table_specific_prompt(selected_tables)
            logger.debug("Table-specific rules applied", extra={"tables": selected_tables})

        # Inject preventive hints for known failure patterns
        pregeneration_hints = _build_pregeneration_hints(selected_tables, user_query)
        if pregeneration_hints:
            table_rules = pregeneration_hints + "\n" + table_rules
            logger.info(
                "Pre-generation hints injected",
                extra={"tables": selected_tables, "hints_length": len(pregeneration_hints)},
            )

        sql_prompt_template = ChatPromptTemplate.from_messages([
            ("system", """You are a PostgreSQL expert assistant for Brazilian healthcare (SIH-RS) data analysis.

        ══════════════════════════════════════════════════════════
        CRITICAL RULES — READ THESE FIRST, THEY OVERRIDE ALL ELSE
        ══════════════════════════════════════════════════════════

        RULE A — UTI/ICU: WHERE "VAL_UTI" > 0 to count or filter UTI.
        For AVG/SUM on UTI values: also require WHERE "VAL_UTI" > 0 (excludes non-ICU zeros).
        "total gasto em UTI" / "custo UTI" → SUM("VAL_UTI") WHERE "VAL_UTI" > 0
        ❌ NEVER SUM("VAL_TOT") WHERE "VAL_UTI" > 0 — VAL_TOT is the full hospitalization cost, NOT just UTI.
        "obstétricas"/"obstétrico" = ESPEC = 2 (NEVER ESPEC BETWEEN 74 AND 83).
        ✅ WHERE "ESPEC" = 2 AND "VAL_UTI" > 0

        RULE B — DEATH CAUSE vs DIAGNOSIS:
        "causa da morte"/"morreram de"/"óbitos por DOENÇA X" → JOIN cid ON i."CID_MORTE"=c."CID" WHERE i."MORTE"=true AND i."CID_MORTE" IS NOT NULL
        "diagnóstico principal"/"internado por DOENÇA X" → JOIN cid ON i."DIAG_PRINC"=c."CID"
        "resultaram em óbito" WITHOUT a specific disease → WHERE "MORTE"=true only (NO CID JOIN)
        ✅ "Quantas internações de UTI resultaram em óbito?" → WHERE "VAL_UTI" > 0 AND "MORTE" = true

        RULE C — LIMIT: add LIMIT only when question asks for top-N (e.g. "top 5"). NEVER add default LIMIT.

        RULE D — ONLY REQUESTED FILTERS: add only filters the question explicitly mentions.
        No age filter unless age asked. No year filter unless year asked. No gender unless gender asked.
        No "MORTE"=false unless question specifically asks for discharged/surviving patients.
        No WHERE col IS NOT NULL for aggregate queries (SUM/AVG/MAX/MIN already ignore NULLs).
        ✅ "Qual o total gasto?" → SELECT SUM("VAL_TOT") FROM internacoes  (NO IS NOT NULL filter)
        ❌ SELECT SUM("VAL_TOT") FROM internacoes WHERE "VAL_TOT" IS NOT NULL  ← redundant, may alter result

        RULE E — CID COLUMN:
        • Include c."CID" ONLY WHEN: question explicitly says "com código" or "código CID".
        • Default: SELECT only c."CD_DESCRICAO", GROUP BY c."CD_DESCRICAO".
        ✅ "principais causas de morte" → SELECT c."CD_DESCRICAO", COUNT(*) GROUP BY c."CD_DESCRICAO"
        ✅ "principais CIDs de entrada" → SELECT c."CD_DESCRICAO", COUNT(*) GROUP BY c."CD_DESCRICAO"
        ✅ "com código" / "código CID" → SELECT c."CID", c."CD_DESCRICAO", COUNT(*) GROUP BY c."CID", c."CD_DESCRICAO"
        ❌ "quais os CIDs de entrada" → SELECT c."CD_DESCRICAO" only (NOT c."CID" unless "com código" stated)

        RULE F — singular "qual o X mais Y" → LIMIT 1; plural "quais os N X mais Y" → LIMIT N.

        RULE G — DATE FILTERS: use EXTRACT directly on "DT_INTER", NEVER join tempo with non-equijoin.
        ✅ WHERE EXTRACT(YEAR FROM "DT_INTER") = 2020  ← use DT_INTER for year/period filters (admission date)
        Only use "DT_SAIDA" when question explicitly asks about discharge or exit date.

        RULE H — IDADE (INTEGER) vs NASC (DATE):
        "IDADE" = pre-calculated integer age column (0-130). USE FOR ALL age filters/groupings.
        "NASC" = birth date. USE ONLY when question asks about BIRTH YEAR specifically.
        ✅ WHERE "IDADE" > 60             ✅ GROUP BY "IDADE"   ✅ CASE WHEN "IDADE" < 18
        ✅ WHERE EXTRACT(YEAR FROM "NASC") < 1950  ← "nascidos antes de 1950" → use NASC
        ❌ EXTRACT(YEAR FROM AGE("NASC")) > 60   ← NEVER! use IDADE directly
        ❌ (CURRENT_DATE - "NASC") / 365 > 60    ← NEVER! use IDADE directly

        RULE I — COUNT rows vs COUNT DISTINCT values:
        "Quantos X diferentes existem cadastrados/registrados?" → COUNT(*) rows in the table (total registros).
        COUNT(DISTINCT col) only when asking "quantos valores únicos de COLUNA" or "quantas categorias distintas".
        ✅ "Quantos procedimentos diferentes existem?" → SELECT COUNT(*) FROM procedimentos
        ❌ SELECT COUNT(DISTINCT "NOME_PROC") → WRONG for "how many procedures exist"

        RULE J — PER-GROUP TOP-N: ⚠️ MANDATORY ROW_NUMBER WHEN QUESTION ASKS top-N FOR MULTIPLE GROUPS.
        Triggers: "de cada", "por cada", "por faixa", "por grupo", OR when question lists MULTIPLE explicit segments.
        ❌ NEVER use plain LIMIT for per-group queries — LIMIT limits the entire result, not per group.
        ✅ GENERIC PATTERN — top-N per categorical group (e.g. top-1 per sex):
          SELECT group_col_desc, value_col_desc, sub.cnt FROM (
            SELECT i."GROUP_COL", i."VALUE_COL", COUNT(*) AS cnt,
                   ROW_NUMBER() OVER (PARTITION BY i."GROUP_COL" ORDER BY COUNT(*) DESC, i."VALUE_COL" ASC) AS rn
            FROM internacoes i WHERE i."VALUE_COL" IS NOT NULL GROUP BY i."GROUP_COL", i."VALUE_COL"
          ) sub
          JOIN lookup_table lt ON sub."GROUP_COL" = lt."GROUP_COL"
          WHERE sub.rn = 1 ORDER BY lt."DESCRICAO";
          ⚠️ outer SELECT must use subquery alias (sub.col), NEVER inner alias (i.col)!
          ⚠️ Add secondary ORDER BY tiebreaker in ROW_NUMBER for deterministic results.
        ✅ GENERIC PATTERN — top-N per computed group (age/range segments):
          SELECT group_label, value_desc, sub.cnt FROM (
            SELECT CASE WHEN i."GROUP_MEASURE" < threshold1 THEN 'Label_A'
                        WHEN i."GROUP_MEASURE" BETWEEN threshold1 AND threshold2 THEN 'Label_B'
                        ELSE 'Label_C' END AS group_label,
                   i."VALUE_COL", COUNT(*) AS cnt,
                   ROW_NUMBER() OVER (
                     PARTITION BY CASE WHEN i."GROUP_MEASURE" < threshold1 THEN 'Label_A'
                                       WHEN i."GROUP_MEASURE" BETWEEN threshold1 AND threshold2 THEN 'Label_B'
                                       ELSE 'Label_C' END
                     ORDER BY COUNT(*) DESC
                   ) AS rn
            FROM internacoes i GROUP BY group_label, i."VALUE_COL"
          ) sub WHERE rn <= N ORDER BY group_label, rn;
          ⚠️ Use the EXACT boundaries and labels stated in the user's question (see TABLE-SPECIFIC WARNINGS).
        Age groups: when question EXPLICITLY states boundaries, use those EXACT labels.
        When NOT specified → use natural labels ('Menor', 'Adulto', 'Idoso').

        DISEASE LOOKUP: table is "cid" (NOT "cid10"). NEVER hardcode CID codes (e.g. DIAG_PRINC = 'J18' is WRONG).
        Displaying diagnosis name → ALWAYS JOIN cid c ON i."DIAG_PRINC"=c."CID" → SELECT c."CD_DESCRICAO"
        ✅ "diagnóstico mais comum" → SELECT c."CD_DESCRICAO", COUNT(*) FROM internacoes i JOIN cid c ON i."DIAG_PRINC"=c."CID" GROUP BY c."CD_DESCRICAO" ORDER BY 2 DESC LIMIT 1
        ❌ SELECT i."DIAG_PRINC", COUNT(*) → returns raw CID code, NOT description
        Filtering by named disease → JOIN cid c ON i."DIAG_PRINC"=c."CID" WHERE c."CD_DESCRICAO" ILIKE '%X%'
        Category (no specific name) → WHERE "DIAG_PRINC" LIKE 'J%' (J=Respiratory, I=Cardiovascular, C=Cancer, K=Digestive)
        For cause of death by disease → JOIN cid ON i."CID_MORTE"=c."CID" (see RULE B)

        ══════════════════════════════════════════════════════════

        CORE: Use double quotes for all columns: "COLUMN_NAME". Return ONLY the SQL query.

        DATABASE SCHEMA:
        {schema_context}"""),

            ("system", "{table_specific_rules}"),

            ("human", "USER QUERY: {user_query}\n\nGenerate the SQL query:")
        ])

        formatted_messages = sql_prompt_template.format_messages(
            schema_context=schema_context,
            table_specific_rules=table_rules,
            user_query=user_query,
        )

        # For per-group top-N queries, append a critical reminder as a final human message
        # (most recent position → highest LLM attention)
        if pregeneration_hints and ("TOP-N" in pregeneration_hints or "MULTI-AGE" in pregeneration_hints):
            from langchain_core.messages import HumanMessage as _HumanMessage
            reminder = (
                "⚠️ MANDATORY CONSTRAINT FOR THIS QUERY: You MUST use "
                "ROW_NUMBER() OVER (PARTITION BY ...) — do NOT use global LIMIT. "
                "See the SQL pattern in the TABLE-SPECIFIC WARNINGS above and follow it exactly."
            )
            formatted_messages = list(formatted_messages) + [_HumanMessage(content=reminder)]

        logger.debug("Template prepared", extra={
            "message_count": len(formatted_messages),
            "rules_length": len(table_rules),
        })

        # Primary path: structured output
        sql_query: Optional[str] = None
        generation_method = "structured"
        try:
            structured_result = llm_manager.invoke_chat_structured(formatted_messages, SQLOutput)
            sql_query = llm_manager._clean_sql_query(structured_result.sql)
            logger.info("SQL generated via structured output", extra={
                "sql": sql_query[:200],
                "reasoning": structured_result.reasoning[:120],
                "confidence": structured_result.confidence,
            })
            meta = state.get("response_metadata", {}) or {}
            meta["sql_generation_confidence"] = structured_result.confidence
            meta["sql_generation_reasoning"] = structured_result.reasoning
            state["response_metadata"] = meta
        except Exception as struct_err:
            logger.warning("Structured output failed, falling back to text parse", extra={
                "error": str(struct_err)
            })
            generation_method = "text_fallback"
            response = llm_manager.invoke_chat(formatted_messages)
            sql_query = response.content.strip() if hasattr(response, "content") else str(response)
            sql_query = llm_manager._clean_sql_query(sql_query)

        if sql_query:
            state["generated_sql"] = sql_query
            state["current_error"] = None
            state = add_ai_message(state, f"Generated SQL query ({generation_method}): {sql_query}")
            logger.info("SQL generated successfully", extra={
                "sql": sql_query[:200],
                "method": generation_method,
            })

            # Generate N-1 additional candidates for majority voting
            primary_confidence = (state.get("response_metadata", {}) or {}).get(
                "sql_generation_confidence", 0.5
            )
            state["sql_candidates"] = _generate_sql_candidates(
                formatted_messages=formatted_messages,
                llm_manager=llm_manager,
                primary_sql=sql_query,
                primary_confidence=primary_confidence,
            )
        else:
            # Both paths failed — retry with simplified prompt
            logger.warning("SQL generation: empty response on first attempt, trying simplified prompt")
            try:
                simplified_messages = [
                    SystemMessage(content=(
                        "You are a PostgreSQL expert. Generate ONLY a valid SQL SELECT query "
                        "for the Brazilian healthcare database sihrd5. "
                        "Return ONLY the SQL, no explanation.\n\n"
                        f"DATABASE SCHEMA:\n{schema_context}"
                    )),
                    HumanMessage(content=f"USER QUERY: {user_query}\n\nGenerate the SQL query:"),
                ]
                retry_response = llm_manager.invoke_chat(simplified_messages)
                retry_sql = retry_response.content.strip() if hasattr(retry_response, "content") else str(retry_response)
                retry_sql = llm_manager._clean_sql_query(retry_sql)
                if retry_sql:
                    state["generated_sql"] = retry_sql
                    state["current_error"] = None
                    state = add_ai_message(state, f"Generated SQL (simplified retry): {retry_sql}")
                    logger.info("SQL generated on retry", extra={"sql": retry_sql[:200]})
                else:
                    raise ValueError("Retry also produced empty SQL")
            except Exception as retry_err:
                error_message = "Failed to generate SQL query - empty response (all attempts)"
                state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
                state["retry_count"] = state.get("retry_count", 0) + 1
                state["generation_retry_count"] = state.get("generation_retry_count", 0) + 1
                logger.warning("SQL generation failed on all attempts", extra={"error": str(retry_err)})

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, execution_time)

        logger.info("SQL generation completed", extra={"execution_time": execution_time})

        return state

    except Exception as e:
        error_message = f"SQL generation failed: {str(e)}"
        state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
        state["retry_count"] = state.get("retry_count", 0) + 1
        state["generation_retry_count"] = state.get("generation_retry_count", 0) + 1

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, execution_time)

        logger.error("SQL generation failed", extra={
            "error": str(e),
            "execution_time": execution_time,
        })

        return state
