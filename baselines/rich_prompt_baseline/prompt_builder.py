"""
Rich Prompt Builder for the Rich Prompt Baseline.

Injects the same context as the LangGraph agent's generate_sql_node:
  - RULES A-O (domain-specific SQL generation rules, mirrors sql_generation.py exactly)
  - SUS_MAPPINGS (critical column value mappings for sihrd5)
  - Dynamic pre-generation hints via _build_pregeneration_hints (same function as agent)
  - TABLE_DESCRIPTIONS (schema metadata via context_loader)
  - TABLE_TEMPLATES (per-table rules + few-shot examples)

Key difference from LangGraph agent: single-shot call, no table selection, no validation/repair.

This allows isolating the architectural impact of LangGraph from prompt engineering.
"""
from __future__ import annotations

from typing import List, Tuple


# ---------------------------------------------------------------------------
# RULES A-O — mirrors src/agent/sql_generation.py generate_sql_node system prompt exactly
# ---------------------------------------------------------------------------
RULES_AO = """
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
CRITICAL RULES \u2014 READ THESE FIRST, THEY OVERRIDE ALL ELSE
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

RULE A \u2014 UTI/ICU: WHERE "VAL_UTI" > 0 to count or filter UTI.
For AVG/SUM on UTI values: also require WHERE "VAL_UTI" > 0 (excludes non-ICU zeros).
"total gasto em UTI" / "custo UTI" \u2192 SUM("VAL_UTI") WHERE "VAL_UTI" > 0
\u274c NEVER SUM("VAL_TOT") WHERE "VAL_UTI" > 0 \u2014 VAL_TOT is the full hospitalization cost, NOT just UTI.
"obst\u00e9tricas"/"obst\u00e9trico" = ESPEC = 2 (NEVER ESPEC BETWEEN 74 AND 83).
\u2705 WHERE "ESPEC" = 2 AND "VAL_UTI" > 0

RULE B \u2014 DEATH CAUSE vs DIAGNOSIS:
"causa da morte"/"morreram de"/"\u00f3bitos por DOEN\u00c7A X" \u2192 JOIN cid ON i."CID_MORTE"=c."CID" WHERE i."MORTE"=true AND i."CID_MORTE" IS NOT NULL
"diagn\u00f3stico principal"/"internado por DOEN\u00c7A X" \u2192 JOIN cid ON i."DIAG_PRINC"=c."CID"
"resultaram em \u00f3bito" WITHOUT a specific disease \u2192 WHERE "MORTE"=true only (NO CID JOIN)
\u2705 "Quantas interna\u00e7\u00f5es de UTI resultaram em \u00f3bito?" \u2192 WHERE "VAL_UTI" > 0 AND "MORTE" = true

RULE C \u2014 LIMIT: add LIMIT only when question asks for top-N (e.g. "top 5"). NEVER add default LIMIT.

RULE D \u2014 ONLY REQUESTED FILTERS: add only filters the question explicitly mentions.
No age filter unless age asked. No year filter unless year asked. No gender unless gender asked.
No "MORTE"=false unless question specifically asks for discharged/surviving patients.
No WHERE col IS NOT NULL for aggregate queries (SUM/AVG/MAX/MIN already ignore NULLs).
\u2705 "Qual o total gasto?" \u2192 SELECT SUM("VAL_TOT") FROM internacoes  (NO IS NOT NULL filter)
\u274c SELECT SUM("VAL_TOT") FROM internacoes WHERE "VAL_TOT" IS NOT NULL  \u2190 redundant, may alter result

RULE E \u2014 CID COLUMN:
\u2022 Include c."CID" ONLY WHEN: question explicitly says "com c\u00f3digo" or "c\u00f3digo CID".
\u2022 Default: SELECT only c."CD_DESCRICAO", GROUP BY c."CD_DESCRICAO".
\u2705 "principais causas de morte" \u2192 SELECT c."CD_DESCRICAO", COUNT(*) GROUP BY c."CD_DESCRICAO"
\u2705 "principais CIDs de entrada" \u2192 SELECT c."CD_DESCRICAO", COUNT(*) GROUP BY c."CD_DESCRICAO"
\u2705 "com c\u00f3digo" / "c\u00f3digo CID" \u2192 SELECT c."CID", c."CD_DESCRICAO", COUNT(*) GROUP BY c."CID", c."CD_DESCRICAO"
\u274c "quais os CIDs de entrada" \u2192 SELECT c."CD_DESCRICAO" only (NOT c."CID" unless "com c\u00f3digo" stated)

RULE F \u2014 singular "qual o X mais Y" \u2192 LIMIT 1; plural "quais os N X mais Y" \u2192 LIMIT N.

RULE G \u2014 DATE FILTERS: use EXTRACT directly on "DT_INTER", NEVER join tempo with non-equijoin.
\u2705 WHERE EXTRACT(YEAR FROM "DT_INTER") = 2020  \u2190 use DT_INTER for year/period filters (admission date)
Only use "DT_SAIDA" when question explicitly asks about discharge or exit date.

RULE H \u2014 IDADE (INTEGER) vs NASC (DATE):
"IDADE" = pre-calculated integer age column (0-130). USE FOR ALL age filters/groupings.
"NASC" = birth date. USE ONLY when question asks about BIRTH YEAR specifically.
\u2705 WHERE "IDADE" > 60             \u2705 GROUP BY "IDADE"   \u2705 CASE WHEN "IDADE" < 18
\u2705 WHERE EXTRACT(YEAR FROM "NASC") < 1950  \u2190 "nascidos antes de 1950" \u2192 use NASC
\u274c EXTRACT(YEAR FROM AGE("NASC")) > 60   \u2190 NEVER! use IDADE directly
\u274c (CURRENT_DATE - "NASC") / 365 > 60    \u2190 NEVER! use IDADE directly

RULE I \u2014 COUNT rows vs COUNT DISTINCT values:
"Quantos X diferentes existem cadastrados/registrados?" \u2192 COUNT(*) rows in the table (total registros).
COUNT(DISTINCT col) only when asking "quantos valores \u00fanicos de COLUNA" or "quantas categorias distintas".
\u2705 "Quantos procedimentos diferentes existem?" \u2192 SELECT COUNT(*) FROM procedimentos
\u274c SELECT COUNT(DISTINCT "NOME_PROC") \u2192 WRONG for "how many procedures exist"

RULE J \u2014 PER-GROUP TOP-N: \u26a0\ufe0f MANDATORY ROW_NUMBER WHEN QUESTION ASKS top-N FOR MULTIPLE GROUPS.
Triggers: "de cada", "por cada", "por faixa", "por grupo", OR when question lists MULTIPLE explicit segments.
\u274c NEVER use plain LIMIT for per-group queries \u2014 LIMIT limits the entire result, not per group.
\u2705 GENERIC PATTERN \u2014 top-N per categorical group (e.g. top-1 per sex):
  SELECT group_col_desc, value_col_desc, sub.cnt FROM (
    SELECT i."GROUP_COL", i."VALUE_COL", COUNT(*) AS cnt,
           ROW_NUMBER() OVER (PARTITION BY i."GROUP_COL" ORDER BY COUNT(*) DESC, i."VALUE_COL" ASC) AS rn
    FROM internacoes i WHERE i."VALUE_COL" IS NOT NULL GROUP BY i."GROUP_COL", i."VALUE_COL"
  ) sub
  JOIN lookup_table lt ON sub."GROUP_COL" = lt."GROUP_COL"
  WHERE sub.rn = 1 ORDER BY lt."DESCRICAO";
  \u26a0\ufe0f outer SELECT must use subquery alias (sub.col), NEVER inner alias (i.col)!
  \u26a0\ufe0f Add secondary ORDER BY tiebreaker in ROW_NUMBER for deterministic results.
\u2705 GENERIC PATTERN \u2014 top-N per computed group (age/range segments):
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
  \u26a0\ufe0f Use the EXACT boundaries and labels stated in the user's question (see TABLE-SPECIFIC WARNINGS).
Age groups: when question EXPLICITLY states boundaries, use those EXACT labels.
When NOT specified \u2192 use natural labels ('Menor', 'Adulto', 'Idoso').

RULE K \u2014 ANTI-JOIN (absence pattern):
When question says "nunca tiveram", "sem", "n\u00e3o aparece", "jamais" \u2192 use NOT EXISTS, not NOT IN.
\u2705 SELECT COUNT(*) FROM (SELECT DISTINCT "CNES" FROM internacoes i WHERE NOT EXISTS
      (SELECT 1 FROM internacoes d WHERE d."CNES" = i."CNES" AND d."MORTE" = true)) sub
\u274c NOT IN (SELECT "CNES" FROM internacoes WHERE "MORTE" = true)  \u2190 breaks on NULLs

RULE L \u2014 PIVOT FORMAT (side-by-side comparison):
When question explicitly compares two named groups side-by-side ("X vs Y", "comparar X com Y", "lado a lado"):
\u2705 Return WIDE format: SELECT categoria, AVG(CASE WHEN grp='A' THEN val END) AS media_A, AVG(CASE WHEN grp='B' THEN val END) AS media_B FROM ... GROUP BY categoria
\u274c Do NOT return long format (one row per category+group = double the rows)

RULE M \u2014 AGGREGATION-FIRST (count facts, not dimension rows):
When counting/aggregating events by entity (hospital, municipality, specialty):
\u2705 GROUP BY directly on the fact table (internacoes) by the FK key, THEN join for labels:
   SELECT h_info.nome, sub.total FROM (SELECT "CNES", COUNT(*) AS total FROM internacoes GROUP BY "CNES") sub JOIN hospital h_info ...
\u274c Do NOT: JOIN hospital first then GROUP BY \u2014 this drops CNES values that exist in internacoes but not in hospital

RULE N \u2014 GLOBAL vs LOCAL AVERAGE:
When filtering entities above/below an average, compute the reference from the FULL dataset:
\u2705 WITH ref AS (SELECT SUM(CASE WHEN MORTE THEN 1 ELSE 0 END)*100.0/COUNT(*) AS taxa FROM internacoes [WHERE full_scope_filter])
   ...HAVING local_taxa > (SELECT taxa FROM ref)
\u274c Do NOT use AVG(per_group_rate) FROM already-filtered-subgroup \u2014 that computes avg-of-avgs (wrong denominator)

RULE O \u2014 DESCRIPTION vs CODE:
When question asks "quais m\u00e9todos", "quais diagn\u00f3sticos", "quais n\u00edveis de instru\u00e7\u00e3o" \u2192 JOIN lookup table and return DESCRICAO, not raw code.
instrucao level \u2192 JOIN instrucao ins ON i."INSTRU" = ins."INSTRU" \u2192 SELECT ins."DESCRICAO"
m\u00e9todo contraceptivo \u2192 JOIN contraceptivos c ON i."CONTRACEP1" = c."CONTRACEPTIVO" \u2192 SELECT c."DESCRICAO"
ra\u00e7a/cor description \u2192 JOIN raca_cor r ON i."RACA_COR" = r."RACA_COR" \u2192 SELECT r."DESCRICAO"

DISEASE LOOKUP: table is "cid" (NOT "cid10"). NEVER hardcode CID codes (e.g. DIAG_PRINC = 'J18' is WRONG).
Displaying diagnosis name \u2192 ALWAYS JOIN cid c ON i."DIAG_PRINC"=c."CID" \u2192 SELECT c."CD_DESCRICAO"
\u2705 "diagn\u00f3stico mais comum" \u2192 SELECT c."CD_DESCRICAO", COUNT(*) FROM internacoes i JOIN cid c ON i."DIAG_PRINC"=c."CID" GROUP BY c."CD_DESCRICAO" ORDER BY 2 DESC LIMIT 1
\u274c SELECT i."DIAG_PRINC", COUNT(*) \u2192 returns raw CID code, NOT description
Filtering by named disease \u2192 JOIN cid c ON i."DIAG_PRINC"=c."CID" WHERE c."CD_DESCRICAO" ILIKE '%X%'
Category (no specific name) \u2192 WHERE "DIAG_PRINC" LIKE 'J%' (J=Respiratory, I=Cardiovascular, C=Cancer, K=Digestive)
For cause of death by disease \u2192 JOIN cid ON i."CID_MORTE"=c."CID" (see RULE B)

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
"""


# ---------------------------------------------------------------------------
# SUS_MAPPINGS \u2014 extracted from _enhance_sus_schema_context() in schema_node.py
# ---------------------------------------------------------------------------
SUS_MAPPINGS = """
CRITICAL COLUMN VALUE MAPPINGS (sihrd5 \u2014 override DDL if conflicting):
=======================================================================
internacoes:
  "SEXO"    INTEGER: 1=Masculino, 3=Feminino (NUNCA usar 2)
  "MORTE"   BOOLEAN: true=\u00f3bito, false=alta
  "IND_VDRL" BOOLEAN: true=positivo (filtrar sem JOIN cid)
  "IDADE"   INTEGER (0-130): idade pr\u00e9-calculada \u2014 USAR para todos filtros de idade
  "NASC"    DATE: data de nascimento \u2014 usar SOMENTE para "nascidos antes/ap\u00f3s ano X"
            \u274c EXTRACT(YEAR FROM AGE("NASC")) \u2014 ERRADO, usar "IDADE" diretamente
            \u274c (CURRENT_DATE - "NASC") / 365 > 60 \u2014 ERRADO, usar "IDADE" diretamente
  "VAL_TOT" NUMERIC: custo total   | "VAL_SH": servi\u00e7o hospitalar | "VAL_UTI": UTI
            "valor do servi\u00e7o hospitalar" \u2192 VAL_SH  (N\u00c3O VAL_TOT!)
  "ESPEC"   INTEGER: 1=Cir\u00fargico, 2=Obst\u00e9trico, 3=Cl\u00ednico, 4=Cr\u00f4nico, 5=Psiquiatria, 7=Pedi\u00e1trico
  "MUNIC_RES" FK\u2192municipios.codigo_6d: munic\u00edpio de RESID\u00caNCIA do paciente (onde o paciente mora)
  \u26a0\ufe0f  MUNIC_MOV n\u00e3o existe em internacoes \u2014 est\u00e1 APENAS em hospital.MUNIC_MOV
  MUNIC_RES vs MUNIC_MOV \u2014 REGRA DEFAULT + EXCE\u00c7\u00d5ES:
    DEFAULT (sem contexto de localiza\u00e7\u00e3o hospitalar): \u2192 MUNIC_RES (resid\u00eancia do paciente)
    \u2705 "munic\u00edpios com mais interna\u00e7\u00f5es" \u2192 MUNIC_RES (JOIN municipios mu ON i."MUNIC_RES" = mu.codigo_6d)
    \u2705 "5 munic\u00edpios com mais interna\u00e7\u00f5es obst\u00e9tricas" \u2192 MUNIC_RES
    \u2705 "munic\u00edpios com mais interna\u00e7\u00f5es por especialidade/ra\u00e7a/diagn\u00f3stico" \u2192 MUNIC_RES
    EXCE\u00c7\u00c3O \u2014 usar MUNIC_MOV quando a pergunta menciona localiza\u00e7\u00e3o dos hospitais ou cidades onde ocorrem os procedimentos:
    \u2705 "munic\u00edpios que mais ATENDEM / recebem / onde est\u00e3o os hospitais" \u2192 MUNIC_MOV
    \u2705 "procedimentos/atendimentos NAS CIDADES de X / em hospitais de X" \u2192 MUNIC_MOV
        JOIN hospital h ON i."CNES" = h."CNES"
        JOIN municipios mu ON h."MUNIC_MOV" = mu.codigo_6d
    \u274c NUNCA usar JOIN hospital para "munic\u00edpios com mais interna\u00e7\u00f5es" sem contexto hospitalar
  "DIAG_PRINC" FK\u2192cid."CID": diagn\u00f3stico principal de entrada
  "CID_MORTE"  FK\u2192cid."CID": causa da morte (somente quando MORTE=true)

socioeconomico (long-format \u2014 SEMPRE filtrar por metrica):
  metrica='populacao_total'             | metrica='idhm'
  metrica='mortalidade_infantil_1ano'   | metrica='bolsa_familia_total'
  metrica='esgotamento_sanitario_domicilio' | metrica='taxa_envelhecimento'
  \u26a0\ufe0f SEM WHERE metrica=? \u2192 SUM soma TODAS as m\u00e9tricas \u2192 resultado sem sentido!

raca_cor:
  0=Sem informa\u00e7\u00e3o, 1=Branca, 2=Preta, 3=Parda, 4=Amarela, 5=Ind\u00edgena, 99=Sem informa\u00e7\u00e3o
  Filtrar inline: WHERE "RACA_COR" = 5 (sem JOIN)
  Descri\u00e7\u00e3o: JOIN raca_cor r ON i."RACA_COR" = r."RACA_COR" \u2192 SELECT r."DESCRICAO"
  DISTRIBUI\u00c7\u00c3O/COMPOSI\u00c7\u00c3O (inclui SEM INFORMACAO) \u2192 JOIN sem filtro; codes 0 e 99 \u2192 'SEM INFORMACAO'
  AN\u00c1LISE por ra\u00e7a (taxa, m\u00e9dia) \u2192 excluir unknowns: WHERE "RACA_COR" NOT IN (0, 99)

JOIN RULES:
  munic\u00edpio do paciente (resid\u00eancia) \u2014 DEFAULT \u2192 JOIN municipios mu ON i."MUNIC_RES" = mu.codigo_6d
  munic\u00edpio do hospital  (atendimento) \u2014 SOMENTE quando h\u00e1 contexto de hospital/localiza\u00e7\u00e3o:
                                          JOIN hospital h ON i."CNES" = h."CNES"
                                          JOIN municipios mu ON h."MUNIC_MOV" = mu.codigo_6d
  CR\u00cdTICO: "munic\u00edpios com mais interna\u00e7\u00f5es" (sem contexto hospitalar) \u2192 MUNIC_RES (DEFAULT)
  CR\u00cdTICO: "munic\u00edpios que atendem / recebem / onde ficam hospitais" \u2192 MUNIC_MOV via hospital JOIN
  CR\u00cdTICO: "procedimentos/atendimentos NAS CIDADES de X" \u2192 MUNIC_MOV (hospital location)
  especialidade         \u2192 JOIN especialidade e ON i."ESPEC" = e."ESPEC" \u2192 SELECT e."DESCRICAO"
  diagn\u00f3stico           \u2192 JOIN cid c ON i."DIAG_PRINC" = c."CID" \u2192 SELECT c."CD_DESCRICAO"
  causa de morte        \u2192 JOIN cid c ON i."CID_MORTE" = c."CID" WHERE i."MORTE" = true
"""


# ---------------------------------------------------------------------------
# All table names — used to fire all applicable dynamic hints (no table selection step)
# ---------------------------------------------------------------------------
_ALL_TABLES = [
    "internacoes", "municipios", "hospital", "cid", "especialidade",
    "socioeconomico", "atendimentos", "procedimentos", "tempo",
    "raca_cor", "instrucao", "contraceptivos", "vincprev",
]


def build_system_prompt(schema_context: str) -> str:
    """
    Build rich system prompt identical in content to generate_sql_node in sql_generation.py,
    but without table selection (all tables included in schema_context).
    """
    return (
        "You are a PostgreSQL expert assistant for Brazilian healthcare (SIH-RS) data analysis.\n"
        + RULES_AO
        + "\nCORE: Use double quotes for all columns: \"COLUMN_NAME\". Return ONLY the SQL query.\n"
        + SUS_MAPPINGS
        + "\nDATABASE SCHEMA:\n"
        + schema_context
    )


def build_user_prompt(question: str) -> str:
    """
    Build user prompt with all TABLE_TEMPLATES (rules + few-shot examples for all tables)
    plus dynamic pre-generation hints — exactly the same function used by the agent.

    In the LangGraph agent, only selected tables' templates are injected and hints are
    table-filtered; here we inject all templates and pass all table names so every
    applicable keyword-based hint fires.
    """
    from src.application.config.table_templates import TABLE_TEMPLATES
    from src.agent.sql_generation import _build_pregeneration_hints

    all_templates = "\n\n".join(
        f"=== {name.upper()} ===\n{tmpl}"
        for name, tmpl in TABLE_TEMPLATES.items()
    )

    # Generate dynamic hints with all tables available (same function as agent)
    dynamic_hints = _build_pregeneration_hints(_ALL_TABLES, question)

    prompt = (
        (dynamic_hints + "\n" if dynamic_hints else "")
        + "TABLE-SPECIFIC RULES AND EXAMPLES:\n"
        + all_templates
        + f"\n\nUSER QUERY: {question}\n\nGenerate the SQL query:"
    )

    # Mirror the agent's per-group top-N mandatory reminder (highest-attention final message)
    per_named_group_triggers = [
        "por estado", "em cada estado", "de cada estado", "para cada estado",
        "por especialidade", "em cada especialidade", "de cada especialidade",
        "por hospital", "em cada hospital", "de cada hospital", "para cada hospital",
        "por munic\u00edpio", "em cada munic\u00edpio", "de cada munic\u00edpio",
        "por cidade", "em cada cidade",
        "por ra\u00e7a", "por sexo", "por v\u00ednculo", "por faixa et\u00e1ria",
        "por ano", "em cada ano", "de cada ano",
        "por m\u00eas", "em cada m\u00eas",
        "de cada", "por cada", "por grupo", "por faixa", "por nacionalidade",
    ]
    if dynamic_hints and ("TOP-N" in dynamic_hints or "MULTI-AGE" in dynamic_hints):
        prompt += (
            "\n\n\u26a0\ufe0f MANDATORY CONSTRAINT FOR THIS QUERY: You MUST use "
            "ROW_NUMBER() OVER (PARTITION BY ...) \u2014 do NOT use global LIMIT. "
            "See the SQL pattern in the TABLE-SPECIFIC WARNINGS above and follow it exactly."
        )

    return prompt


def build_prompts(question: str, schema_context: str) -> Tuple[str, str]:
    """Convenience wrapper returning (system_prompt, user_prompt)."""
    return build_system_prompt(schema_context), build_user_prompt(question)
