from typing import List, Dict, Optional


# PostgreSQL-specific templates for all sihrd5 tables
TABLE_TEMPLATES = {
    "internacoes": """
         INTERNACOES TABLE RULES - MAIN HOSPITALIZATION DATA (sihrd5):

        ⚠️ CRITICAL SCHEMA CHANGES FROM PREVIOUS VERSION ⚠️

        2. IND_VDRL IS A BOOLEAN COLUMN — there is NO separate "condicoes_especificas" table:
           ✅ WHERE "IND_VDRL" = true        (correct — VDRL positive)
           ❌ JOIN condicoes_especificas     (table does not exist!)
           CRITICAL: For ANY VDRL query → ONLY use WHERE "IND_VDRL" = true.
           NEVER join the cid table for VDRL — VDRL has no CID code category!

        3. DIAG_SECUN IS DIRECTLY IN internacoes — there is NO separate "diagnosticos_secundarios" table:
           ✅ WHERE "DIAG_SECUN" IS NOT NULL (correct)
           ❌ JOIN diagnosticos_secundarios  (table does not exist!)

        4. PROCEDURES require a JUNCTION TABLE — there is NO PROC_REA column in internacoes:
           ✅ JOIN atendimentos a ON i."N_AIH" = a."N_AIH" JOIN procedimentos p ON a."PROC_REA" = p."PROC_REA"
           ❌ i."PROC_REA"                   (column does not exist in internacoes!)

        5. DIAS_PERM substitutes QT_DIARIAS:
           ✅ "DIAS_PERM"   (days of stay — use this)
           ❌ "QT_DIARIAS"  (does not exist in sihrd5!)

        6. IDADE vs NASC — REGRA CRÍTICA DE IDADE:
           "IDADE" é coluna INTEGER pré-calculada (0-130). USE SEMPRE para filtros de idade.
           "NASC" é data de nascimento. USE SOMENTE quando a pergunta for sobre ANO DE NASCIMENTO.
           ✅ WHERE "IDADE" > 60                          ← filtro de idade → use IDADE
           ✅ GROUP BY "IDADE"                             ← agrupar por idade → use IDADE
           ✅ CASE WHEN "IDADE" < 18 THEN 'Menor'          ← faixa etária → use IDADE
           ✅ WHERE EXTRACT(YEAR FROM "NASC") < 1950        ← "nascidos antes de 1950" → use NASC
           ❌ EXTRACT(YEAR FROM AGE("NASC")) > 60           ← ERRADO! use IDADE diretamente!
           ❌ (CURRENT_DATE - "NASC") / 365 > 60           ← ERRADO! use IDADE diretamente!

        ─────────────────────────────────────────────────────────────────

        MANDATORY VALUE MAPPINGS (NEVER MAKE MISTAKES):
        - SEXO: 1=Masculino, 3=Feminino (NEVER use 2! — use inline value, no JOIN needed)
        - RACA_COR: 1=Branca, 2=Preta, 3=Parda, 4=Amarela, 5=Indígena, 0/99=Sem info
          · Para FILTRAR: WHERE "RACA_COR" = 5  (sem JOIN)
          · Para DESCRIÇÃO: JOIN raca_cor r ON i."RACA_COR" = r."RACA_COR" → SELECT r."DESCRICAO"
        - MORTE: true=death, false=discharge (boolean)
        - IND_VDRL: true=positive, false=negative (boolean)
          ✅ SELECT COUNT(*) FROM internacoes WHERE "IND_VDRL" = true;
          ❌ JOIN cid c ON ... WHERE c."CD_DESCRICAO" ILIKE '%vdrl%'  (no CID code for VDRL!)
        - DIAS_PERM: days of stay (integer)
        - Financial: "VAL_TOT" (total cost), "VAL_SH" (serviço hospitalar), "VAL_SP" (professional), "VAL_UTI" (ICU cost)
          · "valor do serviço hospitalar" → "VAL_SH"  (NÃO VAL_TOT!)
          · "valor total da internação" / "valor médio" (sem especificar) → "VAL_TOT"
        - Dates: "DT_INTER" (admission), "DT_SAIDA" (discharge/death), "NASC" (birth date — see rule 6)
        - IDADE: integer age (0-130) — SEMPRE usar para filtros/grupos de idade (ver regra 6)

        CRITICAL UTI VALUE QUERIES — always include VAL_UTI > 0 filter:
        - "valor médio de UTI" / "custo de UTI" → AVG("VAL_UTI") WHERE "VAL_UTI" > 0
        - Without VAL_UTI > 0, the average includes all hospitalizations (most with VAL_UTI = 0)
          ✅ SELECT AVG("VAL_UTI") FROM internacoes WHERE "SEXO" = 1 AND "VAL_UTI" > 0
          ❌ SELECT AVG("VAL_UTI") FROM internacoes WHERE "SEXO" = 1  ← includes zeros!

        CRITICAL JOIN RELATIONSHIPS:
        - → hospital: internacoes."CNES" = hospital."CNES"
        - → cid: internacoes."DIAG_PRINC" = cid."CID"  (diagnóstico principal)
        - → cid: internacoes."CID_MORTE" = cid."CID"   (causa da morte — apenas quando MORTE=true)
        - → municipios (residência paciente): internacoes."MUNIC_RES" = municipios."codigo_6d"
        - → municipios (localização hospital): JOIN hospital h ON i."CNES" = h."CNES"
                                               JOIN municipios m ON h."MUNIC_MOV" = m.codigo_6d
        - → atendimentos (for procedures): internacoes."N_AIH" = atendimentos."N_AIH"
        - → especialidade: internacoes."ESPEC" = especialidade."ESPEC" (para obter nome da especialidade)
        - → raca_cor: internacoes."RACA_COR" = raca_cor."RACA_COR" (para obter descrição da raça)
        - → instrucao: internacoes."INSTRU" = instrucao."INSTRU" (para obter nome do nível de instrução)

        MUNIC_RES vs MUNIC_MOV — QUANDO USAR CADA UM:
        - "municípios de RESIDÊNCIA dos pacientes" / "onde os pacientes moram" → i."MUNIC_RES" → municipios
        - "municípios que ATENDEM mais pacientes" / "por localização do hospital" / "médias por município (hospital)" →
          JOIN hospital h ON i."CNES" = h."CNES"
          JOIN municipios m ON h."MUNIC_MOV" = m.codigo_6d  ← usa hospital.MUNIC_MOV!

        === FEW-SHOT EXAMPLES ===

        --- EASY EXAMPLES ---

        -- Q: "Quantos registros de internação em UTI existem?"
        SELECT COUNT(*) AS total_uti FROM internacoes WHERE "VAL_UTI" > 0;

        -- Q: "Quantas internações de UTI resultaram em óbito?"
        -- NOTE: "resultaram em óbito" = MORTE = true only; no CID JOIN needed (not asking which disease caused it)
        SELECT COUNT(*) AS uti_com_obito FROM internacoes WHERE "VAL_UTI" > 0 AND "MORTE" = true;

        -- Q: "Quantas internações obstétricas foram registradas em UTI?"
        -- NOTE: "obstétricas"/"obstétrico" = ESPEC = 2. NEVER search CID codes for obstetric!
        SELECT COUNT(*) AS obstetricos_uti FROM internacoes WHERE "ESPEC" = 2 AND "VAL_UTI" > 0;

        --- MEDIUM EXAMPLES ---

        -- Q: "Quantas internações por doença respiratória no inverno (junho a agosto)?"
        -- NOTE: "respiratória" = CID chapter J → use LIKE 'J%' on DIAG_PRINC (no JOIN needed for category)
        -- NOTE: do NOT use ILIKE '%respiratória%' — that term doesn't exist in CD_DESCRICAO!
        SELECT COUNT(*) FROM internacoes
        WHERE "DIAG_PRINC" LIKE 'J%'
          AND EXTRACT(MONTH FROM "DT_INTER") IN (6, 7, 8);

        -- Q: "Qual o diagnóstico mais comum nas internações?"
        -- NOTE: SINGULAR "qual o X mais Y" → LIMIT 1 (not LIMIT 5 or no LIMIT!)
        -- NOTE: include c."CID" ONLY when question says "com código" — default: only description
        SELECT c."CD_DESCRICAO" AS diagnostico, COUNT(*) AS total
        FROM internacoes i
        JOIN cid c ON i."DIAG_PRINC" = c."CID"
        WHERE i."DIAG_PRINC" IS NOT NULL
        GROUP BY c."CD_DESCRICAO"
        ORDER BY total DESC
        LIMIT 1;

        -- Q: "Quantas internações por ano?"
        SELECT EXTRACT(YEAR FROM "DT_INTER") AS ano, COUNT(*) AS total
        FROM internacoes
        WHERE "DT_INTER" IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM "DT_INTER")
        ORDER BY ano;

        --- HARD EXAMPLES ---

        -- Q: "Quais as 10 principais causas de morte?"
        -- Q: "Quais são as 10 principais causas de morte?"
        -- RULE: (1) use CID_MORTE (not DIAG_PRINC); (2) include c."CID" ONLY when question says "com código";
        --       (3) GROUP BY c."CD_DESCRICAO" by default; (4) always filter MORTE = true
        SELECT c."CD_DESCRICAO" AS causa_morte, COUNT(*) AS total_mortes
        FROM internacoes i
        JOIN cid c ON i."CID_MORTE" = c."CID"
        WHERE i."MORTE" = true AND i."CID_MORTE" IS NOT NULL
        GROUP BY c."CD_DESCRICAO"
        ORDER BY total_mortes DESC
        LIMIT 10;

        -- Q: "Internações por [doença] que ocasionaram morte (óbito)?"
        -- NOTE: doença + óbito → JOIN via CID_MORTE (não DIAG_PRINC!)
        -- Example: meningite com óbito:
        SELECT COUNT(*) AS total_obitos_meningite
        FROM internacoes i
        JOIN cid c ON i."CID_MORTE" = c."CID"
        WHERE c."CD_DESCRICAO" ILIKE '%meningite%'
          AND i."MORTE" = true;

        -- Q: "Qual a quantidade de internações por especialidade?"
        -- NOTE: for specialty NAME join especialidade; for UTI specifically use VAL_UTI > 0 not ESPEC
        -- NOTE: "quantidade por especialidade" = all specialties, no LIMIT
        SELECT e."DESCRICAO" AS especialidade, COUNT(*) AS total_consultas
        FROM internacoes i
        JOIN especialidade e ON i."ESPEC" = e."ESPEC"
        GROUP BY e."DESCRICAO"
        ORDER BY total_consultas DESC;

        -- Q: "Qual o custo médio de UTI por faixa etária dos pacientes?"
        -- NOTE: "faixa etária" always means CASE WHEN age bands (NOT GROUP BY exact IDADE)
        -- NOTE: always include VAL_UTI > 0 for UTI cost queries
        SELECT CASE WHEN "IDADE" < 18 THEN 'Menor' WHEN "IDADE" < 60 THEN 'Adulto' ELSE 'Idoso' END AS faixa_etaria,
               AVG("VAL_UTI") AS custo_medio_uti
        FROM internacoes
        WHERE "IDADE" IS NOT NULL AND "VAL_UTI" > 0
        GROUP BY CASE WHEN "IDADE" < 18 THEN 'Menor' WHEN "IDADE" < 60 THEN 'Adulto' ELSE 'Idoso' END;

        -- Q: "Qual a taxa de mortalidade para faixa etária 30-45?"
        -- NOTE: taxa de mortalidade = SUM(CASE WHEN MORTE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
        -- NOTE: always ROUND(..., 2) and include total_internacoes + total_mortes for context
        SELECT '30-45 anos' AS faixa_etaria,
               COUNT(*) AS total_internacoes,
               SUM(CASE WHEN "MORTE" = true THEN 1 ELSE 0 END) AS total_mortes,
               ROUND(SUM(CASE WHEN "MORTE" = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS taxa_mortalidade
        FROM internacoes
        WHERE "IDADE" BETWEEN 30 AND 45;

        -- Q: "Quais sao os 10 municípios com a maior taxa de mortalidade?"
        -- NOTE: taxa de mortalidade = SUM(CASE WHEN MORTE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
        -- NOTE: HAVING COUNT(*) > 100 to exclude low-volume municipalities
        SELECT mu.nome AS municipio,
               COUNT(*) AS total_internacoes,
               SUM(CASE WHEN i."MORTE" = true THEN 1 ELSE 0 END) AS total_mortes,
               ROUND(SUM(CASE WHEN i."MORTE" = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS taxa_mortalidade
        FROM internacoes i
        JOIN municipios mu ON i."MUNIC_RES" = mu.codigo_6d
        GROUP BY mu.nome
        HAVING COUNT(*) > 100
        ORDER BY taxa_mortalidade DESC
        LIMIT 10;

        -- Q: "Quantas mortes foram registradas nos estados do MA e no RS?"
        -- NOTE: "nos estados do X e Y" or "por estado" → GROUP BY estado to return per-state breakdown (NOT total)
        SELECT mu.estado, COUNT(*) AS total_mortes
        FROM internacoes i
        JOIN municipios mu ON i."MUNIC_RES" = mu.codigo_6d
        WHERE i."MORTE" = true AND mu.estado IN ('MA', 'RS')
        GROUP BY mu.estado
        ORDER BY total_mortes DESC;

        -- Q: "Qual o nível de instrução dos pacientes internados?"
        -- NOTE: "nível de instrução" bare query → use raw INSTRU numeric column (NOT JOIN instrucao)
        -- Only JOIN instrucao when question asks for human-readable DESCRIPTIONS of instruction levels
        SELECT "INSTRU", COUNT(*) AS total
        FROM internacoes
        GROUP BY "INSTRU"
        ORDER BY total DESC;

        -- Q: "Qual o custo médio de internação por hospital?"
        SELECT h."CNES", h."NATUREZA", AVG(i."VAL_TOT") AS custo_medio
        FROM internacoes i
        JOIN hospital h ON i."CNES" = h."CNES"
        WHERE i."VAL_TOT" IS NOT NULL
        GROUP BY h."CNES", h."NATUREZA"
        ORDER BY custo_medio DESC
        LIMIT 10;

        -- Q: "Quais os procedimentos mais realizados?"
        SELECT p."NOME_PROC", COUNT(*) AS total
        FROM internacoes i
        JOIN atendimentos a ON i."N_AIH" = a."N_AIH"
        JOIN procedimentos p ON a."PROC_REA" = p."PROC_REA"
        GROUP BY p."NOME_PROC"
        ORDER BY total DESC
        LIMIT 10;
""",

    "atendimentos": """
        ATENDIMENTOS TABLE RULES - PROCEDURES PER HOSPITALIZATION (JUNCTION TABLE):

        MANDATORY USAGE RULES:
        - This is the JUNCTION TABLE between internacoes and procedimentos
        - Each row = one procedure performed during one hospitalization
        - A single hospitalization can have MANY procedure records (1:N)
        - 37M+ records in this table

        CRITICAL: TO GET PROCEDURE DATA you MUST use a TWO-JOIN pattern:
        internacoes → atendimentos → procedimentos

        ❌ WRONG (PROC_REA does not exist in internacoes):
        SELECT i."PROC_REA" FROM internacoes i

        ✅ CORRECT:
        SELECT p."NOME_PROC", COUNT(*) AS total
        FROM atendimentos a
        JOIN procedimentos p ON a."PROC_REA" = p."PROC_REA"
        GROUP BY p."NOME_PROC"
        ORDER BY total DESC;

        POSTGRESQL COLUMN QUOTING:
        - "id_atendimento" (PK), "N_AIH" (FK → internacoes), "PROC_REA" (FK → procedimentos)

        === FEW-SHOT EXAMPLES ===

        -- Q: "Quais os procedimentos mais realizados?"
        SELECT p."NOME_PROC", COUNT(*) AS total
        FROM atendimentos a
        JOIN procedimentos p ON a."PROC_REA" = p."PROC_REA"
        GROUP BY p."NOME_PROC"
        ORDER BY total DESC
        LIMIT 10;

        -- Q: "Quantos procedimentos por internação em média?"
        SELECT AVG(proc_count) AS media_procedimentos
        FROM (
          SELECT "N_AIH", COUNT(*) AS proc_count
          FROM atendimentos
          GROUP BY "N_AIH"
        ) t;

        -- Q: "Quantos procedimentos cirúrgicos foram realizados?"
        SELECT COUNT(*) AS total
        FROM atendimentos a
        JOIN procedimentos p ON a."PROC_REA" = p."PROC_REA"
        WHERE p."NOME_PROC" ILIKE '%cirurgia%';

        -- Q: "Procedimentos realizados em internações de mulheres"
        SELECT p."NOME_PROC", COUNT(*) AS total
        FROM internacoes i
        JOIN atendimentos a ON i."N_AIH" = a."N_AIH"
        JOIN procedimentos p ON a."PROC_REA" = p."PROC_REA"
        WHERE i."SEXO" = 3
        GROUP BY p."NOME_PROC"
        ORDER BY total DESC
        LIMIT 10;

        -- Q: "Quais os 10 procedimentos mais comuns nas cidades do RS?" (filtro por ESTADO)
        -- PATTERN: filter by hospital state → MUST join internacoes + hospital + municipios
        -- hospital.MUNIC_MOV = city where the HOSPITAL is located (use for state filtering!)
        SELECT p."NOME_PROC" AS procedimento, COUNT(a."N_AIH") AS total_procedimentos
        FROM atendimentos a
        JOIN procedimentos p ON a."PROC_REA" = p."PROC_REA"
        JOIN internacoes i ON a."N_AIH" = i."N_AIH"
        JOIN hospital h ON i."CNES" = h."CNES"
        JOIN municipios m ON h."MUNIC_MOV" = m.codigo_6d
        WHERE m.estado = 'RS'
        GROUP BY p."NOME_PROC"
        ORDER BY total_procedimentos DESC
        LIMIT 10;
""",

    "cid": """
         CID TABLE RULES - ICD-10 DISEASE CODES (REFERENCE TABLE):

        MANDATORY USAGE RULES:
        - Use for: Disease code lookups, descriptions, JOIN operations
        - "CID" = ICD-10 code column (contains codes like 'J18', 'I21', 'C50')
        - "CD_DESCRICAO" = TEXT description column (contains 'Pneumonia', 'Infarto', 'Cancer mama')
        - TABLE NAME IS "cid" (NOT "cid10" — name changed in sihrd5!)

        ⚠️ CRITICAL COLUMN QUOTING — ALL COLUMNS REQUIRE DOUBLE QUOTES:
        ✅ c."CID"           (correct)   ❌ c.cid   (WRONG — will cause DB error!)
        ✅ c."CD_DESCRICAO"  (correct)   ❌ c.cd_descricao  (WRONG — will cause DB error!)
        ✅ c."CD_DESCRICAO"  (correct)   ❌ c.CD_DESCRICAO  (WRONG — must have quotes!)

        POSTGRESQL COLUMN QUOTING:
        - "CID" (ICD-10 code), "CD_DESCRICAO" (description)

        CRITICAL SEARCH PATTERNS:
        - Description search (disease name): WHERE "CD_DESCRICAO" ILIKE '%pneumonia%'
        - Code range search (disease category): WHERE "CID" LIKE 'I%'
        - NEVER: WHERE "CD_DESCRICAO" LIKE 'I%' (codes are in CID, not in CD_DESCRICAO!)

        JOIN PATTERNS WITH internacoes:
        - Primary diagnosis: JOIN cid c ON i."DIAG_PRINC" = c."CID"
        - Secondary diagnosis: JOIN cid c ON i."DIAG_SECUN" = c."CID"
        - Death cause: JOIN cid c ON i."CID_MORTE" = c."CID"

        EXACT QUERY EXAMPLES:
        -- Find specific code description
        SELECT "CD_DESCRICAO" FROM cid WHERE "CID" = 'A15';

        -- Search diabetes codes
        SELECT "CID", "CD_DESCRICAO"
        FROM cid
        WHERE "CID" LIKE 'E1%';

        -- Top diagnoses with descriptions (CID explicitly asked = include c."CID")
        SELECT c."CID", c."CD_DESCRICAO", COUNT(*) AS total
        FROM internacoes i
        JOIN cid c ON i."DIAG_PRINC" = c."CID"
        WHERE i."DIAG_PRINC" IS NOT NULL
        GROUP BY c."CID", c."CD_DESCRICAO"
        ORDER BY total DESC
        LIMIT 10;

        -- Q: "Quais são os principais CIDs de entrada de CADA nacionalidade?"
        -- PATTERN "principal X por cada Y" → ROW_NUMBER() OVER (PARTITION BY Y ORDER BY count DESC) = 1
        -- NOTE: NACIONAL is a numeric code in internacoes — do NOT join nacionalidade table unless asked
        SELECT "NACIONAL", "DIAG_PRINC", total_internacoes
        FROM (
            SELECT i."NACIONAL", i."DIAG_PRINC", COUNT(i."N_AIH") AS total_internacoes,
                   ROW_NUMBER() OVER (PARTITION BY i."NACIONAL" ORDER BY COUNT(i."N_AIH") DESC) AS rn
            FROM internacoes i
            GROUP BY i."NACIONAL", i."DIAG_PRINC"
        ) ranked
        WHERE rn = 1
        ORDER BY total_internacoes DESC
        LIMIT 10;

        -- Q: "Quais os principais motivos de internação para pacientes <18, 18-64, >64 anos?"
        -- PATTERN: age bands + top diagnosis per band → ROW_NUMBER() PARTITION BY faixa_etaria
        SELECT faixa_etaria, "DIAG_PRINC", "CD_DESCRICAO", total_internacoes
        FROM (
            SELECT CASE WHEN i."IDADE" < 18 THEN 'Menor de 18'
                        WHEN i."IDADE" BETWEEN 18 AND 64 THEN '18 a 64'
                        ELSE '65 ou mais' END AS faixa_etaria,
                   i."DIAG_PRINC", c."CD_DESCRICAO",
                   COUNT(i."N_AIH") AS total_internacoes,
                   ROW_NUMBER() OVER (
                       PARTITION BY CASE WHEN i."IDADE" < 18 THEN 'Menor de 18'
                                         WHEN i."IDADE" BETWEEN 18 AND 64 THEN '18 a 64'
                                         ELSE '65 ou mais' END
                       ORDER BY COUNT(i."N_AIH") DESC
                   ) AS rn
            FROM internacoes i
            JOIN cid c ON i."DIAG_PRINC" = c."CID"
            GROUP BY faixa_etaria, i."DIAG_PRINC", c."CD_DESCRICAO"
        ) ranked
        WHERE rn = 1
        ORDER BY faixa_etaria;
""",

    "hospital": """
         HOSPITAL TABLE RULES - HEALTHCARE FACILITIES:

        MANDATORY USAGE RULES:
        - Use for: Hospital counts, facility analysis, public/private classification
        - "CNES" = National Health Facility Registry code (primary key)
        - "MUNIC_MOV" = FK → municipios.codigo_6d (municipality where hospital is located)
        - "NATUREZA" = Facility nature (public/private classification)

        CRITICAL COUNTING RULES:
        - To count hospitals: COUNT(DISTINCT h."CNES")
        - Do NOT count by admissions (admissions are in internacoes)

        MUNICIPALITY RESOLUTION FOR HOSPITAL:
        ✅ CORRECT (hospital has MUNIC_MOV → municipios directly):
           JOIN municipios mu ON h."MUNIC_MOV" = mu."codigo_6d"

        ❌ WRONG (old pattern with dado_ibge — table does not exist):
           JOIN dado_ibge d ON mu."codigo_ibge" = d."codigo_municipio_completo"

        ✅ For socioeconomic data of hospital's municipality:
           JOIN municipios mu ON h."MUNIC_MOV" = mu."codigo_6d"
           JOIN socioeconomico s ON s."codigo_6d" = mu."codigo_6d" WHERE s.metrica = 'populacao_total'

        POSTGRESQL COLUMN QUOTING:
        - "CNES", "MUNIC_MOV", "NATUREZA", "GESTAO", "NAT_JUR"

        NATUREZA VALUES (approximate):
        - 0 = Público federal, 20/22 = Público municipal/estadual
        - 30/40 = Filantrópico/Sem fins lucrativos
        - 50 = Privado lucrativo, 60/61 = Privado filantrópico

        EXACT QUERY EXAMPLES:
        -- Total hospitals
        SELECT COUNT(*) FROM hospital;

        -- Hospitals with admissions
        SELECT COUNT(DISTINCT h."CNES")
        FROM hospital h
        JOIN internacoes i ON h."CNES" = i."CNES";

        -- Hospital activity volume
        SELECT h."CNES", h."NATUREZA", COUNT(i."N_AIH") AS internacoes
        FROM hospital h
        JOIN internacoes i ON h."CNES" = i."CNES"
        GROUP BY h."CNES", h."NATUREZA"
        ORDER BY internacoes DESC
        LIMIT 10;

        -- Hospital by municipality name
        SELECT h."CNES", mu."nome", mu."estado", h."NATUREZA"
        FROM hospital h
        JOIN municipios mu ON h."MUNIC_MOV" = mu."codigo_6d"
        WHERE mu."estado" = 'RS';
""",

    "municipios": """
        MUNICIPIOS TABLE RULES - BRAZILIAN MUNICIPALITIES:

        MANDATORY USAGE RULES:
        - Use for: Geographic queries, municipality names, state, coordinates
        - "codigo_6d" = 6-digit code (primary key — used in FKs from internacoes and hospital)
        - "codigo_ibge" = IBGE code (7 digits)
        - "nome" = municipality name, "estado" = state abbreviation (RS, SP, RJ...)

        POSTGRESQL COLUMN QUOTING:
        - "codigo_6d", "codigo_ibge", "nome", "estado", "latitude", "longitude"

        CRITICAL RELATIONSHIPS:
        - internacoes → municipios: internacoes."MUNIC_RES" = municipios."codigo_6d"
        - hospital → municipios: hospital."MUNIC_MOV" = municipios."codigo_6d"
        - socioeconomico → municipios: socioeconomico."codigo_6d" = municipios."codigo_6d"

        ❌ WRONG (dado_ibge does not exist in sihrd5):
           JOIN dado_ibge d ON mu."codigo_ibge" = d."codigo_municipio_completo"

        ✅ CORRECT for socioeconomic data:
           JOIN socioeconomico s ON s."codigo_6d" = mu."codigo_6d" WHERE s.metrica = 'populacao_total'

        EXACT QUERY EXAMPLES:
        -- Total municipalities
        SELECT COUNT(*) FROM municipios;

        -- RS state municipalities
        SELECT COUNT(*) FROM municipios WHERE "estado" = 'RS';

        -- Municipalities by state
        SELECT "estado", COUNT(*) AS total_municipios
        FROM municipios
        GROUP BY "estado"
        ORDER BY total_municipios DESC;

        -- Internações by state (via municipality of patient's residence)
        SELECT mu."estado", COUNT(*) AS total_internacoes
        FROM internacoes i
        JOIN municipios mu ON i."MUNIC_RES" = mu."codigo_6d"
        GROUP BY mu."estado"
        ORDER BY total_internacoes DESC;

        -- Q: "Quantas mortes nos estados do MA e RS?" → GROUP BY estado (per-state breakdown!)
        -- PATTERN "nos estados do X e Y" → always GROUP BY estado to return one row per state
        SELECT mu.estado, COUNT(*) AS total_mortes
        FROM internacoes i
        JOIN municipios mu ON i."MUNIC_RES" = mu.codigo_6d
        WHERE i."MORTE" = true AND mu.estado IN ('MA', 'RS')
        GROUP BY mu.estado
        ORDER BY total_mortes DESC;
""",

    "procedimentos": """
         PROCEDIMENTOS TABLE RULES - MEDICAL PROCEDURES REFERENCE:

        MANDATORY USAGE RULES:
        - Reference table for procedure codes and descriptions
        - To count PROCEDURES PERFORMED, MUST use atendimentos as junction table:
          internacoes → atendimentos → procedimentos
        - "PROC_REA" = Procedure code (primary key)
        - "NOME_PROC" = Procedure description/name

        CRITICAL: There is NO PROC_REA column in internacoes — always use atendimentos!

        ⚠️ CRITICAL: For "quais procedimentos"/"nomes dos procedimentos" queries:
        - ALWAYS SELECT p."NOME_PROC" (the human-readable name), NEVER p."PROC_REA" (the code)!
          ✅ SELECT p."NOME_PROC", COUNT(*) AS total  → "Cirurgia Cardíaca", "Parto Normal", etc.
          ❌ SELECT p."PROC_REA", COUNT(*) AS total   → "0301060096", "0310010039" (codes, not names!)

        POSTGRESQL COLUMN QUOTING:
        - ALWAYS use double quotes: "PROC_REA", "NOME_PROC"

        EXACT QUERY EXAMPLES:
        -- Total procedure types in reference table
        SELECT COUNT(*) AS total_procedimentos FROM procedimentos;

        -- Count procedures containing "CIRURGIA"
        SELECT COUNT(*) AS procedimentos_cirurgia
        FROM procedimentos
        WHERE "NOME_PROC" ILIKE '%CIRURGIA%';

        -- Most common procedures performed (via atendimentos junction)
        SELECT p."NOME_PROC", COUNT(*) AS frequency
        FROM atendimentos a
        JOIN procedimentos p ON a."PROC_REA" = p."PROC_REA"
        GROUP BY p."NOME_PROC"
        ORDER BY frequency DESC
        LIMIT 10;

        -- Procedures per hospitalization
        SELECT p."NOME_PROC", COUNT(DISTINCT a."N_AIH") AS internacoes_count
        FROM atendimentos a
        JOIN procedimentos p ON a."PROC_REA" = p."PROC_REA"
        GROUP BY p."NOME_PROC"
        ORDER BY internacoes_count DESC
        LIMIT 10;
""",

    "instrucao": """
        INSTRUCAO TABLE RULES - EDUCATION LEVEL LOOKUP:

        MANDATORY USAGE RULES:
        - This is a LOOKUP TABLE with INSTRU code + DESCRICAO
        - Education level is stored in internacoes."INSTRU" as a FK
        - Use for: JOIN with internacoes to get education level description

        POSTGRESQL COLUMN QUOTING:
        - "INSTRU" (PK), "DESCRICAO"

        INSTRU VALUE MAPPINGS:
        0=Sem informação, 1=Não sabe ler/escrever, 2=Alfabetizado,
        3=1°grau incompleto, 4=1°grau completo, 5=2°grau incompleto,
        6=2°grau completo, 7=Superior incompleto, 8=Superior completo,
        9=Especialização/Residência, 10=Mestrado, 11=Doutorado

        ⚠️ CRITICAL: "informado" / "registrado" / "tem nível de instrução" means code != 0:
        - CORRECT: WHERE "INSTRU" IS NOT NULL AND "INSTRU" != 0  → only real education data
        - WRONG:   JOIN instrucao WHERE "DESCRICAO" IS NOT NULL  → includes code 0 (18M rows!)
          Code 0 = "Sem informação" — almost ALL patients have this default code.
          Without "!= 0", the JOIN returns virtually the entire internacoes table.

        EXACT QUERY EXAMPLES:
        -- "Quantos pacientes têm nível de instrução informado?"
        SELECT COUNT(*) FROM internacoes WHERE "INSTRU" IS NOT NULL AND "INSTRU" != 0;

        -- Education level distribution of hospitalizations
        SELECT ins."DESCRICAO", COUNT(*) AS total
        FROM internacoes i
        JOIN instrucao ins ON i."INSTRU" = ins."INSTRU"
        WHERE i."INSTRU" IS NOT NULL AND i."INSTRU" != 0
        GROUP BY ins."INSTRU", ins."DESCRICAO"
        ORDER BY total DESC;

        -- Average cost by education level
        SELECT ins."DESCRICAO", AVG(i."VAL_TOT") AS avg_cost
        FROM internacoes i
        JOIN instrucao ins ON i."INSTRU" = ins."INSTRU"
        WHERE i."VAL_TOT" IS NOT NULL
        GROUP BY ins."INSTRU", ins."DESCRICAO"
        ORDER BY avg_cost DESC;
""",

    "vincprev": """
        VINCPREV TABLE RULES - SOCIAL SECURITY LINKAGE LOOKUP:

        MANDATORY USAGE RULES:
        - This is a LOOKUP TABLE with VINCPREV code + DESCRICAO
        - Social security type is stored in internacoes."VINCPREV" as a FK
        - Use for: JOIN with internacoes to get social security description

        POSTGRESQL COLUMN QUOTING:
        - "VINCPREV" (PK), "DESCRICAO"

        VINCPREV VALUE MAPPINGS:
        0=Sem informação, 1=Autônomo, 2=Desempregado, 3=Aposentado,
        4=Não segurado, 5=Empregado, 6=Empregador

        ⚠️ CRITICAL: "informado" / "registrado" / "tem vínculo previdenciário" means code != 0:
        - CORRECT: WHERE "VINCPREV" IS NOT NULL AND "VINCPREV" != 0  → only real social security data
        - WRONG:   JOIN vincprev WHERE "DESCRICAO" IS NOT NULL  → includes code 0 (18M rows!)
          Code 0 = "Sem informação" — almost ALL patients have this default code.
          Without "!= 0", the JOIN returns virtually the entire internacoes table.

        EXACT QUERY EXAMPLES:
        -- "Quantos pacientes têm vínculo previdenciário informado?"
        SELECT COUNT(*) FROM internacoes WHERE "VINCPREV" IS NOT NULL AND "VINCPREV" != 0;

        -- Social security distribution of hospitalizations
        SELECT v."DESCRICAO", COUNT(*) AS total
        FROM internacoes i
        JOIN vincprev v ON i."VINCPREV" = v."VINCPREV"
        WHERE i."VINCPREV" IS NOT NULL AND i."VINCPREV" != 0
        GROUP BY v."VINCPREV", v."DESCRICAO"
        ORDER BY total DESC;

        -- Mortality by social security type
        SELECT v."DESCRICAO",
               COUNT(*) AS total,
               SUM(CASE WHEN i."MORTE" = true THEN 1 ELSE 0 END) AS mortes,
               ROUND(SUM(CASE WHEN i."MORTE" = true THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS taxa_mortalidade
        FROM internacoes i
        JOIN vincprev v ON i."VINCPREV" = v."VINCPREV"
        GROUP BY v."VINCPREV", v."DESCRICAO"
        ORDER BY taxa_mortalidade DESC;
""",

    "sexo": """
        SEXO TABLE RULES - SEX LOOKUP:

        ⚠️ DO NOT JOIN this table — use inline mapping directly in the query.

        SEXO VALUES (memorize — only 2 valid values):
        - 1 = Masculino (male)
        - 3 = Feminino (female)
        - NEVER use SEXO = 2 (does not exist)

        ✅ CORRECT PATTERNS (no JOIN needed):
        -- Filter:
        SELECT COUNT(*) FROM internacoes WHERE "SEXO" = 1;  -- men
        SELECT COUNT(*) FROM internacoes WHERE "SEXO" = 3;  -- women

        -- Group with readable labels:
        SELECT
          CASE WHEN "SEXO" = 1 THEN 'Masculino' WHEN "SEXO" = 3 THEN 'Feminino' END AS sexo,
          COUNT(*) AS total
        FROM internacoes
        WHERE "SEXO" IN (1, 3)
        GROUP BY "SEXO";

        ❌ WRONG (unnecessary JOIN):
        SELECT s."DESCRICAO", COUNT(*) FROM internacoes i JOIN sexo s ON i."SEXO" = s."SEXO" ...
""",

    "raca_cor": """
        RACA_COR TABLE RULES - RACE/COLOR LOOKUP:

        TWO USAGE PATTERNS:
        • Para FILTRAR por raça: WHERE "RACA_COR" = 5  (sem JOIN, usar valor inline)
        • Para DESCRIÇÃO/NOME da raça: JOIN raca_cor r ON i."RACA_COR" = r."RACA_COR" → SELECT r."DESCRICAO"
          Alternativa equivalente: CASE WHEN "RACA_COR" = 1 THEN 'Branca' ... END AS raca_cor

        ⚠️ CRITICAL: "quantos registros de raça/cor estão cadastrados?" = count FROM internacoes:
        - CORRECT: SELECT COUNT(*) FROM internacoes WHERE "RACA_COR" IS NOT NULL;  → 18M patient records
        - WRONG:   SELECT COUNT(*) FROM raca_cor ...  → returns 5 (the 5 lookup code rows, not patients!)
          The raca_cor table is a tiny lookup dictionary — counting from it gives the number of categories,
          not the number of patients with recorded race data.

        RACA_COR VALUES (memorize — only 6 values):
        - 0 or 99 = Sem informação (exclude from analysis)
        - 1 = Branca
        - 2 = Preta
        - 3 = Parda
        - 4 = Amarela
        - 5 = Indígena

        ✅ CORRECT PATTERNS (no JOIN needed):
        -- Group with readable labels:
        SELECT
          CASE
            WHEN "RACA_COR" = 1 THEN 'Branca'
            WHEN "RACA_COR" = 2 THEN 'Preta'
            WHEN "RACA_COR" = 3 THEN 'Parda'
            WHEN "RACA_COR" = 4 THEN 'Amarela'
            WHEN "RACA_COR" = 5 THEN 'Indígena'
          END AS raca_cor,
          COUNT(*) AS total
        FROM internacoes
        WHERE "RACA_COR" NOT IN (0, 99)
        GROUP BY "RACA_COR"
        ORDER BY total DESC;

        -- Filter by specific race:
        SELECT COUNT(*) FROM internacoes WHERE "RACA_COR" = 5;  -- indigenous

        ❌ WRONG (unnecessary JOIN):
        SELECT rc."DESCRICAO", COUNT(*) FROM internacoes i JOIN raca_cor rc ON i."RACA_COR" = rc."RACA_COR" ...
""",

    "etnia": """
        ETNIA TABLE RULES - INDIGENOUS ETHNICITY LOOKUP:

        MANDATORY USAGE RULES:
        - Lookup table with 256 indigenous ethnicity codes
        - Ethnicity is stored in internacoes."ETNIA" as a FK
        - Only relevant for indigenous patients (internacoes."RACA_COR" = 5)
        - Use for: JOIN with internacoes to get ethnicity description

        POSTGRESQL COLUMN QUOTING:
        - "ETNIA" (PK), "DESCRICAO"

        JOIN PATTERN:
        - internacoes → etnia: internacoes."ETNIA" = etnia."ETNIA"

        EXACT QUERY EXAMPLES:
        -- Hospitalizations by indigenous ethnicity
        SELECT e."DESCRICAO", COUNT(*) AS total
        FROM internacoes i
        JOIN etnia e ON i."ETNIA" = e."ETNIA"
        WHERE i."RACA_COR" = 5
        GROUP BY e."ETNIA", e."DESCRICAO"
        ORDER BY total DESC
        LIMIT 10;

        -- Mortality by ethnicity
        SELECT e."DESCRICAO",
               COUNT(*) AS total,
               SUM(CASE WHEN i."MORTE" = true THEN 1 ELSE 0 END) AS mortes
        FROM internacoes i
        JOIN etnia e ON i."ETNIA" = e."ETNIA"
        WHERE i."RACA_COR" = 5
        GROUP BY e."ETNIA", e."DESCRICAO"
        ORDER BY mortes DESC;
""",

    "nacionalidade": """
        NACIONALIDADE TABLE RULES - NATIONALITY LOOKUP:

        MANDATORY USAGE RULES:
        - Lookup table with 333 nationality codes
        - Nationality is stored in internacoes."NACIONAL" as a FK
        - Use for: JOIN with internacoes to get nationality description

        POSTGRESQL COLUMN QUOTING:
        - "NACIONAL" (PK), "DESCRICAO"

        JOIN PATTERN:
        - internacoes → nacionalidade: internacoes."NACIONAL" = nacionalidade."NACIONAL"

        EXACT QUERY EXAMPLES:
        -- Hospitalizations by nationality
        SELECT n."DESCRICAO", COUNT(*) AS total
        FROM internacoes i
        JOIN nacionalidade n ON i."NACIONAL" = n."NACIONAL"
        WHERE i."NACIONAL" IS NOT NULL
        GROUP BY n."NACIONAL", n."DESCRICAO"
        ORDER BY total DESC
        LIMIT 10;

        -- Foreign nationals hospitalized
        SELECT n."DESCRICAO", COUNT(*) AS total
        FROM internacoes i
        JOIN nacionalidade n ON i."NACIONAL" = n."NACIONAL"
        WHERE i."NACIONAL" != 10  -- 10 = Brazil
        GROUP BY n."NACIONAL", n."DESCRICAO"
        ORDER BY total DESC;
""",

    "contraceptivos": """
        CONTRACEPTIVOS TABLE RULES - CONTRACEPTIVE METHOD LOOKUP:

        MANDATORY USAGE RULES:
        - Lookup table with 13 contraceptive method codes (0=Sem informação, 1=LAM, ..., 12=Coito interrompido)
        - Used ONLY for obstetric hospitalizations (internacoes."ESPEC" = 2)
        - TWO FK columns in internacoes: "CONTRACEP1" and "CONTRACEP2" (both reference CONTRACEPTIVO)

        POSTGRESQL COLUMN QUOTING:
        - "CONTRACEPTIVO" (PK), "DESCRICAO"

        CONTRACEPTIVO VALUES:
        0=Sem informação, 1=LAM, 2=Ogino-Knaus, 3=Temp. basal,
        4=Billings, 5=Cinto térmico, 6=DIU, 7=Diafragma,
        8=Preservativo, 9=Espermicida, 10=Hormônio oral,
        11=Hormônio injetável, 12=Coito interrompido

        JOIN PATTERNS:
        - Primary contraceptive: JOIN contraceptivos c1 ON i."CONTRACEP1" = c1."CONTRACEPTIVO"
        - Secondary contraceptive: JOIN contraceptivos c2 ON i."CONTRACEP2" = c2."CONTRACEPTIVO"

        EXACT QUERY EXAMPLES:
        -- Most used contraceptive methods in obstetric admissions
        SELECT c."DESCRICAO", COUNT(*) AS total
        FROM internacoes i
        JOIN contraceptivos c ON i."CONTRACEP1" = c."CONTRACEPTIVO"
        WHERE i."ESPEC" = 2 AND i."CONTRACEP1" != 0
        GROUP BY c."CONTRACEPTIVO", c."DESCRICAO"
        ORDER BY total DESC;

        -- Primary + secondary contraceptive combinations
        SELECT c1."DESCRICAO" AS metodo_1, c2."DESCRICAO" AS metodo_2, COUNT(*) AS total
        FROM internacoes i
        JOIN contraceptivos c1 ON i."CONTRACEP1" = c1."CONTRACEPTIVO"
        JOIN contraceptivos c2 ON i."CONTRACEP2" = c2."CONTRACEPTIVO"
        WHERE i."ESPEC" = 2 AND i."CONTRACEP1" != 0 AND i."CONTRACEP2" != 0
        GROUP BY c1."DESCRICAO", c2."DESCRICAO"
        ORDER BY total DESC
        LIMIT 10;
""",

    "especialidade": """
        ESPECIALIDADE TABLE RULES - MEDICAL SPECIALTY LOOKUP:

        MANDATORY USAGE RULES:
        - Lookup table: ESPEC code + DESCRICAO
        - Use for: JOIN with internacoes to get specialty description in human-readable form
        - CRITICAL: For UTI/ICU admissions, do NOT use ESPEC. Use VAL_UTI > 0 instead:
          ✅ WHERE "VAL_UTI" > 0          (correct UTI detection)
          ❌ WHERE "ESPEC" BETWEEN 74 AND 83  (unreliable for UTI detection — do not use)

        ESPEC RANGES:
        - 1=Cirúrgico, 2=Obstétrico, 3=Clínico, 4=Crônico, 5=Psiquiatria
        - 7=Pediátrico

        CRITICAL COUNTING RULES:
        ⚠️ "Quantas especialidades estão cadastradas?" → COUNT rows in especialidade table DIRECTLY
        ✅ SELECT COUNT(*) AS total_especialidades FROM especialidade;
        ❌ SELECT COUNT(DISTINCT "ESPEC") FROM internacoes;  ← WRONG! Counts ESPEC codes used in
           internacoes (≠ total specialties registered). Some specialties may have zero admissions.

        EXACT QUERY EXAMPLES:

        -- Count registered specialties (DIRECT TABLE COUNT — NOT from internacoes!)
        SELECT COUNT(*) AS total_especialidades FROM especialidade;

        -- Hospitalizations by specialty
        SELECT e."DESCRICAO" AS especialidade, COUNT(*) AS total_consultas
        FROM internacoes i
        JOIN especialidade e ON i."ESPEC" = e."ESPEC"
        GROUP BY e."DESCRICAO"
        ORDER BY total_consultas DESC;

        -- Average cost by specialty type
        SELECT e."DESCRICAO", AVG(i."VAL_TOT") AS avg_cost
        FROM internacoes i
        JOIN especialidade e ON i."ESPEC" = e."ESPEC"
        WHERE i."VAL_TOT" IS NOT NULL
        GROUP BY e."DESCRICAO"
        ORDER BY avg_cost DESC;
""",

    "socioeconomico": """
        SOCIOECONOMICO TABLE RULES - MUNICIPALITY SOCIOECONOMIC DATA:

        MANDATORY USAGE RULES:
        - PRIMARY TABLE for municipality demographic/economic analysis
        - FORMAT: Long format — each row = one metric for one municipality in one year
        - ALWAYS filter by "metrica" column when querying
        - PK is composite: (codigo_6d, ano, metrica)
        - CRITICAL: "taxa de mortalidade infantil" / "mortalidade infantil" data lives HERE,
          NOT in internacoes. Use: WHERE metrica = 'mortalidade_infantil_1ano'
        - CRITICAL: population / "população" data lives HERE:
          Use: WHERE metrica = 'populacao_total'

        ⚠️ ANTI-PATTERNS TO NEVER USE:
        ❌ SELECT mu.nome, SUM(s.valor) FROM municipios JOIN socioeconomico ... GROUP BY mu.nome
           (SUM without metrica filter sums ALL metrics — gives nonsense result!)
        ✅ SELECT mu.nome FROM socioeconomico s JOIN municipios mu WHERE s.metrica = 'populacao_total' ORDER BY s.valor DESC

        CRITICAL: When looking for "maior X" in socioeconomico:
        - ALWAYS start FROM socioeconomico (it has .valor)
        - JOIN municipios for the name
        - ALWAYS add WHERE metrica = '<metric_name>' BEFORE ordering
        - Use ORDER BY s.valor DESC (not SUM/AVG) for finding max

        AVAILABLE METRICS (metrica column values):
        - 'populacao_total'                   — total population
        - 'idhm'                              — Human Development Index
        - 'bolsa_familia_total'               — Bolsa Família (R$)
        - 'esgotamento_sanitario_domicilio'   — sanitation coverage
        - 'mortalidade_infantil_1ano'         — infant mortality rate
        - 'pop_economicamente_ativa'          — economically active population
        - 'taxa_envelhecimento'               — aging rate

        ❌ WRONG (dado_ibge does not exist in sihrd5):
           JOIN dado_ibge d ON ...

        ✅ CORRECT:
           JOIN socioeconomico s ON s."codigo_6d" = mu."codigo_6d"
           WHERE s.metrica = 'populacao_total'

        POSTGRESQL COLUMN QUOTING:
        - "codigo_6d", "ano", "metrica", "valor", "escala"

        EXACT QUERY EXAMPLES:
        -- Q: "Quantos municípios têm dados socioeconômicos registrados?"
        SELECT COUNT(DISTINCT codigo_6d) AS total_municipios FROM socioeconomico;

        -- Q: "Qual a taxa de mortalidade infantil média?"
        SELECT AVG(valor) AS taxa_media_mortalidade_infantil
        FROM socioeconomico
        WHERE metrica = 'mortalidade_infantil_1ano';

        -- Q: "Qual município tem a maior população?"
        -- Q: "Qual município tem a maior população segundo dados do IBGE?"
        -- NOTE: START with socioeconomico (has .valor), JOIN municipios to get .nome
        -- NEVER write: FROM municipios ORDER BY socioeconomico.valor (no join = WRONG!)
        SELECT mu.nome AS municipio_maior_populacao
        FROM socioeconomico s
        JOIN municipios mu ON s.codigo_6d = mu.codigo_6d
        WHERE s.metrica = 'populacao_total'
        ORDER BY s.valor DESC
        LIMIT 1;

        -- Highest population municipalities
        SELECT mu."nome", mu."estado", s."valor" AS populacao
        FROM socioeconomico s
        JOIN municipios mu ON s."codigo_6d" = mu."codigo_6d"
        WHERE s.metrica = 'populacao_total' AND s."ano" = 2010
        ORDER BY s."valor" DESC
        LIMIT 10;

        -- IDHM by state
        SELECT mu."estado", AVG(s."valor") AS avg_idhm
        FROM socioeconomico s
        JOIN municipios mu ON s."codigo_6d" = mu."codigo_6d"
        WHERE s.metrica = 'idhm'
        GROUP BY mu."estado"
        ORDER BY avg_idhm DESC;

        -- Municipalities with high Bolsa Família
        SELECT mu."nome", s."valor" AS bolsa_familia
        FROM socioeconomico s
        JOIN municipios mu ON s."codigo_6d" = mu."codigo_6d"
        WHERE s.metrica = 'bolsa_familia_total'
        ORDER BY s."valor" DESC
        LIMIT 10;
""",

    "tempo": """
        TEMPO TABLE RULES - DATE DIMENSION:

        MANDATORY USAGE RULES:
        - Date dimension table: one row per date
        - PREFERRED: Use EXTRACT() directly on internacoes."DT_INTER" — NO JOIN NEEDED for most queries

        POSTGRESQL COLUMN QUOTING:
        - "data" (PK, date type), "ano", "mes", "trimestre", "dia_semana"

        ⚠️⚠️ CRITICAL ANTI-PATTERN — NEVER JOIN TEMPO ON COMPUTED EXPRESSIONS:
        ❌ CATASTROPHIC: JOIN tempo t ON EXTRACT(YEAR FROM i."DT_INTER") = t.ano
           → Each internacao joins to ALL 365/366 rows in that year = 366× row explosion!
           → "Quantas internações em 2015?" GT=1,179,761 but WRONG result = 6,736,089,123!
        ❌ CATASTROPHIC: JOIN tempo t ON EXTRACT(MONTH FROM i."DT_INTER") BETWEEN 6 AND 8
           → Each row joins to ~90 date rows = 90× explosion!

        ✅ ALWAYS USE EXTRACT DIRECTLY WITHOUT ANY JOIN:
        -- "Quantas internações ocorreram em 2015?"
        SELECT COUNT(*) FROM internacoes WHERE EXTRACT(YEAR FROM "DT_INTER") = 2015;

        -- "Quantas internações no inverno (junho a agosto)?"
        SELECT COUNT(*) FROM internacoes WHERE EXTRACT(MONTH FROM "DT_INTER") IN (6, 7, 8);

        -- Filter by month range:
        SELECT COUNT(*) FROM internacoes WHERE EXTRACT(MONTH FROM "DT_INTER") BETWEEN 6 AND 8;

        -- Only join tempo when grouping BY date attributes (equijoin on exact date):
        SELECT t."mes", COUNT(*) AS total
        FROM internacoes i
        JOIN tempo t ON i."DT_INTER" = t."data"
        GROUP BY t."mes"
        ORDER BY t."mes";
"""
}


# Base PostgreSQL template for SQL generation
BASE_SQL_TEMPLATE = """You are a PostgreSQL expert assistant for Brazilian healthcare (SIH-RD) data analysis.

CORE POSTGRESQL INSTRUCTIONS:
1. Generate syntactically correct PostgreSQL queries
2. Use proper table and column names with double quotes
3. Handle Portuguese language questions appropriately
4. Return only the SQL query, no explanation
5. Use appropriate WHERE clauses for filtering
6. Include LIMIT clauses when appropriate (default LIMIT 100)
7. Use proper JOINs when querying multiple tables
8. Use PostgreSQL-specific functions when needed (EXTRACT, ILIKE, etc.)

DATABASE SCHEMA CONTEXT:
{schema_context}

{table_specific_rules}

USER QUERY: {user_query}

Generate the PostgreSQL query:"""


def build_table_specific_prompt(selected_tables: List[str]) -> str:
    """
    Builds dynamic prompt based on selected tables for PostgreSQL sihrd5 database

    Args:
        selected_tables: List of selected table names

    Returns:
        String with specific rules for selected tables
    """
    if not selected_tables:
        return "No specific table rules available."

    rules = []
    rules.append(" POSTGRESQL TABLE-SPECIFIC RULES AND EXAMPLES:")
    rules.append("=" * 60)

    for table in selected_tables:
        if table in TABLE_TEMPLATES:
            rules.append(f"\n{TABLE_TEMPLATES[table]}")
        else:
            # Generic template for unmapped tables
            rules.append(f"""
        {table.upper()} - GENERAL POSTGRESQL RULES:
        - Use proper column names with double quotes: "COLUMN_NAME"
        - Apply appropriate WHERE conditions for filtering
        - Use LIMIT for large result sets to improve performance
        - Consider NULL values in WHERE clauses
        - Use PostgreSQL-specific functions when appropriate
        """)

    return "\n".join(rules)


def get_table_template(table_name: str) -> Optional[str]:
    """
    Gets specific template for a table

    Args:
        table_name: Name of the table

    Returns:
        Table template or None if doesn't exist
    """
    return TABLE_TEMPLATES.get(table_name)


def get_available_templates() -> List[str]:
    """
    Returns list of tables with available templates

    Returns:
        List of table names with templates
    """
    return list(TABLE_TEMPLATES.keys())


def validate_template_coverage(tables: List[str]) -> Dict[str, bool]:
    """
    Validates if tables have available templates

    Args:
        tables: List of table names

    Returns:
        Dictionary mapping table -> has_template
    """
    return {table: table in TABLE_TEMPLATES for table in tables}


# Multi-table JOIN rules for PostgreSQL — sihrd5
MULTI_TABLE_RULES = """
MULTI-TABLE POSTGRESQL JOIN RULES (sihrd5):

CRITICAL JOIN PATTERNS:
- internacoes ↔ hospital: internacoes."CNES" = hospital."CNES"
- internacoes ↔ cid (primary diag): internacoes."DIAG_PRINC" = cid."CID"
- internacoes ↔ cid (secondary diag): internacoes."DIAG_SECUN" = cid."CID"
- internacoes ↔ cid (death cause): internacoes."CID_MORTE" = cid."CID"
- internacoes ↔ atendimentos: internacoes."N_AIH" = atendimentos."N_AIH"
- atendimentos ↔ procedimentos: atendimentos."PROC_REA" = procedimentos."PROC_REA"
- internacoes ↔ municipios: internacoes."MUNIC_RES" = municipios."codigo_6d"
- hospital ↔ municipios: hospital."MUNIC_MOV" = municipios."codigo_6d"
- municipios ↔ socioeconomico: municipios."codigo_6d" = socioeconomico."codigo_6d"
- internacoes ↔ sexo: internacoes."SEXO" = sexo."SEXO"
- internacoes ↔ raca_cor: internacoes."RACA_COR" = raca_cor."RACA_COR"
- internacoes ↔ instrucao: internacoes."INSTRU" = instrucao."INSTRU"
- internacoes ↔ vincprev: internacoes."VINCPREV" = vincprev."VINCPREV"
- internacoes ↔ especialidade: internacoes."ESPEC" = especialidade."ESPEC"

TABLES THAT NO LONGER EXIST IN sihrd5 — NEVER USE:
- mortes (use internacoes."MORTE" = true instead)
- cid10 (renamed to cid)
- dado_ibge (replaced by socioeconomico with long format)
- uti_detalhes (use internacoes."VAL_UTI" > 0 — do NOT use ESPEC for UTI detection)
- condicoes_especificas (use internacoes."IND_VDRL" = true)
- obstetricos (use internacoes."INSC_PN", "GESTRICO", "CONTRACEP1", "CONTRACEP2")
- diagnosticos_secundarios (use internacoes."DIAG_SECUN")
- cbor, infehosp (removed from sihrd5)

JOIN BEST PRACTICES:
- Always use table aliases for clarity (e.g., i."SEXO", h."NATUREZA")
- Use INNER JOIN for exact matches, LEFT JOIN to include null records
- Filter before joining when possible for better performance
- Always quote column names with double quotes in PostgreSQL
- When counting hospitals: COUNT(DISTINCT h."CNES")

"""


def build_multi_table_prompt(selected_tables: List[str]) -> str:
    """
    Builds prompt for queries involving multiple tables

    Args:
        selected_tables: List of selected tables

    Returns:
        Prompt with multi-table rules
    """
    if len(selected_tables) <= 1:
        return build_table_specific_prompt(selected_tables)

    single_table_rules = build_table_specific_prompt(selected_tables)

    return f"""
{single_table_rules}

{MULTI_TABLE_RULES}
"""


# Template system configuration
TEMPLATE_CONFIG = {
    "default_template": BASE_SQL_TEMPLATE,
    "include_examples": True,
    "include_mappings": True,
    "max_examples_per_table": 5,
    "enable_multi_table_rules": True,
    "postgresql_mode": True,
    "quote_columns": True,
    "include_performance_hints": True
}


def get_template_stats() -> Dict[str, int]:
    """
    Gets statistics about template coverage

    Returns:
        Dictionary with template statistics
    """
    return {
        "total_templates": len(TABLE_TEMPLATES),
        "fact_tables": 2,        # internacoes, atendimentos
        "reference_tables": 5,   # cid, hospital, municipios, procedimentos, socioeconomico
        "lookup_tables": 9,      # sexo, raca_cor, instrucao, vincprev, especialidade, tempo, etnia, nacionalidade, contraceptivos
        "total_db_tables": 16    # total tables in sihrd5
    }
