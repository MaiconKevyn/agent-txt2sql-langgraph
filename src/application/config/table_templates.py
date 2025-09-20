from typing import List, Dict, Optional


# PostgreSQL-specific templates for all 15 SIH-RS tables
TABLE_TEMPLATES = {
    "internacoes": """
         INTERNACOES TABLE RULES - MAIN HOSPITALIZATION DATA:
        
        MANDATORY VALUE MAPPINGS (NEVER MAKE MISTAKES):
        - For questions about MEN/HOMENS/MASCULINO: ALWAYS use "SEXO" = 1
        - For questions about WOMEN/MULHERES/FEMININO: ALWAYS use "SEXO" = 3
        - NEVER use "SEXO" = 2 (invalid value)
        - Age queries: Use "IDADE" column (numeric, in years)
        - Financial queries: "VAL_TOT", "VAL_SH", "VAL_SP" (total, hospital, professional values)
        - Duration queries: "DIAS_PERM" (days of stay), "QT_DIARIAS" (daily charges)
        
        POSTGRESQL COLUMN QUOTING:
        - ALL columns MUST use double quotes: "SEXO", "IDADE", "VAL_TOT", "N_AIH"
        - Date columns: "DT_INTER" (admission), "DT_SAIDA" (discharge), "NASC" (birth)
        - Municipality: "MUNIC_RES" (residence), "MUNIC_MOV" (movement)
        - Diagnosis: "DIAG_PRINC" (primary), "DIAG_SECUN" (secondary)
        
        CRITICAL JOIN RELATIONSHIPS:
        - → hospital: internacoes."CNES" = hospital."CNES"
        - → cid10: internacoes."DIAG_PRINC" = cid10."CID"
        - → mortes: internacoes."N_AIH" = mortes."N_AIH"
        - → uti_detalhes: internacoes."N_AIH" = uti_detalhes."N_AIH"

        DIAGNOSIS DESCRIPTION RULES (CID LOOKUPS):
        - When a query asks for diagnosis names, rankings, "diagnósticos mais comuns", or any output involving disease names,
          ALWAYS JOIN with the cid10 table on internacoes."DIAG_PRINC" = cid10."CID"
        - SELECT both the code cid10."CID" and the description cid10."CD_DESCRICAO" in the result set, together with the metric (e.g., COUNT(*))
        - Use ILIKE for case-insensitive description searches; use proper GROUP BY over both code and description
        - Seasonal filter (Southern Hemisphere): Inverno (winter) months are 6, 7, and 8 (June, July, August)
        
        EXACT QUERY EXAMPLES:
        -- Men count
        SELECT COUNT(*) FROM internacoes WHERE "SEXO" = 1;
        
        -- Women average age
        SELECT AVG("IDADE") FROM internacoes WHERE "SEXO" = 3 AND "IDADE" IS NOT NULL;
        
        -- Total financial values
        SELECT SUM("VAL_TOT") FROM internacoes WHERE "VAL_TOT" IS NOT NULL;
        
        -- Long stays (>30 days)
        SELECT COUNT(*) FROM internacoes WHERE "DIAS_PERM" > 30;
        
        -- Admissions by year
        SELECT EXTRACT(YEAR FROM "DT_INTER") as year, COUNT(*) 
        FROM internacoes 
        WHERE "DT_INTER" IS NOT NULL 
        GROUP BY EXTRACT(YEAR FROM "DT_INTER");

        -- Top 3 most common diagnoses in winter with descriptions
        SELECT c."CID", c."CD_DESCRICAO", COUNT(*) AS total
        FROM internacoes i
        JOIN cid10 c ON i."DIAG_PRINC" = c."CID"
        WHERE EXTRACT(MONTH FROM i."DT_INTER") IN (6,7,8)
        GROUP BY c."CID", c."CD_DESCRICAO"
        ORDER BY total DESC
        LIMIT 3;
""",

    "mortes": """
        MORTES TABLE RULES - DEATH RECORDS DURING HOSPITALIZATION:
        
        MANDATORY USAGE RULES:
        - PRIMARY TABLE for ALL death counts and mortality statistics
        - Use for: "mortes", "óbitos", "deaths", "mortality", "taxa de mortalidade"
        - "N_AIH" links to internacoes table
        - "CID_MORTE" contains death cause (ICD-10 code)
        
        POSTGRESQL COLUMN QUOTING:
        - "N_AIH" (hospitalization ID), "CID_MORTE" (death cause code)
        
        CRITICAL PATTERNS:
        - Cardiovascular deaths: "CID_MORTE" LIKE 'I%'
        - Respiratory deaths: "CID_MORTE" LIKE 'J%'  
        - Cancer deaths: "CID_MORTE" LIKE 'C%'
        - External causes: "CID_MORTE" LIKE 'V%' OR "CID_MORTE" LIKE 'W%'
        
        EXACT QUERY EXAMPLES:
        -- Total deaths
        SELECT COUNT(*) FROM mortes;
        
        -- Cardiovascular deaths
        SELECT COUNT(*) FROM mortes WHERE "CID_MORTE" LIKE 'I%';
        
        -- Deaths with hospitalization data
        SELECT COUNT(DISTINCT m."N_AIH") 
        FROM mortes m 
        JOIN internacoes i ON m."N_AIH" = i."N_AIH";
        
        -- Death causes ranking
        SELECT "CID_MORTE", COUNT(*) as total_deaths
        FROM mortes 
        WHERE "CID_MORTE" IS NOT NULL 
        GROUP BY "CID_MORTE" 
        ORDER BY total_deaths DESC 
        LIMIT 10;
""",

    "cid10": """
         CID10 TABLE RULES - ICD-10 DISEASE CODES (REFERENCE TABLE):
        
        MANDATORY USAGE RULES:
        - Use for: Disease code lookups, descriptions, JOIN operations
        - For counting available codes: SELECT COUNT(*) FROM cid10
        - For finding descriptions: JOIN with other tables on CID codes
        - "CID" = ICD-10 code, "CD_DESCRICAO" = disease description
        
        POSTGRESQL COLUMN QUOTING:
        - "CID" (ICD-10 code), "CD_DESCRICAO" (description)
        
        CRITICAL SEARCH PATTERNS:
        - Specific code: WHERE "CID" = 'A15'
        - Category search: WHERE "CID" LIKE 'I%' (cardiovascular)
        - Description search: WHERE "CD_DESCRICAO" ILIKE '%pneumonia%'
        
        ICD-10 CATEGORIES:
        - A00-B99: Infectious diseases
        - C00-D48: Neoplasms (cancer)
        - I00-I99: Cardiovascular diseases
        - J00-J99: Respiratory diseases
        - O00-O99: Pregnancy/childbirth
        
        EXACT QUERY EXAMPLES:
        -- Total ICD codes available
        SELECT COUNT(*) FROM cid10;
        
        -- Find specific code description
        SELECT "CD_DESCRICAO" FROM cid10 WHERE "CID" = 'A15';
        
        -- Search diabetes codes
        SELECT "CID", "CD_DESCRICAO" 
        FROM cid10 
        WHERE "CID" LIKE 'E1%' AND "CID" >= 'E10' AND "CID" <= 'E14';
        
        -- Cardiovascular diseases
        SELECT COUNT(*) FROM cid10 WHERE "CID" LIKE 'I%';
""",

    "hospital": """
         HOSPITAL TABLE RULES - HEALTHCARE FACILITIES:

        MANDATORY USAGE RULES:
        - Use for: Hospital counts, facility analysis, public/private classification
        - "CNES" = National Health Facility Registry code (primary key)
        - "NATUREZA" = Facility nature (public/private classification)

        POSTGRESQL COLUMN QUOTING:
        - "CNES" (facility code), "NATUREZA" (nature), "GESTAO" (management), "NAT_JUR" (legal nature)

        CRITICAL VALUE MAPPINGS:
        - Public hospitals: "NATUREZA" containing 'PUBLIC' or 'PUBLICA'
        - Private hospitals: "NATUREZA" containing 'PRIVAD'
        - Use ILIKE for case-insensitive searches

        
        EXACT QUERY EXAMPLES:
        -- Total hospitals
        SELECT COUNT(*) FROM hospital;
        
        -- Public hospitals count
        SELECT COUNT(*) FROM hospital 
        WHERE "NATUREZA" ILIKE '%public%' OR "NATUREZA" ILIKE '%publica%';
        
        -- Hospitals with admissions
        SELECT COUNT(DISTINCT h."CNES") 
        FROM hospital h 
        JOIN internacoes i ON h."CNES" = i."CNES";
        
        -- Hospital activity volume
        SELECT h."CNES", COUNT(i."N_AIH") as admissions
        FROM hospital h 
        JOIN internacoes i ON h."CNES" = i."CNES" 
        GROUP BY h."CNES" 
        HAVING COUNT(i."N_AIH") > 1000;
""",

    "municipios": """
        MUNICIPIOS TABLE RULES - BRAZILIAN MUNICIPALITIES:
        
        MANDATORY USAGE RULES:
        - Use for: Geographic queries, municipality names, coordinates
        - "codigo_6d" = 6-digit code (primary key)
        - "codigo_ibge" = IBGE code (7 digits, unique)
        - "nome" = municipality name, "estado" = state abbreviation
        
        POSTGRESQL COLUMN QUOTING:
        - "codigo_6d", "codigo_ibge", "nome", "estado", "latitude", "longitude"
        
        CRITICAL RELATIONSHIPS:
        - → dado_ibge: municipios."codigo_ibge" = dado_ibge."codigo_municipio_completo"
        - → internacoes: internacoes."MUNIC_RES" = municipios.codigo_6d (6-digit municipality residence code)

        MUNICIPALITY CODE MAPPING:
        - 6-digit codes (codigo_6d): Used in internacoes table as "MUNIC_RES"
        - 7-digit codes (codigo_ibge): Used in dado_ibge table for demographic data
        - To connect hospitals → municipalities → demographics:
          hospital → internacoes → municipios → dado_ibge
        
        EXACT QUERY EXAMPLES:
        -- Total municipalities
        SELECT COUNT(*) FROM municipios;
        
        -- RS state municipalities
        SELECT COUNT(*) FROM municipios WHERE "estado" = 'RS';
        
        -- Porto Alegre coordinates
        SELECT "latitude", "longitude" FROM municipios WHERE "nome" = 'Porto Alegre';
        
        -- States with most municipalities
        SELECT "estado", COUNT(*) as total_cities
        FROM municipios 
        GROUP BY "estado" 
        ORDER BY total_cities DESC;
""",

    "dado_ibge": """
         DADO_IBGE TABLE RULES - MUNICIPALITY SOCIOECONOMIC DATA:
        
        MANDATORY USAGE RULES:
        - PRIMARY TABLE for municipality demographic and economic analysis
        - Use for: Population, salary, education (IDEB), mortality rates
        - "codigo_municipio_completo" = Complete municipality code (primary key)
        - "nome_municipio" = Municipality name
        
        POSTGRESQL COLUMN QUOTING:
        - "codigo_municipio_completo", "nome_municipio", "populacao", "densidade_demografica"
        - "salario_medio", "pessoal_ocupado", "mortalidade_infantil"
        - "ideb_anos_iniciais_ensino_fundamental", "ideb_anos_finais_ensino_fundamental"
        
        CRITICAL DATA FIELDS:
        - "populacao" = Population in thousands
        - "salario_medio" = Average salary
        - "mortalidade_infantil" = Infant mortality rate
        - "densidade_demografica" = Demographic density
        
        EXACT QUERY EXAMPLES:
        -- Highest population municipality
        SELECT "nome_municipio", "populacao" 
        FROM dado_ibge 
        WHERE "populacao" IS NOT NULL 
        ORDER BY "populacao" DESC LIMIT 1;
        
        -- Average infant mortality in Brazil
        SELECT AVG("mortalidade_infantil") 
        FROM dado_ibge 
        WHERE "mortalidade_infantil" IS NOT NULL;
        
        -- Cities with >100k population
        SELECT COUNT(*) FROM dado_ibge WHERE "populacao" > 100;
        
        -- Best education scores
        SELECT "nome_municipio", "ideb_anos_iniciais_ensino_fundamental"
        FROM dado_ibge 
        WHERE "ideb_anos_iniciais_ensino_fundamental" IS NOT NULL
        ORDER BY "ideb_anos_iniciais_ensino_fundamental" DESC LIMIT 10;
""",

    "uti_detalhes": """
         UTI_DETALHES TABLE RULES - INTENSIVE CARE UNIT DATA:
        
        MANDATORY USAGE RULES:
        - PRIMARY TABLE for UTI/ICU statistics and costs
        - Use for: "UTI", "ICU", "terapia intensiva", UTI costs, ICU duration
        - Links to internacoes via "N_AIH"
        
        POSTGRESQL COLUMN QUOTING:
        - "N_AIH", "UTI_MES_TO", "MARCA_UTI", "UTI_INT_TO", "VAL_UTI"
        
        CRITICAL DATA FIELDS:
        - "UTI_MES_TO" = Total UTI time (months)
        - "UTI_INT_TO" = Total intermediate UTI time  
        - "VAL_UTI" = UTI cost/value
        - "MARCA_UTI" = UTI marker/type
        
        EXACT QUERY EXAMPLES:
        -- Total UTI records
        SELECT COUNT(*) FROM uti_detalhes;
        
        -- Average UTI cost
        SELECT AVG("VAL_UTI") FROM uti_detalhes WHERE "VAL_UTI" IS NOT NULL;
        
        -- Average UTI stay time
        SELECT AVG("UTI_MES_TO") FROM uti_detalhes WHERE "UTI_MES_TO" > 0;
        
        -- UTI cost by patient gender
        SELECT i."SEXO", AVG(u."VAL_UTI") as avg_uti_cost
        FROM uti_detalhes u 
        JOIN internacoes i ON u."N_AIH" = i."N_AIH"
        WHERE u."VAL_UTI" IS NOT NULL AND i."SEXO" IN (1,3)
        GROUP BY i."SEXO";
        
        -- UTI cases that resulted in death
        SELECT COUNT(DISTINCT u."N_AIH") 
        FROM uti_detalhes u 
        JOIN mortes m ON u."N_AIH" = m."N_AIH";
""",

    "procedimentos": """
         PROCEDIMENTOS TABLE RULES - MEDICAL PROCEDURES REFERENCE:
        
        MANDATORY USAGE RULES:
        - Reference table for procedure codes and descriptions
        - Use for: Procedure counts, procedure names, procedure analysis
        - "PROC_REA" = Procedure code (primary key)
        - "NOME_PROC" = Procedure description/name
        
        POSTGRESQL COLUMN QUOTING:
        - "PROC_REA", "NOME_PROC"
        
        CRITICAL SEARCH PATTERNS:
        - Procedure name search: WHERE "NOME_PROC" ILIKE '%cirurgia%'
        - Specific procedure: WHERE "PROC_REA" = '0404010032'
        
        EXACT QUERY EXAMPLES:
        -- Total procedures available
        SELECT COUNT(*) FROM procedimentos;
        
        -- Find surgery procedures
        SELECT COUNT(*) FROM procedimentos WHERE "NOME_PROC" ILIKE '%cirurgia%';
        
        -- Most common procedures (from internacoes)
        SELECT p."NOME_PROC", COUNT(*) as frequency
        FROM internacoes i
        JOIN procedimentos p ON i."PROC_REA" = p."PROC_REA"
        WHERE i."PROC_REA" IS NOT NULL
        GROUP BY p."NOME_PROC"
        ORDER BY frequency DESC LIMIT 10;
        
        -- Procedure with highest average cost
        SELECT p."NOME_PROC", AVG(i."VAL_TOT") as avg_cost
        FROM internacoes i
        JOIN procedimentos p ON i."PROC_REA" = p."PROC_REA"
        WHERE i."VAL_TOT" IS NOT NULL
        GROUP BY p."NOME_PROC"
        ORDER BY avg_cost DESC LIMIT 5;
""",

    "obstetricos": """
        OBSTETRICOS TABLE RULES - OBSTETRIC/MATERNITY DATA:
        
        MANDATORY USAGE RULES:
        - Use for: Pregnancy, childbirth, maternity, obstetric cases
        - Keywords: "grávidas", "gestantes", "parto", "obstetric", "prenatal"
        - Links to internacoes via "N_AIH"
        
        POSTGRESQL COLUMN QUOTING:
        - "N_AIH", "INSC_PN"
        
        CRITICAL DATA FIELDS:
        - "INSC_PN" = Prenatal registration/inscription
        - Non-null "INSC_PN" indicates prenatal care
        
        EXACT QUERY EXAMPLES:
        -- Total obstetric cases
        SELECT COUNT(*) FROM obstetricos;
        
        -- Obstetric cases with prenatal care
        SELECT COUNT(*) FROM obstetricos 
        WHERE "INSC_PN" IS NOT NULL AND "INSC_PN" != '';
        
        -- Pregnant women in public hospitals
        SELECT COUNT(DISTINCT i."N_AIH") 
        FROM internacoes i 
        JOIN obstetricos o ON i."N_AIH" = o."N_AIH"
        JOIN hospital h ON i."CNES" = h."CNES"
        WHERE i."SEXO" = 3 AND h."NATUREZA" ILIKE '%public%';
        
        -- Obstetric cases requiring UTI
        SELECT COUNT(DISTINCT o."N_AIH")
        FROM obstetricos o
        JOIN uti_detalhes u ON o."N_AIH" = u."N_AIH";
""",

    "condicoes_especificas": """
         CONDICOES_ESPECIFICAS TABLE RULES - SPECIAL MEDICAL CONDITIONS:
        
        MANDATORY USAGE RULES:
        - Use for: Special conditions, VDRL testing, syphilis screening
        - "N_AIH" links to internacoes
        - "IND_VDRL" = VDRL indicator (syphilis test)
        
        POSTGRESQL COLUMN QUOTING:
        - "N_AIH", "IND_VDRL"
        
        CRITICAL VALUE MAPPINGS:
        - "IND_VDRL" = '1' means VDRL positive
        - Only positive/special cases are recorded in this table
        
        EXACT QUERY EXAMPLES:
        -- Total special conditions recorded
        SELECT COUNT(*) FROM condicoes_especificas;
        
        -- VDRL positive cases
        SELECT COUNT(*) FROM condicoes_especificas WHERE "IND_VDRL" = '1';
        
        -- VDRL positive cases that resulted in death
        SELECT COUNT(DISTINCT c."N_AIH") 
        FROM condicoes_especificas c 
        JOIN mortes m ON c."N_AIH" = m."N_AIH" 
        WHERE c."IND_VDRL" = '1';
        
        -- Special conditions with hospital data
        SELECT h."NATUREZA", COUNT(*) as special_cases
        FROM condicoes_especificas c
        JOIN internacoes i ON c."N_AIH" = i."N_AIH"
        JOIN hospital h ON i."CNES" = h."CNES"
        GROUP BY h."NATUREZA";
""",

    "instrucao": """
        INSTRUCAO TABLE RULES - EDUCATION LEVEL DATA:
        
        MANDATORY USAGE RULES:
        - Use for: Education analysis, schooling levels, literacy statistics
        - "N_AIH" links to internacoes
        - "INSTRU" = Education level code
        
        POSTGRESQL COLUMN QUOTING:
        - "N_AIH", "INSTRU"
        
        CRITICAL VALUE MAPPINGS:
        - "INSTRU" codes:   01= Não sabe ler e/ou escrever, 
                            02= Alfabetizado, 
                            03= 1 Grau incompleto,
        -                   04= 1 Grau completo, 
                            05= 2 Grau incompleto,
                            06= 2 Grau completo,
        -                   07= Ensino Superior incompleto, 
                            08= Ensino Superior completo,,    
                            09= Especialização/Residência,
        -                   10= Mestrado, 
                            11= Doutorado 
                                                         
        EXACT QUERY EXAMPLES:
        -- Total education records
        SELECT COUNT(*) FROM instrucao;
        
        -- Education level distribution
        SELECT "INSTRU", COUNT(*) as total
        FROM instrucao 
        WHERE "INSTRU" IS NOT NULL
        GROUP BY "INSTRU" 
        ORDER BY "INSTRU";
        
        -- Education vs hospitalization costs correlation
        SELECT ins."INSTRU", AVG(i."VAL_TOT") as avg_cost
        FROM instrucao ins
        JOIN internacoes i ON ins."N_AIH" = i."N_AIH"
        WHERE ins."INSTRU" IS NOT NULL AND i."VAL_TOT" IS NOT NULL
        GROUP BY ins."INSTRU"
        ORDER BY avg_cost DESC;
""",

    "vincprev": """
        VINCPREV TABLE RULES - SOCIAL SECURITY LINKAGE:
        
        MANDATORY USAGE RULES:
        - Use for: Social security, employment status, pension analysis
        - "N_AIH" links to internacoes
        - "VINCPREV" = Social security link type
        
        POSTGRESQL COLUMN QUOTING:
        - "N_AIH", "VINCPREV"
        
        CRITICAL VALUE MAPPINGS:
        - "VINCPREV" codes: 1=Employee, 2=Domestic worker, 3=Self-employed
        - 5=Retired, 6=Unemployed
        
        EXACT QUERY EXAMPLES:
        -- Total social security records
        SELECT COUNT(*) FROM vincprev;
        
        -- Social security type distribution
        SELECT "VINCPREV", COUNT(*) as total
        FROM vincprev 
        WHERE "VINCPREV" IS NOT NULL
        GROUP BY "VINCPREV" 
        ORDER BY "VINCPREV";
        
        -- Employment status vs health outcomes
        SELECT v."VINCPREV", COUNT(m."N_AIH") as deaths
        FROM vincprev v
        LEFT JOIN mortes m ON v."N_AIH" = m."N_AIH"
        GROUP BY v."VINCPREV";
""",

    "cbor": """
        CBOR TABLE RULES - PROFESSIONAL OCCUPATION CLASSIFICATION:
        
        MANDATORY USAGE RULES:
        - Use for: Occupational analysis, professional classification, job-health correlation
        - "N_AIH" links to internacoes
        - "CBOR" = Brazilian Occupation Classification code
        
        POSTGRESQL COLUMN QUOTING:
        - "N_AIH", "CBOR"
        
        CRITICAL PATTERNS:
        - Healthcare professionals often have codes starting with 225
        - Use for occupational health analysis
        
        EXACT QUERY EXAMPLES:
        -- Total CBOR records
        SELECT COUNT(*) FROM cbor;
        
        -- Most common occupations
        SELECT "CBOR", COUNT(*) as frequency
        FROM cbor 
        WHERE "CBOR" IS NOT NULL
        GROUP BY "CBOR" 
        ORDER BY frequency DESC LIMIT 10;
        
        -- Healthcare professionals who died
        SELECT COUNT(DISTINCT c."CBOR") 
        FROM cbor c 
        JOIN mortes m ON c."N_AIH" = m."N_AIH" 
        WHERE c."CBOR" IS NOT NULL;
        
        -- CBOR cases requiring UTI
        SELECT COUNT(DISTINCT c."N_AIH")
        FROM cbor c
        JOIN uti_detalhes u ON c."N_AIH" = u."N_AIH";
""",

    "infehosp": """
        INFEHOSP TABLE RULES - HOSPITAL INFECTIONS:
        
        MANDATORY USAGE RULES:
        - Use for: Hospital-acquired infections, nosocomial infections
        - "N_AIH" links to internacoes  
        - "INFEHOSP" = Hospital infection indicator
        -  WARNING: This table is currently EMPTY (0 records)
        
        POSTGRESQL COLUMN QUOTING:
        - "N_AIH", "INFEHOSP"
        
        CRITICAL NOTES:
        - Table exists but contains no data
        - Do not use for actual infection analysis
        
        EXACT QUERY EXAMPLES:
        -- Check if infections recorded (will return 0)
        SELECT COUNT(*) FROM infehosp;
        
        -- This query will return no results
        SELECT * FROM infehosp LIMIT 10;
""",

    "diagnosticos_secundarios": """
         DIAGNOSTICOS_SECUNDARIOS TABLE RULES - SECONDARY DIAGNOSES:
        
        MANDATORY USAGE RULES:
        - Use for: Secondary diagnoses, comorbidities, additional conditions
        - Composite key: ("N_AIH", "ordem_diagnostico")
        - "codigo_cid_secundario" = Secondary ICD-10 code
        -  WARNING: This table is currently EMPTY (0 records)
        
        POSTGRESQL COLUMN QUOTING:
        - "N_AIH", "codigo_cid_secundario", "ordem_diagnostico"
        
        CRITICAL NOTES:
        - Table exists but contains no data
        - Do not use for actual secondary diagnosis analysis
        - Use internacoes."DIAG_SECUN" instead for secondary diagnoses
        
        EXACT QUERY EXAMPLES:
        -- Check if secondary diagnoses recorded (will return 0)
        SELECT COUNT(*) FROM diagnosticos_secundarios;
        
        -- For actual secondary diagnoses, use internacoes table
        SELECT COUNT(*) FROM internacoes 
        WHERE "DIAG_SECUN" IS NOT NULL AND "DIAG_SECUN" != '';
"""
}


# Base PostgreSQL template for SQL generation
BASE_SQL_TEMPLATE = """You are a PostgreSQL expert assistant for Brazilian healthcare (SIH-RS) data analysis.

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
    Builds dynamic prompt based on selected tables for PostgreSQL SIH-RS database
    
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
    
    # NOTE: Multi-table JOIN rules are handled by build_multi_table_prompt() only
    # Removing the multi-table logic here prevents duplication when build_multi_table_prompt() 
    # calls this function and then adds MULTI_TABLE_RULES separately
    
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


# Multi-table JOIN rules for PostgreSQL
MULTI_TABLE_RULES = """
MULTI-TABLE POSTGRESQL JOIN RULES:

CRITICAL JOIN PATTERNS:
- internacoes ↔ hospital: internacoes."CNES" = hospital."CNES"
- internacoes ↔ cid10: internacoes."DIAG_PRINC" = cid10."CID"
- internacoes ↔ mortes: internacoes."N_AIH" = mortes."N_AIH"
- internacoes ↔ uti_detalhes: internacoes."N_AIH" = uti_detalhes."N_AIH"
- municipios ↔ dado_ibge: municipios."codigo_ibge" = dado_ibge."codigo_municipio_completo"
- internacoes ↔ municipios: internacoes."MUNIC_RES" = municipios.codigo_6d


JOIN BEST PRACTICES:
- Always use table aliases for clarity (e.g., i.\"SEXO\", h.\"NATUREZA\")
- Use INNER JOIN for exact matches, LEFT JOIN to include null records
- Filter before joining when possible for better performance
- Always quote column names with double quotes in PostgreSQL

MULTI-TABLE EXAMPLES:

-- Deaths with disease descriptions
SELECT c."CD_DESCRICAO", COUNT(*) as deaths 
FROM mortes m 
INNER JOIN cid10 c ON m."CID_MORTE" = c."CID" 
WHERE m."CID_MORTE" IS NOT NULL
GROUP BY c."CD_DESCRICAO" 
ORDER BY deaths DESC;

-- Hospital mortality analysis
SELECT h."NATUREZA", 
       COUNT(DISTINCT i."N_AIH") as total_admissions,
       COUNT(DISTINCT m."N_AIH") as deaths,
       ROUND(COUNT(DISTINCT m."N_AIH")::numeric / COUNT(DISTINCT i."N_AIH") * 100, 2) as mortality_rate
FROM internacoes i
LEFT JOIN mortes m ON i."N_AIH" = m."N_AIH"
JOIN hospital h ON i."CNES" = h."CNES"
WHERE h."NATUREZA" IS NOT NULL
GROUP BY h."NATUREZA";

-- Municipality health statistics
SELECT mu."estado", d."nome_municipio", d."populacao",
       COUNT(i."N_AIH") as admissions
FROM internacoes i
JOIN municipios mu ON i."MUNIC_RES" = mu."codigo_6d"
JOIN dado_ibge d ON mu."codigo_ibge" = d."codigo_municipio_completo"
WHERE d."populacao" > 100
GROUP BY mu."estado", d."nome_municipio", d."populacao"
ORDER BY admissions DESC;

-- Most common diagnoses with descriptions (seasonal filter - winter months 6,7,8)
SELECT c."CID", c."CD_DESCRICAO", COUNT(*) AS total
FROM internacoes i
JOIN cid10 c ON i."DIAG_PRINC" = c."CID"
WHERE EXTRACT(MONTH FROM i."DT_INTER") IN (6,7,8)
GROUP BY c."CID", c."CD_DESCRICAO"
ORDER BY total DESC
LIMIT 3;
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
    
    # If multiple tables, add JOIN rules
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
        "populated_tables": 13,  # Tables with data
        "empty_tables": 2,       # infehosp, diagnosticos_secundarios
        "reference_tables": 5,   # cid10, hospital, municipios, dado_ibge, procedimentos
        "transaction_tables": 10 # internacoes, mortes, uti_detalhes, etc.
    }
