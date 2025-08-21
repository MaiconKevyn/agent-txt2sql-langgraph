"""
Table-Specific Prompt Templates
Sistema de templates de prompt específicos por tabela usando ChatPromptTemplate do LangChain

Este módulo implementa prompt templates dinâmicos que são aplicados baseado nas tabelas
selecionadas durante o processo de geração de SQL.
"""

from typing import List, Dict, Optional


# Templates específicos por tabela
TABLE_TEMPLATES = {
    "sus_data": """
        SUS DATA SPECIFIC RULES - CRITICAL FOR ACCURACY:
        
        MANDATORY VALUE MAPPINGS (NEVER MAKE MISTAKES):
        - For questions about MEN/HOMENS/MASCULINO: ALWAYS use SEXO = 1
        - For questions about WOMEN/MULHERES/FEMININO: ALWAYS use SEXO = 3  
        - For questions about DEATHS/MORTES/ÓBITOS: ALWAYS use MORTE = 1
        - For questions about ALIVE/VIVOS: ALWAYS use MORTE = 0
        - For questions about CITIES/CIDADES: ALWAYS use CIDADE_RESIDENCIA_PACIENTE
        
        CRITICAL NOTES:
        - SEXO values: 1=Masculino, 3=Feminino (NOT 2!)
        - MORTE values: 1=Óbito, 0=Vivo
        - Use CIDADE_RESIDENCIA_PACIENTE for city analysis (not MUNIC_RES)
        - DIAG_PRINC contains CID-10 diagnostic codes
        
        EXACT QUERY EXAMPLES FOR SUS_DATA:
        - "Quantos homens morreram?" 
          → SELECT COUNT(*) FROM sus_data WHERE SEXO = 1 AND MORTE = 1;
        
        - "Qual cidade com mais mortes de homens?"
          → SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) FROM sus_data 
             WHERE SEXO = 1 AND MORTE = 1 
             GROUP BY CIDADE_RESIDENCIA_PACIENTE 
             ORDER BY COUNT(*) DESC LIMIT 1;
        
        - "Mulheres por diagnóstico"
          → SELECT DIAG_PRINC, COUNT(*) FROM sus_data 
             WHERE SEXO = 3 
             GROUP BY DIAG_PRINC;
        
        - "Pacientes vivos por idade"
          → SELECT IDADE, COUNT(*) FROM sus_data 
             WHERE MORTE = 0 
             GROUP BY IDADE ORDER BY IDADE;
        
        - "Total de atendimentos por cidade"
          → SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) FROM sus_data 
             GROUP BY CIDADE_RESIDENCIA_PACIENTE 
             ORDER BY COUNT(*) DESC;
""",

    "cid_detalhado": """
        CID_DETALHADO SPECIFIC RULES - ICD-10 DIAGNOSTIC CODES:
        
        MANDATORY SEARCH PATTERNS:
        - For CID codes: Use LIKE pattern matching with 'codigo' column
        - For disease descriptions: Search in 'descricao' column  
        - For disease categories: Use codigo prefix patterns (e.g., 'J%' for respiratory)
        
        CRITICAL NOTES:
        - CID codes follow ICD-10 format (e.g., J44.0, I10.9, R06.2)
        - Use UPPER() for case-insensitive searches
        - Use % wildcards for partial matches
        - Column 'codigo' contains the CID code, 'descricao' contains description
        
        EXACT QUERY EXAMPLES FOR CID_DETALHADO:
        - "O que significa CID J44.0?"
          → SELECT codigo, descricao FROM cid_detalhado WHERE codigo = 'J44.0';
        
        - "CID para pneumonia"  
          → SELECT codigo, descricao FROM cid_detalhado 
             WHERE UPPER(descricao) LIKE '%PNEUMONIA%';
        
        - "Todos os CIDs respiratórios (categoria J)"
          → SELECT codigo, descricao FROM cid_detalhado 
             WHERE codigo LIKE 'J%' 
             ORDER BY codigo;
        
        - "CIDs que começam com I10"
          → SELECT codigo, descricao FROM cid_detalhado 
             WHERE codigo LIKE 'I10%';
        
        - "Buscar doença por palavra-chave"
          → SELECT codigo, descricao FROM cid_detalhado 
             WHERE UPPER(descricao) LIKE '%[KEYWORD]%'
             LIMIT 10;
"""
}


# Templates base para diferentes tipos de queries
BASE_SQL_TEMPLATE = """You are a SQL expert assistant for Brazilian healthcare (SUS) data analysis.

                    CORE INSTRUCTIONS:
                        1. Generate syntactically correct SQLite queries
                        2. Use proper table and column names from the schema
                        3. Handle Portuguese language questions appropriately  
                        4. Return only the SQL query, no explanation
                        5. Use appropriate WHERE clauses for filtering
                        6. Include LIMIT clauses when appropriate (default LIMIT 100)
                        7. Use proper JOINs when querying multiple tables
                    
                    SCHEMA CONTEXT:
                    {schema_context}
                    
                    {table_specific_rules}
                    
                    USER QUERY: {user_query}
                    
                    Generate the SQL query:"""


def build_table_specific_prompt(selected_tables: List[str]) -> str:
    """
    Constrói prompt específico baseado nas tabelas selecionadas
    
    Args:
        selected_tables: Lista de nomes das tabelas selecionadas
        
    Returns:
        String com regras específicas das tabelas selecionadas
    """
    if not selected_tables:
        return "No specific table rules available."
    
    rules = []
    rules.append("📋 TABLE-SPECIFIC RULES AND EXAMPLES:")
    
    for table in selected_tables:
        if table in TABLE_TEMPLATES:
            rules.append(f"\n{TABLE_TEMPLATES[table]}")
        else:
            # Template genérico para tabelas não mapeadas
            rules.append(f"""
            {table.upper()} - GENERAL RULES:
            - Use proper column names from schema
            - Apply appropriate WHERE conditions
            - Use LIMIT for large result sets
            - Consider performance implications
            """)
    
    return "\n".join(rules)


def get_table_template(table_name: str) -> Optional[str]:
    """
    Obtém template específico para uma tabela
    
    Args:
        table_name: Nome da tabela
        
    Returns:
        Template da tabela ou None se não existir
    """
    return TABLE_TEMPLATES.get(table_name)


def get_available_templates() -> List[str]:
    """
    Retorna lista de tabelas com templates disponíveis
    
    Returns:
        Lista de nomes de tabelas com templates
    """
    return list(TABLE_TEMPLATES.keys())


def validate_template_coverage(tables: List[str]) -> Dict[str, bool]:
    """
    Valida se as tabelas têm templates disponíveis
    
    Args:
        tables: Lista de nomes de tabelas
        
    Returns:
        Dicionário mapeando tabela -> tem_template
    """
    return {table: table in TABLE_TEMPLATES for table in tables}


# Template para queries que envolvem múltiplas tabelas
MULTI_TABLE_RULES = """
    MULTI-TABLE QUERY RULES:
    - When joining sus_data with cid_detalhado, use: sus_data.DIAG_PRINC = cid_detalhado.codigo
    - Always use table aliases for clarity (e.g., s.SEXO, c.descricao)
    - Consider performance: filter before joining when possible
    - Use INNER JOIN for exact matches, LEFT JOIN to include records without matches
    
    MULTI-TABLE EXAMPLES:
    - "Mortes por descrição de doença"
      → SELECT c.descricao, COUNT(*) as mortes 
         FROM sus_data s 
         INNER JOIN cid_detalhado c ON s.DIAG_PRINC = c.codigo 
         WHERE s.MORTE = 1 
         GROUP BY c.descricao 
         ORDER BY mortes DESC;
"""


def build_multi_table_prompt(selected_tables: List[str]) -> str:
    """
    Constrói prompt para queries envolvendo múltiplas tabelas
    
    Args:
        selected_tables: Lista de tabelas selecionadas
        
    Returns:
        Prompt com regras para múltiplas tabelas
    """
    if len(selected_tables) <= 1:
        return build_table_specific_prompt(selected_tables)
    
    # Se múltiplas tabelas, adicionar regras de JOIN
    single_table_rules = build_table_specific_prompt(selected_tables)
    
    return f"""
{single_table_rules}

{MULTI_TABLE_RULES}
"""


# Configuração do sistema de templates
TEMPLATE_CONFIG = {
    "default_template": BASE_SQL_TEMPLATE,
    "include_examples": True,
    "include_mappings": True,
    "max_examples_per_table": 5,
    "enable_multi_table_rules": True
}