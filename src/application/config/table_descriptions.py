"""
Table Descriptions Configuration - VERSÃO CONCISA E OTIMIZADA
Descrições simplificadas para máxima performance (reduzido 80%+ de tokens)
"""

TABLE_DESCRIPTIONS = {
    "internacoes": {
        "title": "🏥 Internações SIH-RS",
        "description": "Dados completos de internações hospitalares do Rio Grande do Sul",
        "purpose": "Análises de internações, pacientes, hospitais e óbitos",
        "use_cases": ["Estatísticas de internações por cidade/sexo/idade/hospital", "Análises de mortalidade e procedimentos"],
        "key_columns": ["SEXO", "IDADE", "MUNIC_RES", "DIAG_PRINC", "CNES", "N_AIH"],
        "value_mappings": {
            "SEXO": "1=Masculino, 3=Feminino",
            "IDADE": "Idade em anos",
            "MUNIC_RES": "Código município residência (6 dígitos)",
            "DIAG_PRINC": "Código CID-10 diagnóstico principal"
        },
        "critical_notes": ["SEXO=1(M) 3(F)", "N_AIH é chave primária", "JOIN com mortes via N_AIH para óbitos"]
    },
    
    "cid10": {
        "title": "📚 Códigos CID-10",
        "description": "Códigos e descrições completas CID-10 para diagnósticos",
        "purpose": "Lookup de códigos de doenças e descrições",
        "use_cases": ["Buscar descrições de códigos CID", "Validar diagnósticos"],
        "key_columns": ["CID", "CD_DESCRICAO"],
        "value_mappings": {"CID": "Código CID-10 (ex: F190, S623)"},
        "critical_notes": ["Relaciona com internacoes.DIAG_PRINC"]
    },
    
    "municipios": {
        "title": "🌍 Municípios Brasileiros",
        "description": "Dados geográficos completos dos municípios brasileiros",
        "purpose": "Análises geográficas e localização",
        "use_cases": ["Análises por região/estado", "Dados geográficos"],
        "key_columns": ["codigo_ibge", "nome", "estado", "latitude", "longitude"],
        "value_mappings": {"codigo_ibge": "Código IBGE 7 dígitos"},
        "critical_notes": ["Relaciona com internacoes.MUNIC_RES via código"]
    }
}

# Configuração OTIMIZADA para performance
TOOL_CONFIGURATION = {
    "include_samples": True,        # 🔥 Removido: reduz ~40% dos tokens
    "include_mappings": True,        # ✅ Mantido: crítico para SUS
    "include_selection_guide": True, # ✅ Mantido: essencial
    "max_use_cases_shown": 1,        # 🔥 Reduzido: 1 caso apenas
    "max_sample_queries_shown": 0,   # 🔥 Removido: sem exemplos
    "max_sample_length": 0,          # 🔥 Removido: sem amostras
    "concise_mode": True             # 🔥 Modo conciso ativo
}

# Guia SUPER CONCISO de seleção (3 tabelas SIH-RS)
SELECTION_GUIDES = {
    "concise_guide": """
🎯 internações/pacientes/óbitos→internacoes | códigos CID/doenças→cid10 | cidades/geografia→municipios
⚠️  SEXO=1(M) 3(F), JOIN mortes via N_AIH para óbitos, use MUNIC_RES para cidades
"""
}