"""
Table Descriptions Configuration - VERSÃO CONCISA E OTIMIZADA
Descrições simplificadas para máxima performance (reduzido 80%+ de tokens)
"""

TABLE_DESCRIPTIONS = {
    "sus_data": {
        "title": "Dados SUS",
        "description": "Atendimentos SUS - pacientes, mortes, cidades",
        "purpose": "Estatísticas de pacientes e mortalidade",
        "use_cases": ["Análises de pacientes/mortes por cidade/sexo/idade"],
        "key_columns": ["SEXO", "MORTE", "CIDADE_RESIDENCIA_PACIENTE"],
        "value_mappings": {
            "SEXO": "1=Masculino, 3=Feminino",
            "MORTE": "1=Óbito, 0=Vivo"
        },
        "critical_notes": ["SEXO=1(homem) 3(mulher), MORTE=1(óbito)"]
    },
    
    "cid_detalhado": {
        "title": "📚 Códigos CID",
        "description": "Códigos CID-10 e descrições de doenças",
        "purpose": "Buscar códigos e descrições específicas",
        "use_cases": ["Lookup de códigos CID ou descrições"],
        "key_columns": ["codigo", "descricao"],
        "value_mappings": {"codigo": "Formato CID (ex: J44.0)"},
        "critical_notes": ["Para códigos específicos"]
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

# Guia SUPER CONCISO de seleção (apenas 2 tabelas)
SELECTION_GUIDES = {
    "concise_guide": """
🎯 pacientes/mortes/estatísticas→sus_data | códigos CID/doenças→cid_detalhado
⚠️  SEXO=1(homem) 3(mulher), MORTE=1(óbito), use CIDADE_RESIDENCIA_PACIENTE
"""
}