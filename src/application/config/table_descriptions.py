TABLE_DESCRIPTIONS = {
    "internacoes": {
        "title": "Internações SIH-RS (MAIN TABLE)",
        "description": "Tabela que registra as internações hospitalares autorizadas pelo SIH-SUS. Cada registro corresponde a uma AIH (Autorização de Internação Hospitalar), contendo informações sobre o hospital de atendimento, características do paciente (idade, sexo, nascimento, residência), dados clínicos (datas de internação e saída, diagnósticos principais), duração da permanência, custos da internação (valores pagos pelo SUS) e variáveis sociodemográficas. Para mortes use 'mortes', para procedimentos use 'procedimentos'.",
        "purpose": "Análises de internações, pacientes, hospitais e características demográficas",
        "use_cases": ["Estatísticas de internações por cidade/sexo/idade/hospital", "Custos de internação", "Tempo de permanência", "Dados demográficos de pacientes"],
        "key_columns": ["\"CNES\"", "\"N_AIH\"", "\"SEXO\"", "\"IDADE\"", "\"MUNIC_RES\"", "\"DIAG_PRINC\"", "\"VAL_TOT\"", "\"QT_DIARIAS\"", "\"DIAS_PERM\""],
        "value_mappings": {
            "\"SEXO\"": "1=Masculino, 3=Feminino (NEVER use 2!)",
            "\"IDADE\"": "Idade em anos completos",
            "\"MUNIC_RES\"": "Código município residência (6 dígitos IBGE)",
            "\"DIAG_PRINC\"": "Código CID-10 diagnóstico principal",
            "\"CNES\"": "Código Nacional Estabelecimento Saúde",
            "\"COBRANCA\"": "12=Óbito durante internação"
        },
        "critical_notes": ["\"SEXO\"=1(Masculino) 3(Feminino)", "\"N_AIH\" é chave primária", "NOT for death counts (use mortes table)", "NOT for procedure counts (use procedimentos table)", "JOIN com hospital via \"CNES\"", "JOIN com cid10 via \"DIAG_PRINC\""],
        "relationships": ["→ hospital(\"CNES\")", "→ cid10(\"DIAG_PRINC\")", "→ municipios(via \"MUNIC_RES\")", "← mortes(\"N_AIH\")", "← diagnosticos_secundarios(\"N_AIH\")"]
    },
    
    "cid10": {
        "title": "Códigos CID-10 (REFERENCE TABLE ONLY)",
        "description": "TABELA DE REFERÊNCIA APENAS - Use SOMENTE para JOINs e lookups de descrições. NUNCA para contagem ou como fonte primária de dados. Contém os significados dos códigos da Classificação Internacional de Doenças (CID-10).",
        "purpose": "REFERENCE ONLY - JOIN operations and lookups, NEVER for counting",
        "use_cases": ["JOIN with internacoes for disease descriptions", "Lookup disease names", "NEVER use for counting diseases or procedures"],
        "key_columns": ["\"CID\"", "\"CD_DESCRICAO\""],
        "value_mappings": {
            "\"CID\"": "Código CID-10 (ex: F190, J44, I25)",
            "\"CD_DESCRICAO\"": "Descrição completa da doença/condição"
        },
        "critical_notes": [
            "LOOKUP TABLE ONLY - Never use for counting!",
            "\"CID\" é chave primária",
            "Use for JOINs: internacoes.\"DIAG_PRINC\" = cid10.\"CID\" e mortes.\"CID_MORTE\" = cid10.\"CID\"",
            "Use descrições para localizar grupos clínicos (ex.: cardiovascular) e aplicar os códigos resultantes em mortes/internacoes",
            "Do NOT use for: 'Quantos códigos', 'Quantas doenças'"
        ],
        "relationships": ["← internacoes(\"DIAG_PRINC\")", "← mortes(\"CID_MORTE\")"]
    },
    
    "municipios": {
        "title": "Municípios Brasileiros",
        "description": "Tabela de referência com os municípios do Brasil. Contém códigos oficiais, nome do município, estado (UF) e localização geográfica (latitude e longitude).",
        "purpose": "Análises geográficas e localização de pacientes e hospitais",
        "use_cases": ["Análises por região/estado/cidade", "Dados geográficos", "Mapeamento de internações"],
        "key_columns": ["\"codigo_6d\"", "\"codigo_ibge\"", "\"nome\"", "\"estado\"", "\"latitude\"", "\"longitude\""],
        "value_mappings": {
            "\"codigo_ibge\"": "Código IBGE 7 dígitos",
            "\"codigo_6d\"": "Código 6 dígitos (chave primária)",
            "\"estado\"": "Sigla do estado (RS, SP, RJ...)",
            "\"nome\"": "Nome completo do município"
        },
        "critical_notes": ["\"codigo_6d\" é chave primária", "Relaciona com internacoes.\"MUNIC_RES\" via código", "Contém lat/long para geo-análises"],
        "relationships": ["← internacoes(via códigos municipais)", "← dado_ibge(\"codigo_ibge\")"]
    },
    
    "hospital": {
        "title": "Estabelecimentos de Saúde",
        "description": "Tabela de referência dos estabelecimentos de saúde. Contém o código CNES, município onde o hospital está localizado, tipo de gestão e natureza jurídica.",
        "purpose": "Informações sobre hospitais e estabelecimentos de saúde",
        "use_cases": ["Análises por tipo de hospital", "Gestão pública vs privada", "Natureza jurídica dos estabelecimentos"],
        "key_columns": ["\"CNES\"", "\"NATUREZA\"", "\"GESTAO\"", "\"NAT_JUR\""],
        "value_mappings": {
            "\"CNES\"": "Código Nacional Estabelecimento Saúde",
            "\"NATUREZA\"": "00=Público, 50=Privado lucrativo, 60=Privado filantrópico, 61=Privado sem fins lucrativos",
            "\"GESTAO\"": "Tipo de gestão do estabelecimento",
            "\"NAT_JUR\"": "Natureza jurídica do estabelecimento"
        },
        "critical_notes": ["\"CNES\" é chave primária", "Relaciona com internacoes.\"CNES\"", "\"NATUREZA\" define público/privado"],
        "relationships": ["← internacoes(\"CNES\")"]
    },
    
    "mortes": {
        "title": "Óbitos Durante Internação (PRIMARY FOR DEATH STATISTICS)",
        "description": "TABELA PRIMÁRIA PARA ESTATÍSTICAS DE MORTE - Use esta tabela para contar mortes, óbitos e análises de mortalidade. Registra os óbitos ocorridos durante a internação com o número da AIH e código CID-10 da causa da morte.",
        "purpose": "PRIMARY TABLE FOR ALL DEATH COUNTS AND MORTALITY STATISTICS",
        "use_cases": ["Quantas mortes foram registradas", "Contagem de óbitos", "Estatísticas de mortalidade", "Óbitos por causa (CID_MORTE)", "Mortality analysis"],
        "key_columns": ["\"N_AIH\"", "\"CID_MORTE\""],
        "value_mappings": {
            "\"N_AIH\"": "Número da AIH (link com internacoes)",
            "\"CID_MORTE\"": "Código CID-10 da causa da morte"
        },
        "critical_notes": ["USE THIS TABLE FOR: 'Quantas mortes', 'óbitos registrados', 'mortality'", "\"N_AIH\" é chave primária", "569,405 death records available", "JOIN com internacoes via \"N_AIH\"", "JOIN com cid10 via \"CID_MORTE\" para causas específicas por descrição (evite depender apenas de prefixos de código)"],
        "relationships": ["→ internacoes(\"N_AIH\")", "→ cid10(\"CID_MORTE\")"]
    },
    
    "procedimentos": {
        "title": "Procedimentos Médicos (PRIMARY FOR PROCEDURE STATISTICS)",
        "description": "TABELA PRIMÁRIA PARA ESTATÍSTICAS DE PROCEDIMENTOS - Use esta tabela para contar procedimentos médicos diferentes. Contém códigos de procedimentos do SIH-SUS (SIGTAP) com suas descrições oficiais.",
        "purpose": "PRIMARY TABLE FOR PROCEDURE COUNTS AND STATISTICS",
        "use_cases": ["Quantos procedimentos diferentes foram realizados", "Contagem de procedimentos", "Tipos de procedimento", "Medical procedure statistics", "Procedure analysis"],
        "key_columns": ["\"PROC_REA\"", "\"NOME_PROC\""],
        "value_mappings": {
            "\"PROC_REA\"": "Código do procedimento realizado",
            "\"NOME_PROC\"": "Nome/descrição oficial do procedimento"
        },
        "critical_notes": ["USE THIS TABLE FOR: 'Quantos procedimentos', 'procedure counts'", "\"PROC_REA\" é chave primária (NOT CID codes!)", "5,394 different procedures available", "Different from CID10 - these are procedures, not diseases"],
        "relationships": ["internacoes(\"PROC_REA\")"]
    },
    
    "diagnosticos_secundarios": {
        "title": "Diagnósticos Secundários",
        "description": "Tabela que registra diagnósticos secundários associados às internações. Complementa o diagnóstico principal com condições adicionais do paciente.",
        "purpose": "Registro de diagnósticos complementares e comorbidades",
        "use_cases": ["Análise de comorbidades", "Diagnósticos secundários por internação", "Complexidade clínica"],
        "key_columns": ["\"N_AIH\"", "\"codigo_cid_secundario\"", "\"ordem_diagnostico\""],
        "value_mappings": {
            "\"N_AIH\"": "Número da AIH (link com internacoes)",
            "\"codigo_cid_secundario\"": "Código CID-10 do diagnóstico secundário",
            "\"ordem_diagnostico\"": "Ordem/sequência do diagnóstico secundário"
        },
        "critical_notes": ["Chave composta: (\"N_AIH\", \"ordem_diagnostico\")", "Multiple diagnósticos por internação"],
        "relationships": ["→ internacoes(\"N_AIH\")", "→ cid10(\"codigo_cid_secundario\")"]
    },
    
    "condicoes_especificas": {
        "title": "Condições Específicas (PRIMARY FOR SPECIAL CONDITIONS)",
        "description": "TABELA PRIMÁRIA PARA ESTATÍSTICAS DE CONDIÇÕES ESPECÍFICAS - Use para contar condições médicas especiais como VDRL. Registra apenas casos identificados como positivos.",
        "purpose": "PRIMARY TABLE FOR SPECIAL MEDICAL CONDITIONS STATISTICS",
        "use_cases": ["Quantos registros de condições específicas", "VDRL statistics", "Special conditions analysis", "Health indicators"],
        "key_columns": ["\"N_AIH\"", "\"IND_VDRL\""],
        "value_mappings": {
            "\"N_AIH\"": "Número da AIH (link com internacoes)",
            "\"IND_VDRL\"": "Indicador VDRL (teste para sífilis)"
        },
        "critical_notes": ["USE THIS TABLE FOR: 'condições específicas', 'VDRL' queries", "\"N_AIH\" é chave primária", "1,118,626 special condition records", "Only positive cases recorded"],
        "relationships": ["→ internacoes(\"N_AIH\")"]
    },
    
    "obstetricos": {
        "title": "Dados Obstétricos",
        "description": "Tabela preenchida apenas em internações obstétricas. Registra informações específicas de gestação e parto, como variáveis PN_* (pré-natal) e GESTRICO (gestação de risco).",
        "purpose": "Dados específicos de internações relacionadas à gravidez e parto",
        "use_cases": ["Análise de partos", "Dados de pré-natal", "Gestações de risco"],
        "key_columns": ["\"N_AIH\"", "\"INSC_PN\""],
        "value_mappings": {
            "\"N_AIH\"": "Número da AIH (link com internacoes)",
            "\"INSC_PN\"": "Inscrição/dados de pré-natal; quando preenchido (não nulo/não vazio) indica acompanhamento pré-natal"
        },
        "critical_notes": [
            "Para 'acompanhamento pré-natal' use SEMPRE obstetricos.\"INSC_PN\" (não procurar em DIAG_PRINC/DIAG_SECUN)",
            "Junte com internacoes (via \"N_AIH\") apenas se precisar de datas/sexo/hospital",
            "Use ILIKE/EXTRACT somente quando cruzar com textos/datas em outras tabelas"
        ],
        "critical_notes": ["\"N_AIH\" é chave primária", "Apenas internações obstétricas"],
        "relationships": ["→ internacoes(\"N_AIH\")"]
    },
    
    "instrucao": {
        "title": "Nível de Instrução",
        "description": "Tabela que registra informações de instrução/educação do paciente, preenchida apenas quando essa variável está disponível na AIH.",
        "purpose": "Dados educacionais e socioeconômicos dos pacientes",
        "use_cases": ["Análise por escolaridade", "Perfil educacional", "Indicadores socioeconômicos"],
        "key_columns": ["\"N_AIH\"", "\"INSTRU\""],
        "value_mappings": {
            "\"N_AIH\"": "Número da AIH (link com internacoes)",
            "\"INSTRU\"": "01=Analfabeto, 02=1°grau incompleto, 03=1°grau completo, 04=2°grau incompleto, 06=Superior"
        },
        "critical_notes": ["\"N_AIH\" é chave primária", "Dados quando disponíveis", "Códigos 01-06 por nível educacional"],
        "relationships": ["→ internacoes(\"N_AIH\")"]
    },
    
    "vincprev": {
        "title": "Vínculo Previdenciário (PRIMARY FOR SOCIAL SECURITY)",
        "description": "TABELA PRIMÁRIA PARA ESTATÍSTICAS DE VÍNCULO PREVIDENCIÁRIO - Use para contar tipos de vínculo previdenciário dos pacientes.",
        "purpose": "PRIMARY TABLE FOR SOCIAL SECURITY LINK STATISTICS",
        "use_cases": ["Quantos registros de vínculo previdenciário", "Tipos diferentes de vínculo previdenciário", "Social security analysis", "Previdencia statistics"],
        "key_columns": ["\"N_AIH\"", "\"VINCPREV\""],
        "value_mappings": {
            "\"N_AIH\"": "Número da AIH (link com internacoes)",
            "\"VINCPREV\"": "1=Empregado, 2=Doméstico, 3=Autônomo, 5=Aposentado, 6=Desempregado"
        },
        "critical_notes": ["USE THIS TABLE FOR: 'vínculo previdenciário', 'previdencia' queries", "\"N_AIH\" é chave primária", "Social security linkage data"],
        "relationships": ["→ internacoes(\"N_AIH\")"]
    },
    
    "cbor": {
        "title": "Ocupação do Paciente (PRIMARY FOR CBOR STATISTICS)",
        "description": "TABELA PRIMÁRIA PARA ESTATÍSTICAS DE OCUPAÇÃO CBOR - Use para contar registros de ocupação e análise de saúde ocupacional.",
        "purpose": "PRIMARY TABLE FOR OCCUPATION (CBOR) STATISTICS",
        "use_cases": ["Quantos registros CBOR existem", "CBOR statistics", "Occupational analysis", "Professional profile"],
        "key_columns": ["\"N_AIH\"", "\"CBOR\""],
        "value_mappings": {
            "\"N_AIH\"": "Número da AIH (link com internacoes)",
            "\"CBOR\"": "Código Brasileiro de Ocupação Reformulado"
        },
        "critical_notes": ["USE THIS TABLE FOR: 'CBOR', 'ocupação', 'occupational' queries", "\"N_AIH\" é chave primária", "6,461 occupation records available"],
        "relationships": ["→ internacoes(\"N_AIH\")"]
    },
    
    "infehosp": {
        "title": "Infecção Hospitalar",
        "description": "Tabela que registra casos de infecção hospitalar, indicando quando ocorreu infecção durante a internação.",
        "purpose": "Controle e análise de infecções hospitalares",
        "use_cases": ["Estatísticas de infecção hospitalar", "Controle de qualidade", "Indicadores de segurança"],
        "key_columns": ["\"N_AIH\"", "\"INFEHOSP\""],
        "value_mappings": {
            "\"N_AIH\"": "Número da AIH (link com internacoes)",
            "\"INFEHOSP\"": "Indicador de infecção hospitalar"
        },
        "critical_notes": ["\"N_AIH\" é chave primária", "Apenas casos com infecção"],
        "relationships": ["→ internacoes(\"N_AIH\")"]
    },
    
    "uti_detalhes": {
        "title": "Detalhes de UTI (PRIMARY FOR ICU COSTS/MARKERS)",
        "description": "TABELA PARA CUSTOS E MARCADORES DE UTI/ICU - Use esta tabela para estatísticas de UTI (presença, custo, marcação). NÃO usar para cálculos de dias de permanência em UTI.",
        "purpose": "UTI/ICU statistics focusing on costs and markers",
        "use_cases": ["Quantos registros de UTI existem", "Custos de UTI", "Análise de marcadores de UTI"],
        "key_columns": ["\"N_AIH\"", "\"MARCA_UTI\"", "\"VAL_UTI\""],
        "value_mappings": {
            "\"N_AIH\"": "Número da AIH (link com internacoes)",
            "\"VAL_UTI\"": "Valor pago pela UTI",
            "\"MARCA_UTI\"": "Marcador de UTI"
        },
        "critical_notes": [
            "NÃO usar para dias/permanência de UTI (use internacoes.\"QT_DIARIAS\")",
            "USE THIS TABLE FOR: 'UTI', 'ICU', custos e marcadores",
            "\"N_AIH\" é chave primária"
        ],
        "relationships": ["→ internacoes(\"N_AIH\")"]
    },
    
    "dado_ibge": {
        "title": "Dados IBGE (PRIMARY FOR MUNICIPALITY DATA)",
        "description": "TABELA PRIMÁRIA PARA DADOS DE MUNICÍPIOS - Use esta tabela para análises demográficas e econômicas de municípios. Contém dados do IBGE com população, economia, educação e indicadores sociais.",
        "purpose": "PRIMARY TABLE FOR MUNICIPALITY DEMOGRAPHIC AND ECONOMIC DATA",
        "use_cases": ["Quantos municípios têm população registrada", "Municipality population", "Economic indicators", "Demographic analysis", "IDEB scores"],
        "key_columns": ["\"uf\"", "\"nome_uf\"", "\"municipio\"", "\"codigo_municipio_completo\"", "\"nome_municipio\"", "\"populacao\"", "\"densidade_demografica\""],
        "value_mappings": {
            "\"codigo_municipio_completo\"": "Código completo do município (chave primária)",
            "\"uf\"": "Código da Unidade Federativa",
            "\"populacao\"": "População do município",
            "\"densidade_demografica\"": "Densidade demográfica (hab/km²)"
        },
        "critical_notes": ["USE THIS TABLE FOR: municipality population, economic data, IDEB", "\"codigo_municipio_completo\" é chave primária", "5,570 municipality records with full data", "Different from 'municipios' table"],
        "relationships": ["→ municipios(\"codigo_ibge\")"]
    }
}

# Configuração OTIMIZADA para PostgreSQL
TOOL_CONFIGURATION = {
    "include_samples": True,
    "include_mappings": True,
    "include_selection_guide": False,
    "max_use_cases_shown": 2,
    "max_sample_queries_shown": 0,
    "max_sample_length": 0,
    "concise_mode": False,  # Full mode for comprehensive descriptions
    "postgresql_mode": True  # Enable PostgreSQL-specific features
}

# Guias de seleção para PostgreSQL
SELECTION_GUIDES = {
    "concise_guide": """
POSTGRESQL TABLE SELECTION GUIDE:
• internacoes = Main table (patients, admissions, demographics)
• cid10 = Disease codes and descriptions (ICD-10) 
• hospital = Healthcare facilities (CNES codes)
• mortes = Deaths during hospitalization
• municipios = Brazilian cities (geographic data)
• Always use "COLUMN_NAME" (with double quotes) for PostgreSQL columns
• Key relationships: internacoes  hospital(CNES), cid10(DIAG_PRINC), municipios(MUNIC_RES)
""",
    
    "full_guide": """
COMPREHENSIVE POSTGRESQL TABLE RELATIONSHIPS:

MAIN DATA FLOW:
internacoes (main) → Links to all other tables via N_AIH or specific codes
├── hospital (via CNES) - Hospital information
├── cid10 (via DIAG_PRINC, CID_NOTIF) - Disease codes  
├── municipios (via MUNIC_RES) - Patient residence
├── mortes (via N_AIH) - Deaths during admission
├── procedimentos (via PROC_REA) - Medical procedures
└── [Detailed tables via N_AIH]: diagnosticos_secundarios, uti_detalhes, obstetricos, etc.

CRITICAL POSTGRESQL SYNTAX RULES:
- Column names MUST use double quotes: "SEXO", "IDADE", "CD_DESCRICAO"
- SEXO values: 1=Masculino, 3=Feminino (NEVER use 2!)
- For geographic queries: JOIN internacoes with municipios
- For disease descriptions: JOIN with cid10 table
- For hospital info: JOIN with hospital table via CNES
 - ICU permanence (tempo médio em UTI): compute via internacoes."QT_DIARIAS" (use > 0); do NOT use uti_detalhes for days
"""
}
