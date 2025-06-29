# Documentação das Tabelas do Banco SUS (sus_database.db)

## Visão Geral
O banco de dados `sus_database.db` é um sistema SQLite que armazena dados do Sistema Único de Saúde (SUS) integrado com informações da Classificação Internacional de Doenças (CID-10). O banco possui 58.655 registros de dados hospitalares e 2.045 categorias de doenças catalogadas.

## Tabelas Principais

### 1. sus_data
**Propósito**: Tabela principal contendo dados de internações hospitalares do SUS
**Registros**: 58.655

#### Estrutura:
- `DIAG_PRINC` (TEXT) - Código do diagnóstico principal (CID-10)
- `MUNIC_RES` (INTEGER) - Código do município de residência do paciente
- `MUNIC_MOV` (INTEGER) - Código do município de movimentação/atendimento
- `PROC_REA` (INTEGER) - Código do procedimento realizado
- `IDADE` (INTEGER) - Idade do paciente
- `SEXO` (INTEGER) - Sexo do paciente (1=Masculino, 3=Feminino)
- `CID_MORTE` (TEXT) - Código CID da causa de morte (se aplicável)
- `MORTE` (INTEGER) - Indicador de óbito (0=Não, 1=Sim)
- `CNES` (INTEGER) - Código Nacional de Estabelecimentos de Saúde
- `VAL_TOT` (REAL) - Valor total da internação
- `UTI_MES_TO` (INTEGER) - Total de dias em UTI
- `DT_INTER` (INTEGER) - Data de internação (formato YYYYMMDD)
- `DT_SAIDA` (INTEGER) - Data de saída (formato YYYYMMDD)
- `total_ocorrencias` (INTEGER) - Total de ocorrências similares
- `UF_RESIDENCIA_PACIENTE` (TEXT) - Unidade Federativa de residência
- `CIDADE_RESIDENCIA_PACIENTE` (TEXT) - Cidade de residência do paciente
- `LATI_CIDADE_RES` (REAL) - Latitude da cidade de residência
- `LONG_CIDADE_RES` (REAL) - Longitude da cidade de residência

**Utilização**: Análises epidemiológicas, estatísticas de saúde pública, geolocalização de casos, custos hospitalares e mortalidade.

### 2. cid_chapters
**Propósito**: Catálogo dos 22 capítulos da Classificação Internacional de Doenças (CID-10)
**Registros**: 22

#### Estrutura:
- `id` (INTEGER) - Identificador único do capítulo
- `chapter_number` (INTEGER) - Número do capítulo CID-10
- `start_code` (VARCHAR) - Código inicial do intervalo do capítulo
- `end_code` (VARCHAR) - Código final do intervalo do capítulo
- `description` (TEXT) - Descrição completa do capítulo
- `abbreviated_description` (TEXT) - Descrição abreviada
- `created_at` (TIMESTAMP) - Data de criação do registro

**Utilização**: Organização hierárquica das doenças, navegação por categorias médicas, relatórios por grupos de doenças.

### 3. cid_categories
**Propósito**: Catálogo detalhado de todas as categorias e subcategorias CID-10
**Registros**: 2.045

#### Estrutura:
- `id` (INTEGER) - Identificador único da categoria
- `code` (VARCHAR) - Código CID-10 (ex: A00, C168)
- `classification` (VARCHAR) - Classificação adicional
- `description` (TEXT) - Descrição completa da doença/condição
- `abbreviated_description` (TEXT) - Descrição abreviada para relatórios
- `reference` (TEXT) - Referências médicas adicionais
- `excluded` (TEXT) - Condições excluídas desta categoria
- `chapter_id` (INTEGER) - Referência ao capítulo pai (FK para cid_chapters)
- `keywords` (TEXT) - Palavras-chave para busca semântica (JSON)
- `created_at` (TIMESTAMP) - Data de criação do registro

**Utilização**: Busca e classificação de diagnósticos, mapeamento semântico, validação de códigos CID-10.

## Tabelas de Sistema

### 4. sqlite_sequence
**Propósito**: Tabela interna do SQLite para controle de sequências AUTO_INCREMENT
**Registros**: 2

Gerencia automaticamente os incrementos dos IDs das tabelas `cid_chapters` e `cid_categories`.

### 5. Tabelas FTS (Full Text Search)
- `cid_categories_fts` - Índice de busca textual
- `cid_categories_fts_data` - Dados do índice FTS
- `cid_categories_fts_idx` - Estrutura do índice
- `cid_categories_fts_docsize` - Tamanhos dos documentos
- `cid_categories_fts_config` - Configuração do FTS

**Propósito**: Permitem busca textual avançada nas descrições e palavras-chave dos códigos CID-10, melhorando a performance de consultas por texto livre.

## Relacionamentos
- `sus_data.DIAG_PRINC` → `cid_categories.code` (diagnóstico principal)
- `cid_categories.chapter_id` → `cid_chapters.id` (hierarquia CID-10)

## Casos de Uso
1. **Análises Epidemiológicas**: Cruzamento de dados geográficos com diagnósticos
2. **Gestão Hospitalar**: Controle de custos, ocupação de UTI, mortalidade
3. **Pesquisa Médica**: Estudos populacionais e distribuição de doenças
4. **Sistemas de BI**: Dashboards e relatórios gerenciais de saúde pública
5. **Busca Semântica**: Localização de códigos CID-10 por descrição textual