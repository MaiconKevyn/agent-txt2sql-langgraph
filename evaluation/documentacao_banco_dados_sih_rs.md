# Documentação da Estrutura do Banco de Dados SIH-RS

## Visão Geral
Este documento fornece uma análise abrangente da estrutura do banco de dados PostgreSQL para o SIH-RS (Sistema de Informações Hospitalares - Rio Grande do Sul). O banco contém 15 tabelas com mais de 11 milhões de registros de internações hospitalares e dados relacionados.

**Conexão do Banco:** postgresql://postgres:1234@localhost:5432/sih_rs

---

## Resumo do Banco de Dados
- **Total de Tabelas:** 15
- **Total de Registros:** ~11,02 milhões de internações hospitalares principais
- **Entidade Principal:** Registros de internações (tabela internacoes)
- **Abrangência Geográfica:** Municípios brasileiros (principalmente Rio Grande do Sul)
- **Domínio dos Dados:** Sistema de informações de internações hospitalares

---

## Estrutura e Análise das Tabelas

### 1. INTERNACOES (Tabela Principal)
**Finalidade:** Tabela central contendo informações detalhadas sobre internações hospitalares

**Estrutura:**
- **Linhas:** 11.022.199 registros
- **Colunas:** 26 colunas
- **Chave Primária:** N_AIH (Número da autorização de internação hospitalar)
- **Chaves Estrangeiras:** 5 relacionamentos

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Número único da autorização de internação hospitalar
- `CNES` (text): Código do estabelecimento de saúde → Referencia hospital.CNES
- `DT_INTER` (timestamp): Data da internação
- `DT_SAIDA` (timestamp): Data da alta
- `QT_DIARIAS` (integer): Quantidade de diárias
- `PROC_REA` (text): Código do procedimento realizado
- `VAL_SH` (double precision): Valor dos serviços hospitalares
- `VAL_SP` (double precision): Valor dos serviços profissionais
- `VAL_TOT` (double precision): Valor total
- `DIAS_PERM` (integer): Dias de permanência
- `COMPLEX` (text): Nível de complexidade
- `DIAG_PRINC` (text): Diagnóstico principal → Referencia cid10.CID
- `DIAG_SECUN` (text): Diagnóstico secundário → Referencia cid10.CID
- `CID_NOTIF` (text): Código CID de notificação → Referencia cid10.CID
- `CID_ASSO` (text): Código CID associado → Referencia cid10.CID
- `MUNIC_MOV` (text): Município de movimentação
- `MUNIC_RES` (text): Município de residência
- `NASC` (timestamp): Data de nascimento
- `SEXO` (integer): Sexo (codificado)
- `IDADE` (double precision): Idade
- `NACIONAL` (text): Código da nacionalidade
- `NUM_FILHOS` (integer): Número de filhos
- `RACA_COR` (text): Código de raça/cor

**Dados de Exemplo:**
- AIH: 4316103450186, Data: 2016-07-22, Procedimento: 0404010032, Diagnóstico: J350
- AIH: 4310100786662, Data: 2010-01-24, Procedimento: 0303100044, Diagnóstico: O141

---

### 2. MORTES (Óbitos)
**Finalidade:** Registra óbitos ocorridos durante as internações hospitalares

**Estrutura:**
- **Linhas:** 569.405 registros
- **Colunas:** 2 colunas
- **Chave Primária:** N_AIH
- **Chaves Estrangeiras:** 1 relacionamento com internacoes

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Número da autorização de internação → Referencia internacoes.N_AIH
- `CID_MORTE` (text): Código CID da causa da morte

**Dados de Exemplo:**
- Registros de morte mostram códigos CID como "G459" ou "0" (não especificado)

---

### 3. PROCEDIMENTOS (Procedimentos)
**Finalidade:** Tabela mestre para códigos e descrições de procedimentos médicos

**Estrutura:**
- **Linhas:** 5.394 procedimentos
- **Colunas:** 2 colunas
- **Chave Primária:** PROC_REA
- **Chaves Estrangeiras:** Nenhuma (tabela de referência)

**Principais Colunas:**
- `PROC_REA` (text, NOT NULL): Código do procedimento
- `NOME_PROC` (text): Descrição do procedimento

**Dados de Exemplo:**
- "101010010": "ATIVIDADE EDUCATIVA / ORIENTACAO EM GRUPO NA ATENCAO PRIMARIA"
- "101010028": "ATIVIDADE EDUCATIVA / ORIENTACAO EM GRUPO NA ATENCAO ESPECIALIZADA"

---

### 4. CID10 (Classificação Internacional de Doenças)
**Finalidade:** Tabela mestre para códigos CID-10 e descrições de doenças

**Estrutura:**
- **Linhas:** 14.254 códigos de doenças
- **Colunas:** 2 colunas
- **Chave Primária:** CID
- **Chaves Estrangeiras:** Nenhuma (tabela de referência)

**Principais Colunas:**
- `CID` (text, NOT NULL): Código CID-10 da doença
- `CD_DESCRICAO` (text): Descrição da doença

**Dados de Exemplo:**
- "P545": "Hemorragia cutanea neonatal"
- "Y085": "Areas de comercio e de servicos"
- "W434": "Rua e estrada"

---

### 5. HOSPITAL
**Finalidade:** Tabela mestre com informações dos hospitais

**Estrutura:**
- **Linhas:** 366 hospitais
- **Colunas:** 4 colunas
- **Chave Primária:** CNES
- **Chaves Estrangeiras:** Nenhuma (tabela de referência)

**Principais Colunas:**
- `CNES` (text, NOT NULL): Código do Cadastro Nacional de Estabelecimentos de Saúde
- `NATUREZA` (text): Natureza/tipo do hospital
- `GESTAO` (text): Tipo de gestão
- `NAT_JUR` (text): Natureza jurídica

**Dados de Exemplo:**
- Códigos CNES como "0104523", "0181927", "2223538"

---

### 6. MUNICIPIOS (Municípios)
**Finalidade:** Tabela mestre para municípios brasileiros

**Estrutura:**
- **Linhas:** 5.570 municípios
- **Colunas:** 6 colunas
- **Chave Primária:** codigo_6d
- **Chaves Estrangeiras:** Nenhuma (tabela de referência)

**Principais Colunas:**
- `codigo_6d` (text, NOT NULL): Código do município com 6 dígitos
- `codigo_ibge` (text, UNIQUE): Código IBGE do município
- `nome` (text): Nome do município
- `latitude` (double precision): Latitude geográfica
- `longitude` (double precision): Longitude geográfica
- `estado` (text): Sigla do estado

**Dados de Exemplo:**
- Abadia de Goiás (GO): -16.7573, -49.4412
- Abadia dos Dourados (MG): -18.4831, -47.3916

---

### 7. DADO_IBGE (Dados Socioeconômicos IBGE)
**Finalidade:** Indicadores socioeconômicos abrangentes para municípios

**Estrutura:**
- **Linhas:** 5.570 municípios
- **Colunas:** 22 indicadores
- **Chave Primária:** codigo_municipio_completo
- **Chaves Estrangeiras:** 1 relacionamento com municipios

**Principais Colunas:**
- `codigo_municipio_completo` (text, NOT NULL): Código completo do município → Referencia municipios.codigo_ibge
- `nome_municipio` (text): Nome do município
- `populacao` (double precision): População (em milhares)
- `densidade_demografica` (double precision): Densidade demográfica
- `salario_medio` (double precision): Salário médio
- `pessoal_ocupado` (double precision): Percentual de pessoas ocupadas
- `ideb_anos_iniciais_ensino_fundamental` (double precision): IDEB anos iniciais
- `ideb_anos_finais_ensino_fundamental` (double precision): IDEB anos finais
- `receita_bruta` (double precision): Receita bruta
- `mortalidade_infantil` (double precision): Taxa de mortalidade infantil
- `area_cidade` (double precision): Área da cidade

---

### 8. UTI_DETALHES (Detalhes da UTI)
**Finalidade:** Informações específicas sobre permanência em UTI durante internações

**Estrutura:**
- **Linhas:** 81.287 registros de UTI
- **Colunas:** 5 colunas
- **Chave Primária:** N_AIH
- **Chaves Estrangeiras:** 1 relacionamento com internacoes

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Autorização de internação → Referencia internacoes.N_AIH
- `UTI_MES_TO` (integer): Total de dias de UTI por mês
- `MARCA_UTI` (text): Marcador/tipo de UTI
- `UTI_INT_TO` (integer): Total de dias de UTI intermediária
- `VAL_UTI` (double precision): Valor total da UTI

**Dados de Exemplo:**
- Valores de UTI variando de aproximadamente 21.000 a 27.000 unidades monetárias

---

### 9. CONDICOES_ESPECIFICAS (Condições Especiais)
**Finalidade:** Registra condições médicas especiais durante a internação

**Estrutura:**
- **Linhas:** 1.118.626 registros
- **Colunas:** 2 colunas
- **Chave Primária:** N_AIH
- **Chaves Estrangeiras:** 1 relacionamento com internacoes

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Autorização de internação → Referencia internacoes.N_AIH
- `IND_VDRL` (text): Indicador VDRL (teste de sífilis)

---

### 10. OBSTETRICOS (Informações Obstétricas)
**Finalidade:** Informações específicas para casos de maternidade

**Estrutura:**
- **Linhas:** 376.202 registros
- **Colunas:** 2 colunas
- **Chave Primária:** N_AIH
- **Chaves Estrangeiras:** 1 relacionamento com internacoes

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Autorização de internação → Referencia internacoes.N_AIH
- `INSC_PN` (text): Número de inscrição do pré-natal

---

### 11. INSTRUCAO (Nível de Instrução)
**Finalidade:** Informações sobre nível educacional dos pacientes

**Estrutura:**
- **Linhas:** 68.389 registros
- **Colunas:** 2 colunas
- **Chave Primária:** N_AIH
- **Chaves Estrangeiras:** 1 relacionamento com internacoes

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Autorização de internação → Referencia internacoes.N_AIH
- `INSTRU` (text): Código do nível de instrução

---

### 12. VINCPREV (Vínculo Previdenciário)
**Finalidade:** Informações sobre vinculação com sistema previdenciário/pensões

**Estrutura:**
- **Linhas:** 6.461 registros
- **Colunas:** 2 colunas
- **Chave Primária:** N_AIH
- **Chaves Estrangeiras:** 1 relacionamento com internacoes

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Autorização de internação → Referencia internacoes.N_AIH
- `VINCPREV` (text): Indicador de vínculo previdenciário

---

### 13. CBOR (Classificação Profissional)
**Finalidade:** Classificação Brasileira de Ocupações para profissionais

**Estrutura:**
- **Linhas:** 6.461 registros
- **Colunas:** 2 colunas
- **Chave Primária:** N_AIH
- **Chaves Estrangeiras:** 1 relacionamento com internacoes

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Autorização de internação → Referencia internacoes.N_AIH
- `CBOR` (text): Código de classificação profissional

**Dados de Exemplo:**
- Códigos como "225125", "225250" (categorias profissionais médicas)

---

### 14. INFEHOSP (Infecções Hospitalares)
**Finalidade:** Informações sobre infecções adquiridas no hospital

**Estrutura:**
- **Linhas:** 0 registros (tabela vazia)
- **Colunas:** 2 colunas
- **Chave Primária:** N_AIH
- **Chaves Estrangeiras:** 1 relacionamento com internacoes

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Autorização de internação → Referencia internacoes.N_AIH
- `INFEHOSP` (text): Indicador de infecção hospitalar

---

### 15. DIAGNOSTICOS_SECUNDARIOS (Diagnósticos Secundários)
**Finalidade:** Diagnósticos secundários adicionais para internações

**Estrutura:**
- **Linhas:** 0 registros (tabela vazia)
- **Colunas:** 3 colunas
- **Chave Primária:** N_AIH, ordem_diagnostico
- **Chaves Estrangeiras:** Nenhuma

**Principais Colunas:**
- `N_AIH` (text, NOT NULL): Autorização de internação
- `codigo_cid_secundario` (text): Código CID secundário
- `ordem_diagnostico` (text, NOT NULL): Ordem do diagnóstico

---

## Relacionamentos do Banco de Dados

### Relacionamentos Principais
1. **internacoes** (N_AIH) ← **mortes** (N_AIH)
2. **internacoes** (N_AIH) ← **uti_detalhes** (N_AIH)
3. **internacoes** (N_AIH) ← **condicoes_especificas** (N_AIH)
4. **internacoes** (N_AIH) ← **obstetricos** (N_AIH)
5. **internacoes** (N_AIH) ← **instrucao** (N_AIH)
6. **internacoes** (N_AIH) ← **vincprev** (N_AIH)
7. **internacoes** (N_AIH) ← **cbor** (N_AIH)
8. **internacoes** (N_AIH) ← **infehosp** (N_AIH)

### Relacionamentos de Referência
1. **hospital** (CNES) ← **internacoes** (CNES)
2. **cid10** (CID) ← **internacoes** (DIAG_PRINC, DIAG_SECUN, CID_NOTIF, CID_ASSO)
3. **municipios** (codigo_ibge) ← **dado_ibge** (codigo_municipio_completo)

## Observações sobre Qualidade dos Dados

### Tabelas Populadas (com dados)
- **internacoes**: 11.022.199 registros (tabela principal)
- **mortes**: 569.405 registros (5,2% das internações resultaram em óbito)
- **condicoes_especificas**: 1.118.626 registros (10,1% das internações)
- **obstetricos**: 376.202 registros (3,4% das internações)
- **uti_detalhes**: 81.287 registros (0,7% das internações necessitaram de UTI)
- **instrucao**: 68.389 registros (0,6% têm dados de nível educacional)
- **vincprev**: 6.461 registros (0,06% têm dados previdenciários)
- **cbor**: 6.461 registros (0,06% têm classificação profissional)

### Tabelas Vazias
- **infehosp**: 0 registros (rastreamento de infecções hospitalares não implementado)
- **diagnosticos_secundarios**: 0 registros (diagnósticos secundários não implementados)

### Tabelas de Referência
- **procedimentos**: 5.394 códigos de procedimentos
- **cid10**: 14.254 códigos de doenças
- **hospital**: 366 hospitais
- **municipios**: 5.570 municípios
- **dado_ibge**: 5.570 dados socioeconômicos municipais

## Padrões de Uso e Insights

1. **Entidade Central**: A tabela `internacoes` é a tabela de fatos central contendo todos os registros de internações
2. **Detalhes Opcionais**: A maioria das tabelas suplementares (UTI, obstétrico, etc.) contém detalhes opcionais para casos específicos
3. **Cobertura Geográfica**: Cobertura completa de municípios brasileiros (5.570 municípios)
4. **Período Temporal**: Dados abrangem de 2010 a 2016 baseado nas datas de exemplo
5. **Taxa de Mortalidade**: Aproximadamente 5,2% de taxa de mortalidade baseada nos registros de óbitos
6. **Uso de UTI**: Menos de 1% das internações necessitaram de cuidados intensivos
7. **Casos de Maternidade**: Cerca de 3,4% das internações são relacionadas à obstetrícia

## Observações Técnicas de Implementação

- Todos os campos de texto usam o tipo `text` do PostgreSQL (comprimento ilimitado)
- Timestamps armazenados como `timestamp without time zone`
- Valores numéricos usam tipos `double precision` ou `integer`
- Chaves primárias são de coluna única exceto para `diagnosticos_secundarios` (chave composta)
- Restrições de chave estrangeira garantem integridade referencial
- Índices únicos existem nas chaves primárias e algumas colunas de referência

Esta estrutura de banco de dados suporta análises abrangentes de padrões de internação, resultados de saúde, disparidades geográficas em saúde e correlações socioeconômicas de saúde no Brasil.