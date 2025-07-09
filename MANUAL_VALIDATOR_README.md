# Manual SQL Validator 🔍

Ferramenta para executar queries SQL manualmente no banco de dados e comparar com as respostas do agente TXT2SQL, permitindo validação manual e detecção de alucinações.

## ✨ Funcionalidades

- 🔧 **Execução manual de queries SQL** diretamente no banco
- 🤖 **Teste de perguntas** no agente TXT2SQL
- ⚖️ **Comparação automática** entre resultados manuais e do agente
- 📊 **Análise detalhada** de diferenças e problemas
- 💾 **Logs de validação** para acompanhamento histórico
- 🗄️ **Exploração da estrutura** do banco de dados
- 🖥️ **Interface interativa** fácil de usar

## 🚀 Instalação e Uso

### Modo Interativo (Recomendado)

```bash
python manual_sql_validator.py
```

Este modo oferece um menu interativo com todas as funcionalidades:

```
📋 OPÇÕES:
1. Executar query SQL manual
2. Testar pergunta no agente  
3. Comparar query manual vs agente
4. Mostrar estrutura do banco
5. Sair
```

### Modo Linha de Comando

#### Executar apenas query manual
```bash
python manual_sql_validator.py --query "SELECT COUNT(*) FROM sus_data;"
```

#### Testar apenas no agente
```bash
python manual_sql_validator.py --natural "Quantos pacientes existem?"
```

#### Comparação automática
```bash
python manual_sql_validator.py --compare \
  --query "SELECT COUNT(*) FROM sus_data;" \
  --natural "Quantos pacientes existem?"
```

#### Especificar banco diferente
```bash
python manual_sql_validator.py --database "outro_banco.db"
```

## 📋 Exemplos Práticos

### 1. Validação de Contagem Total

**Pergunta natural:** "Quantos pacientes existem?"
**Query manual:** `SELECT COUNT(*) FROM sus_data;`

```bash
python manual_sql_validator.py --compare \
  --query "SELECT COUNT(*) FROM sus_data;" \
  --natural "Quantos pacientes existem?"
```

### 2. Validação de Top Municípios

**Pergunta natural:** "Quais são os 5 municípios com mais mortes?"
**Query manual:** `SELECT MUNIC_RES, COUNT(*) as total_mortes FROM sus_data WHERE MORTE = 1 GROUP BY MUNIC_RES ORDER BY total_mortes DESC LIMIT 5;`

### 3. Validação de Média de Idade

**Pergunta natural:** "Qual a média de idade dos pacientes?"
**Query manual:** `SELECT AVG(IDADE) as media_idade FROM sus_data WHERE IDADE IS NOT NULL;`

### 4. Validação de Filtros por Sexo

**Pergunta natural:** "Quantos pacientes do sexo feminino?"
**Query manual:** `SELECT COUNT(*) FROM sus_data WHERE SEXO = 'F';`

## 📊 Interpretação dos Resultados

### ✅ Resultado Positivo
```
✅ RESULTADO: Agente funcionou corretamente!
   📊 Manual: 24485 registros
   📊 Agente: 24485 registros  
   🔧 SQL: ✅ SQL idêntico
   🤖 Agente: ✅ Executou sem erros
```

### ❌ Resultado com Problemas
```
❌ RESULTADO: Possível problema detectado!
   📊 Manual: 24485 registros
   📊 Agente: 0 registros
   🔧 SQL: ❌ SQL diferente
   🤖 Agente: ❌ Agente teve erro
   - Número de resultados diferente
   - SQL gerado diferente
```

## 🗂️ Estrutura dos Logs

Os logs de comparação são salvos em formato JSON com:

```json
{
  "timestamp": "2025-07-02T19:30:00",
  "natural_query": "Quantos pacientes existem?",
  "manual_query": "SELECT COUNT(*) FROM sus_data;",
  "manual_results_count": 24485,
  "agent_sql": "SELECT COUNT(*) FROM sus_data;", 
  "agent_results_count": 24485,
  "analysis": {
    "results_count_match": true,
    "sql_match": true,
    "agent_error": false,
    "results_count_status": "✅ Mesmo número de resultados",
    "sql_status": "✅ SQL idêntico",
    "agent_status": "✅ Agente executou sem erros"
  }
}
```

## 🔍 Casos de Uso para Detecção de Alucinação

### 1. Validação de Dados Específicos
Quando o agente retorna nomes de municípios, diagnósticos ou outros dados específicos, compare com queries manuais para verificar se os dados existem realmente no banco.

### 2. Validação de Agregações
Médias, somas, contagens e outras agregações podem ser facilmente validadas:
```sql
-- Verificar se a média está correta
SELECT AVG(IDADE) FROM sus_data WHERE IDADE IS NOT NULL;

-- Verificar se as contagens batem
SELECT COUNT(*) FROM sus_data WHERE MORTE = 1;
```

### 3. Validação de Filtros
Quando o agente aplica filtros (por idade, sexo, município), valide se os critérios estão corretos:
```sql
-- Verificar filtro de idade
SELECT COUNT(*) FROM sus_data WHERE IDADE > 65;

-- Verificar filtro de município
SELECT COUNT(*) FROM sus_data WHERE MUNIC_RES = '430300';
```

### 4. Validação de Ordenação
Verifique se os resultados estão realmente ordenados corretamente:
```sql
-- Top 10 por idade
SELECT * FROM sus_data ORDER BY IDADE DESC LIMIT 10;

-- Verificar se a ordenação está correta
SELECT MUNIC_RES, COUNT(*) as total 
FROM sus_data 
GROUP BY MUNIC_RES 
ORDER BY total DESC 
LIMIT 5;
```

## ⚙️ Configurações Avançadas

### Timeout do Agente
O validador usa timeout de 2 minutos para execução do agente. Para queries mais complexas, ajuste no código:
```python
timeout=120  # segundos
```

### Normalização de SQL
As queries são normalizadas para comparação (maiúsculas, espaços). Para comparação mais rigorosa, modifique a função `normalize_sql()`.

### Formato de Saída
Os resultados podem ser salvos em CSV para análise posterior. O validador pergunta automaticamente se deseja salvar.

## 🛠️ Solução de Problemas

### Erro de Conexão com Banco
```
❌ Erro ao conectar ao banco: no such file: sus_database.db
```
**Solução:** Verifique se o arquivo do banco existe no diretório atual ou especifique o caminho correto com `--database`.

### Timeout do Agente
```
⏰ Timeout na execução do agente
```
**Solução:** A query pode ser muito complexa. Teste primeiro manualmente para verificar se é executável.

### Agente Não Encontrado
```
❌ Erro na execução do agente: No such file or directory: 'python'
```
**Solução:** Verifique se o Python está no PATH e se o arquivo `txt2sql_agent_clean.py` existe.

## 📈 Métricas de Validação

O validador coleta automaticamente:
- ⏱️ **Tempo de execução** (manual vs agente)
- 📊 **Número de registros** retornados
- 🔧 **SQL gerado** pelo agente
- ✅ **Taxa de acerto** na comparação
- 📝 **Logs detalhados** para análise posterior

## 🎯 Boas Práticas

1. **Teste casos simples primeiro**: Comece com queries básicas como `COUNT(*)` antes de testar consultas complexas.

2. **Valide dados específicos**: Quando o agente retorna nomes ou códigos específicos, sempre verifique se existem no banco.

3. **Compare agregações**: Médias, somas e contagens são pontos críticos para validação.

4. **Salve logs importantes**: Mantenha histórico de validações para casos críticos.

5. **Teste edge cases**: Queries com filtros complexos, JOINs, ou condições especiais.

6. **Verifique ordenação**: Se a pergunta pede "top N" ou "maiores/menores", valide a ordenação.

## 🔗 Integração com CLAUDE.md

Para usar este validador de forma sistemática, adicione ao seu workflow:

```bash
# Validação automática de casos críticos
python manual_sql_validator.py --compare \
  --query "SELECT COUNT(*) FROM sus_data;" \
  --natural "Quantos pacientes existem?"

python manual_sql_validator.py --compare \
  --query "SELECT MUNIC_RES, COUNT(*) FROM sus_data WHERE MORTE = 1 GROUP BY MUNIC_RES ORDER BY COUNT(*) DESC LIMIT 5;" \
  --natural "Quais são os 5 municípios com mais mortes?"
```

Este validador é uma ferramenta essencial para garantir a confiabilidade do agente TXT2SQL e detectar possíveis alucinações de dados.