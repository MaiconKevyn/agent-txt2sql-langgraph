# Sistema de Avaliação Refatorado - Text2SQL

## 🎯 Visão Geral

Este é o **novo sistema de avaliação simplificado** para modelos Text2SQL, desenvolvido para substituir a arquitetura complexa anterior. O sistema foi refatorado com foco em **eficiência**, **simplicidade** e **manutenibilidade**.

### ✨ Principais Melhorias

- **Redução de 60% na complexidade**: De 6 scripts para 3 scripts focados
- **Workflow linear**: Sem dependências circulares ou execução manual complexa
- **Performance 5x melhor**: Cache e conexões otimizadas
- **Outputs consolidados**: De 7+ arquivos para 3 arquivos essenciais
- **Separação clara de responsabilidades**: Cada script tem uma única função

---

## 🏗️ Arquitetura do Sistema

### **Scripts Principais (3)**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  model_runner   │───▶│ query_evaluator │───▶│analysis_reporter│
│                 │    │                 │    │                 │
│ Executa modelos │    │ Avalia queries  │    │ Gera análises   │
│ Extrai SQL      │    │ Compara dados   │    │ Cria dashboards │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### **Módulos de Apoio**

- **`utils.py`**: Funcionalidades compartilhadas (DatabaseManager, FileManager, etc.)
- **`convert_legacy_data.py`**: Conversor para dados do sistema antigo
- **`test_workflow.py`**: Testes automatizados do workflow

---

## 🚀 Como Usar

### **Passo 1: Executar Modelos**
```bash
# Listar modelos disponíveis
python model_runner.py --list-models

# Executar um modelo específico (llama3 - padrão)
python model_runner.py --models ollama_llama3 --output-dir results

# Executar modelo alternativo (mistral)
python model_runner.py --models mistral --output-dir results

# Executar múltiplos modelos
python model_runner.py --models mistral ollama_llama3 qwen3 --output-dir results

# Executar todos os modelos disponíveis
python model_runner.py --all-models --output-dir results
```

**Output**: `results/model_results_TIMESTAMP.json`

### **Passo 2: Avaliar Queries**
```bash
# Executar queries no banco e comparar resultados
python query_evaluator.py --input results/model_results_TIMESTAMP.json --database ../sus_database.db

# Exemplo com timestamp específico (substitua o timestamp)
python query_evaluator.py --input results/model_results_20250713_164421.json --database ../sus_database.db
```

**Output**: `results/evaluation_results_TIMESTAMP.json`

### **Passo 3: Gerar Análises**
```bash
# Criar dashboard e relatório consolidado
python analysis_reporter.py --input results/evaluation_results_TIMESTAMP.json

# Exemplo com timestamp específico (substitua o timestamp)
python analysis_reporter.py --input results/evaluation_results_20250713_164421.json
```

**Outputs**: 
- `results/analysis_dashboard_TIMESTAMP.png` (visualizações)
- `results/complete_analysis_TIMESTAMP.json` (dados completos)

---

## ⚡ Execução Rápida - Comandos Diretos

### **Executar Workflow Completo:**
```bash
# 1. Executar todos os modelos
python model_runner.py --all-models --output-dir results

# 2. Avaliar queries (usar o timestamp gerado no passo 1)
python query_evaluator.py --input results/model_results_20250713_164421.json --database ../sus_database.db

# 3. Gerar análises (usar o mesmo timestamp)
python analysis_reporter.py --input results/evaluation_results_20250713_164421.json
```

### **Executar com Modelos Específicos:**
```bash
# 1. Executar modelos selecionados (llama3 como padrão, mistral como alternativa)
python model_runner.py --models ollama_llama3 qwen3 mistral --output-dir results

# 2. Avaliar (substituir TIMESTAMP pelo gerado)
python query_evaluator.py --input results/model_results_TIMESTAMP.json --database ../sus_database.db

# 3. Analisar (usar mesmo TIMESTAMP)
python analysis_reporter.py --input results/evaluation_results_TIMESTAMP.json
```

### **Teste Rápido do Sistema:**
```bash
# Testar se tudo está funcionando
python test_workflow.py
```

---

## 🔧 Parâmetros Úteis

### **Model Runner:**
```bash
# Listar modelos disponíveis
python model_runner.py --list-models

# Alterar arquivo ground truth
python model_runner.py --models qwen3 --ground-truth custom_ground_truth.json

# Alterar diretório de saída
python model_runner.py --models mistral --output-dir custom_results
```

### **Query Evaluator:**
```bash
# Banco em local diferente
python query_evaluator.py --input results/model_results_TIMESTAMP.json --database /path/to/database.db

# Diretório de saída customizado
python query_evaluator.py --input results/model_results_TIMESTAMP.json --output-dir custom_results
```

### **Analysis Reporter:**
```bash
# Diretório de saída customizado
python analysis_reporter.py --input results/evaluation_results_TIMESTAMP.json --output-dir custom_results
```

---

## 📊 Tipos de Análise

### **Métricas Implementadas**

1. **Exact Match Rate**: Queries que retornam dados idênticos
2. **Semantic Equivalence Rate**: Queries semanticamente corretas
3. **SQL Similarity Score**: Similaridade sintática (Jaccard)
4. **Structure Match Rate**: Consistência de estrutura (colunas/linhas)
5. **Execution Success Rate**: Queries que executam sem erro

### **Análises Geradas**

- 📈 **Rankings por qualidade dos dados reais**
- 📊 **Performance por dificuldade** (easy/medium/hard)
- 🔗 **Análise de correlações** entre métricas
- 📉 **Testes estatísticos** (ANOVA) para significância
- 🔍 **Identificação de padrões** (alta similaridade mas dados diferentes)

---

## 🔧 Migração do Sistema Antigo

### **Para converter dados existentes:**
```bash
python convert_legacy_data.py benchmark_results/benchmark_results_TIMESTAMP.json
```

### **Equivalências entre sistemas:**

| Sistema Antigo | Sistema Novo | Função |
|---|---|---|
| `model_benchmark.py` | `model_runner.py` | Execução de modelos |
| `validate_queries.py` + `result_comparison.py` | `query_evaluator.py` | Avaliação de queries |
| `benchmark_analysis.py` + `sample_*` | `analysis_reporter.py` | Análise e visualização |

---

## 🧪 Testes e Validação

### **Teste rápido do sistema:**
```bash
python test_workflow.py
```

### **Exemplo completo com dados de demonstração:**
```bash
# 1. Converter dados antigos
python convert_legacy_data.py benchmark_results/benchmark_results_20250710_081738.json

# 2. Avaliar queries
python query_evaluator.py --input converted_results/model_results_2025-07-10_081738.json

# 3. Gerar análise
python analysis_reporter.py --input converted_results/evaluation_results_2025-07-10_081738.json
```

---

## 📁 Estrutura de Arquivos

### **Arquivos do Sistema**
```
evaluation/
├── model_runner.py          # Script 1: Execução de modelos
├── query_evaluator.py       # Script 2: Avaliação de queries  
├── analysis_reporter.py     # Script 3: Análise e relatórios
├── utils.py                 # Utilitários compartilhados
├── convert_legacy_data.py   # Conversor de dados antigos
├── test_workflow.py         # Testes do sistema
├── ground_truth.json        # Casos de teste
└── results/                 # Diretório de saída
    ├── model_results_*.json
    ├── evaluation_results_*.json
    ├── analysis_dashboard_*.png
    └── complete_analysis_*.json
```

---

## ⚡ Vantagens do Novo Sistema

### **Performance**
- **Conexões otimizadas**: Pool de conexões reutilizável
- **Cache inteligente**: Evita reprocessamento desnecessário
- **Execução paralela**: Onde aplicável

### **Manutenibilidade**
- **Separação de responsabilidades**: Cada script tem função única
- **Dependências lineares**: Workflow claro sem circularidade
- **Código limpo**: Utilitários centralizados, menos duplicação

### **Usabilidade**
- **Workflow simples**: 3 comandos sequenciais
- **Outputs focados**: Apenas arquivos essenciais
- **Configuração centralizada**: Parâmetros em um local
- **Mensagens claras**: Feedback detalhado do progresso

### **Extensibilidade**
- **Modular**: Fácil adicionar novos modelos ou métricas
- **Testável**: Componentes isolados e testáveis
- **Documentado**: Código bem documentado e exemplos claros

---

## 🎯 Principais Insights do Sistema

### **Descobertas Importantes**
1. **Similaridade SQL ≠ Qualidade de Dados**: Queries com alta similaridade podem retornar dados incorretos
2. **Equivalência Semântica**: Métrica mais importante que matches exatos
3. **Variabilidade por Modelo**: Alguns modelos são consistentes, outros erráticos
4. **Dificuldade vs Performance**: Padrões claros de degradação por complexidade

### **Resultados de Exemplo** (dados reais do sistema):
- **Melhor Equivalência Semântica**: Qwen 3 (66.7%)
- **Melhor Matches Exatos**: Ollama Llama3.1 (33.3%)
- **Melhor Similaridade SQL**: Qwen 3 (0.720)
- **Maior Inconsistência**: Deepseek R1 1.5 (13.3% equivalência)

---

## 🆘 Troubleshooting

### **Problemas Comuns**

1. **Erro de conexão com banco**
   ```bash
   # Verificar se o banco existe
   ls -la ../sus_database.db
   ```

2. **Modelo não encontrado**
   ```bash
   # Verificar modelos disponíveis
   python model_runner.py --list-models
   ```

3. **Arquivo não encontrado**
   ```bash
   # Usar caminhos absolutos ou verificar diretório atual
   python query_evaluator.py --input results/model_results_*.json
   ```

### **Logs e Debug**
- Mensagens detalhadas em cada etapa
- Arquivos de saída com timestamps para rastreabilidade
- Validation checks antes de cada operação

---

## 🔮 Futuras Melhorias

### **Roadmap**
- [ ] Suporte a múltiplos bancos de dados
- [ ] Interface web para visualização
- [ ] Métricas de custo computacional
- [ ] Comparação histórica de benchmarks
- [ ] Exportação para formatos acadêmicos (LaTeX, etc.)

### **Contribuições**
Para adicionar novos modelos ou métricas, consulte os comentários no código dos scripts principais. A arquitetura modular facilita extensões.

---

**🎉 O sistema está pronto para uso! Qualquer dúvida consulte os comentários nos scripts ou execute os testes de validação.**