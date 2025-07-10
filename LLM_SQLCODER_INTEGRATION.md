# 🚀 Guia de Integração: defog/sqlcoder-7b-2

**Sistema TXT2SQL - Integração com SQLCoder-7b-2 da Hugging Face**

---

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Instalação das Dependências](#instalação-das-dependências)
3. [Configuração](#configuração)
4. [Como Usar](#como-usar)
5. [Comparação: Llama3 vs SQLCoder-7b-2](#comparação-llama3-vs-sqlcoder-7b-2)
6. [Otimizações de Performance](#otimizações-de-performance)
7. [Troubleshooting](#troubleshooting)

---

## 🎯 Visão Geral

O **defog/sqlcoder-7b-2** é um modelo especializado em geração de SQL, otimizado especificamente para tarefas de conversão de linguagem natural para SQL. Esta integração permite usar este modelo no lugar do Llama3 para potencialmente melhorar a qualidade das queries SQL geradas.

### ✨ **Vantagens do SQLCoder-7b-2:**
- 🎯 **Especialização em SQL**: Treinado especificamente para tarefas de SQL
- ⚡ **Menor latência**: Modelo mais focado = respostas mais rápidas
- 🧠 **Melhor compreensão**: Entende melhor estruturas relacionais
- 🔧 **SQL mais limpo**: Gera consultas SQL mais precisas e corretas

---

## 📦 Instalação das Dependências

### **1. Dependências Python**

As dependências já foram adicionadas ao `requirements.txt`:

```bash
pip install transformers torch accelerate
```

### **2. Requisitos de Hardware**

#### **📊 Requisitos Mínimos:**
- **RAM**: 16GB+ recomendado
- **GPU**: CUDA compatível (opcional, mas recomendado)
- **Espaço**: ~15GB para o modelo

#### **🚀 Requisitos Recomendados:**
- **RAM**: 32GB+
- **GPU**: RTX 3080/4070+ ou similar (8GB+ VRAM)
- **Espaço**: 20GB+

### **3. Quantização para Hardware Limitado**

O sistema suporta quantização automática:
- **4-bit**: Reduz uso de memória em ~75% (padrão)
- **8-bit**: Reduz uso de memória em ~50%
- **Full precision**: Melhor qualidade, maior uso de memória

---

## ⚙️ Configuração

### **Opção 1: Configuração Rápida (Recomendada)**

Edite o arquivo `src/application/config/simple_config.py`:

```python
@dataclass
class ApplicationConfig:
    # LLM configuration
    llm_provider: str = "huggingface"  # Mudança aqui!
    llm_model: str = "defog/sqlcoder-7b-2"  # Mudança aqui!
    llm_temperature: float = 0.0
    llm_timeout: int = 120
    llm_max_retries: int = 3
    
    # Hugging Face specific configuration
    llm_device: str = "auto"  # auto detecta GPU/CPU
    llm_load_in_8bit: bool = False
    llm_load_in_4bit: bool = True  # Recomendado para economia de memória
```

### **Opção 2: Configuração Manual**

```python
from src.application.services.llm_communication_service import LLMCommunicationFactory

# Criar serviço SQLCoder
llm_service = LLMCommunicationFactory.create_huggingface_service(
    model_name="defog/sqlcoder-7b-2",
    temperature=0.0,
    device="auto",  # ou "cuda", "cpu"
    load_in_4bit=True  # Economia de memória
)
```

---

## 🚦 Como Usar

### **1. Modo CLI**

```bash
# Usar SQLCoder-7b-2 via CLI
python txt2sql_agent_clean.py

# Verificar se o modelo está carregado
python txt2sql_agent_clean.py --health-check
```

### **2. API REST**

```bash
# Iniciar servidor com SQLCoder
python api_server.py
```

Teste via curl:
```bash
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"question": "Quantos pacientes tiveram alta em 2023?"}'
```

### **3. Interface Web**

```bash
cd frontend && npm start
```

---

## ⚔️ Comparação: Llama3 vs SQLCoder-7b-2

| Aspecto | Llama3 (Ollama) | SQLCoder-7b-2 (HF) |
|---------|-----------------|---------------------|
| **Especialização** | Generalista | 🎯 Específico para SQL |
| **Tamanho** | ~4.7GB | ~7GB (quantizado: ~2GB) |
| **Latência** | Média | ⚡ Mais rápido para SQL |
| **Qualidade SQL** | Boa | 🏆 Excelente |
| **Configuração** | Simples | Moderada |
| **Recursos** | Baixo | Médio-Alto |

### **📊 Teste Comparativo de Performance**

```python
# Exemplo de query complexa para teste
query = "Qual é o tempo médio de internação para pacientes com doenças respiratórias em Porto Alegre?"

# Llama3: Pode gerar arithmetic subtraction incorreta
# SQLCoder-7b-2: Mais propenso a usar JULIANDAY corretamente
```

---

## 🔧 Otimizações de Performance

### **1. Configurações de GPU**

```python
# Para GPU com bastante VRAM (8GB+)
llm_device: str = "cuda"
llm_load_in_4bit: bool = False
llm_load_in_8bit: bool = False

# Para GPU com VRAM limitada (4-6GB)
llm_device: str = "cuda"
llm_load_in_4bit: bool = True

# Para CPU apenas
llm_device: str = "cpu"
llm_load_in_8bit: bool = True
```

### **2. Otimizações de Memória**

```python
# Configuração para máxima economia
@dataclass
class ApplicationConfig:
    llm_load_in_4bit: bool = True
    llm_device: str = "auto"
    llm_timeout: int = 180  # Maior timeout para processamento
```

### **3. Cache de Modelo**

O modelo é baixado automaticamente na primeira execução e armazenado em:
- Linux/Mac: `~/.cache/huggingface/`
- Windows: `%USERPROFILE%\.cache\huggingface\`

---

## 🔄 Alternando Entre Modelos

### **Para Llama3 (Ollama):**
```python
llm_provider: str = "ollama"
llm_model: str = "llama3"
```

### **Para SQLCoder-7b-2 (Hugging Face):**
```python
llm_provider: str = "huggingface"
llm_model: str = "defog/sqlcoder-7b-2"
```

### **Para outros modelos Hugging Face:**
```python
llm_provider: str = "huggingface"
llm_model: str = "codellama/CodeLlama-7b-Instruct-hf"  # Exemplo
```

---

## 🛠️ Troubleshooting

### **❌ Erro: "Package requirements not satisfied"**

**Solução:**
```bash
pip install transformers torch accelerate
```

### **❌ Erro: "CUDA out of memory"**

**Soluções:**
1. **Ativar quantização 4-bit:**
   ```python
   llm_load_in_4bit: bool = True
   ```

2. **Usar CPU:**
   ```python
   llm_device: str = "cpu"
   ```

3. **Fechar outros processos** que usam GPU

### **❌ Erro: "Model download failed"**

**Soluções:**
1. **Verificar conexão** com internet
2. **Espaço em disco** (precisa de ~15GB)
3. **Autenticação Hugging Face** (se necessário):
   ```bash
   huggingface-cli login
   ```

### **❌ Performance muito lenta**

**Soluções:**
1. **Verificar se está usando GPU:**
   ```python
   import torch
   print(torch.cuda.is_available())  # Deve retornar True
   ```

2. **Reduzir max_new_tokens** na geração
3. **Usar quantização** mais agressiva

### **❌ Erro: "Failed to initialize model"**

**Solução:**
1. **Verificar requisitos de hardware**
2. **Limpar cache** do Hugging Face:
   ```bash
   rm -rf ~/.cache/huggingface/
   ```

---

## 📈 Monitoramento

### **Verificar Status do Modelo:**

```python
# Via API
curl http://localhost:8000/health

# Via CLI
python txt2sql_agent_clean.py --health-check
```

### **Informações do Modelo:**

```python
from src.application.services.llm_communication_service import LLMCommunicationFactory

service = LLMCommunicationFactory.create_huggingface_service()
info = service.get_model_info()
print(info)
```

**Exemplo de output:**
```json
{
  "provider": "HuggingFace",
  "model_name": "defog/sqlcoder-7b-2",
  "device": "cuda:0",
  "load_in_4bit": true,
  "available": true,
  "cuda_available": true
}
```

---

## 🎯 Exemplo Prático de Uso

### **1. Configurar SQLCoder-7b-2:**

```python
# src/application/config/simple_config.py
llm_provider: str = "huggingface"
llm_model: str = "defog/sqlcoder-7b-2"
llm_load_in_4bit: bool = True
```

### **2. Testar Query:**

```bash
python txt2sql_agent_clean.py
> Quantas mulheres com menos de 40 anos morreram por doenças respiratórias?
```

### **3. Comparar Resultados:**

**Com SQLCoder-7b-2:**
```sql
SELECT COUNT(*) FROM sus_data s 
JOIN cid_capitulos c ON s.DIAG_PRINC BETWEEN c.codigo_inicio AND c.codigo_fim 
WHERE c.categoria_geral = 'J' 
AND s.SEXO = 3 
AND s.IDADE < 40 
AND s.MORTE = 1;
```

**Esperado:** SQL mais preciso e semanticamente correto

---

## 🔮 Próximos Passos

1. **Teste com seu dataset** específico
2. **Compare performance** com Llama3
3. **Ajuste parâmetros** conforme necessário
4. **Monitore uso de recursos**
5. **Considere fine-tuning** para seu domínio específico

---

## 📞 Suporte

Para problemas ou dúvidas:
1. **Verifique logs** do sistema
2. **Consulte documentação** do transformers
3. **Teste configurações** diferentes de hardware
4. **Reporte issues** se necessário

---

**✅ Integração Completa!** 

O sistema agora suporta tanto Ollama (Llama3) quanto Hugging Face (SQLCoder-7b-2), com troca simples via configuração.