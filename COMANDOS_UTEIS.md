# 🚀 Comandos Úteis - TXT2SQL Agent

Este arquivo contém comandos bash úteis para gerenciar o TXT2SQL Agent. Você pode clicar diretamente nos blocos de código para executá-los.

## 📡 Gerenciamento de Porta da API

### Verificar o que está rodando na porta 8000
```bash
lsof -i :8000
```

### Verificar processos Python rodando
```bash
ps aux | grep python | grep -v grep
```

### Matar processo na porta 8000
```bash
sudo kill -9 $(lsof -t -i:8000)
```

### Matar todos os processos Python (cuidado!)
```bash
pkill -f python
```

## 🖥️ Executar a API

### Iniciar API Server (modo desenvolvimento)
```bash
python api_server.py
```

### Iniciar API Server em background
```bash
nohup python api_server.py > api.log 2>&1 &
```

### Verificar se a API está rodando
```bash
curl http://localhost:8000/health
```

### Ver logs da API em tempo real
```bash
tail -f api.log
```

## 🤖 Executar o Agent Interativo

### Agent básico (modo interativo)
```bash
python txt2sql_agent_clean.py
```

### Agent com modelo específico
```bash
python txt2sql_agent_clean.py --model llama3
```

### Verificar saúde do sistema
```bash
python txt2sql_agent_clean.py --health-check
```

## 💬 Exemplos de Queries em Português Brasileiro

### Query única - Contagem de pacientes
```bash
python txt2sql_agent_clean.py --query "Quantos pacientes existem no banco de dados?"
```

### Query única - Idade média
```bash
python txt2sql_agent_clean.py --query "Qual é a idade média dos pacientes?"
```

### Query única - Mortes por cidade
```bash
python txt2sql_agent_clean.py --query "Quantas mortes ocorreram em Porto Alegre?"
```

### Query única - Top diagnósticos
```bash
python txt2sql_agent_clean.py --query "Quais são os 5 diagnósticos mais comuns?"
```

### Query única - Pacientes por sexo
```bash
python txt2sql_agent_clean.py --query "Quantos pacientes são do sexo masculino?"
```

### Query única - Custos por estado
```bash
python txt2sql_agent_clean.py --query "Qual o custo total de internações por estado?"
```

### Query única - Tempo de UTI
```bash
python txt2sql_agent_clean.py --query "Qual a média de dias de UTI por paciente?"
```

### Query única - Procedimentos mais caros
```bash
python txt2sql_agent_clean.py --query "Quais são os 10 procedimentos mais caros realizados?"
```

## 🧪 Testes e Validação

### Teste simples da API
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Quantos pacientes existem?"}'
```

### Teste do schema
```bash
curl http://localhost:8000/schema
```

### Executar teste básico de conectividade
```bash
python -c "
from src.application.config.simple_config import ApplicationConfig, OrchestratorConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator

app_config = ApplicationConfig()
orchestrator_config = OrchestratorConfig()
agent = Text2SQLOrchestrator(app_config, orchestrator_config)
health = agent.health_check()
print(f'Sistema: {health[\"status\"]}')
"
```

## 🛠️ Gerenciamento do Ollama

### Verificar se Ollama está rodando
```bash
ollama list
```

### Iniciar Ollama (se não estiver rodando)
```bash
ollama serve
```

### Verificar modelos disponíveis
```bash
ollama list
```

### Baixar modelo llama3 (se necessário)
```bash
ollama pull llama3
```

### Testar Ollama diretamente
```bash
ollama run llama3 "Olá, como você está?"
```

## 📂 Navegação e Informações

### Ir para o diretório do projeto
```bash
cd /home/maiconkevyn/PycharmProjects/txt2sql_claude_s
```

### Ver estrutura do projeto
```bash
tree -I '__pycache__|*.pyc|.git|.venv' -L 3
```

### Ver logs recentes
```bash
ls -la *.log 2>/dev/null || echo "Nenhum arquivo de log encontrado"
```

### Verificar espaço em disco
```bash
df -h
```

### Ver processos de maior uso de CPU
```bash
top -n 1 | head -20
```

## 🗄️ Gerenciamento do Banco de Dados

### Verificar se o banco existe
```bash
ls -la sus_database.db
```

### Backup do banco de dados
```bash
cp sus_database.db sus_database_backup_$(date +%Y%m%d_%H%M%S).db
```

### Ver tamanho do banco
```bash
du -h sus_database.db
```

### Conectar ao banco SQLite (modo interativo)
```bash
sqlite3 sus_database.db
```

### Query rápida no banco (contar tabelas)
```bash
sqlite3 sus_database.db "SELECT name FROM sqlite_master WHERE type='table';"
```

## 🐛 Debug e Troubleshooting

### Ver últimos erros do sistema
```bash
dmesg | tail -10
```

### Verificar dependências Python
```bash
pip list | grep -E "(langchain|ollama|fastapi|flask)"
```

### Teste de conectividade com timeout
```bash
timeout 30 python txt2sql_agent_clean.py --health-check
```

### Verificar variáveis de ambiente
```bash
env | grep -E "(PYTHON|PATH)" | head -10
```

## 🔄 Reiniciar Serviços

### Reiniciar API completa
```bash
sudo kill -9 $(lsof -t -i:8000) 2>/dev/null; sleep 2; python api_server.py
```

### Limpar cache Python e reiniciar
```bash
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null; python api_server.py
```

## 📊 Monitoramento

### Ver uso de recursos em tempo real
```bash
htop
```

### Monitorar conexões de rede
```bash
netstat -tulpn | grep :8000
```

### Ver logs do sistema
```bash
journalctl -f
```

---

## 🎯 Comandos Mais Usados (Quick Reference)

```bash
# Matar porta 8000 e iniciar API
sudo kill -9 $(lsof -t -i:8000) 2>/dev/null; python api_server.py

# Teste rápido do agente
python txt2sql_agent_clean.py --query "Quantos pacientes existem?"

# Verificar saúde completa
python txt2sql_agent_clean.py --health-check

# Teste da API
curl -X POST http://localhost:8000/query -H "Content-Type: application/json" -d '{"question": "Quantos pacientes existem?"}'
```

---

> **💡 Dica:** Você pode clicar diretamente nos blocos de código acima para executá-los no terminal!