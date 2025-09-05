# TXT2SQL Claude - LangGraph V3

A modern Text-to-SQL agent using LangGraph framework with PostgreSQL support for Brazilian healthcare data (SIH-RS).

## Quick Start

### CLI Usage
```bash
# Simple query
python src/interfaces/cli/agent.py --query "Quantas mortes ocorreram?"

# Debug mode with step-by-step workflow
python src/interfaces/cli/agent.py --query "Quantos hospitais existem?" --debug-steps

# Interactive session
python src/interfaces/cli/agent.py

# Health check
python src/interfaces/cli/agent.py --health-check

# Generate workflow diagram
python src/interfaces/cli/agent.py --visualize-workflow
```

### API Server
```bash
# Start API server
python src/interfaces/api/main.py

# Access API at http://localhost:8000
# Documentation at http://localhost:8000/docs
```

## Project Structure

```
txt2sql_claude_s/
├── src/                           # All source code
│   ├── application/               # Business logic & configuration
│   ├── infrastructure/            # External integrations (DB, etc)
│   ├── langgraph_migration/       # LangGraph V3 workflow implementation
│   ├── interfaces/                # Entry points & user interfaces
│   │   ├── api/                   # FastAPI server
│   │   │   └── main.py           # API endpoints
│   │   └── cli/                   # Command-line interface
│   │       └── agent.py          # CLI application
│   └── utils/                     # Shared utilities
├── evaluation/                    # Testing and metrics
├── tests/                         # Automated tests
├── frontend/                      # Web interface (Node.js)
└── logs/                          # Application logs
```

## Features

- 🚀 **LangGraph V3**: Modern workflow-based architecture
- 🏥 **Healthcare Domain**: Specialized for Brazilian SIH-RS data
- 🔍 **Intelligent Table Selection**: 75%+ accuracy in table selection
- 📊 **PostgreSQL**: 15 specialized tables with 11M+ records
- 🔍 **LangSmith Integration**: Complete observability and tracing
- 🎯 **Multi-LLM Support**: Ollama, HuggingFace models
- 🛠️ **Debug Mode**: Step-by-step workflow visualization

## Architecture

- **Query Classification**: Intelligent routing (DATABASE/CONVERSATIONAL/SCHEMA)
- **Table Discovery**: Smart table selection based on query context
- **Schema Analysis**: Dynamic schema introspection with healthcare mappings
- **SQL Generation**: PostgreSQL-optimized query generation
- **SQL Validation**: Syntax and semantic validation
- **Execution**: Safe query execution with error handling
- **Response**: Natural language response generation

## Requirements

- Python 3.8+
- PostgreSQL database
- Ollama with LLama3.1:8b model
- Dependencies: `pip install -r requirements.txt`

## Environment Setup

1. Copy `.env.example` to `.env`
2. Configure database and LLM settings
3. Start Ollama: `ollama serve`
4. Pull model: `ollama pull llama3.1:8b`

## Migration from V2

This project has been migrated from legacy clean architecture to LangGraph V3. The legacy code is preserved but not actively used.