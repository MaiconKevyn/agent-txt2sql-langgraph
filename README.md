# TXT2SQL - DataVisSUS

A modern Text-to-SQL agent using LangGraph framework with PostgreSQL support for Brazilian healthcare data (SIH-RS).

## 🚀 Initial Setup

### 1. Prerequisites

- **Python 3.8+** (recommended: Python 3.11+)
- **Node.js 16+** (for web interface)
- **PostgreSQL** (local or remote)
- **Git**

### 2. Install Ollama and Model

```bash
# Install Ollama (Linux/macOS)
curl -fsSL https://ollama.com/install.sh | sh

# Start Ollama service
ollama serve

# Pull the required model (in another terminal)
ollama pull llama3.1:8b

# Verify installation
ollama list
```

### 3. Clone and Setup Python Environment

```bash
# Clone repository
git clone <repository-url>
cd txt2sql_claude_s

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate     # Windows

# Install Python dependencies
pip install -r requirements.txt
```

### 4. Environment Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your settings
nano .env
```

Required `.env` configuration:
```env
# LangSmith Configuration (for tracing)
LANGSMITH_TRACING=true
LANGSMITH_API_KEY=your_langsmith_api_key_here
LANGCHAIN_PROJECT=txt2sql

# Database Configuration  
DATABASE_PATH=postgresql+psycopg2://postgres:your_password@localhost:5432/sih_rs
```

### 5. Web Interface Setup (Optional)

```bash
# Navigate to frontend directory
cd frontend

# Install Node.js dependencies
npm install

# Start web interface (development mode)
npm run dev

# Access at http://localhost:3000
```

**Web Interface Features:**
- Modern responsive design
- Real-time query processing
- Database schema visualization
- Query history and examples
- Error handling and feedback

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
│   ├── agent/                     # LangGraph TXT2SQL agent core
│   │   ├── orchestrator.py       # Main orchestration logic
│   │   ├── workflow.py            # LangGraph workflow definition
│   │   ├── nodes.py               # Workflow nodes (classification, SQL gen, etc)
│   │   ├── state.py               # Workflow state management
│   │   ├── llm_manager.py         # LLM and database integration
│   │   └── tools/                 # Custom tools (enhanced list tables)
│   ├── application/               # Business logic & configuration
│   │   └── config/                # Configuration files
│   ├── infrastructure/            # External integrations (DB, etc)
│   │   └── database/              # Database connection services
│   ├── interfaces/                # Entry points & user interfaces
│   │   ├── api/                   # FastAPI server
│   │   │   └── main.py           # API endpoints
│   │   └── cli/                   # Command-line interface
│   │       └── agent.py          # CLI application
│   └── utils/                     # Shared utilities
├── evaluation/                    # Testing and metrics
├── tests/                         # Automated tests
├── frontend/                      # Web interface (Node.js)
│   ├── package.json              # Node.js dependencies
│   ├── server.js                 # Express.js server
│   └── public/                   # Static web assets
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

## Dependencies

The project uses a minimal set of carefully selected dependencies:

### Python Dependencies (15 packages)
```bash
# Install all Python dependencies
pip install -r requirements.txt
```

**Core Components:**
- **LangGraph 0.6.6** - Workflow orchestration
- **LangChain 0.3.x** - LLM integration and tools
- **FastAPI 0.115.13** - Modern web API framework
- **PostgreSQL** - Database support via psycopg2-binary
- **LangSmith 0.3.45** - Observability and tracing

### Node.js Dependencies (for Web Interface)
```bash
# Install frontend dependencies
cd frontend && npm install
```

**Web Stack:**
- **Express.js** - Web server framework
- **CORS, Helmet** - Security middleware
- **Rate Limiting** - API protection

## Migration from V2

This project has been migrated from legacy clean architecture to LangGraph V3. The legacy code is preserved but not actively used.