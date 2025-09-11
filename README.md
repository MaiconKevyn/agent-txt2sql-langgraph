# TXT2SQL - DataVisSUS

## Project Description

TXT2SQL is a natural language to SQL query system designed for Brazilian healthcare data analysis. The system enables users to query healthcare databases using natural language questions in Portuguese, automatically generating and executing SQL queries against a PostgreSQL database containing Sistema de Informações Hospitalares (SIH-RS) data.

The system processes over 11 million healthcare records across 15 specialized tables, including patient mortality data, medical procedures, hospital admissions, and diagnostic information. Built using the LangGraph workflow framework, it provides both command-line and web-based interfaces for healthcare data analysis.

**Key capabilities:**
- Natural language query processing in Portuguese
- Automated SQL generation and validation
- Healthcare-specific table selection and schema analysis
- Multi-interface access (CLI, REST API, web frontend)
- Conversation history and follow-up query support
- Comprehensive logging and monitoring
- SQL injection prevention and security controls

**Target use cases:**
- Healthcare data analysis and reporting
- Medical research query assistance
- Hospital administration data insights
- Public health data exploration

The system integrates with local language models through Ollama, supporting various model providers while maintaining data privacy and security for sensitive healthcare information.

## Initial Setup

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
git clone https://github.com/MaiconKevyn/agent_llama3_txt2sql.git
cd agent_llama3_txt2sql

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

## System Workflow

```mermaid
graph TD
    A["<b>User Input</b><br/>Natural Language Query"] --> B{"<b>Interface</b>"}
    
    B -->|CLI| C["<b>CLI Agent</b><br/>agent.py"]
    B -->|API| D["<b>FastAPI Server</b><br/>main.py"]
    B -->|Web| E["<b>Frontend</b><br/>Express.js"]
    
    C --> F["<b>LangGraph Orchestrator</b><br/>orchestrator.py"]
    D --> F
    E --> D
    
    F --> G["<b>Query Classification Node</b><br/>DATABASE/CONVERSATIONAL/SCHEMA"]
    
    G -->|DATABASE| H["<b>Table Discovery Node</b><br/>Select Relevant Tables"]
    G -->|CONVERSATIONAL| I["<b>Direct Response Node</b><br/>General Conversation"]
    G -->|SCHEMA| J["<b>Schema Info Node</b><br/>Database Structure"]
    
    H --> K["<b>Schema Analysis Node</b><br/>Get Table Schemas"]
    K --> L["<b>SQL Generation Node</b><br/>Create SQL Query"]
    L --> M["<b>SQL Validation Node</b><br/>Syntax & Safety Check"]
    
    M -->|Valid| N["<b>SQL Execution Node</b><br/>Execute Against PostgreSQL"]
    M -->|Invalid| O["<b>Error Response</b>"]
    
    N --> P["<b>Response Generation Node</b><br/>Format Natural Language Response"]
    I --> P
    J --> P
    O --> P
    
    P --> Q["<b>Final Response</b><br/>Natural Language + SQL"]
    
    R[("<b>PostgreSQL Database</b><br/>SIH-RS Healthcare Data<br/>15 Tables, 11M+ Records")] --> N
    
    S["<b>LLM Manager</b><br/>Ollama Integration"] --> L
    S --> P
    
    T["<b>Logging System</b><br/>Structured Logs"] --> F
    U["<b>Session Management</b><br/>Conversation History"] --> F
    
    style A fill:#e1f5fe
    style Q fill:#e8f5e8
    style R fill:#fff3e0
    style F fill:#f3e5f5
    style N fill:#ffebee
```

## Testing

This project includes a comprehensive testing suite focused on security and reliability.

### Running Tests

```bash
# Run all tests with verbose output
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_sql_safety.py -v

# Run tests with coverage report (requires pytest-cov)
python -m pytest tests/ --cov=src --cov-report=html

# Collect tests without running (show all discovered tests)
python -m pytest --collect-only

# Run tests in parallel (requires pytest-xdist)
python -m pytest tests/ -n auto

# Run tests with detailed output for debugging
python -m pytest tests/ -v -s --tb=long
```

### Test Structure

```
tests/
├── conftest.py                    # Pytest configuration
├── test_sql_safety.py            # SQL security validation tests
└── test_sql_execution_block.py   # SQL execution blocking tests
```

### Unit Tests Overview

#### 1. SQL Safety Tests (`test_sql_safety.py`)
- **Purpose**: Validates SQL injection prevention and query safety
- **Module Tested**: `src.utils.sql_safety.is_select_only`
- **Test Cases**: 14 parametrized tests covering:
  - ✅ Valid SELECT statements (basic, CTE, with comments)
  - ❌ Dangerous statements (UPDATE, DROP, CREATE, DELETE, TRUNCATE)
  - ❌ Multiple statements and SQL injection attempts
  - Proper error message validation

#### 2. SQL Execution Blocking Tests (`test_sql_execution_block.py`)
- **Purpose**: Ensures dangerous SQL is blocked at multiple layers
- **Modules Tested**: 
  - `src.agent.nodes.execute_sql_node` (workflow level)
  - `src.agent.llm_manager.HybridLLMManager.execute_sql_query` (manager level)
- **Test Cases**: 2 comprehensive tests using:
  - Mock objects to avoid heavy database initialization
  - Monkeypatching for isolated testing
  - State-based testing for workflow nodes

### Test Framework

- **Framework**: pytest 8.4.2
- **Plugins**: anyio-4.9.0, typeguard-4.4.4, langsmith-0.3.45
- **Configuration**: Custom setup in `conftest.py` for src-layout project structure

### Test Status

**All 16 tests are currently passing** ✅

### Additional Testing Resources

#### Evaluation Framework
The project includes an extensive evaluation system in `/evaluation/` directory:
- Database structure analysis
- Agent extraction testing  
- Query execution validation
- Table selection metrics generation

#### Test Notebooks
- `/notebooks/test.ipynb`: Database connection testing and table listing

### Logging System Testing

The project includes a production-ready logging system with structured output:

```bash
# Check logging implementation after running API or CLI
ls -la logs/

# View structured logs by component
tail -f logs/txt2sql_orchestrator.log  # Main orchestration
tail -f logs/txt2sql_nodes.log         # Workflow nodes
tail -f logs/txt2sql_llm_manager.log   # LLM operations
tail -f logs/txt2sql_api.log           # API requests
tail -f logs/txt2sql_cli.log           # CLI usage

# Test logging by running a query
python src/interfaces/cli/agent.py --query "How many patients?"
```

**Logging Features:**
- ✅ RotatingFileHandler (100MB max size, 10 backups)
- ✅ Component-specific log files
- ✅ Structured format with timestamps and context
- ✅ Production-ready configuration
- ✅ Replaced all 269 print() statements with structured logging

### Future Test Expansion

Areas for additional test coverage:
- End-to-end LangGraph workflow tests
- API endpoint integration tests
- Database connection and operation tests
- CLI interface tests
- Performance and load tests
- Logging system validation tests

## Migration from V2

This project has been migrated from legacy clean architecture to LangGraph V3. The legacy code is preserved but not actively used.