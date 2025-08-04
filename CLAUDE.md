# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Commands

### Development Workflow
- **Install dependencies**: `pip install -r requirements.txt`
- **Setup database**: `python database_setup.py`
- **Start Ollama service**: `ollama serve` (required for LLM models)
- **Install LLM models**: `ollama pull llama3` and `ollama pull mistral`

### Running the Application
- **Main CLI interface**: `python txt2sql_agent_clean.py`
- **Single query execution**: `python txt2sql_agent_clean.py --query "Your question"`
- **Health check**: `python txt2sql_agent_clean.py --health-check`
- **Start API server**: `python api_server.py`

### Frontend Development
- **Install frontend deps**: `cd frontend && npm install`
- **Start web interface**: `cd frontend && npm run dev` (requires API server running)

### Testing
- **No unified test runner** - run individual test files:
  - `python tests/test_api.py`
  - `python tests/test_decomposition_integration.py`
  - `python evaluation/model_runner.py` (for model evaluation)

### Troubleshooting
- **Kill processes on port 8000**: `sudo kill -9 $(lsof -t -i:8000)`
- **Check Ollama status**: `ollama list`
- **Database connection test**: `sqlite3 sus_database.db "SELECT COUNT(*) FROM pacientes;"`

## Architecture Overview

This is a **Clean Architecture** text-to-SQL system for Brazilian SUS (healthcare) data with intelligent query processing capabilities.

### High-Level System Design

```
┌─────────────────────────────────────────────────────────┐
│                    User Interfaces                     │
│  CLI Interface │ REST API │ Web Interface │ Streamlit  │
├─────────────────────────────────────────────────────────┤
│                Text2SQL Orchestrator                   │
│              (Central Coordinator)                     │
├─────────────────────────────────────────────────────────┤
│ Query Classification │ Query Decomposition │ Response   │
│     Service          │     Service        │ Generation │
├─────────────────────────────────────────────────────────┤
│    Database Route    │                   │ Conversational │
│                      │                   │    Route       │
├─────────────────────────────────────────────────────────┤
│ Schema Introspection │ LLM Communication │ SUS Domain    │
│      Service         │      Service      │   Service     │
├─────────────────────────────────────────────────────────┤
│        SQLite Database (SUS Data)    │ Ollama LLMs     │
│           24,485 records              │ (llama3/mistral)│
└─────────────────────────────────────────────────────────┘
```

### Core Architecture Layers

#### Application Layer (`src/application/`)
- **Text2SQLOrchestrator**: Main coordinator with intelligent query routing and session management
- **Services**: 15+ specialized services following Single Responsibility Principle:
  - `QueryClassificationService`: Intelligent routing between database and conversational queries
  - `SimpleQueryDecomposer`: Decomposes complex queries into manageable components
  - `LLMCommunicationService`: Multi-provider LLM communication (Ollama, HuggingFace)
  - `QueryProcessingService`: Central query coordinator with fallback strategies
  - `SchemaIntrospectionService`: Database schema analysis and context generation
  - `ConversationalResponseService`: Direct conversational response generation
  - `ErrorHandlingService`: Comprehensive error management with categorization

#### Domain Layer (`src/domain/`)
- **Entities**: Core business objects (`CIDChapter`, `Diagnosis`, `QueryDecomposition`)
- **Repositories**: Abstract interfaces (`ICIDRepository`) for data access
- **Services**: Domain-specific business logic (CID semantic search)
- **Value Objects**: Immutable data structures with validation

#### Infrastructure Layer (`src/infrastructure/`)
- **Database**: SQLite connection management with LangChain integration
- **Repositories**: Concrete implementations (`SQLiteCIDRepository`)

### Key Architectural Features

#### Intelligent Query Routing System
The system automatically classifies queries into two routes:
- **Database Route**: Statistical queries requiring SQL execution (e.g., "Quantos pacientes existem?")
- **Conversational Route**: Explanatory questions answered directly (e.g., "O que significa CID J90?")

This provides:
- 🚀 Faster responses for explanatory questions (3-7s vs 15-30s)
- 💡 More appropriate answers for each query type
- 📊 Visual routing indicators in all interfaces

#### Query Decomposition System
For complex database queries, the system automatically:
- **Detects complexity** using 8 pattern categories (ranking, correlation, trends, etc.)
- **Applies decomposition strategies** (Sequential Filter, Aggregate Split, Temporal Split)
- **Maintains fallback guarantee** - always falls back to standard processing if needed
- **Provides metadata** about decomposition decisions and performance

#### Multi-LLM Architecture
- **Primary Model**: llama3 (for SQL generation)
- **Conversational Model**: llama3.2 (for direct explanations)
- **Fallback Models**: mistral and other local models
- **Provider Support**: Ollama (primary), HuggingFace (alternative)

### Data Sources

- **Primary Database**: `sus_database.db` (SQLite) - 24,485 Brazilian SUS healthcare records
- **CID-10 Data**: `data/cid10*.csv` files for diagnostic code lookup and semantic search
- **Geographic Data**: Brazilian cities and regions for geographic analysis

### Configuration Management

The system uses simple dataclass-based configuration:
- `ApplicationConfig`: Core system settings (database, LLM provider, models)
- `OrchestratorConfig`: Query processing settings (routing, decomposition thresholds)
- Environment variables supported for API and decomposition settings

### Error Handling Strategy

- **Centralized Error Service**: Categorizes and manages all exceptions
- **Graceful Degradation**: System continues with reduced functionality on failures
- **Multi-level Fallbacks**: Query decomposition → standard processing → basic error response
- **Comprehensive Logging**: Detailed error tracking with file rotation

### Performance Characteristics

- **Simple queries**: 2-5 seconds (standard processing)
- **Conversational queries**: 3-7 seconds (direct response, no SQL)
- **Complex queries**: 8-15 seconds (with decomposition + fallback)
- **Database queries**: 15-30 seconds (full SQL pipeline)
- **Classification accuracy**: 95%+ for query routing
- **Decomposition success**: 100% (with guaranteed fallback)

## Development Guidelines

### Code Patterns
- **Clean Architecture**: Follow SOLID principles and maintain layer boundaries
- **Single Responsibility**: Each service has one clear purpose
- **Dependency Injection**: Use the orchestrator's service management pattern
- **Repository Pattern**: Abstract data access through interfaces
- **Factory Pattern**: Use factories for complex service creation

### Testing Approach
- **No pytest or unified runner** - tests are individual Python files
- Run specific tests directly: `python tests/test_*.py`
- Integration tests for complex workflows in `evaluation/` directory
- Model evaluation using `evaluation/model_runner.py`

### Healthcare Domain Specialization
- **SUS Data Focus**: System designed for Brazilian healthcare data analysis
- **CID-10 Integration**: Diagnostic code semantic search and validation
- **Medical Terminology**: Healthcare-specific prompts and templates
- **Geographic Analysis**: Brazilian city and region data handling

### Common Development Tasks

#### Adding New Services
1. Create service in `src/application/services/`
2. Follow existing service patterns (error handling, logging)
3. Add to orchestrator's service initialization
4. Create tests in `tests/` directory

#### Modifying Query Processing
- **Query Classification**: Modify patterns in `QueryClassificationService`
- **Decomposition Logic**: Update strategies in `SimpleQueryDecomposer`
- **SQL Generation**: Enhance prompts in `QueryProcessingService`

#### Database Changes
1. Update `database_setup.py` for schema changes
2. Modify `SchemaIntrospectionService` for new tables
3. Update repository interfaces and implementations

### API Integration

#### FastAPI Server (`api_server.py`)
- **Primary endpoint**: `POST /query` with JSON payload `{"question": "query"}`
- **Health check**: `GET /health`
- **Automatic features**: Query decomposition, routing, CORS support
- **Configuration**: Environment variables for decomposition settings

#### Web Interface (`frontend/`)
- **Independent Express.js server** on port 3000
- **API proxy**: Routes requests to FastAPI server (port 8000)
- **Real-time processing**: Visual indicators for routing and decomposition
- **Start command**: `npm run dev` (requires API server running)

### Deployment Notes

#### Prerequisites
- **Python 3.8+** with packages from requirements.txt
- **Ollama service** running with llama3 and mistral models
- **Node.js 16+** for web interface
- **SQLite database** initialized with `database_setup.py`

#### Production Considerations
- Use `uvicorn api_server:app --host 0.0.0.0 --port 8000` for production API
- Monitor Ollama service health and model availability
- Database backup recommended for `sus_database.db`
- Log rotation configured for error files

This system demonstrates enterprise-level clean architecture applied to healthcare AI, with intelligent query processing, robust error handling, and comprehensive domain specialization for Brazilian SUS data analysis.