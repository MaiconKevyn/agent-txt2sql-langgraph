# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Main Application
- **Run the main TXT2SQL agent**: `python txt2sql_agent_clean.py`
- **Run with basic interface**: `python txt2sql_agent_clean.py --basic`
- **Single query execution**: `python txt2sql_agent_clean.py --query "Your question here"`
- **Health check**: `python txt2sql_agent_clean.py --health-check`
- **Architecture info**: `python txt2sql_agent_clean.py --version`

### Database Setup
- **Initialize database**: `python database_setup.py`

### API Server
- **Start FastAPI server**: `python api_server.py`
- **Alternative simple API**: `python simple_api.py`

### Frontend
- **Start Node.js frontend** (from frontend/ directory): `npm start`
- **Development mode**: `npm run dev`

### Testing
- **Run API tests**: `python tests/test_api.py`
- **Comprehensive API tests**: `python tests/comprehensive_api_tests.py`
- **Clean architecture tests**: `python tests/test_clean_arch.py`
- **CID integration tests**: `python test_cid_integration.py`

### Dependencies
- **Install Python dependencies**: `pip install -r requirements.txt`
- **Install Node.js dependencies** (in frontend/): `npm install`

## Architecture Overview

This is a clean architecture TXT2SQL system for Brazilian SUS (healthcare) data that follows SOLID principles.

### Core Components

**Main Entry Points:**
- `txt2sql_agent_clean.py` - Primary CLI interface with multiple modes
- `api_server.py` - FastAPI REST API server
- `frontend/` - Node.js Express web interface

**Clean Architecture Layers:**

1. **Application Layer** (`src/application/`)
   - `orchestrator/text2sql_orchestrator.py` - Main coordinator that orchestrates all services
   - `container/dependency_injection.py` - DI container managing service dependencies
   - `services/` - 9 specialized services following Single Responsibility Principle:
     - `database_connection_service.py` - Database connection management with SQLite/LangChain integration
     - `llm_communication_service.py` - LLM communication (Ollama/local models) with retry logic
     - `schema_introspection_service.py` - Database schema analysis and context generation
     - `user_interface_service.py` - CLI/API interface handling (interactive/basic modes)
     - `error_handling_service.py` - Centralized error management with categorization
     - `query_processing_service.py` - SQL query processing using LangChain SQL Agent
     - `conversational_response_service.py` - Multi-LLM conversational response generation
     - `conversational_llm_service.py` - Specialized conversational LLM service
     - `sus_prompt_template_service.py` - SUS domain-specific prompt templates

2. **Domain Layer** (`src/domain/`)
   - `entities/` - Core business entities (patient, diagnosis, procedure, query_result)
   - `value_objects/` - Immutable value objects (diagnosis_code, municipality_code, patient_age)
   - `services/` - Domain services (CID semantic search)
   - `repositories/` - Repository interfaces
   - `exceptions/` - Custom domain exceptions

3. **Infrastructure Layer** (`src/infrastructure/`)
   - `repositories/` - Concrete repository implementations (SQLite CID repository)

### Key Design Patterns

- **Dependency Injection**: All services are injected through the DI container
- **Repository Pattern**: Database access abstracted through repository interfaces
- **Service Layer**: Business logic encapsulated in specialized services
- **Factory Pattern**: Service creation managed by factories

### Data Sources

- **Primary Database**: `sus_database.db` (SQLite) - 24,485 SUS patient records
- **CID-10 Data**: `data/cid10*.csv` files for diagnosis code lookup
- **Additional Data**: Various CSV files in `data/` directory for healthcare categories

### LLM Integration

- **Primary LLM Provider**: Ollama (local)
- **Default Model**: llama3
- **Fallback Models**: mistral, other local models
- **Communication**: Through `LLMCommunicationService` with retry logic and error handling

### Testing Strategy

- Multiple test files in `tests/` directory covering:
  - API endpoints (`test_api.py`)
  - Clean architecture components (`test_clean_arch.py`)
  - Conversational flows (`test_conversational*.py`)
  - Integration tests (`test_cid_integration.py`)

### Configuration

- Service configuration through `ServiceConfig` dataclass
- Orchestrator configuration through `OrchestratorConfig`
- Environment variables supported for API settings
- Database path configurable via command line arguments

### Frontend Integration

- Express.js server with Python bridge communication
- Real-time query processing through API calls
- Professional healthcare-focused UI design
- Rate limiting and security middleware

### Error Handling

- Centralized error handling through `ErrorHandlingService`
- Categorized error types with appropriate user messaging
- Comprehensive logging with rotation support
- Graceful degradation for LLM communication failures

## Complete System Flow and Logic

### Data Flow: From User Input to SQL Execution

**Step 1: Input Reception**
- **CLI Mode**: `UserInterfaceService` captures user input in interactive or basic mode
- **API Mode**: FastAPI endpoints (`/query`) receive JSON requests with CORS support
- **Web Interface**: JavaScript client sends AJAX requests through Express.js server

**Step 2: Request Orchestration**
- `Text2SQLOrchestrator.process_single_query()` coordinates the entire flow
- Input validation and sanitization (length limits, safety checks)
- `QueryRequest` object creation with metadata (timestamp, session info)
- Dependency injection ensures all services are properly initialized

**Step 3: Schema Context Generation**
- `SchemaIntrospectionService` analyzes the SQLite database structure
- Generates comprehensive schema context including:
  - Table definitions with column types and constraints
  - Sample data from each table for context
  - SUS-specific healthcare domain information
  - Geographic data context (Brazilian cities, coordinates)

**Step 4: Natural Language to SQL Conversion**
- `QueryProcessingService` creates enhanced prompts with schema context
- **LangChain SQL Agent** processes natural language queries using:
  - SQLDatabase wrapper around SQLite connection
  - Ollama LLM (default: llama3) for SQL generation
  - SUS-specific prompt templates for healthcare domain
- **Case sensitivity fixes** applied automatically for Brazilian city names
- **Query validation** ensures SQL safety and prevents injection

**Step 5: SQL Execution and Results**
- `DatabaseConnectionService` executes validated SQL queries
- **Direct SQLite execution** with error handling and timeout protection
- Result parsing and formatting into structured `QueryResult` objects
- Statistical data collection (query count, execution time)

**Step 6: Response Generation**
- **Standard Response**: SQL query results with metadata
- **Conversational Response** (optional): `ConversationalResponseService` uses secondary LLM to generate user-friendly explanations
- **Multi-format support**: JSON for API, formatted text for CLI, structured data for web

**Step 7: Error Handling and Recovery**
- `ErrorHandlingService` categorizes and handles all exceptions:
  - Database connection errors
  - LLM communication failures
  - SQL execution errors
  - Network and timeout issues
- **Graceful degradation** with fallback mechanisms
- **User-friendly error messages** with troubleshooting suggestions

### Service Interaction Architecture

```
Text2SQLOrchestrator (Central Coordinator)
│
├── DependencyContainer (Manages all service instances)
│   │
│   ├── DatabaseConnectionService
│   │   ├── SQLite connection management
│   │   ├── LangChain SQLDatabase wrapper
│   │   └── Connection pooling and reuse
│   │
│   ├── LLMCommunicationService
│   │   ├── Ollama API communication
│   │   ├── Model management (llama3, mistral)
│   │   ├── Retry logic with exponential backoff
│   │   └── Timeout and error handling
│   │
│   ├── SchemaIntrospectionService
│   │   ├── Database schema analysis
│   │   ├── Table structure documentation
│   │   ├── Sample data extraction
│   │   └── Context formatting for LLM
│   │
│   ├── QueryProcessingService
│   │   ├── LangChain SQL Agent integration
│   │   ├── Prompt engineering with schema context
│   │   ├── SQL query generation and validation
│   │   └── Case sensitivity handling
│   │
│   ├── UserInterfaceService
│   │   ├── CLI interface management (interactive/basic)
│   │   ├── Input/output formatting
│   │   ├── Session management
│   │   └── Progress indicators
│   │
│   ├── ErrorHandlingService
│   │   ├── Exception categorization
│   │   ├── Error logging and rotation
│   │   ├── User-friendly error messages
│   │   └── Recovery mechanisms
│   │
│   └── ConversationalResponseService
│       ├── Secondary LLM for explanations
│       ├── Multi-LLM fallback chain
│       ├── Response formatting
│       └── Context-aware explanations
│
└── Configuration Management
    ├── ServiceConfig (Database, LLM, UI settings)
    ├── OrchestratorConfig (Limits, timeouts, features)
    └── Environment variables integration
```

### Key Technical Decisions and Patterns

**Clean Architecture Implementation:**
- **Dependency Inversion**: All services depend on abstractions, not concretions
- **Single Responsibility**: Each service has one clearly defined purpose
- **Open/Closed Principle**: New services can be added without modifying existing code
- **Interface Segregation**: Focused interfaces for specific functionality
- **Dependency Injection**: All dependencies injected through the container

**Performance Optimizations:**
- **Connection Reuse**: Database connections are pooled and reused
- **Schema Caching**: Schema context is cached to avoid repeated introspection
- **Async Support**: FastAPI endpoints use async/await for better concurrency
- **Batch Processing**: Multiple queries can be processed efficiently

**Healthcare Domain Specialization:**
- **SUS Data Focus**: Specialized for Brazilian healthcare system data
- **CID-10 Integration**: Diagnostic code semantic search and validation
- **Geographic Context**: Brazilian city and coordinate data handling
- **Medical Terminology**: Healthcare-specific prompt templates and validation

**Error Resilience:**
- **Retry Mechanisms**: Automatic retry for transient failures
- **Fallback Chains**: Multiple LLM models for redundancy
- **Graceful Degradation**: System continues operation with reduced functionality
- **Comprehensive Logging**: Detailed error tracking with rotation

**Security Considerations:**
- **SQL Injection Prevention**: Query validation and parameterization
- **Input Sanitization**: All user inputs are validated and sanitized
- **CORS Configuration**: Proper cross-origin resource sharing setup
- **Rate Limiting**: API endpoints have built-in rate limiting

This system demonstrates enterprise-level clean architecture principles applied to a healthcare AI application, with comprehensive error handling, performance optimization, and domain-specific specialization for Brazilian SUS data analysis.