# 🏗️ Fluxo Arquitetural Completo - TXT2SQL System

## 🌊 **Fluxo Completo de Execução**

```mermaid
graph TD
    User["👤 USER INPUT<br/>Quantos pacientes?"] 
    
    %% Entry Point
    Orchestrator["🎭 TEXT2SQL ORCHESTRATOR<br/>📍 Entry Point<br/>• Route queries<br/>• Initialize services<br/>• Return responses"]
    
    %% Classification
    Classifier["🔍 QUERY CLASSIFIER<br/>📊 Analyze Intent<br/>• Database query: 90%<br/>• Conversational: 10%"]
    
    %% Main Coordinator
    QueryProcessor["🎯 QUERY PROCESSING SERVICE<br/>🎭 Main Coordinator<br/>• Strategy management<br/>• Fallback handling<br/>• Error recovery"]
    
    %% Specialized Services
    SQLGen["🔧 SQL GENERATION SERVICE<br/>🎨 SQL Specialist<br/>• Create prompts<br/>• Extract SQL<br/>• Clean & fix"]
    
    QueryExec["⚡ QUERY EXECUTION SERVICE<br/>🛡️ Security Specialist<br/>• Validate SQL<br/>• Execute safely<br/>• Return results"]
    
    %% External Services  
    LLMService["🤖 LLM COMMUNICATION<br/>🔗 External Interface<br/>• Ollama integration<br/>• Model management<br/>• Prompt handling"]
    
    DBService["🏗️ DATABASE CONNECTION<br/>💾 Infrastructure<br/>• SQLite management<br/>• Connection pooling<br/>• Health checks"]
    
    %% Final Response
    ConvResponse["💬 CONVERSATIONAL RESPONSE<br/>📝 Natural Language<br/>• Format results<br/>• Generate response<br/>• User-friendly"]
    
    %% External Systems
    Ollama["🦙 OLLAMA LLM<br/>🧠 AI Models<br/>• qwen3 (primary)<br/>• llama3 (fallback)<br/>• mistral (conv)"]
    
    Database["🗄️ SQLITE SUS DATABASE<br/>📊 58,655+ Records<br/>• Patient data<br/>• Medical codes<br/>• Geographic info"]
    
    %% Main Flow
    User --> Orchestrator
    Orchestrator --> Classifier
    Classifier --> QueryProcessor
    QueryProcessor --> SQLGen
    SQLGen --> LLMService
    LLMService --> Ollama
    Ollama --> LLMService
    LLMService --> SQLGen
    SQLGen --> QueryExec
    QueryExec --> DBService
    DBService --> Database
    Database --> DBService
    DBService --> QueryExec
    QueryExec --> QueryProcessor
    QueryProcessor --> ConvResponse
    ConvResponse --> Orchestrator
    Orchestrator --> User
    
    %% Fallback Flows
    QueryProcessor -.->|"❌ Primary fails"| QueryProcessor
    QueryProcessor -.->|"🔄 Retry strategy"| SQLGen
    
    %% Styling
    classDef entryPoint fill:#e1f5fe,stroke:#01579b,stroke-width:3px,color:#000000
    classDef coordinator fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000000
    classDef specialist fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px,color:#000000
    classDef external fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000000
    classDef infrastructure fill:#fce4ec,stroke:#880e4f,stroke-width:2px,color:#000000
    
    class User,Orchestrator entryPoint
    class Classifier,QueryProcessor coordinator
    class SQLGen,QueryExec,ConvResponse specialist
    class LLMService,Ollama external
    class DBService,Database infrastructure
```

## 📋 **Detalhamento por Camada**

### 🎯 **1. Text2SQL Orchestrator (Entry Point)**
**Localização:** `src/application/orchestrator/text2sql_orchestrator.py`

**Função:**
- 📥 Recebe input do usuário (CLI/API)
- 🔄 Inicializa todos os serviços
- 🎭 Classifica tipo de query (database vs conversational)
- 📊 Delega para QueryProcessingService
- 📤 Retorna resposta formatada

### 🎯 **2. Query Processing Service (Coordenador Principal)**
**Localização:** `src/application/services/query_processing_service.py`

**Estratégias de Processamento:**

```mermaid
graph TD
    Start([🚀 User Query Received])
    
    %% Strategy 1: Primary Method
    Primary["🎯 Strategy 1: Direct LLM Primary<br/>🔧 Best Performance<br/>• Use primary model (qwen3)<br/>• Standard prompt template<br/>• Full validation pipeline"]
    
    PrimarySteps["📋 Primary Steps:<br/>1️⃣ SQLGenerationService<br/>2️⃣ SQL Validation<br/>3️⃣ QueryExecutionService"]
    
    Success([✅ Success: Return Result])
    
    %% Strategy 2: Llama3 Fallback
    Fallback1["🦙 Strategy 2: Llama3 Fallback<br/>🔄 Alternative Model<br/>• Switch to llama3 model<br/>• Specialized fallback prompt<br/>• Include error context"]
    
    %% Strategy 3: Error-Aware
    Fallback2["🧠 Strategy 3: Error-Aware Retry<br/>💡 Enhanced Prompting<br/>• Critical instructions added<br/>• Previous error context<br/>• Specific corrections applied"]
    
    %% Strategy 4: Simplified
    Fallback3["📊 Strategy 4: Simplified Approach<br/>🎯 Last Resort<br/>• Simplify complex query<br/>• Remove problematic elements<br/>• Basic retry attempt"]
    
    %% Final Failure
    Failure([❌ All Strategies Failed])
    
    %% Flow
    Start --> Primary
    Primary --> PrimarySteps
    PrimarySteps --> Success
    PrimarySteps -.->|❌ Fails| Fallback1
    
    Fallback1 --> Success
    Fallback1 -.->|❌ Fails| Fallback2
    
    Fallback2 --> Success
    Fallback2 -.->|❌ Fails| Fallback3
    
    Fallback3 --> Success
    Fallback3 -.->|❌ Fails| Failure
    
    %% Styling
    classDef primary fill:#e8f5e8,stroke:#2e7d32,stroke-width:3px,color:#000000
    classDef fallback fill:#fff3e0,stroke:#f57c00,stroke-width:2px,color:#000000
    classDef success fill:#e1f5fe,stroke:#0277bd,stroke-width:2px,color:#000000
    classDef failure fill:#ffebee,stroke:#c62828,stroke-width:2px,color:#000000
    classDef process fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000000
    
    class Start,Primary,PrimarySteps primary
    class Fallback1,Fallback2,Fallback3 fallback
    class Success success
    class Failure failure
```

### 🔧 **3. SQL Generation Service (Especialista)**
**Localização:** `src/application/services/sql_generation_service.py`

**Pipeline de Geração:**

```mermaid
graph LR
    Input["🗣️ User Query<br/>Natural Language<br/>'Quantos pacientes?'"]
    
    Prompt["🎨 Create Prompt<br/>Domain Expert<br/>• SUS context<br/>• Medical terms<br/>• SQL patterns"]
    
    LLM["🤖 Call LLM<br/>AI Model<br/>• qwen3/llama3<br/>• Specialized prompt<br/>• Generate response"]
    
    Extract["🔍 Extract SQL<br/>Regex Parse<br/>• Multiple patterns<br/>• Markdown cleanup<br/>• Multi-line support"]
    
    Clean["🧹 Clean & Fix<br/>Rules Apply<br/>• Remove comments<br/>• Fix case sensitivity<br/>• SQLite compatibility"]
    
    Output["✅ Valid SQL<br/>SQLite Ready<br/>'SELECT COUNT(*) FROM sus_data;'"]
    
    Input --> Prompt
    Prompt --> LLM
    LLM --> Extract
    Extract --> Clean
    Clean --> Output
    
    %% Styling
    classDef input fill:#e3f2fd,stroke:#1976d2,stroke-width:2px,color:#000000
    classDef process fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000000
    classDef output fill:#e8f5e8,stroke:#388e3c,stroke-width:2px,color:#000000
    
    class Input input
    class Prompt,LLM,Extract,Clean process
    class Output output
```

**Especializações:**
- 🎨 **Prompts SUS-específicos**: Contexto médico brasileiro
- 🔍 **Extração robusta**: Múltiplos padrões regex para SQL
- 🧹 **Limpeza inteligente**: Remove comentários problemáticos
- 🔧 **Correções SQLite**: YEAR() → strftime(), case sensitivity

### ⚡ **4. Query Execution Service (Especialista)**
**Localização:** `src/application/services/query_execution_service.py`

**Pipeline de Execução Segura:**

```mermaid
graph LR
    SQLInput["🔧 SQL Query<br/>From Generation<br/>'SELECT COUNT(*) FROM sus_data;'"]
    
    Security["🛡️ Security Validation<br/>Block Dangerous Operations<br/>• No DROP/DELETE<br/>• SQL injection check<br/>• Pattern analysis"]
    
    Execute["⚡ Database Execution<br/>SQLite Execution<br/>• Raw connection<br/>• Cursor execute<br/>• Fetch results"]
    
    Process["📊 Result Processing<br/>Rows to Dicts<br/>• Column mapping<br/>• Type conversion<br/>• Metadata collection"]
    
    Output["📋 Structured Output<br/>QueryResult with Metadata<br/>• Success status<br/>• Execution time<br/>• Row count<br/>• Error handling"]
    
    SQLInput --> Security
    Security --> Execute
    Execute --> Process
    Process --> Output
    
    %% Security Branches
    SecurityFail["❌ Security Failed<br/>Blocked Operation"]
    Security -.->|🚨 Dangerous SQL| SecurityFail
    
    %% Execution Branches
    ExecuteFail["❌ Execution Failed<br/>Database Error"]
    Execute -.->|💥 SQL Error| ExecuteFail
    
    %% Styling
    classDef input fill:#e3f2fd,stroke:#1976d2,stroke-width:2px,color:#000000
    classDef security fill:#ffebee,stroke:#d32f2f,stroke-width:2px,color:#000000
    classDef process fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000000
    classDef output fill:#e8f5e8,stroke:#388e3c,stroke-width:2px,color:#000000
    classDef error fill:#fff3e0,stroke:#f57c00,stroke-width:2px,color:#000000
    
    class SQLInput input
    class Security security
    class Execute,Process process
    class Output output
    class SecurityFail,ExecuteFail error
```

**Validações de Segurança:**
- 🚨 **SQL Injection**: Bloqueia palavras perigosas (DROP, DELETE, etc.)
- 🚨 **Date Arithmetic**: Força uso de JULIANDAY para cálculos
- 🚨 **Pattern Detection**: Detecta comentários suspeitos
- 🚨 **SELECT Only**: Permite apenas operações de leitura

### 🏗️ **5. Database Connection Service (Infrastructure)**
**Localização:** `src/infrastructure/database/connection_service.py`

**Tipos de Conexão:**
- 🔌 **LangChain SQLDatabase**: Para operations de alto nível
- ⚡ **Raw SQLite**: Para execução direta e performance
- 🧪 **Test Connections**: Para health checks isolados

## 🔄 **Exemplo de Execução Completa**

### **Input:** "Quantos pacientes existem?"

```mermaid
sequenceDiagram
    participant User as 👤 User
    participant Orch as 🎭 Orchestrator
    participant Class as 🔍 Classifier
    participant Proc as 🎯 QueryProcessor
    participant Gen as 🔧 SQLGenerator
    participant LLM as 🤖 LLM Service
    participant Exec as ⚡ QueryExecutor
    participant DB as 🗄️ Database
    participant Conv as 💬 Conversational

    User->>Orch: "Quantos pacientes existem?"
    
    Orch->>Class: Classify query type
    Class-->>Orch: "database_query" (90% confidence)
    
    Orch->>Proc: process_query(request)
    
    Note over Proc: Strategy: Direct LLM Primary
    
    Proc->>Gen: generate_sql_query(user_query)
    Gen->>LLM: send_prompt(specialized_prompt)
    LLM-->>Gen: SQL response
    Gen-->>Proc: "SELECT COUNT(*) FROM sus_data;"
    
    Proc->>Exec: execute_sql_query(sql)
    
    Note over Exec: Security validation ✅
    
    Exec->>DB: Execute query
    DB-->>Exec: Raw results: [(58655,)]
    Exec-->>Proc: QueryResult(success=True, results=[{"COUNT(*)": 58655}])
    
    Proc->>Conv: format_response(results)
    Conv-->>Proc: "Foram registrados 58.655 pacientes no SUS."
    
    Proc-->>Orch: Final response
    Orch-->>User: "Foram registrados 58.655 pacientes no SUS."
    
    Note over User,Conv: ✅ Total time: ~22 seconds
```

### **Detalhamento por Etapa:**

1. **🎭 Orchestrator**: Recebe query e inicializa workflow
2. **🔍 Classifier**: Identifica como "database_query" (90% confiança) 
3. **🎯 QueryProcessor**: Escolhe estratégia "Direct LLM Primary"
4. **🔧 SQLGenerator**: Cria prompt SUS-específico → chama LLM → extrai/limpa SQL
5. **⚡ QueryExecutor**: Valida segurança → executa no SQLite → processa resultados
6. **💬 Conversational**: Formata resposta em linguagem natural
7. **👤 User**: Recebe resposta final formatada

## 📊 **Métricas de Performance**

| Componente | Tempo Típico | Responsabilidade |
|------------|--------------|------------------|
| **Text2SQL Orchestrator** | ~0.1s | Inicialização e roteamento |
| **Query Classification** | ~0.5s | Análise de tipo de query |
| **SQL Generation** | ~10-15s | LLM call + processamento |
| **SQL Validation** | ~0.1s | Verificações de segurança |
| **Query Execution** | ~0.1s | Execução no SQLite |
| **Response Generation** | ~5-8s | LLM conversational call |
| **TOTAL** | ~16-24s | Pipeline completo |

## 🏗️ **Visão Arquitetural por Camadas**

```mermaid
graph TB
    subgraph "🌐 PRESENTATION LAYER"
        CLI["🖥️ CLI Interface<br/>txt2sql_agent_clean.py"]
        API["🌐 REST API<br/>api_server.py"] 
        WEB["💻 Web Interface<br/>frontend/"]
    end
    
    subgraph "🎭 APPLICATION LAYER"
        direction TB
        ORCH["🎭 Text2SQL Orchestrator<br/>Main Entry Point"]
        
        subgraph "📋 Coordinators"
            PROC["🎯 Query Processing Service<br/>Main Coordinator"]
            CLASS["🔍 Query Classification Service"]
        end
        
        subgraph "🔧 Specialists"
            SQLGEN["🔧 SQL Generation Service"]
            QUERYEXEC["⚡ Query Execution Service"]
            CONV["💬 Conversational Response Service"]
        end
        
        subgraph "🛠️ Support Services"
            LLM["🤖 LLM Communication Service"]
            SCHEMA["📊 Schema Introspection Service"]
            ERROR["🚨 Error Handling Service"]
            VALID["✅ SQL Validation Service"]
        end
    end
    
    subgraph "🧠 DOMAIN LAYER"
        direction TB
        subgraph "📦 Entities"
            ENTITIES["🏥 Medical Entities<br/>• Diagnosis<br/>• CID Chapter<br/>• Query Plans"]
        end
        
        subgraph "🔧 Domain Services"
            DOMSVC["🔍 CID Semantic Search<br/>Medical Logic"]
        end
        
        subgraph "📋 Repositories (Interfaces)"
            REPOS["📋 Repository Interfaces<br/>• ICIDRepository<br/>• Abstract Data Access"]
        end
    end
    
    subgraph "🏗️ INFRASTRUCTURE LAYER"
        direction TB
        subgraph "💾 Database"
            DBCONN["🔌 Database Connection Service"]
            DBREPO["🗄️ SQLite CID Repository"]
        end
        
        subgraph "🌐 External Services"
            OLLAMA["🦙 Ollama LLM Integration"]
            SQLITE["🗄️ SQLite SUS Database<br/>58,655+ Records"]
        end
    end
    
    %% Presentation to Application
    CLI --> ORCH
    API --> ORCH
    WEB --> ORCH
    
    %% Application Internal Flow
    ORCH --> CLASS
    ORCH --> PROC
    PROC --> SQLGEN
    PROC --> QUERYEXEC
    PROC --> CONV
    
    SQLGEN --> LLM
    QUERYEXEC --> VALID
    CLASS --> LLM
    CONV --> LLM
    
    PROC --> SCHEMA
    PROC --> ERROR
    
    %% Application to Domain
    SQLGEN -.-> DOMSVC
    QUERYEXEC -.-> REPOS
    SCHEMA -.-> REPOS
    
    %% Domain to Infrastructure
    REPOS --> DBREPO
    DOMSVC --> DBREPO
    
    %% Infrastructure Connections
    QUERYEXEC --> DBCONN
    SCHEMA --> DBCONN
    DBCONN --> SQLITE
    LLM --> OLLAMA
    
    %% Styling
    classDef presentation fill:#e3f2fd,stroke:#1976d2,stroke-width:2px,color:#000000
    classDef application fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000000
    classDef domain fill:#e8f5e8,stroke:#388e3c,stroke-width:2px,color:#000000
    classDef infrastructure fill:#fff3e0,stroke:#f57c00,stroke-width:2px,color:#000000
    
    class CLI,API,WEB presentation
    class ORCH,PROC,CLASS,SQLGEN,QUERYEXEC,CONV,LLM,SCHEMA,ERROR,VALID application
    class ENTITIES,DOMSVC,REPOS domain
    class DBCONN,DBREPO,OLLAMA,SQLITE infrastructure
```

## 🛡️ **Pontos de Segurança**

1. **SQL Injection Prevention** - QueryExecutionService
2. **Dangerous Operations Blocking** - Multiple validation layers  
3. **Data Leak Prevention** - Read-only operations only
4. **Input Sanitization** - SQL cleaning and validation
5. **Error Information Filtering** - Controlled error messages

## 🎯 **Benefícios da Arquitetura Modular**

1. **🧪 Testabilidade**: Cada serviço pode ser testado isoladamente
2. **🔧 Manutenibilidade**: Bug fixes limitados ao serviço específico
3. **📈 Escalabilidade**: Serviços podem ser otimizados independentemente
4. **🔄 Flexibilidade**: Fácil adição de novas estratégias ou validações
5. **📊 Observabilidade**: Métricas granulares por componente
6. **🛡️ Robustez**: Falha de um componente não quebra o sistema todo

## 📊 **Fluxo de Dados Detalhado**

```mermaid
flowchart TD
    subgraph "📥 INPUT LAYER"
        UserInput["👤 User Input<br/>'Quantos pacientes existem?'<br/>🔤 Natural Language"]
    end
    
    subgraph "🔄 PROCESSING PIPELINE"
        Request["📋 QueryRequest<br/>• user_query: string<br/>• session_id: optional<br/>• timestamp: datetime<br/>• context: dict"]
        
        Classification["🔍 Classification Result<br/>• type: 'database_query'<br/>• confidence: 0.90<br/>• reasoning: patterns"]
        
        SQLRaw["🤖 Raw LLM Response<br/>• model_output: text<br/>• length: ~1800 chars<br/>• format: mixed"]
        
        SQLClean["🔧 Clean SQL<br/>• query: 'SELECT COUNT(*) FROM sus_data;'<br/>• validated: true<br/>• safe: true"]
        
        DBResult["🗄️ Database Result<br/>• raw: [(58655,)]<br/>• columns: ['COUNT(*)']<br/>• rows: 1"]
        
        Structured["📊 Structured Result<br/>• results: [{\"COUNT(*)\": 58655}]<br/>• success: true<br/>• execution_time: 0.1s<br/>• row_count: 1"]
    end
    
    subgraph "📤 OUTPUT LAYER"
        NaturalResponse["💬 Natural Response<br/>'Foram registrados 58.655 pacientes no SUS.'<br/>🗣️ User-Friendly"]
        
        APIResponse["🌐 API Response<br/>• question: string<br/>• sql_query: string<br/>• results: array<br/>• metadata: object<br/>• timestamp: datetime"]
    end
    
    %% Data Flow
    UserInput --> Request
    Request --> Classification
    Classification --> SQLRaw
    SQLRaw --> SQLClean
    SQLClean --> DBResult
    DBResult --> Structured
    Structured --> NaturalResponse
    Structured --> APIResponse
    
    %% Data Transformations
    Request -.->|"🎨 Prompt Engineering"| SQLRaw
    SQLRaw -.->|"🧹 Cleaning & Validation"| SQLClean
    DBResult -.->|"📋 Row to Dict Conversion"| Structured
    Structured -.->|"💬 LLM Conversational"| NaturalResponse
    
    %% Styling
    classDef input fill:#e3f2fd,stroke:#1976d2,stroke-width:2px,color:#000000
    classDef process fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000000
    classDef output fill:#e8f5e8,stroke:#388e3c,stroke-width:2px,color:#000000
    
    class UserInput input
    class Request,Classification,SQLRaw,SQLClean,DBResult,Structured process
    class NaturalResponse,APIResponse output
```

## 🔄 **Estados dos Dados por Etapa**

| Etapa | Formato | Exemplo | Responsável |
|-------|---------|---------|-------------|
| **Input** | Natural Language | "Quantos pacientes existem?" | User Interface |
| **Request** | QueryRequest Object | `{user_query: str, session_id: str, ...}` | Orchestrator |
| **Classification** | Classification Result | `{type: "database_query", confidence: 0.90}` | Query Classifier |
| **LLM Response** | Raw Text | Mixed format with SQL embedded | LLM Service |
| **Clean SQL** | SQL String | `"SELECT COUNT(*) FROM sus_data;"` | SQL Generator |
| **DB Result** | Raw Tuples | `[(58655,)]` | Database |
| **Structured** | QueryResult | `{results: [{"COUNT(*)": 58655}], success: true}` | Query Executor |
| **Natural** | User Response | "Foram registrados 58.655 pacientes no SUS." | Conversational Service |
| **API** | JSON Response | Complete API response object | API Layer |

---

**🏆 Resultado:** Arquitetura limpa, maintível e robusta que preserva 100% da funcionalidade original com melhor organização, extensibilidade e **visualização completa através de diagramas Mermaid interativos**.