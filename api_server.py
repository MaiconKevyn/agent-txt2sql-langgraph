#!/usr/bin/env python3
"""
FastAPI Server for TXT2SQL Agent - Clean Architecture
Provides REST API endpoints for the text-to-SQL functionality
"""

import sys
import os
from typing import Dict, Any, Optional
from datetime import datetime
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.application.config.simple_config import (
    ApplicationConfig,
    OrchestratorConfig
)
from src.application.orchestrator.text2sql_orchestrator import (
    Text2SQLOrchestrator
)
from src.application.services.user_interface_service import InterfaceType

# Global agent instance
agent: Optional[Text2SQLOrchestrator] = None

# FastAPI app
app = FastAPI(
    title="TXT2SQL API",
    description="Clean Architecture Text-to-SQL API for SUS Healthcare Data",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware for web frontend
allowed_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173,http://127.0.0.1:3000,http://0.0.0.0:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Request/Response models
class QueryRequest(BaseModel):
    question: str
    model: str = "llama3"

class QueryResponse(BaseModel):
    success: bool
    question: str
    sql_query: Optional[str] = None
    results: Optional[Any] = None
    row_count: Optional[int] = None
    execution_time: Optional[float] = None
    error_message: Optional[str] = None
    response: Optional[str] = None  # Conversational response
    timestamp: str

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    services: Dict[str, Any]

class SchemaResponse(BaseModel):
    schema: str
    timestamp: str

def initialize_agent(model_name: str = None) -> Text2SQLOrchestrator:
    """Initialize the clean architecture agent"""
    # Use simple_config as base configuration
    base_config = ApplicationConfig()
    
    if model_name is None:
        model_name = os.getenv("LLM_MODEL", base_config.llm_model)
    
    app_config = ApplicationConfig(
        database_type=os.getenv("DATABASE_TYPE", base_config.database_type),
        database_path=os.getenv("DATABASE_PATH", base_config.database_path),
        llm_provider=os.getenv("LLM_PROVIDER", base_config.llm_provider),
        llm_model=model_name,
        llm_temperature=float(os.getenv("LLM_TEMPERATURE", str(base_config.llm_temperature))),
        llm_timeout=int(os.getenv("LLM_TIMEOUT", str(base_config.llm_timeout))),
        llm_max_retries=int(os.getenv("LLM_MAX_RETRIES", str(base_config.llm_max_retries))),
        llm_device=os.getenv("LLM_DEVICE", base_config.llm_device),
        llm_load_in_8bit=os.getenv("LLM_LOAD_IN_8BIT", str(base_config.llm_load_in_8bit)).lower() == "true",
        llm_load_in_4bit=os.getenv("LLM_LOAD_IN_4BIT", str(base_config.llm_load_in_4bit)).lower() == "true",
        schema_type=os.getenv("SCHEMA_TYPE", base_config.schema_type),
        ui_type=os.getenv("UI_TYPE", base_config.ui_type),
        interface_type=InterfaceType.CLI_BASIC,
        error_handling_type=os.getenv("ERROR_HANDLING_TYPE", base_config.error_handling_type),
        enable_error_logging=os.getenv("ENABLE_ERROR_LOGGING", str(base_config.enable_error_logging)).lower() == "true",
        query_processing_type=os.getenv("QUERY_PROCESSING_TYPE", base_config.query_processing_type)
    )
    
    orchestrator_config = OrchestratorConfig(
        max_query_length=int(os.getenv("MAX_QUERY_LENGTH", "1000")),
        enable_query_history=os.getenv("ENABLE_QUERY_HISTORY", "true").lower() == "true",
        enable_statistics=os.getenv("ENABLE_STATISTICS", "true").lower() == "true",
        session_timeout=int(os.getenv("SESSION_TIMEOUT", "3600")),
        enable_conversational_response=os.getenv("ENABLE_CONVERSATIONAL_RESPONSE", "true").lower() == "true",
        conversational_fallback=os.getenv("CONVERSATIONAL_FALLBACK", "true").lower() == "true",
        enable_query_routing=os.getenv("ENABLE_QUERY_ROUTING", "true").lower() == "true",
        routing_confidence_threshold=float(os.getenv("ROUTING_CONFIDENCE_THRESHOLD", "0.7")),
        # Simple Query Decomposition Configuration
        enable_query_decomposition=os.getenv("ENABLE_QUERY_DECOMPOSITION", "true").lower() == "true",
        decomposition_complexity_threshold=int(os.getenv("DECOMPOSITION_COMPLEXITY_THRESHOLD", "2")),
        decomposition_timeout_seconds=float(os.getenv("DECOMPOSITION_TIMEOUT_SECONDS", "60.0")),
        decomposition_fallback_enabled=os.getenv("DECOMPOSITION_FALLBACK_ENABLED", "true").lower() == "true",
        decomposition_debug_mode=os.getenv("DECOMPOSITION_DEBUG_MODE", "false").lower() == "true"
    )
    
    return Text2SQLOrchestrator(app_config, orchestrator_config)

@app.on_event("startup")
async def startup_event():
    """Initialize agent on startup"""
    global agent
    try:
        # Get configuration details for logging - use simple_config as default
        config = ApplicationConfig()
        llm_provider = os.getenv("LLM_PROVIDER", config.llm_provider)
        llm_model = os.getenv("LLM_MODEL", config.llm_model)
        
        print(f"🤖 Initializing LLM: {llm_model} ({llm_provider.title()})")
        
        agent = initialize_agent()
        
        # Get detailed model information
        try:
            model_info = agent._llm_service.get_model_info()
            print("✅ TXT2SQL Agent initialized successfully")
            print()
            print("📊 Model Information:")
            print(f"   🔸 Provider: {model_info.get('provider', 'Unknown')}")
            print(f"   🔸 Model: {model_info.get('model_name', 'Unknown')}")
            
            # Show device info if available
            if 'device' in model_info:
                device_info = model_info['device']
                if 'cuda' in str(device_info).lower():
                    print(f"   🔸 Device: {device_info} 🚀")
                else:
                    print(f"   🔸 Device: {device_info}")
            
            # Show quantization info for HuggingFace models
            if model_info.get('provider') == 'HuggingFace':
                if model_info.get('load_in_4bit'):
                    print("   🔸 Quantization: 4-bit (enabled) 💾")
                elif model_info.get('load_in_8bit'):
                    print("   🔸 Quantization: 8-bit (enabled) 💾")
                else:
                    print("   🔸 Quantization: Full precision")
                
                if model_info.get('cuda_available'):
                    print("   🔸 CUDA: Available ⚡")
                else:
                    print("   🔸 CUDA: Not available")
            
            # Show availability status
            status_icon = "✅" if model_info.get('available', False) else "❌"
            print(f"   🔸 Status: Available {status_icon}")
            print()
            
        except Exception as model_info_error:
            print("✅ TXT2SQL Agent initialized successfully")
            print(f"⚠️ Could not retrieve detailed model info: {str(model_info_error)}")
            
    except Exception as e:
        print(f"❌ Failed to initialize agent: {str(e)}")
        raise

@app.get("/", response_class=HTMLResponse)
async def root():
    """Simple HTML interface"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>TXT2SQL API</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .container { max-width: 800px; margin: 0 auto; }
            .query-box { width: 100%; padding: 10px; margin: 10px 0; border: 1px solid #ccc; border-radius: 4px; }
            .button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
            .button:hover { background: #0056b3; }
            .result { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 4px; background: #f9f9f9; }
            .error { border-color: #dc3545; background: #f8d7da; }
            .success { border-color: #28a745; background: #d4edda; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🗃️ TXT2SQL API Interface</h1>
            <p>Ask questions about the healthcare database in natural language!</p>
            
            <div>
                <input type="text" id="question" class="query-box" placeholder="Enter your question here..." />
                <button class="button" onclick="queryAPI()">Send Query</button>
                <button class="button" onclick="getSchema()">Show Schema</button>
            </div>
            
            <div id="result"></div>
            
            <h3>Example Questions:</h3>
            <ul>
                <li><a href="#" onclick="askExample('How many patients are there?')">How many patients are there?</a></li>
                <li><a href="#" onclick="askExample('What is the average age of patients?')">What is the average age of patients?</a></li>
                <li><a href="#" onclick="askExample('How many deaths occurred?')">How many deaths occurred?</a></li>
                <li><a href="#" onclick="askExample('Show patients from Porto Alegre')">Show patients from Porto Alegre</a></li>
            </ul>
            
            <h3>API Documentation:</h3>
            <p><a href="/docs" target="_blank">Swagger UI</a> | <a href="/redoc" target="_blank">ReDoc</a></p>
        </div>
        
        <script>
            async function queryAPI() {
                const question = document.getElementById('question').value;
                if (!question.trim()) return;
                
                const resultDiv = document.getElementById('result');
                resultDiv.innerHTML = '<div class="result">Processing...</div>';
                
                try {
                    const response = await fetch('/query', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ question: question })
                    });
                    
                    const data = await response.json();
                    
                    if (data.success) {
                        resultDiv.innerHTML = `
                            <div class="result success">
                                <h4>✅ Success</h4>
                                <p><strong>Question:</strong> ${data.question}</p>
                                <p><strong>Result:</strong> ${JSON.stringify(data.results)}</p>
                                <p><strong>Rows:</strong> ${data.row_count}</p>
                                <p><strong>Time:</strong> ${data.execution_time?.toFixed(2)}s</p>
                                <details>
                                    <summary>SQL Query</summary>
                                    <pre>${data.sql_query}</pre>
                                </details>
                            </div>
                        `;
                    } else {
                        resultDiv.innerHTML = `
                            <div class="result error">
                                <h4>❌ Error</h4>
                                <p><strong>Question:</strong> ${data.question}</p>
                                <p><strong>Error:</strong> ${data.error_message}</p>
                            </div>
                        `;
                    }
                } catch (error) {
                    resultDiv.innerHTML = `<div class="result error">Network error: ${error.message}</div>`;
                }
            }
            
            async function getSchema() {
                const resultDiv = document.getElementById('result');
                resultDiv.innerHTML = '<div class="result">Loading schema...</div>';
                
                try {
                    const response = await fetch('/schema');
                    const data = await response.json();
                    
                    resultDiv.innerHTML = `
                        <div class="result">
                            <h4>📊 Database Schema</h4>
                            <pre>${data.schema}</pre>
                        </div>
                    `;
                } catch (error) {
                    resultDiv.innerHTML = `<div class="result error">Error loading schema: ${error.message}</div>`;
                }
            }
            
            function askExample(question) {
                document.getElementById('question').value = question;
                queryAPI();
            }
            
            // Enter key support
            document.getElementById('question').addEventListener('keypress', function(e) {
                if (e.key === 'Enter') queryAPI();
            });
        </script>
    </body>
    </html>
    """

@app.post("/query", response_model=QueryResponse)
async def query_database(request: QueryRequest):
    """Process natural language query"""
    if not agent:
        raise HTTPException(status_code=500, detail="Agent not initialized")
    
    if not request.question or not request.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    
    try:
        # Use conversational query method for proper response formatting
        result = agent.process_conversational_query(request.question)
        
        return QueryResponse(
            success=result["success"],
            question=result["question"],
            sql_query=result["metadata"].get("sql_query") if result.get("metadata") else None,
            results=result["metadata"].get("results") if result.get("metadata") else None,
            row_count=result["metadata"].get("row_count") if result.get("metadata") else None,
            execution_time=result["execution_time"],
            error_message=result["error_message"],
            timestamp=result["timestamp"],
            response=result["response"]  # This is the conversational response
        )
    except Exception as e:
        return QueryResponse(
            success=False,
            question=request.question,
            error_message=f"Internal server error: {str(e)}",
            timestamp=datetime.now().isoformat()
        )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
        if not agent:
            return HealthResponse(
                status="unhealthy",
                timestamp=datetime.now().isoformat(),
                services={"agent": "not_initialized"}
            )
        
        health_status = agent.health_check()
        return HealthResponse(
            status=health_status["status"],
            timestamp=datetime.now().isoformat(),
            services=health_status["services"]
        )
    except Exception as e:
        return HealthResponse(
            status="error",
            timestamp=datetime.now().isoformat(),
            services={"error": str(e)}
        )

@app.get("/schema", response_model=SchemaResponse)
async def get_schema(table: Optional[str] = None):
    """Get database schema information"""
    if not agent:
        raise HTTPException(status_code=503, detail="Agent not initialized")
    
    try:
        schema_service = agent.get_schema_introspection_service()
        
        if table:
            # Get specific table schema
            table_info = schema_service.get_table_info(table)
            schema_text = f"""
TABELA: {table_info.name}
Total de registros: {table_info.row_count:,}

COLUNAS:
"""
            for col in table_info.columns:
                pk_indicator = " (PK)" if col.primary_key else ""
                null_indicator = " (NULL)" if col.nullable else " (NOT NULL)"
                schema_text += f"- {col.name} ({col.type}){pk_indicator}{null_indicator}\n"
            
            # Format sample data as HTML table (only 10 records)
            if table_info.sample_data and len(table_info.sample_data) > 0:
                schema_text += f"\nDADOS DE EXEMPLO ({len(table_info.sample_data)} registros de amostra):\n"
                
                # Get column names for table header
                column_names = [col.name for col in table_info.columns]
                
                # Create simple table without pagination
                schema_text += '<div class="schema-table-container">\n'
                
                # Add simple records counter header
                schema_text += f'''<div class="records-counter">
                    <div class="counter-info">
                        <i class="fas fa-database"></i>
                        <span class="total-records">Amostra: {len(table_info.sample_data)} registros (Total: {table_info.row_count:,})</span>
                    </div>
                </div>\n'''
                
                # Filter bar above table
                schema_text += '<div class="filter-bar">\n'
                for i, col_name in enumerate(column_names):
                    # Create filter for each column
                    placeholder = f"Filtrar {col_name.lower()}"
                    if 'id' in col_name.lower():
                        placeholder = f"ID"
                    elif 'codigo' in col_name.lower() or 'code' in col_name.lower():
                        placeholder = f"Código"
                    elif 'descr' in col_name.lower() or 'desc' in col_name.lower():
                        placeholder = f"Descrição"
                    elif 'data' in col_name.lower() or 'date' in col_name.lower():
                        placeholder = f"Data"
                    elif 'nome' in col_name.lower() or 'name' in col_name.lower():
                        placeholder = f"Nome"
                    
                    schema_text += f'''<div class="filter-column" data-column="{i}">
                        <label class="filter-label">{col_name}</label>
                        <input type="text" 
                               class="column-filter" 
                               data-column="{i}" 
                               placeholder="{placeholder}"
                               autocomplete="off"
                               spellcheck="false">
                    </div>'''
                
                schema_text += '</div>\n'
                
                # Scrollable table wrapper
                schema_text += '<div class="table-scroll-wrapper">\n'
                schema_text += '<table class="schema-table" id="schema-data-table">\n'
                
                # Simple table header
                schema_text += '<thead>\n<tr>\n'
                for col_name in column_names:
                    schema_text += f'<th>{col_name}</th>\n'
                schema_text += '</tr>\n</thead>\n'
                
                # Table body
                schema_text += '<tbody>\n'
                for sample in table_info.sample_data:
                    if isinstance(sample, dict):
                        schema_text += '<tr>\n'
                        for col_name in column_names:
                            value = sample.get(col_name, 'NULL')
                            # Format value
                            if value is None:
                                value = '<span class="null-value">NULL</span>'
                            else:
                                value = str(value)
                                # Escape HTML special characters first
                                escaped_value = value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
                                
                                if len(value) > 100:
                                    # Create tooltip with full text
                                    value = f'<span title="{escaped_value}" class="truncated-value">{escaped_value[:97]}...</span>'
                                else:
                                    value = escaped_value
                            
                            schema_text += f'<td>{value}</td>\n'
                        schema_text += '</tr>\n'
                    else:
                        # Fallback for non-dict format
                        escaped_sample = str(sample).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                        if len(escaped_sample) > 100:
                            escaped_sample = escaped_sample[:97] + "..."
                        schema_text += f'<tr><td colspan="{len(column_names)}">{escaped_sample}</td></tr>\n'
                
                schema_text += '</tbody>\n'
                schema_text += '</table>\n'
                schema_text += '</div>\n'  # Close table-scroll-wrapper
                schema_text += '</div>\n'  # Close schema-table-container
            else:
                schema_text += "\nDADOS DE EXEMPLO: Nenhum dado disponível\n"
            
        else:
            # Get full schema context
            schema_context = schema_service.get_schema_context()
            schema_text = schema_context.formatted_context
        
        return SchemaResponse(
            schema=schema_text,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema error: {str(e)}")

@app.get("/schema/tables")
async def get_available_tables():
    """Get list of available tables"""
    if not agent:
        raise HTTPException(status_code=503, detail="Agent not initialized")
    
    try:
        schema_service = agent.get_schema_introspection_service()
        tables = schema_service.get_table_names()
        
        # Filter to only show main tables
        main_tables = [table for table in tables if table in ['sus_data', 'cid_detalhado']]
        
        return {
            "tables": main_tables,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tables error: {str(e)}")

@app.get("/agent-health")
async def agent_health_check():
    """Agent health check endpoint"""
    try:
        if not agent:
            return {
                "agent_status": "offline",
                "timestamp": datetime.now().isoformat()
            }
        
        health_status = agent.health_check()
        return {
            "agent_status": "online" if health_status["status"] == "healthy" else "offline",
            "timestamp": datetime.now().isoformat(),
            "services": health_status["services"]
        }
    except Exception as e:
        return {
            "agent_status": "offline",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

@app.get("/models")
async def get_available_models():
    """Get available LLM models"""
    return {
        "models": ["llama3", "mistral"],
        "default": "llama3",
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    print("🚀 Starting TXT2SQL API Server...")
    print("📍 Make sure Ollama is running with llama3 or mistral model")
    print("🌐 API will be available at http://localhost:8000")
    print("📚 Documentation at http://localhost:8000/docs")
    print("⏹️  Press Ctrl+C to stop")
    
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    print(f"🌐 API will be available at http://{host}:{port}")
    print(f"📚 Documentation at http://{host}:{port}/docs")
    
    uvicorn.run(
        "api_server:app",
        host=host,
        port=port,
        reload=True,
        log_level="info"
    )