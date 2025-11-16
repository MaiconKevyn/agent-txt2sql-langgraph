"""
FastAPI REST API for Text-to-SQL Agent
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import subprocess
import time
from datetime import datetime

app = FastAPI(title="DATASUS Text-to-SQL API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    query: str
    include_sql: bool = True

class QueryResponse(BaseModel):
    status: str
    answer: str
    sql: Optional[str] = None
    execution_time: float
    timestamp: str

@app.post("/api/v1/query")
async def process_query(request: QueryRequest):
    start_time = time.time()
    
    try:
        cmd = ["python", "src/interfaces/cli/agent.py", "--query", request.query]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        lines = result.stdout.split('\n')
        answer = "Resposta não disponível"
        sql = None
        
        for i, line in enumerate(lines):
            if "Processando consulta:" in line and i + 1 < len(lines):
                answer = lines[i + 1].strip()
            if "SQL:" in line:
                sql = line.split("SQL:", 1)[1].strip()
        
        return QueryResponse(
            status="success",
            answer=answer,
            sql=sql if request.include_sql else None,
            execution_time=round(time.time() - start_time, 2),
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        return QueryResponse(
            status="error",
            answer=f"Erro: {str(e)}",
            sql=None,
            execution_time=round(time.time() - start_time, 2),
            timestamp=datetime.now().isoformat()
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
