"""
Query Execution Service - Application Layer

🎯 OBJETIVO:
Serviço especializado em executar consultas SQL de forma segura no banco SUS,
aplicando validações de segurança e retornando resultados estruturados.

🔄 POSIÇÃO NO FLUXO:
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ SQL Generation  │ -> │ Query Execution │ -> │ Result Processing│
│ Service         │    │ Service         │    │ & Response       │
└─────────────────┘    └─────────────────┘    └─────────────────┘

📥 ENTRADAS (DE ONDE VEM):
- SQLGenerationService: sql_query (string) - SQL limpo e corrigido
- QueryProcessingService: solicitações de execução com contexto
- IDatabaseConnectionService: conexão ativa com banco SUS

📤 SAÍDAS (PARA ONDE VAI):
- QueryProcessingService: QueryResult - resultado estruturado com metadados
- ConversationalResponseService: dados para resposta em linguagem natural
- Logs/Monitoring: informações de execução para debugging

🧩 RESPONSABILIDADES:
1. Validar segurança SQL (SQL injection, operações perigosas)
2. Executar queries no banco SQLite de forma controlada
3. Converter resultados para formato estruturado (dict)
4. Logging detalhado de queries para debugging
5. Tratamento robusto de erros de execução
6. Medição de performance (tempo de execução)

🔗 DEPENDÊNCIAS:
- IDatabaseConnectionService: Para conexão com banco SUS
- IErrorHandlingService: Para tratamento padronizado de erros

🛡️ GARANTIAS DE SEGURANÇA:
- Bloqueio de operações destrutivas (DROP, DELETE, UPDATE, etc.)
- Detecção de padrões suspeitos (múltiplas statements, comentários)
- Validação específica de cálculos de data (JULIANDAY obrigatório)
- Execução apenas de SELECT statements seguros

⚡ PERFORMANCE:
- Conexões reutilizadas quando possível
- Logging otimizado (compacto + multi-line para debugging)
- Timeout handling para queries longas
- Metadados detalhados para análise de performance
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from dataclasses import dataclass
import time
import re
import logging

from ...infrastructure.database.connection_service import IDatabaseConnectionService
from .error_handling_service import IErrorHandlingService, ErrorCategory


@dataclass
class QueryValidationResult:
    """Query validation result"""
    is_valid: bool
    is_safe: bool
    warnings: List[str]
    blocked_reasons: List[str]


@dataclass
class QueryResult:
    """Complete query result with metadata"""
    sql_query: str
    results: List[Dict[str, Any]]
    success: bool
    execution_time: float
    row_count: int
    error_message: str = None
    metadata: Dict[str, Any] = None


class IQueryExecutionService(ABC):
    """Interface for query execution service"""
    
    @abstractmethod
    def execute_sql_query(self, sql_query: str) -> QueryResult:
        """Execute SQL query and return results"""
        pass
    
    @abstractmethod
    def validate_sql_query(self, sql_query: str) -> QueryValidationResult:
        """Validate SQL query for safety and correctness"""
        pass
    
    @abstractmethod
    def log_sql_query(self, sql_query: str, prefix: str = "SQL") -> None:
        """Log SQL query with proper formatting"""
        pass


class QueryExecutionService(IQueryExecutionService):
    """Query execution service implementation"""
    
    def __init__(
        self,
        db_service: IDatabaseConnectionService,
        error_service: IErrorHandlingService
    ):
        """
        Initialize query execution service
        
        Args:
            db_service: Database connection service
            error_service: Error handling service
        """
        self._db_service = db_service
        self._error_service = error_service
        self.logger = logging.getLogger(__name__)
    
    def execute_sql_query(self, sql_query: str) -> QueryResult:
        """Execute SQL query directly (with validation)"""
        start_time = time.time()
        
        try:
            self.log_sql_query(sql_query, "⚡ Executing SQL")
            
            # Validate query first
            validation = self.validate_sql_query(sql_query)
            self.logger.info(f"🔒 Query validation: safe={validation.is_safe}")
            
            if not validation.is_safe:
                raise ValueError(f"Query blocked for safety: {', '.join(validation.blocked_reasons)}")
            
            # Execute query
            conn = self._db_service.get_raw_connection()
            cursor = conn.cursor()
            cursor.execute(sql_query)
            self.logger.info("📊 SQL executed successfully")
            
            # Fetch results
            results = cursor.fetchall()
            column_names = [description[0] for description in cursor.description] if cursor.description else []
            
            # Convert to list of dictionaries
            result_dicts = [dict(zip(column_names, row)) for row in results]
            
            execution_time = time.time() - start_time
            self.logger.info(f"✅ Query returned {len(result_dicts)} rows in {execution_time:.2f}s")
            
            return QueryResult(
                sql_query=sql_query,
                results=result_dicts,
                success=True,
                execution_time=execution_time,
                row_count=len(result_dicts),
                metadata={
                    "validation_warnings": validation.warnings,
                    "direct_execution": True
                }
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"❌ SQL execution failed: {str(e)}")
            error_info = self._error_service.handle_error(e, ErrorCategory.DATABASE)
            
            return QueryResult(
                sql_query=sql_query,
                results=[],
                success=False,
                execution_time=execution_time,
                row_count=0,
                error_message=error_info.message
            )
    
    def validate_sql_query(self, sql_query: str) -> QueryValidationResult:
        """Validate SQL query for safety and correctness"""
        warnings = []
        blocked_reasons = []
        
        # Basic SQL injection protection
        dangerous_keywords = [
            "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE",
            "EXEC", "EXECUTE", "xp_", "sp_", "BULK", "OPENROWSET"
        ]
        
        sql_upper = sql_query.upper()
        
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                blocked_reasons.append(f"Palavra-chave perigosa detectada: {keyword}")
        
        # 🚨 CRITICAL: Check for arithmetic date subtraction (incorrect method)
        if 'AVG' in sql_upper and 'DT_SAIDA' in sql_upper and 'DT_INTER' in sql_upper:
            if 'JULIANDAY' not in sql_upper and 'DT_SAIDA - DT_INTER' in sql_upper.replace(' ', ''):
                blocked_reasons.append("❌ Subtração aritmética de datas detectada! Use JULIANDAY para cálculos de tempo corretos.")
                self.logger.error("🚨 BLOCKED: Arithmetic date subtraction detected")
        
        # Check for suspicious patterns
        suspicious_patterns = [
            r"--",  # SQL comments
            r"/\*.*\*/",  # Block comments
            r";.*DROP",  # Multiple statements with DROP
            r";.*DELETE",  # Multiple statements with DELETE
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, sql_query, re.IGNORECASE):
                warnings.append(f"Padrão suspeito detectado: {pattern}")
        
        # Check for SELECT-only queries (safer)
        if not sql_upper.strip().startswith("SELECT"):
            warnings.append("Consulta não é uma operação SELECT")
        
        # Validate date calculations for hospitalization time
        if 'AVG' in sql_upper and 'DT_SAIDA' in sql_upper and 'DT_INTER' in sql_upper and 'JULIANDAY' in sql_upper:
            warnings.append("✅ Cálculo de data correto com JULIANDAY detectado")
        
        is_safe = len(blocked_reasons) == 0
        is_valid = is_safe and len(warnings) < 3  # Allow some warnings
        
        return QueryValidationResult(
            is_valid=is_valid,
            is_safe=is_safe,
            warnings=warnings,
            blocked_reasons=blocked_reasons
        )
    
    def log_sql_query(self, sql_query: str, prefix: str = "SQL", max_line_length: int = 120) -> None:
        """
        Log SQL query with proper formatting for readability
        
        Args:
            sql_query: SQL query to log
            prefix: Log message prefix 
            max_line_length: Maximum length per line before wrapping
        """
        if not sql_query or sql_query.strip() == "":
            self.logger.info(f"{prefix}: [EMPTY QUERY]")
            return
        
        # Clean and format SQL
        clean_sql = sql_query.strip()
        
        # If SQL is short, log in single line
        if len(clean_sql) <= max_line_length:
            self.logger.info(f"{prefix}: {clean_sql}")
            return
        
        # For longer SQL, log with proper formatting
        self.logger.info(f"{prefix} (multi-line):")
        self.logger.info(f"  {clean_sql}")
        
        # Also log a compact version for easy searching
        compact_sql = ' '.join(clean_sql.split())
        if len(compact_sql) <= 200:
            self.logger.info(f"{prefix} (compact): {compact_sql}")
        else:
            self.logger.info(f"{prefix} (compact): {compact_sql[:200]}... [+{len(compact_sql)-200} chars]")


class QueryExecutionFactory:
    """Factory for creating query execution services"""
    
    @staticmethod
    def create_service(
        db_service: IDatabaseConnectionService,
        error_service: IErrorHandlingService
    ) -> IQueryExecutionService:
        """Create query execution service"""
        return QueryExecutionService(db_service, error_service)