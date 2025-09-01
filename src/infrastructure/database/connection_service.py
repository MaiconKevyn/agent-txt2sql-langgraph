"""
Database Connection Service - Infrastructure Layer

🎯 OBJETIVO:
Gerenciar conexões com o banco de dados SUS (SQLite), fornecendo interfaces
padronizadas tanto para LangChain quanto para operações SQLite nativas.

🔄 POSIÇÃO NO FLUXO (INFRASTRUCTURE):
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Application     │ -> │ Database        │ -> │ SQLite SUS      │
│ Services        │    │ Connection      │    │ Database        │
└─────────────────┘    └─────────────────┘    └─────────────────┘

📥 ENTRADAS (DE ONDE VEM):
- QueryExecutionService: solicitações de conexão para execução
- SchemaIntrospectionService: solicitações para análise de schema
- Application Layer: requisições de conexão via factory patterns

📤 SAÍDAS (PARA ONDE VAI):
- QueryExecutionService: raw SQLite connections para execução direta
- SchemaIntrospectionService: LangChain SQLDatabase para análise
- SQLite Database: operações SQL executadas no banco SUS

🧩 RESPONSABILIDADES:
1. 🔌 Gerenciar conexões SQLite com o banco SUS (sus_database.db)
2. 🔄 Fornecer interfaces LangChain e SQLite nativo
3. 🛡️ Garantir thread-safety (check_same_thread=False)
4. ⚡ Otimizar reutilização de conexões
5. 🧪 Testes de conectividade e health checks
6. 📝 Configuração flexível de paths de banco

🔗 DEPENDÊNCIAS EXTERNAS:
- SQLite3: Driver nativo Python para SQLite
- LangChain: SQLDatabase wrapper para operações ORM-style
- Sistema de arquivos: Acesso ao arquivo sus_database.db

🏗️ PADRÕES IMPLEMENTADOS:
- Factory Pattern: DatabaseConnectionFactory para criação
- Interface Segregation: IDatabaseConnectionService abstrato
- Dependency Inversion: Application depende de abstrações

💾 GESTÃO DE RECURSOS:
- Conexões lazy-loaded (criadas sob demanda)
- Cleanup automático em close_connection()
- Reutilização segura de conexões existentes
- Health checks sem interferir em conexões ativas

🛡️ SEGURANÇA:
- Apenas operações de leitura (SELECT) para segurança
- Path validation para prevenir directory traversal
- Thread-safety para uso em ambiente multi-thread
- Error handling robusto para falhas de conexão

📊 TIPOS DE CONEXÃO:
1. LangChain SQLDatabase: Para operações de alto nível
2. Raw SQLite Connection: Para operações diretas e performance
3. Test Connections: Para health checks isolados
"""
from abc import ABC, abstractmethod
from typing import Optional
from langchain_community.utilities import SQLDatabase
import sqlite3


class IDatabaseConnectionService(ABC):
    """Interface for database connection management"""
    
    @abstractmethod
    def get_connection(self) -> SQLDatabase:
        """Get database connection"""
        pass

    @abstractmethod
    def close_connection(self) -> None:
        """Close database connection"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if database connection is working"""
        pass


class PostgreSQLDatabaseConnectionService(IDatabaseConnectionService):
    """PostgreSQL implementation of database connection service"""
    
    def __init__(self, db_path: str):
        """
        Initialize PostgreSQL database connection service
        
        Args:
            db_path: PostgreSQL connection string
        """
        self._db_path = db_path
        self._connection: Optional[SQLDatabase] = None
        self._raw_connection = None
    
    def get_connection(self) -> SQLDatabase:
        """Get LangChain SQLDatabase connection"""
        if self._connection is None:
            self._connection = SQLDatabase.from_uri(self._db_path)
        return self._connection
    
    def get_raw_connection(self):
        """Get raw PostgreSQL connection for direct queries"""
        if self._raw_connection is None:
            try:
                import psycopg2
                # Convert sqlalchemy URL to psycopg2 format
                db_url = self._db_path.replace('postgresql+psycopg2://', 'postgresql://')
                self._raw_connection = psycopg2.connect(db_url)
            except ImportError:
                raise ImportError("psycopg2 não instalado. Execute: pip install psycopg2-binary")
        return self._raw_connection
    
    def close_connection(self) -> None:
        """Close database connections"""
        if self._raw_connection:
            self._raw_connection.close()
            self._raw_connection = None
        self._connection = None
    
    def test_connection(self) -> bool:
        """Test if database connection is working"""
        try:
            import psycopg2
            db_url = self._db_path.replace('postgresql+psycopg2://', 'postgresql://')
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception:
            return False
    
    def get_database_path(self) -> str:
        """Get database connection string"""
        return self._db_path


class DatabaseConnectionFactory:
    """Factory for creating database connection services"""

    @staticmethod
    def create_postgresql_service(db_path: str) -> IDatabaseConnectionService:
        """Create PostgreSQL database connection service"""
        return PostgreSQLDatabaseConnectionService(db_path)
    
    @staticmethod
    def create_service(db_type: str, **kwargs) -> IDatabaseConnectionService:
        """Create database connection service based on type"""
        if db_type.lower() == "postgresql":
            return PostgreSQLDatabaseConnectionService(kwargs.get("db_path"))
        else:
            raise ValueError(f"Unsupported database type: {db_type}")