"""
Schema Introspection Service - Single Responsibility: Handle database schema analysis and context generation
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from ...infrastructure.database.connection_service import IDatabaseConnectionService


@dataclass
class ColumnInfo:
    """Information about a database column"""
    name: str
    type: str
    nullable: bool
    primary_key: bool
    description: Optional[str] = None


@dataclass
class TableInfo:
    """Information about a database table"""
    name: str
    columns: List[ColumnInfo]
    sample_data: List[Dict[str, Any]]
    row_count: int


@dataclass
class SchemaContext:
    """Complete schema context for LLM"""
    database_info: str
    tables: List[TableInfo]
    query_examples: List[str]
    important_notes: List[str]
    formatted_context: str


class ISchemaIntrospectionService(ABC):
    """Interface for schema introspection"""
    
    @abstractmethod
    def get_table_info(self, table_name: str) -> TableInfo:
        """Get detailed information about a specific table"""
        pass
    
    @abstractmethod
    def get_schema_context(self) -> SchemaContext:
        """Get complete schema context for LLM queries"""
        pass
    
    @abstractmethod
    def get_sample_data(self, table_name: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Get sample data from a table"""
        pass


class SUSSchemaIntrospectionService(ISchemaIntrospectionService):
    """SUS healthcare database schema introspection service"""
    
    def __init__(self, db_service: IDatabaseConnectionService):
        """
        Initialize schema introspection service
        
        Args:
            db_service: Database connection service
        """
        self._db_service = db_service
        self._cached_context: Optional[SchemaContext] = None
    
    def get_table_info(self, table_name: str) -> TableInfo:
        """Get detailed information about a specific table"""
        conn = self._db_service.get_raw_connection()
        cursor = conn.cursor()
        
        # Get column information
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_data = cursor.fetchall()
        
        columns = []
        for col in columns_data:
            columns.append(ColumnInfo(
                name=col[1],
                type=col[2],
                nullable=not col[3],
                primary_key=bool(col[5])
            ))
        
        # Get sample data
        sample_data = self.get_sample_data(table_name, limit=3)
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        return TableInfo(
            name=table_name,
            columns=columns,
            sample_data=sample_data,
            row_count=row_count
        )
    
    def get_sample_data(self, table_name: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Get sample data from a table"""
        conn = self._db_service.get_raw_connection()

        cursor = conn.cursor()
        
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        rows = cursor.fetchall()
        
        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Convert to list of dictionaries
        return [dict(zip(columns, row)) for row in rows]
    
    def get_schema_context(self) -> SchemaContext:
        """Get complete schema context for SUS healthcare database"""
        if self._cached_context:
            return self._cached_context
        
        # Get table information - check for both tables
        tables = []
        sus_table = self.get_table_info("sus_data")
        tables.append(sus_table)
        
        # Check if CID chapters table exists
        try:
            cid_table = self.get_table_info("cid_capitulos")
            tables.append(cid_table)
        except Exception:
            # CID table doesn't exist, continue with just SUS data
            pass
        
        # Define important notes specific to SUS data with multi-table support
        important_notes = [
            "CRÍTICO: Para perguntas sobre CIDADES ou MUNICÍPIOS, SEMPRE use CIDADE_RESIDENCIA_PACIENTE",
            "NUNCA use MUNIC_RES para perguntas sobre cidades - contém apenas códigos IBGE numéricos",
            "MUNIC_RES = códigos como 430300, 430460 (inúteis para usuário final)",
            "CIDADE_RESIDENCIA_PACIENTE = nomes como 'Porto Alegre', 'Santa Maria' (legível)",
            "Use MORTE = 1 para consultas sobre óbitos/mortes",
            "Códigos de sexo: 1=Masculino, 3=Feminino (padrão SUS)",
            "Códigos CID-10 estão na coluna DIAG_PRINC da tabela sus_data",
            "Datas estão no formato AAAAMMDD (DT_INTER, DT_SAIDA)"
        ]
        
        # Add CID-specific notes if CID table exists
        if len(tables) > 1:
            important_notes.extend([
                "TABELA cid_capitulos: Contém informações sobre capítulos CID-10",
                "Para consultas sobre categorias de diagnóstico, use JOIN entre sus_data e cid_capitulos",
                "JOIN CONDITION: s.DIAG_PRINC BETWEEN c.codigo_inicio AND c.codigo_fim",
                "Use a view 'sus_data_with_cid' para consultas simples com dados CID integrados"
            ])
        
        # Define query examples based on available tables
        query_examples = [
            "-- ✅ CORRETO: Cidades com mais mortes (use CIDADE_RESIDENCIA_PACIENTE)",
            "SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_mortes FROM sus_data WHERE MORTE = 1 GROUP BY CIDADE_RESIDENCIA_PACIENTE ORDER BY total_mortes DESC LIMIT 5",
            "",
            "-- ✅ CORRETO: Mortes em cidade específica",
            "SELECT COUNT(*) FROM sus_data WHERE CIDADE_RESIDENCIA_PACIENTE = 'Porto Alegre' AND MORTE = 1",
            "",
            "-- ❌ ERRADO: NÃO use MUNIC_RES para perguntas sobre cidades",
            "-- SELECT MUNIC_RES, COUNT(*) FROM sus_data... (retorna códigos inúteis)",
            "",
            "-- ✅ CORRETO: Pacientes por faixa etária",
            "SELECT CASE WHEN IDADE < 18 THEN 'Menor' WHEN IDADE < 65 THEN 'Adulto' ELSE 'Idoso' END as faixa_etaria, COUNT(*) FROM sus_data GROUP BY faixa_etaria",
            "",
            "-- ✅ CORRETO: Diagnósticos mais comuns",
            "SELECT DIAG_PRINC, COUNT(*) as total FROM sus_data GROUP BY DIAG_PRINC ORDER BY total DESC LIMIT 10",
            "",
            "-- ✅ CORRETO: Custo total por estado",
            "SELECT UF_RESIDENCIA_PACIENTE, SUM(VAL_TOT) as custo_total FROM sus_data GROUP BY UF_RESIDENCIA_PACIENTE"
        ]
        
        # Add CID-specific query examples if CID table exists
        if len(tables) > 1:
            cid_examples = [
                "",
                "-- CONSULTAS COM CID (MULTI-TABELA):",
                "",
                "-- Contar tabelas no banco",
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                "",
                "-- Listar todas as tabelas",
                "SELECT name FROM sqlite_master WHERE type='table'",
                "",
                "-- Pacientes por capítulo CID-10",
                "SELECT c.descricao, COUNT(*) as total_pacientes FROM sus_data s JOIN cid_capitulos c ON s.DIAG_PRINC BETWEEN c.codigo_inicio AND c.codigo_fim GROUP BY c.descricao ORDER BY total_pacientes DESC",
                "",
                "-- Mortes por categoria CID",
                "SELECT c.descricao as categoria_cid, COUNT(*) as mortes FROM sus_data s JOIN cid_capitulos c ON s.DIAG_PRINC BETWEEN c.codigo_inicio AND c.codigo_fim WHERE s.MORTE = 1 GROUP BY c.descricao ORDER BY mortes DESC",
                "",
                "-- View com dados integrados",
                "SELECT * FROM sus_data_with_cid WHERE capitulo_cid IS NOT NULL LIMIT 5",
                "",
                "-- Capítulos CID disponíveis",
                "SELECT numero_capitulo, descricao, codigo_inicio, codigo_fim FROM cid_capitulos ORDER BY numero_capitulo"
            ]
            query_examples.extend(cid_examples)
        
        # Format complete context for all tables
        formatted_context = self._format_context_multi_table(tables, important_notes, query_examples)
        
        # Determine database description based on available tables
        if len(tables) > 1:
            db_info = "Sistema Único de Saúde (SUS) - Dados de Hospitalização com Classificação CID-10"
        else:
            db_info = "Sistema Único de Saúde (SUS) - Dados de Hospitalização"
        
        self._cached_context = SchemaContext(
            database_info=db_info,
            tables=tables,
            query_examples=query_examples,
            important_notes=important_notes,
            formatted_context=formatted_context
        )
        
        return self._cached_context
    
    def _format_context(
        self, 
        table: TableInfo, 
        notes: List[str], 
        examples: List[str]
    ) -> str:
        """Format complete context for LLM"""
        context = f"""
        CONTEXTO DO BANCO DE DADOS - SISTEMA ÚNICO DE SAÚDE (SUS)
        ========================================================
        
        INFORMAÇÕES DA TABELA: {table.name}
        Total de registros: {table.row_count:,}
        
        COLUNAS DISPONÍVEIS:
        """
        
        # Add column descriptions
        column_descriptions = {
            "DIAG_PRINC": "Código do diagnóstico principal (CID-10)",
            "MUNIC_RES": "Código numérico do município de residência do paciente",
            "MUNIC_MOV": "Código numérico do município do estabelecimento da internação",
            "PROC_REA": "Código do procedimento realizado (SUS)",
            "IDADE": "Idade do paciente em anos",
            "SEXO": "Sexo do paciente (1=Masculino, 3=Feminino)",
            "CID_MORTE": "Código da causa da morte (CID-10)",
            "MORTE": "Indicador de óbito (0=Não, 1=Sim)",
            "CNES": "Código Nacional de Estabelecimento de Saúde do hospital",
            "VAL_TOT": "Valor total do procedimento em Reais",
            "UTI_MES_TO": "Quantidade de dias de UTI no mês (NÃO é tempo total de internação)",
            "DT_INTER": "Data de internação (formato DATE YYYY-MM-DD)",
            "DT_SAIDA": "Data de saída (formato DATE YYYY-MM-DD)",
            "UF_RESIDENCIA_PACIENTE": "Estado de residência do paciente",
            "CIDADE_RESIDENCIA_PACIENTE": "Cidade de residência do paciente",
            "LATI_CIDADE_RES": "Latitude da cidade de residência",
            "LONG_CIDADE_RES": "Longitude da cidade de residência"
        }
        
        for col in table.columns:
            description = column_descriptions.get(col.name, "")
            context += f"- {col.name} ({col.type}): {description}\n"
        
        context += "\nNOTAS IMPORTANTES:\n"
        for note in notes:
            context += f"- {note}\n"
        
        context += "\nEXEMPLOS DE CONSULTAS:\n"
        context += "\n".join(examples)
        
        return context
    
    def _format_context_multi_table(
        self, 
        tables: List[TableInfo], 
        notes: List[str], 
        examples: List[str]
    ) -> str:
        """Format complete context for multiple tables"""
        context = f"""
        CONTEXTO DO BANCO DE DADOS - SISTEMA ÚNICO DE SAÚDE (SUS)
        ========================================================
        
        INFORMAÇÕES DAS TABELAS ({len(tables)} tabelas):
        """

        # Add information for each table
        for table in tables:
            context += f"\n\nTABELA: {table.name}\n"
            context += f"Total de registros: {table.row_count:,}\n"
            context += "COLUNAS DISPONÍVEIS:\n"
            
            # Column descriptions for each table
            if table.name == "sus_data":
                column_descriptions = {
                    "DIAG_PRINC": "Código do diagnóstico principal (CID-10)",
                    "MUNIC_RES": "🚨 Código IBGE numérico (ex: 430300) - NÃO USE para perguntas sobre cidades",
                    "MUNIC_MOV": "Código numérico do município de internação",
                    "PROC_REA": "Código do procedimento realizado (SUS)",
                    "IDADE": "Idade do paciente em anos",
                    "SEXO": "Sexo do paciente (1=Masculino, 3=Feminino)",
                    "CID_MORTE": "Código da causa da morte (CID-10)",
                    "MORTE": "Indicador de óbito (0=Não, 1=Sim)",
                    "CNES": "Código Nacional de Estabelecimento de Saúde",
                    "VAL_TOT": "Valor total do procedimento em Reais",
                    "UTI_MES_TO": "Total de dias em UTI (NÃO é tempo total de internação)",
                    "DT_INTER": "Data de internação (formato DATE YYYY-MM-DD)",
                    "DT_SAIDA": "Data de saída (formato DATE YYYY-MM-DD)",
                    "UF_RESIDENCIA_PACIENTE": "Estado de residência do paciente",
                    "CIDADE_RESIDENCIA_PACIENTE": "✅ Nome da cidade do paciente - USE ESTA para perguntas sobre cidades",
                    "LATI_CIDADE_RES": "Latitude da cidade de residência",
                    "LONG_CIDADE_RES": "Longitude da cidade de residência"
                }
            elif table.name == "cid_capitulos":
                column_descriptions = {
                    "id": "Identificador único do capítulo",
                    "numero_capitulo": "Número do capítulo CID-10 (1-22)",
                    "codigo_inicio": "Código CID-10 inicial do capítulo",
                    "codigo_fim": "Código CID-10 final do capítulo",
                    "descricao": "Descrição completa do capítulo",
                    "descricao_abrev": "Descrição abreviada do capítulo",
                    "categoria_geral": "Categoria geral (letra) do capítulo"
                }
            else:
                column_descriptions = {}
            
            for col in table.columns:
                description = column_descriptions.get(col.name, "")
                context += f"- {col.name} ({col.type}): {description}\n"
        
        # Add relationship information if multiple tables
        if len(tables) > 1:
            context += "\n\nRELACIONAMENTOS ENTRE TABELAS:\n"
            context += "- sus_data.DIAG_PRINC relaciona com cid_capitulos via range de códigos\n"
            context += "- JOIN: s.DIAG_PRINC BETWEEN c.codigo_inicio AND c.codigo_fim\n"
            context += "- VIEW DISPONÍVEL: sus_data_with_cid (dados integrados)\n"
        
        context += "\nNOTAS IMPORTANTES:\n"
        for note in notes:
            context += f"- {note}\n"
        
        context += "\nEXEMPLOS DE CONSULTAS:\n"
        context += "\n".join(examples)
        
        return context
    
    def invalidate_cache(self) -> None:
        """Invalidate cached schema context"""
        self._cached_context = None


class SchemaIntrospectionFactory:
    """Factory for creating schema introspection services"""
    
    @staticmethod
    def create_sus_service(db_service: IDatabaseConnectionService) -> ISchemaIntrospectionService:
        """Create SUS healthcare database schema introspection service"""
        return SUSSchemaIntrospectionService(db_service)
    
    @staticmethod
    def create_service(
        schema_type: str, 
        db_service: IDatabaseConnectionService
    ) -> ISchemaIntrospectionService:
        """Create schema introspection service based on type"""
        if schema_type.lower() == "sus":
            return SUSSchemaIntrospectionService(db_service)
        else:
            raise ValueError(f"Unsupported schema type: {schema_type}")