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
    def get_specific_schema_context(self, selected_tables: List[str]) -> SchemaContext:
        """Get schema context for specific selected tables only"""
        pass
    
    @abstractmethod
    def get_sample_data(self, table_name: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Get sample data from a table"""
        pass
    
    @abstractmethod  
    def get_table_names(self) -> List[str]:
        """Get list of all table names"""
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
        
        # Get sample data for schema viewing (5 records for LLM context)
        sample_data = self.get_sample_data(table_name, 5)
        
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
    
    
    def get_table_names(self) -> List[str]:
        """Get list of all table names"""
        conn = self._db_service.get_raw_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]
    
    def get_schema_context(self) -> SchemaContext:
        """Get complete schema context for SUS healthcare database"""
        if self._cached_context:
            return self._cached_context
        
        # Get table information - only sus_data and cid_detalhado
        tables = []
        sus_table = self.get_table_info("sus_data")
        tables.append(sus_table)
        
        # Check if CID detailed codes table exists
        try:
            cid_detalhado_table = self.get_table_info("cid_detalhado")
            tables.append(cid_detalhado_table)
        except Exception:
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
            "Datas estão no formato AAAAMMDD (DT_INTER, DT_SAIDA)",
            "📅 CONSULTAS TEMPORAIS - IMPORTANTE:",
            "  - Para consultas por ANO: use strftime('%Y', dt_inter_date)",
            "  - Para consultas por MÊS: use strftime('%m', dt_inter_date)", 
            "  - Para consultas por DIA: use strftime('%d', dt_inter_date)",
            "  - Para filtrar por ano específico: WHERE dt_inter_date LIKE '2017%'",
            "  - SEMPRE use dt_inter_date e dt_saida_date (formato DATE) para consultas temporais",
            "  - NUNCA use DT_INTER ou DT_SAIDA (formato INTEGER) diretamente",
            "",
            "⏱️ CÁLCULOS DE TEMPO DE INTERNAÇÃO - FÓRMULA OBRIGATÓRIA:",
            "  - Tempo em dias: julianday(dt_saida_date) - julianday(dt_inter_date)",
            "  - Tempo médio: AVG(julianday(dt_saida_date) - julianday(dt_inter_date))",
            "  - SEMPRE filtrar: dt_inter_date IS NOT NULL AND dt_saida_date IS NOT NULL AND dt_saida_date > dt_inter_date",
            "  - Para diagnósticos: GROUP BY DIAG_PRINC HAVING COUNT(*) >= 5 (mínimo 5 casos)",
            "  - Para maior tempo médio: ORDER BY tempo_medio_dias DESC LIMIT 1",
            "",
            "🌡️ ESTAÇÕES DO ANO NO BRASIL (Hemisfério Sul):",
            "  - Verão: 21/12 a 20/03 (meses 12, 01, 02, 03)",
            "  - Outono: 21/03 a 20/06 (meses 03, 04, 05, 06)", 
            "  - Inverno: 21/06 a 22/09 (meses 06, 07, 08, 09)",
            "  - Primavera: 23/09 a 20/12 (meses 09, 10, 11, 12)",
            "Para consultas sazonais: strftime('%m', dt_inter_date)"
        ]
        
        # Add CID-specific notes if CID detalhado table exists
        if len(tables) > 1 and any(t.name == "cid_detalhado" for t in tables):
            cid_notes = [
                "🔍 TABELA cid_detalhado: Códigos CID específicos (I200, I201, etc.) com descrições detalhadas",
                "Para perguntas sobre códigos CID específicos, SEMPRE consulte cid_detalhado!",
                "Para perguntas sobre diagnósticos específicos, SEMPRE use JOIN com cid_detalhado",
                "Exemplo: o que é I200 → SELECT codigo, descricao FROM cid_detalhado WHERE codigo = 'I200'",
                "Para contar casos: JOIN com sus_data ON cid_detalhado.codigo = sus_data.DIAG_PRINC",
                "Para diagnósticos mais comuns: JOIN sus_data com cid_detalhado para mostrar descrições legíveis"
            ]
            important_notes.extend(cid_notes)
        
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
            "SELECT UF_RESIDENCIA_PACIENTE, SUM(VAL_TOT) as custo_total FROM sus_data GROUP BY UF_RESIDENCIA_PACIENTE",
            "",
            "-- ✅ CORRETO: Diagnósticos mais comuns no inverno (jun-set)",
            "SELECT DIAG_PRINC, COUNT(*) as total FROM sus_data WHERE CAST(SUBSTR(CAST(DT_INTER AS TEXT), 5, 2) AS INTEGER) IN (6, 7, 8, 9) GROUP BY DIAG_PRINC ORDER BY total DESC LIMIT 5",
            "",
            "-- ✅ CORRETO: Internações por estação do ano",
            "SELECT CASE WHEN CAST(strftime('%m', dt_inter_date) AS INTEGER) IN (12, 1, 2, 3) THEN 'Verão' WHEN CAST(strftime('%m', dt_inter_date) AS INTEGER) IN (3, 4, 5, 6) THEN 'Outono' WHEN CAST(strftime('%m', dt_inter_date) AS INTEGER) IN (6, 7, 8, 9) THEN 'Inverno' ELSE 'Primavera' END as estacao, COUNT(*) as total FROM sus_data WHERE dt_inter_date IS NOT NULL GROUP BY estacao",
            "",
            "-- ✅ CORRETO: Casos de tumor por ano",
            "SELECT strftime('%Y', dt_inter_date) as ano, COUNT(*) as casos FROM sus_data WHERE dt_inter_date IS NOT NULL AND DIAG_PRINC LIKE 'C%' GROUP BY ano ORDER BY casos DESC",
            "",
            "-- ✅ CORRETO: Mortes por mês em 2017",
            "SELECT strftime('%m', dt_inter_date) as mes, COUNT(*) as mortes FROM sus_data WHERE dt_inter_date LIKE '2017%' AND MORTE = 1 GROUP BY mes ORDER BY mes",
            "",
            "-- ✅ CORRETO: Tempo médio de internação por ano",
            "SELECT strftime('%Y', dt_inter_date) as ano, AVG(julianday(dt_saida_date) - julianday(dt_inter_date)) as tempo_medio_dias FROM sus_data WHERE dt_inter_date IS NOT NULL AND dt_saida_date IS NOT NULL AND dt_saida_date > dt_inter_date GROUP BY ano ORDER BY ano",
            "",
            "-- ✅ CORRETO: Diagnóstico com maior tempo médio de internação",
            "SELECT DIAG_PRINC, COUNT(*) as casos, AVG(julianday(dt_saida_date) - julianday(dt_inter_date)) as tempo_medio_dias FROM sus_data WHERE dt_inter_date IS NOT NULL AND dt_saida_date IS NOT NULL AND dt_saida_date > dt_inter_date GROUP BY DIAG_PRINC HAVING COUNT(*) >= 5 ORDER BY tempo_medio_dias DESC LIMIT 1",
            "",
            "-- ✅ CORRETO: Diagnósticos com maiores tempos médios (top 5)",
            "SELECT DIAG_PRINC, COUNT(*) as casos, ROUND(AVG(julianday(dt_saida_date) - julianday(dt_inter_date)), 1) as tempo_medio_dias FROM sus_data WHERE dt_inter_date IS NOT NULL AND dt_saida_date IS NOT NULL AND dt_saida_date > dt_inter_date GROUP BY DIAG_PRINC HAVING COUNT(*) >= 5 ORDER BY tempo_medio_dias DESC LIMIT 5"
        ]
        
        # Add CID-specific query examples if CID detalhado table exists
        if len(tables) > 1 and any(t.name == "cid_detalhado" for t in tables):
            cid_examples = [
                "",
                "-- CONSULTAS COM CID DETALHADO:",
                "",
                "-- Buscar descrição de código CID específico",
                "SELECT codigo, descricao FROM cid_detalhado WHERE codigo = 'I200'",
                "",
                "-- ✅ DIAGNÓSTICO MAIS COMUM (sempre usar cid_detalhado para diagnósticos específicos)",
                "SELECT cd.codigo, cd.descricao as diagnostico, COUNT(*) as total_casos FROM sus_data s JOIN cid_detalhado cd ON s.DIAG_PRINC = cd.codigo GROUP BY cd.codigo, cd.descricao ORDER BY total_casos DESC LIMIT 1",
                "",
                "-- ✅ DIAGNÓSTICOS MAIS COMUNS EM HOMENS",
                "SELECT cd.codigo, cd.descricao as diagnostico, COUNT(*) as total_casos FROM sus_data s JOIN cid_detalhado cd ON s.DIAG_PRINC = cd.codigo WHERE s.SEXO = 1 GROUP BY cd.codigo, cd.descricao ORDER BY total_casos DESC LIMIT 5",
                "",
                "-- ✅ MORTES POR DIAGNÓSTICO ESPECÍFICO",
                "SELECT cd.codigo, cd.descricao as diagnostico, COUNT(*) as mortes FROM sus_data s JOIN cid_detalhado cd ON s.DIAG_PRINC = cd.codigo WHERE s.MORTE = 1 GROUP BY cd.codigo, cd.descricao ORDER BY mortes DESC"
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
            "DT_INTER": "⚠️ Data de internação em formato INTEGER (YYYYMMDD) - NÃO USE para consultas, use dt_inter_date",
            "DT_SAIDA": "⚠️ Data de saída em formato INTEGER (YYYYMMDD) - NÃO USE para consultas, use dt_saida_date", 
            "dt_inter_date": "✅ Data de internação em formato DATE (YYYY-MM-DD) - USE ESTA para consultas temporais",
            "dt_saida_date": "✅ Data de saída em formato DATE (YYYY-MM-DD) - USE ESTA para consultas temporais",
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
                    "DT_INTER": "⚠️ Data de internação em formato INTEGER (YYYYMMDD) - NÃO USE para consultas, use dt_inter_date",
                    "DT_SAIDA": "⚠️ Data de saída em formato INTEGER (YYYYMMDD) - NÃO USE para consultas, use dt_saida_date",
                    "dt_inter_date": "✅ Data de internação em formato DATE (YYYY-MM-DD) - USE ESTA para consultas temporais",
                    "dt_saida_date": "✅ Data de saída em formato DATE (YYYY-MM-DD) - USE ESTA para consultas temporais",
                    "UF_RESIDENCIA_PACIENTE": "Estado de residência do paciente",
                    "CIDADE_RESIDENCIA_PACIENTE": "✅ Nome da cidade do paciente - USE ESTA para perguntas sobre cidades",
                    "LATI_CIDADE_RES": "Latitude da cidade de residência",
                    "LONG_CIDADE_RES": "Longitude da cidade de residência"
                }
            elif table.name == "cid_detalhado":
                column_descriptions = {
                    "codigo": "Código CID-10 específico (ex: I200, J441, C780)",
                    "descricao": "Descrição detalhada e específica do diagnóstico"
                }
            else:
                column_descriptions = {}
            
            for col in table.columns:
                description = column_descriptions.get(col.name, "")
                context += f"- {col.name} ({col.type}): {description}\n"
            
            # Add sample data for LLM understanding
            if table.sample_data and len(table.sample_data) > 0:
                context += f"\nDADOS DE EXEMPLO ({len(table.sample_data)} registros de amostra):\n"
                
                # Get column names for consistent ordering
                column_names = [col.name for col in table.columns]
                
                # Create formatted table-like display
                context += "```\n"
                # Header
                context += " | ".join(column_names) + "\n"
                context += "-" * (len(" | ".join(column_names))) + "\n"
                
                # Sample rows
                for sample_row in table.sample_data:
                    if isinstance(sample_row, dict):
                        row_values = []
                        for col_name in column_names:
                            value = sample_row.get(col_name, 'NULL')
                            if value is None:
                                value = 'NULL'
                            else:
                                value_str = str(value)
                                # Truncate very long values for readability
                                if len(value_str) > 30:
                                    value_str = value_str[:27] + "..."
                                value = value_str
                            row_values.append(str(value))
                        context += " | ".join(row_values) + "\n"
                    else:
                        # Fallback for non-dict format
                        context += str(sample_row) + "\n"
                
                context += "```\n"
            else:
                context += "\nDADOS DE EXEMPLO: Nenhum dado disponível\n"
        
        # Add relationship information if multiple tables
        if len(tables) > 1:
            context += "\n\nRELACIONAMENTOS ENTRE TABELAS:\n"
            
            # Specific relationships based on selected tables
            table_names = [t.name for t in tables]
            if "sus_data" in table_names and "cid_detalhado" in table_names:
                context += "- sus_data.DIAG_PRINC = cid_detalhado.codigo (JOIN direto por código específico)\n"
                context += "- JOIN: s.DIAG_PRINC = cd.codigo\n"
            elif "sus_data" in table_names and "cid_capitulos" in table_names:
                context += "- sus_data.DIAG_PRINC relaciona com cid_capitulos via range de códigos\n"
                context += "- JOIN: s.DIAG_PRINC BETWEEN c.codigo_inicio AND c.codigo_fim\n"
            
            context += "- VIEW DISPONÍVEL: sus_data_with_cid (dados integrados)\n"
        
        context += "\nNOTAS IMPORTANTES:\n"
        for note in notes:
            context += f"- {note}\n"
        
        context += "\nEXEMPLOS DE CONSULTAS:\n"
        context += "\n".join(examples)
        
        return context
    
    def get_specific_schema_context(self, selected_tables: List[str]) -> SchemaContext:
        """
        Get schema context for specific selected tables only
        
        Args:
            selected_tables: List of table names to include in context
            
        Returns:
            SchemaContext with only the selected tables
        """
        # Filter selected tables to only valid ones
        valid_tables = ["sus_data", "cid_detalhado"]
        selected_tables = [t for t in selected_tables if t in valid_tables]
        
        # If no valid tables, fallback to sus_data
        if not selected_tables:
            selected_tables = ["sus_data"]
        
        # Get table information for selected tables only
        tables = []
        for table_name in selected_tables:
            try:
                table_info = self.get_table_info(table_name)
                tables.append(table_info)
            except Exception as e:
                # Skip table if there's an error
                continue
        
        # If no tables were successfully loaded, fallback to full schema
        if not tables:
            return self.get_schema_context()
        
        # Generate specific notes and examples based on selected tables
        important_notes = self._get_table_specific_notes(selected_tables)
        query_examples = self._get_table_specific_examples(selected_tables)
        
        # Format context for selected tables
        formatted_context = self._format_context_multi_table(tables, important_notes, query_examples)
        
        # Determine database description based on selected tables
        db_info = self._get_database_description(selected_tables)
        
        return SchemaContext(
            database_info=db_info,
            tables=tables,
            query_examples=query_examples,
            important_notes=important_notes,
            formatted_context=formatted_context
        )
    
    def _get_table_specific_notes(self, selected_tables: List[str]) -> List[str]:
        """Get important notes specific to selected tables"""
        notes = []
        
        if "sus_data" in selected_tables:
            notes.extend([
                "CRÍTICO: Para perguntas sobre CIDADES ou MUNICÍPIOS, SEMPRE use CIDADE_RESIDENCIA_PACIENTE",
                "NUNCA use MUNIC_RES para perguntas sobre cidades - contém apenas códigos IBGE numéricos",
                "MUNIC_RES = códigos como 430300, 430460 (inúteis para usuário final)",
                "CIDADE_RESIDENCIA_PACIENTE = nomes como 'Porto Alegre', 'Santa Maria' (legível)",
                "Use MORTE = 1 para consultas sobre óbitos/mortes",
                "Códigos de sexo: 1=Masculino, 3=Feminino (padrão SUS)",
                "Códigos CID-10 estão na coluna DIAG_PRINC da tabela sus_data",
                "Datas estão no formato AAAAMMDD (DT_INTER, DT_SAIDA)",
                "📅 CONSULTAS TEMPORAIS - IMPORTANTE:",
                "  - Para consultas por ANO: use strftime('%Y', dt_inter_date)",
                "  - Para consultas por MÊS: use strftime('%m', dt_inter_date)", 
                "  - Para consultas por DIA: use strftime('%d', dt_inter_date)",
                "  - Para filtrar por ano específico: WHERE dt_inter_date LIKE '2017%'",
                "  - SEMPRE use dt_inter_date e dt_saida_date (formato DATE) para consultas temporais",
                "  - NUNCA use DT_INTER ou DT_SAIDA (formato INTEGER) diretamente",
                "",
                "⏱️ CÁLCULOS DE TEMPO DE INTERNAÇÃO - FÓRMULA OBRIGATÓRIA:",
                "  - Tempo em dias: julianday(dt_saida_date) - julianday(dt_inter_date)",
                "  - Tempo médio: AVG(julianday(dt_saida_date) - julianday(dt_inter_date))",
                "  - SEMPRE filtrar: dt_inter_date IS NOT NULL AND dt_saida_date IS NOT NULL AND dt_saida_date > dt_inter_date",
                "  - Para diagnósticos: GROUP BY DIAG_PRINC HAVING COUNT(*) >= 5 (mínimo 5 casos)",
                "  - Para maior tempo médio: ORDER BY tempo_medio_dias DESC LIMIT 1"
            ])
        
        if "cid_detalhado" in selected_tables:
            notes.extend([
                "🔍 TABELA cid_detalhado: Códigos CID específicos (I200, I201, etc.) com descrições detalhadas",
                "Para perguntas sobre códigos CID específicos, SEMPRE consulte cid_detalhado primeiro!",
                "Para perguntas sobre diagnósticos específicos, SEMPRE use JOIN com cid_detalhado",
                "Exemplo: o que é I200 → SELECT codigo, descricao FROM cid_detalhado WHERE codigo = 'I200'",
                "Para contar casos: JOIN com sus_data ON cid_detalhado.codigo = sus_data.DIAG_PRINC",
                "Para diagnósticos mais comuns: SEMPRE use JOIN para mostrar descrições legíveis"
            ])
        
        return notes
    
    def _get_table_specific_examples(self, selected_tables: List[str]) -> List[str]:
        """Get query examples specific to selected tables"""
        examples = []
        
        if "sus_data" in selected_tables:
            examples.extend([
                "-- ✅ CORRETO: Cidades com mais mortes (use CIDADE_RESIDENCIA_PACIENTE)",
                "SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_mortes FROM sus_data WHERE MORTE = 1 GROUP BY CIDADE_RESIDENCIA_PACIENTE ORDER BY total_mortes DESC LIMIT 5",
                "",
                "-- ✅ CORRETO: Mortes em cidade específica",
                "SELECT COUNT(*) FROM sus_data WHERE CIDADE_RESIDENCIA_PACIENTE = 'Porto Alegre' AND MORTE = 1",
                "",
                "-- ✅ CORRETO: Pacientes HOMENS por diagnóstico (SEXO = 1 para masculino)",
                "SELECT DIAG_PRINC, COUNT(*) as total_homens FROM sus_data WHERE SEXO = 1 GROUP BY DIAG_PRINC ORDER BY total_homens DESC LIMIT 5",
                "",
                "-- ✅ CORRETO: Pacientes MULHERES por diagnóstico (SEXO = 3 para feminino)",
                "SELECT DIAG_PRINC, COUNT(*) as total_mulheres FROM sus_data WHERE SEXO = 3 GROUP BY DIAG_PRINC ORDER BY total_mulheres DESC LIMIT 5",
                "",
                "-- ✅ CORRETO: Mortes por sexo",
                "SELECT CASE WHEN SEXO = 1 THEN 'Masculino' WHEN SEXO = 3 THEN 'Feminino' ELSE 'Outro' END as sexo, COUNT(*) as mortes FROM sus_data WHERE MORTE = 1 GROUP BY SEXO",
                "",
                "-- ✅ CORRETO: Pacientes por faixa etária",
                "SELECT CASE WHEN IDADE < 18 THEN 'Menor' WHEN IDADE < 65 THEN 'Adulto' ELSE 'Idoso' END as faixa_etaria, COUNT(*) FROM sus_data GROUP BY faixa_etaria",
                "",
                "-- ✅ CORRETO: Diagnósticos mais comuns",
                "SELECT DIAG_PRINC, COUNT(*) as total FROM sus_data GROUP BY DIAG_PRINC ORDER BY total DESC LIMIT 10",
                "",
                "-- ✅ CORRETO: Custo total por estado",
                "SELECT UF_RESIDENCIA_PACIENTE, SUM(VAL_TOT) as custo_total FROM sus_data GROUP BY UF_RESIDENCIA_PACIENTE"
            ])
        
        if "cid_detalhado" in selected_tables:
            examples.extend([
                "",
                "-- CONSULTAS COM CID DETALHADO:",
                "",
                "-- Buscar descrição de código CID específico",
                "SELECT codigo, descricao FROM cid_detalhado WHERE codigo = 'I200'",
                "",
                "-- Casos de um código específico",
                "SELECT COUNT(*) FROM sus_data s JOIN cid_detalhado c ON s.DIAG_PRINC = c.codigo WHERE c.codigo = 'I200'",
                "",
                "-- ✅ DIAGNÓSTICO MAIS COMUM (usar cid_detalhado, não cid_capitulos)",
                "SELECT cd.codigo, cd.descricao as diagnostico, COUNT(*) as total_casos FROM sus_data s JOIN cid_detalhado cd ON s.DIAG_PRINC = cd.codigo GROUP BY cd.codigo, cd.descricao ORDER BY total_casos DESC LIMIT 1",
                "",
                "-- ✅ DIAGNÓSTICOS MAIS COMUNS EM HOMENS (usar cid_detalhado)",
                "SELECT cd.codigo, cd.descricao as diagnostico, COUNT(*) as total_casos FROM sus_data s JOIN cid_detalhado cd ON s.DIAG_PRINC = cd.codigo WHERE s.SEXO = 1 GROUP BY cd.codigo, cd.descricao ORDER BY total_casos DESC LIMIT 5"
            ])
        
        
        return examples
    
    def _get_database_description(self, selected_tables: List[str]) -> str:
        """Get database description based on selected tables"""
        if len(selected_tables) == 1:
            if selected_tables[0] == "sus_data":
                return "Sistema Único de Saúde (SUS) - Dados de Hospitalização"
            elif selected_tables[0] == "cid_detalhado":
                return "Códigos CID-10 Específicos com Descrições Detalhadas"
        
        # Multiple tables - only sus_data + cid_detalhado supported
        if "sus_data" in selected_tables and "cid_detalhado" in selected_tables:
            return "Sistema Único de Saúde (SUS) - Dados de Hospitalização com Diagnósticos CID-10 Específicos"
        elif "sus_data" in selected_tables:
            return "Sistema Único de Saúde (SUS) - Dados de Hospitalização"
        else:
            return "Classificação Internacional de Doenças (CID-10)"
    
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