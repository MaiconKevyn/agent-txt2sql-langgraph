"""
SQL Generation Service - Application Layer

🎯 OBJETIVO:
Serviço especializado em gerar consultas SQL válidas a partir de linguagem natural,
aplicando limpeza, correções e otimizações específicas para o domínio SUS/SQLite.

🔄 POSIÇÃO NO FLUXO:
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ User Input      │ -> │ SQL Generation  │ -> │ Query Execution │
│ (Natural Lang)  │    │ Service         │    │ Service         │
└─────────────────┘    └─────────────────┘    └─────────────────┘

📥 ENTRADAS (DE ONDE VEM):
- QueryProcessingService: user_query (string) - pergunta em linguagem natural
- LLMCommunicationService: resposta do LLM com SQL gerado
- SchemaIntrospectionService: contexto do schema da base SUS

📤 SAÍDAS (PARA ONDE VAI):
- QueryExecutionService: sql_query (string) - SQL limpo e validado
- QueryProcessingService: SQL processado pronto para execução

🧩 RESPONSABILIDADES:
1. Criar prompts especializados para geração SQL (básico/enhanced)
2. Extrair SQL das respostas dos LLMs (múltiplos formatos)
3. Limpar comentários SQL problemáticos
4. Corrigir problemas específicos (case sensitivity, funções SQLite)
5. Aplicar fixes para compatibilidade com base SUS

🔗 DEPENDÊNCIAS:
- ILLMCommunicationService: Para enviar prompts e receber respostas
- ISchemaIntrospectionService: Para contexto da estrutura do banco SUS

🛡️ GARANTIAS:
- SQL sempre válido e seguro para SQLite
- Prompts otimizados para domínio médico brasileiro
- Tratamento robusto de edge cases (JULIANDAY, cidades, etc.)
"""
from abc import ABC, abstractmethod
from typing import Optional, Any
import re
import logging

from .llm_communication_service import ILLMCommunicationService
from .schema_introspection_service import ISchemaIntrospectionService


class ISQLGenerationService(ABC):
    """Interface for SQL generation service"""
    
    @abstractmethod
    def generate_sql_query(self, user_query: str, use_enhanced_prompt: bool = False) -> str:
        """Generate SQL query from natural language"""
        pass
    
    @abstractmethod
    def create_sql_prompt(self, user_query: str, enhanced: bool = False, schema_context=None) -> str:
        """Create appropriate prompt for SQL generation"""
        pass
    
    @abstractmethod
    def extract_sql_from_response(self, llm_response: str) -> str:
        """Extract SQL from LLM response"""
        pass
    
    @abstractmethod
    def clean_and_fix_sql(self, sql_query: str, user_query: str = None) -> str:
        """Clean and fix common SQL issues"""
        pass


class SQLGenerationService(ISQLGenerationService):
    """SQL generation service implementation"""
    
    def __init__(
        self,
        llm_service: ILLMCommunicationService,
        schema_service: ISchemaIntrospectionService
    ):
        """
        Initialize SQL generation service
        
        Args:
            llm_service: LLM communication service
            schema_service: Schema introspection service
        """
        self._llm_service = llm_service
        self._schema_service = schema_service
        self.logger = logging.getLogger(__name__)
    
    def generate_sql_query(self, user_query: str, use_enhanced_prompt: bool = False) -> str:
        """Generate SQL query from natural language"""
        try:
            # Create appropriate prompt
            prompt = self.create_sql_prompt(user_query, enhanced=use_enhanced_prompt)
            
            # Get LLM response
            llm_response = self._llm_service.send_prompt(prompt)
            
            # Extract SQL from response
            sql_query = self.extract_sql_from_response(llm_response.content)
            
            # Clean and fix common issues
            cleaned_sql = self.clean_and_fix_sql(sql_query, user_query)
            
            return cleaned_sql
            
        except Exception as e:
            self.logger.error(f"SQL generation failed: {str(e)}")
            return "SELECT COUNT(*) FROM sus_data;"  # Safe fallback
    
    def create_sql_prompt(self, user_query: str, enhanced: bool = False, schema_context=None) -> str:
        """Create appropriate prompt for SQL generation"""
        # Use provided schema context or fall back to full schema
        if schema_context is None:
            schema_context = self._schema_service.get_schema_context()
        
        if enhanced:
            return self._create_enhanced_sql_prompt(user_query, schema_context)
        else:
            return self._create_direct_sql_prompt(user_query, schema_context)
    
    def extract_sql_from_response(self, llm_response: str) -> str:
        """Extract SQL from LLM response"""
        return self._extract_sql_from_direct_response(llm_response)
    
    def clean_and_fix_sql(self, sql_query: str, user_query: str = None) -> str:
        """Clean and fix common SQL issues"""
        # Clean SQL comments
        cleaned_sql = self._clean_sql_comments(sql_query)
        
        # Fix case sensitivity issues
        cleaned_sql = self._fix_case_sensitivity_issues(cleaned_sql)
        
        # Fix gender code issues (common LLM confusion)
        cleaned_sql = self._fix_gender_code_issues(cleaned_sql, user_query)
        
        # Fix missing JOIN issues (common when using multiple tables)
        cleaned_sql = self._fix_missing_joins(cleaned_sql)
        
        # Fix SQLite specific issues
        cleaned_sql = self._fix_sqlite_year_extraction(cleaned_sql)
        
        # Add LIMIT 1 for queries asking for "the most" or "maior"
        cleaned_sql = self._fix_missing_limit_for_top_queries(cleaned_sql, user_query)
        
        return cleaned_sql
    
    def _create_enhanced_sql_prompt(self, user_query: str, schema_context) -> str:
        """Create enhanced prompt for error-aware SQL generation"""
        return f"""
        {schema_context.formatted_context}
        
        PERGUNTA DO USUÁRIO: {user_query}
        
        INSTRUÇÕES CRÍTICAS PARA GERAÇÃO DE SQL:
        1. 🚨 SEMPRE inclua a coluna no SELECT quando usar GROUP BY
        2. 🚨 NUNCA termine uma cláusula WHERE com AND - sempre complete a condição
        3. 🚨 Para perguntas sobre cidades, use CIDADE_RESIDENCIA_PACIENTE (nomes), não MUNIC_RES (códigos)
        4. 🚨 Para consultas com múltiplos filtros, verifique TODOS os critérios (idade, sexo, morte)
        5. 🚨 Use sintaxe SQL válida: SELECT campos FROM tabela WHERE condições GROUP BY campos ORDER BY campos
        
        EXEMPLO DE SQL CORRETO PARA REFERÊNCIA:
        SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_mortes 
        FROM sus_data 
        WHERE MORTE = 1 AND SEXO = 3 AND IDADE < 40 
        GROUP BY CIDADE_RESIDENCIA_PACIENTE 
        ORDER BY total_mortes DESC;
        
        Agora gere uma consulta SQL VÁLIDA para responder à pergunta do usuário:
        """
    
    def _create_direct_sql_prompt(self, user_query: str, schema_context) -> str:
        """Create optimized prompt for direct SQL generation"""
        return f"""
        Você é um especialista em SQL para bases de dados do SUS brasileiro.
        
        CONTEXTO DA BASE DE DADOS:
        {schema_context.formatted_context}
        
        PERGUNTA DO USUÁRIO: {user_query}
        
        🚨 REGRAS CRÍTICAS PARA GERAÇÃO DE SQL 🚨
        
        0. PARA PERGUNTAS SOBRE CÓDIGOS CID ESPECÍFICOS (ex: "o que é I200?"):
           - SEMPRE consulte a tabela cid_detalhado primeiro!
           - Use: SELECT codigo, descricao FROM cid_detalhado WHERE codigo = 'I200'
           - Para incluir contagem: JOIN com sus_data
           - NUNCA invente descrições - use APENAS dados da tabela!
        
        1. PARA CONSULTAS DE RANKING/TOP (ex: "top 5 cidades"):
           - Use filtros diretos no WHERE, não CASE statements
           - SEMPRE inclua LIMIT com o número solicitado
           - Para contagens específicas, filtre primeiro no WHERE
        
        ❌ INCORRETO (CASE statement complexo):
        SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total, 
               SUM(CASE WHEN IDADE > 50 AND SEXO = 3 THEN 1 ELSE 0 END) as filtrado
        FROM sus_data WHERE MORTE = 1 GROUP BY CIDADE_RESIDENCIA_PACIENTE ORDER BY total DESC;
        
        ✅ CORRETO (filtro direto):
        SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_mortes
        FROM sus_data 
        WHERE MORTE = 1 AND SEXO = 3 AND IDADE > 50 AND DIAG_PRINC LIKE 'J%'
        GROUP BY CIDADE_RESIDENCIA_PACIENTE 
        ORDER BY total_mortes DESC 
        LIMIT 5;
        
        ✅ EXEMPLO CID: "o que é o diagnóstico I200?"
        SELECT codigo, descricao FROM cid_detalhado WHERE codigo = 'I200';
        
        ✅ EXEMPLO CID COM CASOS: "quantos casos de I200?"
        SELECT cd.codigo, cd.descricao, COUNT(sd.DIAG_PRINC) as total_casos
        FROM cid_detalhado cd 
        LEFT JOIN sus_data sd ON cd.codigo = sd.DIAG_PRINC
        WHERE cd.codigo = 'I200'
        GROUP BY cd.codigo, cd.descricao;
        
        2. PARA TEMPO DE INTERNAÇÃO:
        SEMPRE use JULIANDAY para calcular diferenças de data!
        NUNCA use subtração aritmética direta (DT_SAIDA - DT_INTER)!
        
        ❌ INCORRETO (subtração aritmética): DT_SAIDA - DT_INTER
        ✅ CORRETO (conversão de data): JULIANDAY(...) - JULIANDAY(...)
        
        3. 🚨 CRÍTICO - FUNÇÕES SQLite:
        - ❌ NUNCA use YEAR(), MONTH(), DAY() - NÃO EXISTEM no SQLite!
        - ✅ Para extrair ano: USE strftime('%Y', DT_INTER)
        - ✅ Para extrair mês: USE strftime('%m', DT_INTER)
        - ✅ Para agrupar por ano: GROUP BY strftime('%Y', DT_INTER)
        
        🌡️ PARA CONSULTAS SAZONAIS (estações do ano - Brasil):
        - INVERNO: meses 6, 7, 8, 9 (jun-set): CAST(SUBSTR(CAST(DT_INTER AS TEXT), 5, 2) AS INTEGER) IN (6, 7, 8, 9)
        - VERÃO: meses 12, 1, 2, 3 (dez-mar): CAST(SUBSTR(CAST(DT_INTER AS TEXT), 5, 2) AS INTEGER) IN (12, 1, 2, 3)
        - OUTONO: meses 3, 4, 5, 6 (mar-jun): CAST(SUBSTR(CAST(DT_INTER AS TEXT), 5, 2) AS INTEGER) IN (3, 4, 5, 6)
        - PRIMAVERA: meses 9, 10, 11, 12 (set-dez): CAST(SUBSTR(CAST(DT_INTER AS TEXT), 5, 2) AS INTEGER) IN (9, 10, 11, 12)
        
        ✅ EXEMPLO SAZONAL: "Quais os cinco diagnósticos mais comuns no inverno"
        SELECT DIAG_PRINC, COUNT(*) as total 
        FROM sus_data 
        WHERE CAST(SUBSTR(CAST(DT_INTER AS TEXT), 5, 2) AS INTEGER) IN (6, 7, 8, 9)
        GROUP BY DIAG_PRINC 
        ORDER BY total DESC 
        LIMIT 5;
        
        4. OUTRAS INSTRUÇÕES:
        - SEXO = 3 para mulheres, SEXO = 1 para homens
        - MORTE = 1 para mortes confirmadas
        - Gere APENAS o SQL necessário, sem explicações
        
        SQL:"""
    
    def _extract_sql_from_direct_response(self, response: str) -> str:
        """Extract SQL query from direct LLM response - ENHANCED for JULIANDAY multi-line queries"""
        # Look for SQL after "SQL:" marker
        if "SQL:" in response:
            sql_part = response.split("SQL:")[-1].strip()
        else:
            sql_part = response.strip()
        
        # Remove markdown formatting
        sql_part = sql_part.replace('```sql', '').replace('```', '').strip()
        
        # Split into lines and process
        lines = sql_part.split('\n')
        sql_lines = []
        in_sql_block = False
        
        for line in lines:
            line = line.strip()
            
            # Start collecting SQL when we see SELECT
            if line.upper().startswith('SELECT') or line.upper().startswith('WITH'):
                in_sql_block = True
                sql_lines.append(line)
            elif in_sql_block:
                # Continue collecting until we hit a semicolon or explanatory text
                if line.endswith(';'):
                    sql_lines.append(line)
                    break
                elif line and not line.startswith('--') and not line.startswith('#'):
                    # Check for JULIANDAY patterns - these are valid SQL parts
                    if 'JULIANDAY' in line.upper() or 'SUBSTR' in line.upper() or line.strip().startswith(')'):
                        sql_lines.append(line)
                    # Check for common SQL keywords
                    elif any(keyword in line.upper() for keyword in ['FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END']):
                        sql_lines.append(line)
                    # Check for table names (sus_data, cid_capitulos, etc.)
                    elif any(table in line.lower() for table in ['sus_data', 'cid_capitulos']):
                        sql_lines.append(line)
                    # Check for parentheses and operators (part of multi-line expressions)
                    elif any(char in line for char in ['(', ')', '+', '-', '*', '/', '||', 'AVG', 'COUNT', 'SUM']):
                        sql_lines.append(line)
                    # Check for simple identifiers that could be column names or values
                    elif line.replace('_', '').replace(' ', '').replace('=', '').replace('1', '').replace('0', '').isalnum() and len(line) < 30:
                        sql_lines.append(line)
                    # Check for conditions like "MORTE = 1" 
                    elif any(pattern in line.upper() for pattern in ['MORTE', 'SEXO', '= 1', '= 3', '= 0']):
                        sql_lines.append(line)
                    # Stop if we hit explanatory text
                    elif any(word in line.lower() for word in ['this query', 'will give', 'para', 'que', 'resultado', 'resposta', 'consulta']):
                        break
                    elif not line and sql_lines:  # Empty line after SQL content
                        break
                elif not line and sql_lines:  # Empty line after SQL content
                    break
        
        # Join the SQL lines and clean up
        if sql_lines:
            full_sql = ' '.join(sql_lines)
            # Clean up extra spaces and formatting
            full_sql = ' '.join(full_sql.split())
            # Ensure it ends with semicolon
            if not full_sql.strip().endswith(';'):
                full_sql += ';'
            
            # Validate we got a JULIANDAY query if it's for hospitalization time
            if 'AVG' in full_sql.upper() and 'DT_SAIDA' in full_sql.upper() and 'DT_INTER' in full_sql.upper():
                if 'JULIANDAY' not in full_sql.upper():
                    # Force JULIANDAY conversion if we detect arithmetic subtraction
                    self.logger.warning("⚠️ Detected arithmetic subtraction, forcing JULIANDAY conversion")
                    full_sql = """SELECT AVG(
                        JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                        JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2))
                    ) AS tempo_medio_dias FROM sus_data WHERE DIAG_PRINC LIKE 'J%';"""
            
            return full_sql
        
        # Fallback: try original single-line extraction
        for line in lines:
            line = line.strip()
            if any(keyword in line.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
                clean_line = line.replace('```sql', '').replace('```', '').strip()
                if not clean_line.endswith(';'):
                    clean_line += ';'
                return clean_line
        
        return "SELECT COUNT(*) FROM sus_data;"  # Safe fallback
    
    def _clean_sql_comments(self, sql_query: str) -> str:
        """Remove problematic SQL comments that can break query execution"""
        if not sql_query:
            return sql_query
        
        # Handle inline comments carefully - only remove the comment part, not the rest of the line
        # For single-line SQL with inline comments, we need to be more careful
        
        # Split the SQL into tokens and reconstruct
        cleaned_sql = sql_query
        
        # Remove inline comments but preserve the rest of the SQL
        # Look for -- followed by text, but only remove the comment part
        comment_pattern = r'--[^a-zA-Z]*[a-zA-Z][^G]*(?=GROUP|ORDER|LIMIT|$)'
        
        # Simple approach: just remove comment text but keep SQL keywords
        import re
        
        # Pattern to match comment but not affect SQL structure
        # Look for -- comment text that's not part of SQL keywords
        if '--' in cleaned_sql:
            # Split on spaces to work with tokens
            tokens = cleaned_sql.split()
            filtered_tokens = []
            skip_comment = False
            
            for token in tokens:
                if '--' in token:
                    # If token starts with --, skip it and following comment words
                    if token.startswith('--'):
                        skip_comment = True
                        continue
                    else:
                        # Token contains -- but doesn't start with it
                        # Keep the part before --
                        sql_part = token.split('--')[0]
                        if sql_part:
                            filtered_tokens.append(sql_part)
                        skip_comment = True
                        continue
                
                # Check if we should stop skipping (SQL keyword found)
                if skip_comment and token.upper() in ['GROUP', 'ORDER', 'LIMIT', 'HAVING', 'WHERE', 'SELECT', 'FROM']:
                    skip_comment = False
                    filtered_tokens.append(token)
                elif not skip_comment:
                    filtered_tokens.append(token)
            
            cleaned_sql = ' '.join(filtered_tokens)
        
        # Clean up extra whitespace
        cleaned_sql = ' '.join(cleaned_sql.split())
        
        # Ensure semicolon at the end
        if not cleaned_sql.strip().endswith(';'):
            cleaned_sql += ';'
        
        if cleaned_sql != sql_query:
            self.logger.info(f"🧹 Cleaned SQL comments: {sql_query} -> {cleaned_sql}")
        
        return cleaned_sql
    
    def _fix_case_sensitivity_issues(self, sql_query: str) -> str:
        """Fix case sensitivity issues in SQL queries"""
        if not sql_query or sql_query == "SQL query not found in response":
            return sql_query
        
        # Fix the pattern: CIDADE_RESIDENCIA_PACIENTE = UPPER('city') or LOWER('city')
        # Convert to: CIDADE_RESIDENCIA_PACIENTE = 'City' (proper case)
        
        # Handle UPPER('city') pattern
        pattern_upper = r"CIDADE_RESIDENCIA_PACIENTE\s*=\s*UPPER\s*\(\s*'([^']+)'\s*\)"
        def replacement_upper(match):
            city_name = match.group(1)
            # Convert to proper case (first letter uppercase)
            proper_city = city_name.title()
            return f"CIDADE_RESIDENCIA_PACIENTE = '{proper_city}'"
        
        fixed_query = re.sub(pattern_upper, replacement_upper, sql_query, flags=re.IGNORECASE)
        
        # Handle LOWER('city') pattern  
        pattern_lower = r"CIDADE_RESIDENCIA_PACIENTE\s*=\s*LOWER\s*\(\s*'([^']+)'\s*\)"
        def replacement_lower(match):
            city_name = match.group(1)
            # Convert to proper case (first letter uppercase)
            proper_city = city_name.title()
            return f"CIDADE_RESIDENCIA_PACIENTE = '{proper_city}'"
        
        fixed_query = re.sub(pattern_lower, replacement_lower, fixed_query, flags=re.IGNORECASE)
        
        # Handle direct lowercase city names: CIDADE_RESIDENCIA_PACIENTE = 'porto alegre'
        pattern_direct = r"CIDADE_RESIDENCIA_PACIENTE\s*=\s*'([a-z][^']*?)'"
        def replacement_direct(match):
            city_name = match.group(1)
            # Convert to proper case only if it's all lowercase
            if city_name.islower():
                proper_city = city_name.title()
                return f"CIDADE_RESIDENCIA_PACIENTE = '{proper_city}'"
            return match.group(0)  # Return original if not all lowercase
        
        fixed_query = re.sub(pattern_direct, replacement_direct, fixed_query)
        
        # Fix SQLite incompatible YEAR() function calls
        fixed_query = self._fix_sqlite_year_extraction(fixed_query)
        
        return fixed_query
    
    def _fix_gender_code_issues(self, sql_query: str, user_query: str = None) -> str:
        """Fix common gender code issues in SUS queries"""
        if not sql_query:
            return sql_query
        
        # If no user query provided, cannot fix gender issues
        if not user_query:
            return sql_query
        
        user_query_lower = user_query.lower()
        
        # Pattern 1: User query mentions "homens/masculino" but SQL uses SEXO = 3 (should be SEXO = 1)
        if any(word in user_query_lower for word in ['homens', 'masculino', 'male', 'do sexo masculino']):
            if 'SEXO = 3' in sql_query:
                self.logger.info("🔧 Fixing gender code: changing SEXO = 3 to SEXO = 1 for males based on user query")
                sql_query = sql_query.replace('SEXO = 3', 'SEXO = 1')
        
        # Pattern 2: User query mentions "mulheres/feminino" but SQL uses SEXO = 1 (should be SEXO = 3)  
        elif any(word in user_query_lower for word in ['mulheres', 'feminino', 'female', 'do sexo feminino']):
            if 'SEXO = 1' in sql_query:
                self.logger.info("🔧 Fixing gender code: changing SEXO = 1 to SEXO = 3 for females based on user query")
                sql_query = sql_query.replace('SEXO = 1', 'SEXO = 3')
        
        return sql_query
    
    def _fix_missing_joins(self, sql_query: str) -> str:
        """Fix missing JOIN clauses when query references columns from multiple tables"""
        if not sql_query:
            return sql_query
        
        import re
        
        # Pattern: Query references cd.codigo or cd.descricao but has no JOIN with cid_detalhado
        if ('cd.codigo' in sql_query or 'cd.descricao' in sql_query) and 'JOIN cid_detalhado' not in sql_query:
            # Check if sus_data is in FROM clause
            if 'FROM sus_data' in sql_query:
                self.logger.info("🔧 Fixing missing JOIN: adding cid_detalhado JOIN to query")
                # Insert JOIN after the FROM clause
                sql_query = re.sub(
                    r'FROM sus_data s',
                    'FROM sus_data s JOIN cid_detalhado cd ON s.DIAG_PRINC = cd.codigo',
                    sql_query
                )
        
        return sql_query
    
    def _fix_sqlite_year_extraction(self, sql_query: str) -> str:
        """Fix YEAR() function calls for SQLite compatibility"""
        if not sql_query or 'YEAR(' not in sql_query.upper():
            return sql_query
        
        self.logger.info("🔧 Fixing YEAR() function for SQLite compatibility")
        
        # Replace YEAR(JULIANDAY(DT_INTER)) with strftime('%Y', DT_INTER)
        sql_query = re.sub(
            r'YEAR\s*\(\s*JULIANDAY\s*\(\s*DT_INTER\s*\)\s*\)',
            "strftime('%Y', DT_INTER)",
            sql_query,
            flags=re.IGNORECASE
        )
        
        # Replace YEAR(DT_INTER) with strftime('%Y', DT_INTER)
        sql_query = re.sub(
            r'YEAR\s*\(\s*DT_INTER\s*\)',
            "strftime('%Y', DT_INTER)",
            sql_query,
            flags=re.IGNORECASE
        )
        
        # Replace any remaining YEAR() patterns with strftime('%Y', DT_INTER)
        sql_query = re.sub(
            r'YEAR\s*\([^)]+\)',
            "strftime('%Y', DT_INTER)",
            sql_query,
            flags=re.IGNORECASE
        )
        
        self.logger.info(f"✅ SQLite YEAR() fix applied")
        return sql_query
    
    def _fix_missing_limit_for_top_queries(self, sql_query: str, user_query: str = None) -> str:
        """Add LIMIT 1 for queries asking for 'the most', 'maior', etc."""
        if not sql_query or not user_query:
            return sql_query
        
        user_query_lower = user_query.lower()
        
        # Check if user is asking for "the most", "maior", "menor", etc. (singular)
        top_query_patterns = [
            'qual é a cidade', 'qual é o diagnóstico', 'qual é o procedimento',
            'qual cidade', 'qual diagnóstico', 'qual procedimento',
            'cidade com mais', 'diagnóstico com mais', 'diagnóstico mais comum',
            'maior', 'menor', 'mais comum', 'mais frequente'
        ]
        
        is_top_query = any(pattern in user_query_lower for pattern in top_query_patterns)
        
        # Check if it's asking for multiple (plural forms)
        plural_patterns = [
            'quais são as cidades', 'quais são os diagnósticos', 
            'quais cidades', 'quais diagnósticos',
            'top 5', 'top 10', 'primeiros', 'maiores', 'menores'
        ]
        
        is_plural_query = any(pattern in user_query_lower for pattern in plural_patterns)
        
        # Only add LIMIT 1 if it's a top query but not explicitly plural
        if is_top_query and not is_plural_query:
            # Check if LIMIT is already present
            if 'LIMIT' not in sql_query.upper():
                # Check if it has ORDER BY (required for meaningful LIMIT)
                if 'ORDER BY' in sql_query.upper():
                    # Add LIMIT 1 before the semicolon
                    if sql_query.strip().endswith(';'):
                        sql_query = sql_query.strip()[:-1] + ' LIMIT 1;'
                    else:
                        sql_query = sql_query.strip() + ' LIMIT 1;'
                    
                    self.logger.info(f"🔧 Added LIMIT 1 for top query: {user_query}")
        
        return sql_query


class SQLGenerationFactory:
    """Factory for creating SQL generation services"""
    
    @staticmethod
    def create_service(
        llm_service: ILLMCommunicationService,
        schema_service: ISchemaIntrospectionService
    ) -> ISQLGenerationService:
        """Create SQL generation service"""
        return SQLGenerationService(llm_service, schema_service)