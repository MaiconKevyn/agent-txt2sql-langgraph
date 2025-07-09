"""
Step Executor Implementation
Implementação do executor de etapas individuais
"""
import re
import logging
from typing import Dict, Any, List
from datetime import datetime

from domain.entities.query_decomposition import (
    QueryStep,
    StepExecutionResult,
    QueryStepType
)
from application.services.execution_orchestrator_service import (
    IStepExecutor,
    StepExecutionContext,
    StepExecutionError
)
from application.services.query_processing_service import IQueryProcessingService


class StepExecutor(IStepExecutor):
    """
    Implementação do executor de etapas que integra com QueryProcessingService existente
    """
    
    def __init__(self, query_processing_service: IQueryProcessingService):
        """
        Inicializa o executor de etapas
        
        Args:
            query_processing_service: Serviço de processamento de queries existente
        """
        self.query_service = query_processing_service
        self.logger = logging.getLogger(__name__)
    
    def execute_step(self, context: StepExecutionContext) -> StepExecutionResult:
        """
        Executa uma etapa específica do plano
        """
        start_time = datetime.now()
        step = context.step
        
        try:
            self.logger.info(f"Executando etapa {step.step_id}: {step.description}")
            
            # Validar pré-requisitos
            prerequisites_issues = self.validate_step_prerequisites(context)
            if prerequisites_issues:
                raise StepExecutionError(
                    step.step_id,
                    f"Pré-requisitos não atendidos: {', '.join(prerequisites_issues)}"
                )
            
            # Preparar SQL final
            sql_query = self.prepare_step_sql(context)
            
            self.logger.info(f"SQL preparado para etapa {step.step_id}: {sql_query[:100]}...")
            
            # Executar SQL usando o serviço existente
            query_result = self.query_service.execute_sql_query(sql_query)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            if query_result.success:
                self.logger.info(
                    f"Etapa {step.step_id} executada com sucesso: "
                    f"{query_result.row_count} registros em {execution_time:.2f}s"
                )
                
                return StepExecutionResult(
                    step_id=step.step_id,
                    success=True,
                    sql_executed=sql_query,
                    results=query_result.results,
                    row_count=query_result.row_count,
                    execution_time=execution_time,
                    metadata={
                        "step_type": step.step_type.value,
                        "step_description": step.description,
                        "original_sql_template": step.sql_template,
                        "query_service_metadata": query_result.metadata
                    }
                )
            else:
                raise StepExecutionError(
                    step.step_id,
                    f"Falha na execução SQL: {query_result.error_message}",
                    Exception(query_result.error_message)
                )
                
        except StepExecutionError:
            # Re-raise step execution errors
            raise
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Erro inesperado na execução da etapa {step.step_id}: {str(e)}"
            self.logger.error(error_msg)
            
            return StepExecutionResult(
                step_id=step.step_id,
                success=False,
                sql_executed=context.step.sql_template,  # Template original
                results=[],
                row_count=0,
                execution_time=execution_time,
                error_message=error_msg,
                metadata={
                    "step_type": step.step_type.value,
                    "error_type": type(e).__name__
                }
            )
    
    def prepare_step_sql(self, context: StepExecutionContext) -> str:
        """
        Prepara o SQL final para execução, substituindo parâmetros
        """
        step = context.step
        sql_template = step.sql_template
        variables = context.execution_variables
        previous_results = context.previous_results
        
        # Substituir variáveis básicas
        sql = sql_template
        for var_name, var_value in variables.items():
            placeholder = f"{{{var_name}}}"
            sql = sql.replace(placeholder, str(var_value))
        
        # Substituir referências a resultados de etapas anteriores
        sql = self._replace_table_references(sql, step, previous_results)
        
        # Substituir parâmetros específicos baseados no tipo de etapa
        sql = self._replace_step_specific_parameters(sql, step, context)
        
        # Limpar SQL (remover espaços extras, etc.)
        sql = self._clean_sql(sql)
        
        return sql
    
    def validate_step_prerequisites(self, context: StepExecutionContext) -> List[str]:
        """
        Valida se os pré-requisitos da etapa foram atendidos
        """
        issues = []
        step = context.step
        previous_results = context.previous_results
        
        # Verificar dependências
        for dep_step_id in step.depends_on_steps:
            if dep_step_id not in previous_results:
                issues.append(f"Etapa dependente {dep_step_id} não foi executada")
            elif not previous_results[dep_step_id].success:
                issues.append(f"Etapa dependente {dep_step_id} falhou")
        
        # Verificar se SQL template tem placeholders necessários
        sql_template = step.sql_template
        if not sql_template or not sql_template.strip():
            issues.append("Template SQL vazio")
        
        # Verificar parâmetros obrigatórios baseados no tipo de etapa
        if step.step_type == QueryStepType.FILTER:
            if "WHERE" not in sql_template.upper() and "FROM" in sql_template.upper():
                issues.append("Etapa de filtro deve ter cláusula WHERE")
        
        elif step.step_type == QueryStepType.AGGREGATE:
            # Relaxar validação - nem todas as agregações precisam de GROUP BY
            pass
        
        elif step.step_type == QueryStepType.RANK:
            if "ORDER BY" not in sql_template.upper():
                issues.append("Etapa de ranking deve ter ORDER BY")
        
        return issues
    
    def _replace_table_references(
        self, 
        sql: str, 
        current_step: QueryStep, 
        previous_results: Dict[int, StepExecutionResult]
    ) -> str:
        """
        Substitui referências de tabelas anteriores por CTEs ou subqueries
        """
        # Procurar por referências do tipo {previous_table} ou step_N_result
        patterns = [
            r'\{previous_table\}',
            r'step_(\d+)_result',
            r'\{step_(\d+)_result\}'
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE)
            for match in matches:
                if pattern.startswith(r'step_'):
                    # Extrair número da etapa
                    step_num = int(match.group(1))
                    replacement = f"step_{step_num}_cte"
                else:
                    # Usar etapa anterior mais recente
                    if current_step.depends_on_steps:
                        latest_dep = max(current_step.depends_on_steps)
                        replacement = f"step_{latest_dep}_cte"
                    else:
                        replacement = "sus_data"  # Fallback para tabela principal
                
                sql = sql.replace(match.group(0), replacement)
        
        return sql
    
    def _replace_step_specific_parameters(
        self, 
        sql: str, 
        step: QueryStep, 
        context: StepExecutionContext
    ) -> str:
        """
        Substitui parâmetros específicos baseados no tipo de etapa
        """
        variables = context.execution_variables
        
        # Mapeamentos comuns
        replacements = {
            '{sexo_code}': '3' if 'mulher' in context.plan.original_query.lower() else '1',
            '{doenca_pattern}': self._get_disease_pattern(context.plan.original_query),
            '{grupo_coluna}': 'CIDADE_RESIDENCIA_PACIENTE',
            '{entidade_coluna}': 'CIDADE_RESIDENCIA_PACIENTE',
            '{detalhamento_coluna}': 'DIAG_PRINC',
            '{nivel_coluna}': 'CIDADE_RESIDENCIA_PACIENTE',
            '{agregacao_func}': 'SUM',
            '{min_casos}': '10',
            '{limite}': self._extract_limit_from_query(context.plan.original_query),
            '{top_n}': self._extract_limit_from_query(context.plan.original_query)
        }
        
        # Aplicar substituições
        for placeholder, value in replacements.items():
            sql = sql.replace(placeholder, value)
        
        # Substituições dinâmicas baseadas no contexto
        if '{idade_op}' in sql or '{idade_valor}' in sql:
            age_op, age_value = self._extract_age_filter(context.plan.original_query)
            sql = sql.replace('{idade_op}', age_op)
            sql = sql.replace('{idade_valor}', age_value)
        
        return sql
    
    def _get_disease_pattern(self, query: str) -> str:
        """Determina padrão de doença baseado na query"""
        query_lower = query.lower()
        
        if any(term in query_lower for term in ['respiratória', 'respiratorio', 'pulmão']):
            return 'J%'
        elif any(term in query_lower for term in ['cardíaca', 'coração', 'infarto']):
            return 'I%'
        elif any(term in query_lower for term in ['neoplasia', 'cancer', 'tumor']):
            return 'C%'
        elif any(term in query_lower for term in ['digestiva', 'estômago', 'intestino']):
            return 'K%'
        else:
            return '%'  # Qualquer diagnóstico
    
    def _extract_limit_from_query(self, query: str) -> str:
        """Extrai limite numérico da query"""
        import re
        
        # Procurar por padrões como "5 cidades", "top 10", "primeiros 3"
        patterns = [
            r'(\d+)\s+(?:cidad|municip)',
            r'top\s+(\d+)',
            r'primeiros?\s+(\d+)',
            r'maiores?\s+(\d+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, query.lower())
            if match:
                return match.group(1)
        
        return '5'  # Default
    
    def _extract_age_filter(self, query: str) -> tuple:
        """Extrai filtro de idade da query"""
        import re
        
        age_match = re.search(
            r'(mais de|menos de|acima de|abaixo de|maior que|menor que)\s+(\d+)',
            query.lower()
        )
        
        if age_match:
            op_text, value = age_match.groups()
            if any(term in op_text for term in ['mais', 'acima', 'maior']):
                return '>', value
            else:
                return '<', value
        
        return '>', '0'  # Default
    
    def _clean_sql(self, sql: str) -> str:
        """Limpa e formata SQL"""
        # Remover espaços extras
        sql = re.sub(r'\s+', ' ', sql.strip())
        
        # Garantir que termina com ponto e vírgula
        if not sql.endswith(';'):
            sql += ';'
        
        return sql