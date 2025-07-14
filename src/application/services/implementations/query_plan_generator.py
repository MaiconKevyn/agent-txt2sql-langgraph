"""
Query Plan Generator Implementation
Implementação do gerador de planos de execução
"""
import uuid
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from domain.entities.query_decomposition import (
    QueryPlan,
    QueryStep,
    QueryStepType,
    ComplexityAnalysis,
    DecompositionStrategy,
    QueryParameters
)
from application.services.query_planner_service import (
    IQueryPlanGenerator, 
    PlanGenerationError,
    PlanValidationError
)
from .query_template_matcher import DecompositionTemplate


class QueryPlanGenerator(IQueryPlanGenerator):
    """
    Implementação do gerador de planos de execução baseado em templates
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._step_counter = 0
    
    def generate_plan(
        self,
        query: str,
        analysis: ComplexityAnalysis,
        strategy: Optional[DecompositionStrategy] = None
    ) -> QueryPlan:
        """
        Gera plano de execução baseado na análise de complexidade
        """
        try:
            # Usar estratégia fornecida ou recomendada pela análise
            target_strategy = strategy or analysis.recommended_strategy
            
            if not target_strategy:
                raise PlanGenerationError("Nenhuma estratégia de decomposição identificada")
            
            # Gerar ID único para o plano
            plan_id = f"plan_{uuid.uuid4().hex[:8]}_{int(datetime.now().timestamp())}"
            
            # Gerar etapas baseadas na estratégia
            steps = self._generate_steps_for_strategy(query, analysis, target_strategy)
            
            if not steps:
                raise PlanGenerationError(f"Não foi possível gerar etapas para estratégia: {target_strategy}")
            
            # Estimar tempo de execução
            estimated_time = self._estimate_plan_execution_time(steps)
            
            # Criar plano
            plan = QueryPlan(
                plan_id=plan_id,
                original_query=query,
                complexity_score=analysis.complexity_score,
                complexity_level=analysis.complexity_level,
                strategy=target_strategy,
                steps=steps,
                estimated_total_time=estimated_time,
                metadata={
                    "generated_at": datetime.now().isoformat(),
                    "patterns_used": analysis.patterns_detected,
                    "complexity_factors": analysis.complexity_factors,
                    "generator_version": "1.0.0"
                }
            )
            
            self.logger.info(f"Plano gerado: {plan_id} com {len(steps)} etapas para estratégia {target_strategy}")
            return plan
            
        except Exception as e:
            self.logger.error(f"Erro na geração do plano: {e}")
            raise PlanGenerationError(f"Falha na geração do plano: {str(e)}")
    
    def optimize_plan(self, plan: QueryPlan) -> QueryPlan:
        """
        Otimiza plano existente
        """
        try:
            optimized_steps = []
            
            for step in plan.steps:
                optimized_step = self._optimize_step(step, plan)
                optimized_steps.append(optimized_step)
            
            # Verificar se podemos combinar etapas
            combined_steps = self._combine_compatible_steps(optimized_steps)
            
            # Reordenar etapas para otimizar dependências
            reordered_steps = self._optimize_step_order(combined_steps)
            
            # Criar plano otimizado
            optimized_plan = QueryPlan(
                plan_id=f"{plan.plan_id}_optimized",
                original_query=plan.original_query,
                complexity_score=plan.complexity_score,
                complexity_level=plan.complexity_level,
                strategy=plan.strategy,
                steps=reordered_steps,
                estimated_total_time=self._estimate_plan_execution_time(reordered_steps),
                metadata={
                    **plan.metadata,
                    "optimized_at": datetime.now().isoformat(),
                    "original_steps": len(plan.steps),
                    "optimized_steps": len(reordered_steps)
                }
            )
            
            self.logger.info(f"Plano otimizado: {len(plan.steps)} -> {len(reordered_steps)} etapas")
            return optimized_plan
            
        except Exception as e:
            self.logger.warning(f"Erro na otimização, usando plano original: {e}")
            return plan
    
    def validate_plan(self, plan: QueryPlan) -> List[str]:
        """
        Valida plano de execução
        """
        warnings = []
        
        try:
            # 1. Validar dependências
            dependency_warnings = self._validate_dependencies(plan)
            warnings.extend(dependency_warnings)
            
            # 2. Validar SQL templates
            sql_warnings = self._validate_sql_templates(plan)
            warnings.extend(sql_warnings)
            
            # 3. Validar estrutura do plano
            structure_warnings = self._validate_plan_structure(plan)
            warnings.extend(structure_warnings)
            
            # 4. Validar performance esperada
            performance_warnings = self._validate_performance_expectations(plan)
            warnings.extend(performance_warnings)
            
            if warnings:
                self.logger.warning(f"Plano {plan.plan_id} tem {len(warnings)} avisos de validação")
            else:
                self.logger.info(f"Plano {plan.plan_id} validado com sucesso")
            
            return warnings
            
        except Exception as e:
            error_msg = f"Erro na validação do plano: {str(e)}"
            self.logger.error(error_msg)
            raise PlanValidationError(error_msg)
    
    def _generate_steps_for_strategy(
        self,
        query: str,
        analysis: ComplexityAnalysis,
        strategy: DecompositionStrategy
    ) -> List[QueryStep]:
        """
        Gera etapas específicas para uma estratégia
        """
        self._step_counter = 0
        
        if strategy == DecompositionStrategy.SEQUENTIAL_FILTERING:
            return self._generate_sequential_filtering_steps(query, analysis)
        elif strategy == DecompositionStrategy.DEMOGRAPHIC_ANALYSIS:
            return self._generate_demographic_analysis_steps(query, analysis)
        elif strategy == DecompositionStrategy.TEMPORAL_BREAKDOWN:
            return self._generate_temporal_breakdown_steps(query, analysis)
        elif strategy == DecompositionStrategy.RANKING_WITH_DETAILS:
            return self._generate_ranking_with_details_steps(query, analysis)
        elif strategy == DecompositionStrategy.DIAGNOSIS_CLASSIFICATION:
            return self._generate_diagnosis_classification_steps(query, analysis)
        elif strategy == DecompositionStrategy.GEOGRAPHIC_ANALYSIS:
            return self._generate_geographic_analysis_steps(query, analysis)
        elif strategy == DecompositionStrategy.FINANCIAL_AGGREGATION:
            return self._generate_financial_aggregation_steps(query, analysis)
        else:
            # Fallback: estratégia genérica
            return self._generate_generic_steps(query, analysis)
    
    def _generate_sequential_filtering_steps(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> List[QueryStep]:
        """
        Gera etapas para filtros sequenciais
        """
        steps = []
        query_lower = query.lower()
        
        # Identificar filtros necessários
        filters = []
        
        # Filtro de sexo
        if any(pattern in query_lower for pattern in ['mulher', 'feminino', 'homem', 'masculino']):
            sexo_code = 3 if any(p in query_lower for p in ['mulher', 'feminino']) else 1
            filters.append({
                'type': 'sexo',
                'sql': f"SEXO = {sexo_code}",
                'description': f"Filtrar por sexo ({'feminino' if sexo_code == 3 else 'masculino'})"
            })
        
        # Filtro de idade
        import re
        idade_match = re.search(r'(mais de|menos de|acima de|abaixo de|maior que|menor que)\s+(\d+)', query_lower)
        if idade_match:
            op_text, valor = idade_match.groups()
            op_sql = '>=' if 'mais' in op_text or 'acima' in op_text or 'maior' in op_text else '<='
            filters.append({
                'type': 'idade',
                'sql': f"IDADE {op_sql} {valor}",
                'description': f"Filtrar por idade {op_text} {valor} anos"
            })
        
        # Filtro de morte
        if any(pattern in query_lower for pattern in ['mort', 'óbit', 'falec']):
            filters.append({
                'type': 'morte',
                'sql': "MORTE = 1",
                'description': "Filtrar por mortes confirmadas"
            })
        
        # Filtro de diagnóstico
        if any(pattern in query_lower for pattern in ['respiratória', 'respiratorio', 'pulmão', 'j90', 'j44']):
            filters.append({
                'type': 'diagnostico',
                'sql': "DIAG_PRINC LIKE 'J%'",
                'description': "Filtrar por doenças respiratórias"
            })
        elif any(pattern in query_lower for pattern in ['cardíaca', 'coração', 'infarto', 'i21', 'i25']):
            filters.append({
                'type': 'diagnostico',
                'sql': "DIAG_PRINC LIKE 'I%'",
                'description': "Filtrar por doenças cardiovasculares"
            })
        elif any(pattern in query_lower for pattern in ['neoplasia', 'cancer', 'tumor', 'c78', 'c80']):
            filters.append({
                'type': 'diagnostico',
                'sql': "DIAG_PRINC LIKE 'C%'",
                'description': "Filtrar por neoplasias"
            })
        
        # Gerar etapas de filtro
        current_table = "sus_data"
        for i, filter_def in enumerate(filters):
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.FILTER,
                description=filter_def['description'],
                sql_template=f"SELECT * FROM {current_table} WHERE {filter_def['sql']}",
                depends_on_steps=[step_id - 1] if step_id > 1 else [],
                metadata={'filter_type': filter_def['type']}
            ))
            current_table = f"step_{step_id}_result"
        
        # Etapa de agregação
        if any(pattern in query_lower for pattern in ['cidade', 'município']):
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.AGGREGATE,
                description="Agrupar por cidade e contar",
                sql_template=f"SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_casos FROM {current_table} GROUP BY CIDADE_RESIDENCIA_PACIENTE",
                depends_on_steps=[step_id - 1] if steps else []
            ))
            current_table = f"step_{step_id}_result"
        
        # Etapa de ranking se solicitado
        limit_match = re.search(r'(?:top\s+|primeiros?\s+)?(\d+)', query_lower)
        if limit_match or any(pattern in query_lower for pattern in ['mais', 'maior', 'principal']):
            step_id = self._next_step_id()
            limit_value = limit_match.group(1) if limit_match else "10"
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.RANK,
                description=f"Ordenar e limitar a {limit_value} resultados",
                sql_template=f"SELECT * FROM {current_table} ORDER BY total_casos DESC LIMIT {limit_value}",
                depends_on_steps=[step_id - 1] if steps else []
            ))
        
        return steps
    
    def _generate_demographic_analysis_steps(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> List[QueryStep]:
        """
        Gera etapas para análise demográfica
        """
        return self._generate_sequential_filtering_steps(query, analysis)
    
    def _generate_temporal_breakdown_steps(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> List[QueryStep]:
        """
        Gera etapas para análise temporal
        """
        steps = []
        query_lower = query.lower()
        
        # Etapa 1: Calcular tempo de internação
        if any(pattern in query_lower for pattern in ['tempo', 'internação', 'média', 'dias']):
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.CALCULATE,
                description="Calcular tempo de internação em dias",
                sql_template="""SELECT *, 
                    JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                    JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2)) 
                    as tempo_internacao_dias
                FROM sus_data""",
                depends_on_steps=[]
            ))
            
            # Etapa 2: Filtrar casos válidos
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.FILTER,
                description="Filtrar casos com alta válida",
                sql_template="SELECT * FROM step_1_result WHERE DT_SAIDA IS NOT NULL AND DT_SAIDA != '' AND tempo_internacao_dias >= 0 AND tempo_internacao_dias <= 365",
                depends_on_steps=[1]
            ))
        
        # Adicionar filtros específicos se necessário
        current_step = len(steps)
        if any(pattern in query_lower for pattern in ['respiratória', 'j%']):
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.FILTER,
                description="Filtrar doenças respiratórias",
                sql_template=f"SELECT * FROM step_{current_step}_result WHERE DIAG_PRINC LIKE 'J%'",
                depends_on_steps=[current_step]
            ))
            current_step = step_id
        
        # Etapa de agregação temporal
        if any(pattern in query_lower for pattern in ['cidade', 'município']):
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.AGGREGATE,
                description="Calcular tempo médio por cidade",
                sql_template=f"SELECT CIDADE_RESIDENCIA_PACIENTE, AVG(tempo_internacao_dias) as tempo_medio_dias, COUNT(*) as casos FROM step_{current_step}_result GROUP BY CIDADE_RESIDENCIA_PACIENTE",
                depends_on_steps=[current_step]
            ))
        
        return steps
    
    def _generate_ranking_with_details_steps(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> List[QueryStep]:
        """
        Gera etapas para ranking com detalhamento
        """
        steps = []
        query_lower = query.lower()
        
        # Etapa 1: Ranking principal
        step_id = self._next_step_id()
        if 'cidade' in query_lower or 'procedimento' in query_lower:
            if 'procedimento' in query_lower:
                group_column = "PROC_REA"
                description = "Calcular ranking de procedimentos"
            else:
                group_column = "CIDADE_RESIDENCIA_PACIENTE"
                description = "Calcular ranking de cidades"
                
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.AGGREGATE,
                description=description,
                sql_template=f"SELECT {group_column}, COUNT(*) as total FROM sus_data GROUP BY {group_column}",
                depends_on_steps=[]
            ))
            
            # Etapa 2: Ordenar e limitar
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.RANK,
                description="Ordenar e limitar resultados",
                sql_template="SELECT * FROM step_1_result ORDER BY total DESC LIMIT 5",
                depends_on_steps=[1]
            ))
        
        return steps
    
    def _generate_diagnosis_classification_steps(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> List[QueryStep]:
        """
        Gera etapas para classificação de diagnósticos (especial para neoplasias)
        """
        steps = []
        query_lower = query.lower()
        
        # Estratégia segura para neoplasias
        if any(pattern in query_lower for pattern in ['neoplasia', 'cancer', 'tumor']):
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.FILTER,
                description="Filtrar diagnósticos de neoplasias (código C)",
                sql_template="SELECT * FROM sus_data WHERE DIAG_PRINC LIKE 'C%'",
                depends_on_steps=[]
            ))
            
            if 'mort' in query_lower:
                step_id = self._next_step_id()
                steps.append(QueryStep(
                    step_id=step_id,
                    step_type=QueryStepType.FILTER,
                    description="Filtrar mortes por neoplasias",
                    sql_template="SELECT * FROM step_1_result WHERE MORTE = 1",
                    depends_on_steps=[1]
                ))
            
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.AGGREGATE,
                description="Contar por categoria de neoplasia",
                sql_template=f"SELECT SUBSTR(DIAG_PRINC, 1, 3) as categoria_neoplasia, COUNT(*) as total FROM step_{step_id-1}_result GROUP BY categoria_neoplasia",
                depends_on_steps=[step_id-1]
            ))
        
        return steps
    
    def _generate_geographic_analysis_steps(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> List[QueryStep]:
        """
        Gera etapas para análise geográfica
        """
        return self._generate_sequential_filtering_steps(query, analysis)
    
    def _generate_financial_aggregation_steps(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> List[QueryStep]:
        """
        Gera etapas para agregação financeira
        """
        steps = []
        query_lower = query.lower()
        
        # Etapa 1: Filtrar registros válidos
        step_id = self._next_step_id()
        steps.append(QueryStep(
            step_id=step_id,
            step_type=QueryStepType.FILTER,
            description="Filtrar registros com valores financeiros válidos",
            sql_template="SELECT * FROM sus_data WHERE VAL_TOT IS NOT NULL AND VAL_TOT > 0",
            depends_on_steps=[]
        ))
        
        # Etapa 2: Agregar por procedimento
        step_id = self._next_step_id()
        steps.append(QueryStep(
            step_id=step_id,
            step_type=QueryStepType.AGGREGATE,
            description="Calcular custo total por procedimento",
            sql_template="SELECT PROC_REA, SUM(VAL_TOT) as custo_total, COUNT(*) as casos FROM step_1_result GROUP BY PROC_REA",
            depends_on_steps=[1]
        ))
        
        # Etapa 3: Filtrar por volume se necessário
        if '100' in query_lower or 'mais de' in query_lower:
            step_id = self._next_step_id()
            steps.append(QueryStep(
                step_id=step_id,
                step_type=QueryStepType.FILTER,
                description="Filtrar procedimentos com mais de 100 casos",
                sql_template="SELECT * FROM step_2_result WHERE casos > 100",
                depends_on_steps=[2]
            ))
        
        return steps
    
    def _generate_generic_steps(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> List[QueryStep]:
        """
        Gera etapas genéricas como fallback
        """
        step_id = self._next_step_id()
        return [QueryStep(
            step_id=step_id,
            step_type=QueryStepType.FILTER,
            description="Consulta genérica (fallback)",
            sql_template="SELECT * FROM sus_data LIMIT 1000",
            depends_on_steps=[]
        )]
    
    def _next_step_id(self) -> int:
        """Gera próximo ID de etapa"""
        self._step_counter += 1
        return self._step_counter
    
    def _estimate_plan_execution_time(self, steps: List[QueryStep]) -> float:
        """Estima tempo de execução do plano"""
        base_time_per_step = 2.0  # segundos
        return len(steps) * base_time_per_step
    
    def _optimize_step(self, step: QueryStep, plan: QueryPlan) -> QueryStep:
        """Otimiza uma etapa individual"""
        # Por enquanto, retorna a etapa original
        return step
    
    def _combine_compatible_steps(self, steps: List[QueryStep]) -> List[QueryStep]:
        """Combina etapas compatíveis para otimização"""
        # Por enquanto, retorna as etapas originais
        return steps
    
    def _optimize_step_order(self, steps: List[QueryStep]) -> List[QueryStep]:
        """Otimiza ordem das etapas"""
        # Por enquanto, retorna a ordem original
        return steps
    
    def _validate_dependencies(self, plan: QueryPlan) -> List[str]:
        """Valida dependências entre etapas"""
        warnings = []
        step_ids = {step.step_id for step in plan.steps}
        
        for step in plan.steps:
            for dep_id in step.depends_on_steps:
                if dep_id not in step_ids:
                    warnings.append(f"Etapa {step.step_id} depende de etapa inexistente {dep_id}")
        
        return warnings
    
    def _validate_sql_templates(self, plan: QueryPlan) -> List[str]:
        """Valida templates SQL das etapas"""
        warnings = []
        
        for step in plan.steps:
            if not step.sql_template or not step.sql_template.strip():
                warnings.append(f"Etapa {step.step_id} tem template SQL vazio")
        
        return warnings
    
    def _validate_plan_structure(self, plan: QueryPlan) -> List[str]:
        """Valida estrutura geral do plano"""
        warnings = []
        
        if len(plan.steps) > 10:
            warnings.append(f"Plano muito complexo com {len(plan.steps)} etapas")
        
        if not plan.steps:
            warnings.append("Plano sem etapas")
        
        return warnings
    
    def _validate_performance_expectations(self, plan: QueryPlan) -> List[str]:
        """Valida expectativas de performance"""
        warnings = []
        
        if plan.estimated_total_time and plan.estimated_total_time > 120:
            warnings.append(f"Tempo estimado muito alto: {plan.estimated_total_time}s")
        
        return warnings