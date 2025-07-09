"""
Query Decomposition Domain Entities
Entidades do domínio para decomposição de queries complexas
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from enum import Enum


class QueryComplexityLevel(Enum):
    """Níveis de complexidade de uma query"""
    SIMPLE = "simple"           # Score 0-39: Query simples, sem decomposição
    MODERATE = "moderate"       # Score 40-59: Pode se beneficiar de decomposição
    COMPLEX = "complex"         # Score 60-79: Decomposição recomendada
    VERY_COMPLEX = "very_complex"  # Score 80-100: Decomposição obrigatória


class QueryStepType(Enum):
    """Tipos de etapas na decomposição"""
    FILTER = "filter"           # Filtro de dados (WHERE clause)
    AGGREGATE = "aggregate"     # Agregação (GROUP BY, COUNT, SUM, etc.)
    CALCULATE = "calculate"     # Cálculos complexos (JULIANDAY, CASE WHEN)
    RANK = "rank"              # Ranking e ordenação (ORDER BY, LIMIT)
    JOIN = "join"              # Junção de tabelas (se necessário)
    TEMPORAL = "temporal"      # Operações temporais específicas
    GEOGRAPHIC = "geographic"   # Operações geográficas


class DecompositionStrategy(Enum):
    """Estratégias de decomposição disponíveis"""
    SEQUENTIAL_FILTERING = "sequential_filtering"        # Filtros em sequência
    DEMOGRAPHIC_ANALYSIS = "demographic_analysis"       # Análise demográfica
    TEMPORAL_BREAKDOWN = "temporal_breakdown"           # Quebra temporal
    RANKING_WITH_DETAILS = "ranking_with_details"       # Ranking + detalhamento
    FINANCIAL_AGGREGATION = "financial_aggregation"     # Agregação financeira
    GEOGRAPHIC_ANALYSIS = "geographic_analysis"         # Análise geográfica
    DIAGNOSIS_CLASSIFICATION = "diagnosis_classification" # Classificação de diagnósticos


@dataclass
class QueryStep:
    """
    Representa uma etapa individual na decomposição de uma query
    """
    step_id: int
    step_type: QueryStepType
    description: str
    sql_template: str
    input_parameters: List[str] = field(default_factory=list)
    expected_output_columns: List[str] = field(default_factory=list)
    depends_on_steps: List[int] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    estimated_execution_time: Optional[float] = None
    is_optional: bool = False
    
    def __post_init__(self):
        """Validação após inicialização"""
        if self.step_id < 1:
            raise ValueError("step_id deve ser >= 1")
        if not self.description.strip():
            raise ValueError("description não pode estar vazia")
        if not self.sql_template.strip():
            raise ValueError("sql_template não pode estar vazio")


@dataclass
class QueryPlan:
    """
    Plano completo de execução para uma query decomposta
    """
    plan_id: str
    original_query: str
    complexity_score: float
    complexity_level: QueryComplexityLevel
    strategy: DecompositionStrategy
    steps: List[QueryStep]
    estimated_total_time: Optional[float] = None
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validação e setup após inicialização"""
        if not self.steps:
            raise ValueError("Plano deve ter pelo menos uma etapa")
        
        # Validar IDs únicos
        step_ids = [step.step_id for step in self.steps]
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("step_ids devem ser únicos")
        
        # Validar dependências
        for step in self.steps:
            for dep_id in step.depends_on_steps:
                if dep_id not in step_ids:
                    raise ValueError(f"Dependência {dep_id} não encontrada para step {step.step_id}")
                if dep_id >= step.step_id:
                    raise ValueError(f"Dependência circular: step {step.step_id} depende de {dep_id}")
    
    @property
    def total_steps(self) -> int:
        """Número total de etapas no plano"""
        return len(self.steps)
    
    @property
    def dependency_graph(self) -> Dict[int, List[int]]:
        """Grafo de dependências entre etapas"""
        return {step.step_id: step.depends_on_steps for step in self.steps}
    
    def get_step_by_id(self, step_id: int) -> Optional[QueryStep]:
        """Busca uma etapa pelo ID"""
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None
    
    def get_executable_steps(self, completed_steps: List[int]) -> List[QueryStep]:
        """Retorna etapas que podem ser executadas baseado nas dependências"""
        executable = []
        for step in self.steps:
            if step.step_id in completed_steps:
                continue
            
            # Verificar se todas as dependências foram completadas
            deps_satisfied = all(dep_id in completed_steps for dep_id in step.depends_on_steps)
            if deps_satisfied:
                executable.append(step)
        
        return executable


@dataclass
class StepExecutionResult:
    """
    Resultado da execução de uma etapa específica
    """
    step_id: int
    success: bool
    sql_executed: str
    results: List[Dict[str, Any]]
    row_count: int
    execution_time: float
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    executed_at: datetime = field(default_factory=datetime.now)


@dataclass
class PlanExecutionResult:
    """
    Resultado completo da execução de um plano de decomposição
    """
    plan_id: str
    success: bool
    step_results: List[StepExecutionResult]
    total_execution_time: float
    final_results: List[Dict[str, Any]]
    final_row_count: int
    completed_steps: List[int]
    failed_step_id: Optional[int] = None
    error_message: Optional[str] = None
    executed_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def completion_rate(self) -> float:
        """Taxa de conclusão do plano (0.0 a 1.0)"""
        if not self.step_results:
            return 0.0
        successful_steps = sum(1 for result in self.step_results if result.success)
        return successful_steps / len(self.step_results)
    
    def get_step_result(self, step_id: int) -> Optional[StepExecutionResult]:
        """Busca resultado de uma etapa específica"""
        for result in self.step_results:
            if result.step_id == step_id:
                return result
        return None


@dataclass
class ComplexityAnalysis:
    """
    Análise de complexidade de uma query
    """
    query: str
    complexity_score: float
    complexity_level: QueryComplexityLevel
    complexity_factors: Dict[str, float]  # Fatores que contribuem para complexidade
    recommended_strategy: Optional[DecompositionStrategy] = None
    patterns_detected: List[str] = field(default_factory=list)
    estimated_decomposition_benefit: float = 0.0  # % de melhoria esperada
    analysis_metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def should_decompose(self) -> bool:
        """Indica se a query deve ser decomposta"""
        return self.complexity_level in [QueryComplexityLevel.COMPLEX, QueryComplexityLevel.VERY_COMPLEX]
    
    @property
    def decomposition_priority(self) -> str:
        """Prioridade de decomposição baseada na complexidade"""
        if self.complexity_level == QueryComplexityLevel.VERY_COMPLEX:
            return "HIGH"
        elif self.complexity_level == QueryComplexityLevel.COMPLEX:
            return "MEDIUM"
        elif self.complexity_level == QueryComplexityLevel.MODERATE:
            return "LOW"
        else:
            return "NONE"


# Type aliases para melhor legibilidade
QueryParameters = Dict[str, Union[str, int, float, List[Any]]]
ExecutionContext = Dict[str, Any]
TemplateVariables = Dict[str, str]