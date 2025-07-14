"""
Query Planner Service Interface
Interface para serviço de planejamento de decomposição de queries
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from domain.entities.query_decomposition import (
    QueryPlan, 
    ComplexityAnalysis,
    DecompositionStrategy,
    QueryComplexityLevel,
    QueryParameters
)


@dataclass
class PlannerConfig:
    """Configuração do serviço de planejamento"""
    complexity_threshold_decompose: float = 45.0  # Score mínimo para decomposição
    max_steps_per_plan: int = 10
    enable_parallel_execution: bool = False  # Para futuras versões
    default_strategy: DecompositionStrategy = DecompositionStrategy.SEQUENTIAL_FILTERING
    enable_query_optimization: bool = True
    cache_plans: bool = True
    cache_ttl_seconds: int = 3600


class IQueryComplexityAnalyzer(ABC):
    """
    Interface para análise de complexidade de queries
    """
    
    @abstractmethod
    def analyze_complexity(self, query: str) -> ComplexityAnalysis:
        """
        Analisa a complexidade de uma query natural
        
        Args:
            query: Query em linguagem natural
            
        Returns:
            ComplexityAnalysis com score e fatores de complexidade
        """
        pass
    
    @abstractmethod
    def detect_patterns(self, query: str) -> List[str]:
        """
        Detecta padrões conhecidos na query
        
        Args:
            query: Query em linguagem natural
            
        Returns:
            Lista de padrões detectados
        """
        pass
    
    @abstractmethod
    def estimate_decomposition_benefit(self, analysis: ComplexityAnalysis) -> float:
        """
        Estima o benefício de decompor a query (0.0 a 1.0)
        
        Args:
            analysis: Análise de complexidade
            
        Returns:
            Score de benefício esperado da decomposição
        """
        pass


class IQueryTemplateMatcher(ABC):
    """
    Interface para matching de queries com templates de decomposição
    """
    
    @abstractmethod
    def find_matching_templates(self, query: str, analysis: ComplexityAnalysis) -> List[Dict[str, Any]]:
        """
        Encontra templates que combinam com a query
        
        Args:
            query: Query em linguagem natural
            analysis: Análise de complexidade
            
        Returns:
            Lista de templates compatíveis com scores de compatibilidade
        """
        pass
    
    @abstractmethod
    def extract_parameters(self, query: str, template: Dict[str, Any]) -> QueryParameters:
        """
        Extrai parâmetros da query para um template específico
        
        Args:
            query: Query em linguagem natural
            template: Template de decomposição
            
        Returns:
            Parâmetros extraídos da query
        """
        pass


class IQueryPlanGenerator(ABC):
    """
    Interface para geração de planos de execução
    """
    
    @abstractmethod
    def generate_plan(
        self, 
        query: str, 
        analysis: ComplexityAnalysis,
        strategy: Optional[DecompositionStrategy] = None
    ) -> QueryPlan:
        """
        Gera um plano de execução para a query
        
        Args:
            query: Query em linguagem natural
            analysis: Análise de complexidade
            strategy: Estratégia específica (opcional)
            
        Returns:
            Plano de execução completo
        """
        pass
    
    @abstractmethod
    def optimize_plan(self, plan: QueryPlan) -> QueryPlan:
        """
        Otimiza um plano de execução existente
        
        Args:
            plan: Plano a ser otimizado
            
        Returns:
            Plano otimizado
        """
        pass
    
    @abstractmethod
    def validate_plan(self, plan: QueryPlan) -> List[str]:
        """
        Valida um plano de execução
        
        Args:
            plan: Plano a ser validado
            
        Returns:
            Lista de avisos/erros encontrados
        """
        pass


class IQueryPlannerService(ABC):
    """
    Interface principal do serviço de planejamento de queries
    """
    
    @abstractmethod
    def should_decompose_query(self, query: str) -> bool:
        """
        Determina se uma query deve ser decomposta
        
        Args:
            query: Query em linguagem natural
            
        Returns:
            True se a query deve ser decomposta
        """
        pass
    
    @abstractmethod
    def create_execution_plan(self, query: str) -> QueryPlan:
        """
        Cria um plano de execução completo para a query
        
        Args:
            query: Query em linguagem natural
            
        Returns:
            Plano de execução otimizado
        """
        pass
    
    @abstractmethod
    def get_complexity_analysis(self, query: str) -> ComplexityAnalysis:
        """
        Obtém análise de complexidade detalhada
        
        Args:
            query: Query em linguagem natural
            
        Returns:
            Análise de complexidade completa
        """
        pass
    
    @abstractmethod
    def suggest_alternative_strategies(self, query: str) -> List[DecompositionStrategy]:
        """
        Sugere estratégias alternativas de decomposição
        
        Args:
            query: Query em linguagem natural
            
        Returns:
            Lista de estratégias ordenadas por adequação
        """
        pass
    
    @abstractmethod
    def estimate_execution_time(self, plan: QueryPlan) -> float:
        """
        Estima tempo de execução de um plano
        
        Args:
            plan: Plano de execução
            
        Returns:
            Tempo estimado em segundos
        """
        pass


class IDecompositionTemplate(ABC):
    """
    Interface para templates de decomposição
    """
    
    @abstractmethod
    def get_template_name(self) -> str:
        """Nome do template"""
        pass
    
    @abstractmethod
    def get_pattern_regex(self) -> str:
        """Regex para matching com queries"""
        pass
    
    @abstractmethod
    def get_strategy(self) -> DecompositionStrategy:
        """Estratégia de decomposição do template"""
        pass
    
    @abstractmethod
    def calculate_compatibility_score(self, query: str, analysis: ComplexityAnalysis) -> float:
        """
        Calcula score de compatibilidade com a query (0.0 a 1.0)
        """
        pass
    
    @abstractmethod
    def generate_plan_steps(self, query: str, parameters: QueryParameters) -> List[Dict[str, Any]]:
        """
        Gera as etapas do plano para este template
        """
        pass


# Exceptions específicas do módulo
class QueryPlanningError(Exception):
    """Erro base para planejamento de queries"""
    pass


class ComplexityAnalysisError(QueryPlanningError):
    """Erro na análise de complexidade"""
    pass


class TemplateMismatchError(QueryPlanningError):
    """Erro quando nenhum template compatível é encontrado"""
    pass


class PlanGenerationError(QueryPlanningError):
    """Erro na geração do plano"""
    pass


class PlanValidationError(QueryPlanningError):
    """Erro na validação do plano"""
    pass