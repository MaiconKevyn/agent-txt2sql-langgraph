"""
Query Complexity Analyzer Implementation
Implementação do analisador de complexidade de queries
"""
import re
import logging
from typing import List, Dict, Any
from dataclasses import dataclass

from domain.entities.query_decomposition import (
    ComplexityAnalysis,
    QueryComplexityLevel,
    DecompositionStrategy
)
from application.services.query_planner_service import IQueryComplexityAnalyzer, ComplexityAnalysisError


@dataclass
class ComplexityPattern:
    """Padrão que contribui para complexidade de uma query"""
    name: str
    regex: str
    weight: float
    description: str
    strategy_hint: DecompositionStrategy


class QueryComplexityAnalyzer(IQueryComplexityAnalyzer):
    """
    Implementação do analisador de complexidade baseado em padrões
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._patterns = self._initialize_complexity_patterns()
        
    def _initialize_complexity_patterns(self) -> List[ComplexityPattern]:
        """
        Inicializa padrões de complexidade baseados na análise do Checkpoint 1
        """
        return [
            # Categoria 1: Filtros Múltiplos (Alta Prioridade)
            ComplexityPattern(
                name="multiple_demographic_filters",
                regex=r"(mulher|homem|feminino|masculino).*(anos?|idade).*(mort|óbit|cidad|municip)",
                weight=25.0,
                description="Múltiplos filtros demográficos (sexo + idade + morte)",
                strategy_hint=DecompositionStrategy.DEMOGRAPHIC_ANALYSIS
            ),
            
            ComplexityPattern(
                name="respiratory_disease_complex",
                regex=r"(respiratória|respiratorio|doença.*respirat|J\d+).*(cidad|municip|mort)",
                weight=20.0,
                description="Doenças respiratórias com análise geográfica",
                strategy_hint=DecompositionStrategy.SEQUENTIAL_FILTERING
            ),
            
            ComplexityPattern(
                name="multiple_criteria_with_ranking",
                regex=r"(\d+\s+cidad|\d+\s+municip|top \d+|mais.*cidad|maior.*cidad).*(mort|óbit)",
                weight=22.0,
                description="Ranking geográfico com múltiplos critérios",
                strategy_hint=DecompositionStrategy.RANKING_WITH_DETAILS
            ),
            
            # Categoria 2: Análises Temporais (Alta Prioridade)
            ComplexityPattern(
                name="temporal_calculation",
                regex=r"(tempo.*internaç|média.*internaç|dias.*internaç)",
                weight=30.0,
                description="Cálculos temporais de internação",
                strategy_hint=DecompositionStrategy.TEMPORAL_BREAKDOWN
            ),
            
            ComplexityPattern(
                name="temporal_period_analysis",
                regex=r"(201\d|202\d|janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro|trimestre|semestre)",
                weight=15.0,
                description="Análise por períodos específicos",
                strategy_hint=DecompositionStrategy.TEMPORAL_BREAKDOWN
            ),
            
            ComplexityPattern(
                name="trend_analysis",
                regex=r"(entre.*e.*|período|mês|ano|evolução|crescimento|tendência)",
                weight=12.0,
                description="Análise de tendências temporais",
                strategy_hint=DecompositionStrategy.TEMPORAL_BREAKDOWN
            ),
            
            # Categoria 3: Análises Financeiras (Média Prioridade)
            ComplexityPattern(
                name="financial_aggregation",
                regex=r"(custo|valor|gasto|financeiro|reais?|R\$|total.*valor)",
                weight=18.0,
                description="Agregações financeiras",
                strategy_hint=DecompositionStrategy.FINANCIAL_AGGREGATION
            ),
            
            ComplexityPattern(
                name="volume_threshold",
                regex=r"(mais de \d+|acima de \d+|maior que \d+|superior a \d+)",
                weight=10.0,
                description="Filtros por threshold de volume",
                strategy_hint=DecompositionStrategy.SEQUENTIAL_FILTERING
            ),
            
            # Categoria 4: Diagnósticos Complexos (Alta Prioridade)
            ComplexityPattern(
                name="neoplasia_queries",
                regex=r"(neoplasia|cancer|tumor|oncolog|C\d+)",
                weight=35.0,  # Alto peso - problemas conhecidos
                description="Consultas sobre neoplasias (timeout conhecido)",
                strategy_hint=DecompositionStrategy.DIAGNOSIS_CLASSIFICATION
            ),
            
            ComplexityPattern(
                name="multiple_cid_categories",
                regex=r"(CID|diagnóstic|doença).*(categoria|tipo|classificação)",
                weight=16.0,
                description="Múltiplas categorias de diagnóstico",
                strategy_hint=DecompositionStrategy.DIAGNOSIS_CLASSIFICATION
            ),
            
            # Categoria 5: Análises Geoespaciais (Baixa Prioridade)
            ComplexityPattern(
                name="geographic_proximity",
                regex=r"(raio|proximidade|perto|distância|km|quilômetro)",
                weight=25.0,
                description="Análises por proximidade geográfica",
                strategy_hint=DecompositionStrategy.GEOGRAPHIC_ANALYSIS
            ),
            
            ComplexityPattern(
                name="coordinate_analysis",
                regex=r"(latitude|longitude|coordenada|GPS)",
                weight=20.0,
                description="Análises usando coordenadas",
                strategy_hint=DecompositionStrategy.GEOGRAPHIC_ANALYSIS
            ),
            
            # Categoria 6: Padrões de Linguagem Natural
            ComplexityPattern(
                name="multiple_questions",
                regex=r"\?.*\?|\be\s+(qual|quanto|como|onde)",
                weight=15.0,
                description="Múltiplas perguntas em uma query",
                strategy_hint=DecompositionStrategy.RANKING_WITH_DETAILS
            ),
            
            ComplexityPattern(
                name="comparison_analysis",
                regex=r"(compar|versus|vs|diferença|entre.*e)",
                weight=18.0,
                description="Análises comparativas",
                strategy_hint=DecompositionStrategy.TEMPORAL_BREAKDOWN
            ),
            
            ComplexityPattern(
                name="ranking_with_details",
                regex=r"(qual.*mais|maior.*qual|principal.*qual|primeiro.*qual)",
                weight=20.0,
                description="Ranking seguido de detalhamento",
                strategy_hint=DecompositionStrategy.RANKING_WITH_DETAILS
            ),
            
            # Categoria 7: Indicadores de UTI e Complexidade Médica
            ComplexityPattern(
                name="uti_analysis",
                regex=r"(UTI|terapia intensiva|internação.*UTI|dias.*UTI)",
                weight=22.0,
                description="Análises de UTI (definição ambígua)",
                strategy_hint=DecompositionStrategy.TEMPORAL_BREAKDOWN
            ),
            
            ComplexityPattern(
                name="procedure_analysis",
                regex=r"(procedimento|cirurgia|operação|intervenção)",
                weight=14.0,
                description="Análises de procedimentos médicos",
                strategy_hint=DecompositionStrategy.SEQUENTIAL_FILTERING
            ),
            
            # Categoria 8: Padrões de Idade Específicos
            ComplexityPattern(
                name="age_range_complex",
                regex=r"(idoso|jovem|criança|adulto|faixa etária|entre \d+ e \d+ anos)",
                weight=12.0,
                description="Classificações complexas de faixa etária",
                strategy_hint=DecompositionStrategy.DEMOGRAPHIC_ANALYSIS
            ),
            
            ComplexityPattern(
                name="age_threshold_multiple",
                regex=r"(menos de \d+|mais de \d+|acima de \d+|abaixo de \d+).*anos",
                weight=8.0,
                description="Múltiplos thresholds de idade",
                strategy_hint=DecompositionStrategy.SEQUENTIAL_FILTERING
            )
        ]
    
    def analyze_complexity(self, query: str) -> ComplexityAnalysis:
        """
        Analisa complexidade da query baseado em padrões identificados
        """
        try:
            query_lower = query.lower()
            
            # Detectar padrões
            detected_patterns = []
            complexity_factors = {}
            total_score = 0.0
            strategy_votes = {}
            
            for pattern in self._patterns:
                if re.search(pattern.regex, query_lower):
                    detected_patterns.append(pattern.name)
                    complexity_factors[pattern.name] = pattern.weight
                    total_score += pattern.weight
                    
                    # Votar na estratégia
                    strategy = pattern.strategy_hint
                    strategy_votes[strategy] = strategy_votes.get(strategy, 0) + pattern.weight
            
            # Ajustamentos baseados em características da query
            total_score = self._apply_query_adjustments(query_lower, total_score)
            
            # Determinar nível de complexidade
            complexity_level = self._determine_complexity_level(total_score)
            
            # Estratégia recomendada (mais votada)
            recommended_strategy = None
            if strategy_votes:
                recommended_strategy = max(strategy_votes.items(), key=lambda x: x[1])[0]
            
            # Estimar benefício da decomposição
            decomposition_benefit = self._estimate_decomposition_benefit(
                total_score, complexity_level, len(detected_patterns)
            )
            
            return ComplexityAnalysis(
                query=query,
                complexity_score=min(total_score, 100.0),  # Cap at 100
                complexity_level=complexity_level,
                complexity_factors=complexity_factors,
                recommended_strategy=recommended_strategy,
                patterns_detected=detected_patterns,
                estimated_decomposition_benefit=decomposition_benefit,
                analysis_metadata={
                    "pattern_count": len(detected_patterns),
                    "strategy_votes": strategy_votes,
                    "query_length": len(query)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Erro na análise de complexidade: {e}")
            raise ComplexityAnalysisError(f"Falha na análise de complexidade: {str(e)}")
    
    def detect_patterns(self, query: str) -> List[str]:
        """
        Detecta padrões específicos na query
        """
        query_lower = query.lower()
        detected = []
        
        for pattern in self._patterns:
            if re.search(pattern.regex, query_lower):
                detected.append(f"{pattern.name}: {pattern.description}")
        
        return detected
    
    def estimate_decomposition_benefit(self, analysis: ComplexityAnalysis) -> float:
        """
        Estima benefício da decomposição baseado na análise
        """
        return self._estimate_decomposition_benefit(
            analysis.complexity_score,
            analysis.complexity_level,
            len(analysis.patterns_detected)
        )
    
    def _apply_query_adjustments(self, query_lower: str, base_score: float) -> float:
        """
        Aplica ajustamentos baseados em características da query
        """
        adjusted_score = base_score
        
        # Ajuste por comprimento da query
        if len(query_lower) > 100:
            adjusted_score += 5.0
        if len(query_lower) > 200:
            adjusted_score += 5.0
        
        # Ajuste por número de palavras-chave SQL implícitas
        sql_keywords = ['and', 'or', 'where', 'group', 'order', 'limit', 'count', 'sum', 'avg']
        keyword_count = sum(1 for keyword in sql_keywords if keyword in query_lower)
        adjusted_score += keyword_count * 2.0
        
        # Ajuste por números (indicam filtros específicos)
        number_count = len(re.findall(r'\d+', query_lower))
        adjusted_score += number_count * 1.5
        
        # Ajuste por conectores (indicam múltiplas condições)
        connectors = [' e ', ' com ', ' que ', ' para ', ' entre ', ' durante ']
        connector_count = sum(1 for connector in connectors if connector in query_lower)
        adjusted_score += connector_count * 3.0
        
        return adjusted_score
    
    def _determine_complexity_level(self, score: float) -> QueryComplexityLevel:
        """
        Determina o nível de complexidade baseado no score
        """
        if score < 40.0:
            return QueryComplexityLevel.SIMPLE
        elif score < 60.0:
            return QueryComplexityLevel.MODERATE
        elif score < 80.0:
            return QueryComplexityLevel.COMPLEX
        else:
            return QueryComplexityLevel.VERY_COMPLEX
    
    def _estimate_decomposition_benefit(
        self, 
        score: float, 
        level: QueryComplexityLevel, 
        pattern_count: int
    ) -> float:
        """
        Estima o benefício da decomposição (0.0 a 1.0)
        """
        base_benefit = {
            QueryComplexityLevel.SIMPLE: 0.0,
            QueryComplexityLevel.MODERATE: 0.2,
            QueryComplexityLevel.COMPLEX: 0.5,
            QueryComplexityLevel.VERY_COMPLEX: 0.8
        }.get(level, 0.0)
        
        # Ajuste baseado no número de padrões
        pattern_bonus = min(pattern_count * 0.1, 0.3)
        
        # Ajuste baseado no score
        score_bonus = min((score - 40.0) / 100.0, 0.4) if score > 40.0 else 0.0
        
        return min(base_benefit + pattern_bonus + score_bonus, 1.0)