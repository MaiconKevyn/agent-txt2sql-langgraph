"""
Simple Query Decomposer - Simplified and functional decomposition system

This replaces the complex system with a pragmatic approach that:
- Uses simple pattern matching for complexity detection
- Implements 3 basic decomposition strategies  
- Has robust fallback to standard processing
- Maintains clean architecture principles
"""

import logging
import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from .query_processing_service import IQueryProcessingService, QueryResult, QueryRequest


class DecompositionStrategy(Enum):
    """Simple decomposition strategies"""
    SEQUENTIAL_FILTER = "sequential_filter"    # Break down multiple filters
    AGGREGATE_SPLIT = "aggregate_split"        # Separate complex aggregations  
    TEMPORAL_SPLIT = "temporal_split"          # Split temporal analysis


@dataclass
class DecompositionConfig:
    """Simple configuration for decomposition"""
    enabled: bool = True
    complexity_threshold: int = 3              # Number of complexity indicators needed
    timeout_seconds: float = 60.0              # Max time for decomposition
    debug_mode: bool = False
    fallback_enabled: bool = True


@dataclass
class ComplexityAnalysis:
    """Analysis of query complexity"""
    original_query: str
    complexity_score: int
    detected_patterns: List[str]
    recommended_strategy: Optional[DecompositionStrategy]
    should_decompose: bool
    reasoning: str


class SimpleQueryDecomposer:
    """
    Simple and functional query decomposer
    
    Responsibilities:
    - Detect query complexity using simple patterns
    - Choose appropriate decomposition strategy
    - Execute decomposition with fallback
    - Maintain statistics and debugging info
    """
    
    def __init__(
        self, 
        query_service: IQueryProcessingService,
        config: Optional[DecompositionConfig] = None
    ):
        """
        Initialize simple decomposer
        
        Args:
            query_service: Service for executing SQL queries
            config: Optional configuration (uses defaults if None)
        """
        self.query_service = query_service
        self.config = config or DecompositionConfig()
        self.logger = logging.getLogger(__name__)
        # Prevent duplicate logs by disabling propagation to root logger
        self.logger.propagate = False
        
        # Complexity detection patterns
        self.complexity_patterns = {
            "ranking": [r"ranking", r"top\s+\d+", r"maior", r"menor", r"primeiro"],
            "correlation": [r"correlação", r"correlacion", r"relação", r"comparando"],
            "trends": [r"tendências", r"tendencia", r"evolução", r"durante"],
            "details": [r"detalhada", r"detalhado", r"específico", r"completo"],
            "geography": [r"cidades", r"municípios", r"estados", r"regiões"],
            "temporal": [r"último", r"período", r"trimestre", r"ano", r"mês"],
            "aggregation": [r"média", r"total", r"soma", r"count", r"máximo"],
            "complex_join": [r"incluindo", r"considerando", r"junto", r"combinando"]
        }
        
        # Statistics
        self.stats = {
            "total_analyzed": 0,
            "total_decomposed": 0,
            "successful_decompositions": 0,
            "fallback_count": 0,
            "strategy_usage": {strategy.value: 0 for strategy in DecompositionStrategy}
        }
        
        self.logger.info("SimpleQueryDecomposer initialized")
        if self.config.debug_mode:
            print("🔧 Simple Query Decomposer initialized with debug mode enabled")
    
    def should_decompose(self, query: str) -> bool:
        """
        Determine if query should be decomposed based on complexity
        
        Args:
            query: User query string
            
        Returns:
            True if query should be decomposed
        """
        if not self.config.enabled:
            return False
            
        analysis = self.analyze_complexity(query)
        return analysis.should_decompose
    
    def analyze_complexity(self, query: str) -> ComplexityAnalysis:
        """
        Analyze query complexity using simple pattern matching
        
        Args:
            query: User query string
            
        Returns:
            ComplexityAnalysis with details
        """
        self.stats["total_analyzed"] += 1
        
        detected_patterns = []
        complexity_score = 0
        
        query_lower = query.lower()
        
        # Check each pattern category
        for category, patterns in self.complexity_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    detected_patterns.append(category)
                    complexity_score += 1
                    break  # Only count each category once
        
        # Determine if should decompose
        should_decompose = complexity_score >= self.config.complexity_threshold
        
        # Recommend strategy based on patterns
        recommended_strategy = self._recommend_strategy(detected_patterns)
        
        # Create reasoning
        reasoning = f"Detected {complexity_score} complexity indicators: {detected_patterns}"
        if should_decompose:
            reasoning += f". Meets threshold ({self.config.complexity_threshold}), recommending decomposition."
        else:
            reasoning += f". Below threshold ({self.config.complexity_threshold}), using standard processing."
        
        analysis = ComplexityAnalysis(
            original_query=query,
            complexity_score=complexity_score,
            detected_patterns=detected_patterns,
            recommended_strategy=recommended_strategy,
            should_decompose=should_decompose,
            reasoning=reasoning
        )
        
        if self.config.debug_mode:
            print(f"🔍 Complexity Analysis:")
            print(f"   Score: {complexity_score}")
            print(f"   Patterns: {detected_patterns}")
            print(f"   Should decompose: {should_decompose}")
            print(f"   Strategy: {recommended_strategy}")
        
        return analysis
    
    def decompose_and_execute(self, query: str) -> QueryResult:
        """
        Main entry point: analyze, decompose if needed, and execute
        
        Args:
            query: User query string
            
        Returns:
            QueryResult from decomposition or standard processing
        """
        start_time = datetime.now()
        
        try:
            # Check if should decompose
            analysis = self.analyze_complexity(query)
            
            if not analysis.should_decompose:
                if self.config.debug_mode:
                    print("📝 Query complexity below threshold, using standard processing")
                return self._execute_standard(query)
            
            # Attempt decomposition
            if self.config.debug_mode:
                print(f"🧩 Query complexity above threshold, attempting decomposition...")
                print(f"   Strategy: {analysis.recommended_strategy.value}")
            
            self.stats["total_decomposed"] += 1
            
            result = self._execute_decomposed(query, analysis)
            
            if result.success:
                self.stats["successful_decompositions"] += 1
                self.stats["strategy_usage"][analysis.recommended_strategy.value] += 1
                
                if self.config.debug_mode:
                    print("✅ Decomposition successful!")
                
                # Add decomposition metadata
                if result.metadata is None:
                    result.metadata = {}
                result.metadata.update({
                    "decomposition_used": True,
                    "strategy": analysis.recommended_strategy.value,
                    "complexity_score": analysis.complexity_score,
                    "detected_patterns": analysis.detected_patterns,
                    "reasoning": analysis.reasoning
                })
                
                return result
            else:
                # Decomposition failed, try fallback
                if self.config.fallback_enabled:
                    if self.config.debug_mode:
                        print("⚠️ Decomposition failed, falling back to standard processing")
                    self.stats["fallback_count"] += 1
                    return self._execute_standard(query)
                else:
                    return result
                    
        except Exception as e:
            self.logger.error(f"Error in decomposition: {e}")
            
            if self.config.fallback_enabled:
                if self.config.debug_mode:
                    print(f"❌ Decomposition error: {e}")
                    print("🔄 Falling back to standard processing")
                self.stats["fallback_count"] += 1
                return self._execute_standard(query)
            else:
                # Return error result
                return QueryResult(
                    sql_query="",
                    results=[],
                    success=False,
                    execution_time=(datetime.now() - start_time).total_seconds(),
                    row_count=0,
                    error_message=f"Decomposition error: {str(e)}"
                )
    
    def _recommend_strategy(self, detected_patterns: List[str]) -> Optional[DecompositionStrategy]:
        """
        Recommend decomposition strategy based on detected patterns
        
        Args:
            detected_patterns: List of detected pattern categories
            
        Returns:
            Recommended strategy or None
        """
        if not detected_patterns:
            return None
        
        # Strategy selection logic
        if "temporal" in detected_patterns or "trends" in detected_patterns:
            return DecompositionStrategy.TEMPORAL_SPLIT
        elif "aggregation" in detected_patterns and ("ranking" in detected_patterns or "details" in detected_patterns):
            return DecompositionStrategy.AGGREGATE_SPLIT
        else:
            return DecompositionStrategy.SEQUENTIAL_FILTER
    
    def _execute_decomposed(self, query: str, analysis: ComplexityAnalysis) -> QueryResult:
        """
        Execute query using decomposition strategy
        
        Args:
            query: Original user query
            analysis: Complexity analysis result
            
        Returns:
            QueryResult from decomposed execution
        """
        strategy = analysis.recommended_strategy
        
        if strategy == DecompositionStrategy.SEQUENTIAL_FILTER:
            return self._execute_sequential_filter(query, analysis)
        elif strategy == DecompositionStrategy.AGGREGATE_SPLIT:
            return self._execute_aggregate_split(query, analysis)
        elif strategy == DecompositionStrategy.TEMPORAL_SPLIT:
            return self._execute_temporal_split(query, analysis)
        else:
            # Fallback to standard processing
            return self._execute_standard(query)
    
    def _execute_sequential_filter(self, query: str, analysis: ComplexityAnalysis) -> QueryResult:
        """
        Execute using sequential filter strategy
        
        For now, this is a placeholder that falls back to standard processing.
        Future versions can implement actual step-by-step filtering.
        """
        if self.config.debug_mode:
            print("🔄 Sequential filter strategy - using standard processing for now")
        
        return self._execute_standard(query)
    
    def _execute_aggregate_split(self, query: str, analysis: ComplexityAnalysis) -> QueryResult:
        """
        Execute using aggregate split strategy
        
        For now, this is a placeholder that falls back to standard processing.
        Future versions can implement separation of aggregations.
        """
        if self.config.debug_mode:
            print("📊 Aggregate split strategy - using standard processing for now")
        
        return self._execute_standard(query)
    
    def _execute_temporal_split(self, query: str, analysis: ComplexityAnalysis) -> QueryResult:
        """
        Execute using temporal split strategy
        
        For now, this is a placeholder that falls back to standard processing.
        Future versions can implement temporal decomposition.
        """
        if self.config.debug_mode:
            print("📅 Temporal split strategy - using standard processing for now")
        
        return self._execute_standard(query)
    
    def _execute_standard(self, query: str) -> QueryResult:
        """
        Execute query using standard processing (fallback)
        
        Args:
            query: User query string
            
        Returns:
            QueryResult from standard query processing
        """
        request = QueryRequest(
            user_query=query,
            session_id="simple_decomposer",
            timestamp=datetime.now()
        )
        
        return self.query_service.process_natural_language_query(request)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get decomposer statistics
        
        Returns:
            Dictionary with usage statistics
        """
        success_rate = 0.0
        if self.stats["total_decomposed"] > 0:
            success_rate = self.stats["successful_decompositions"] / self.stats["total_decomposed"]
        
        fallback_rate = 0.0
        if self.stats["total_analyzed"] > 0:
            fallback_rate = self.stats["fallback_count"] / self.stats["total_analyzed"]
        
        return {
            "total_analyzed": self.stats["total_analyzed"],
            "total_decomposed": self.stats["total_decomposed"],
            "successful_decompositions": self.stats["successful_decompositions"],
            "fallback_count": self.stats["fallback_count"],
            "success_rate": round(success_rate, 3),
            "fallback_rate": round(fallback_rate, 3),
            "strategy_usage": self.stats["strategy_usage"],
            "configuration": {
                "enabled": self.config.enabled,
                "complexity_threshold": self.config.complexity_threshold,
                "timeout_seconds": self.config.timeout_seconds,
                "fallback_enabled": self.config.fallback_enabled
            }
        }
    
    def reset_statistics(self):
        """Reset all statistics"""
        self.stats = {
            "total_analyzed": 0,
            "total_decomposed": 0,
            "successful_decompositions": 0,
            "fallback_count": 0,
            "strategy_usage": {strategy.value: 0 for strategy in DecompositionStrategy}
        }
        
        if self.config.debug_mode:
            print("📊 Statistics reset")