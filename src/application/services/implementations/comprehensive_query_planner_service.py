"""
Comprehensive Query Planner Service Implementation
Implementação completa do serviço de planejamento de queries
"""
import logging
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from domain.entities.query_decomposition import (
    QueryPlan,
    ComplexityAnalysis,
    DecompositionStrategy,
    QueryComplexityLevel
)
from application.services.query_planner_service import (
    IQueryPlannerService,
    IQueryComplexityAnalyzer,
    IQueryTemplateMatcher,
    IQueryPlanGenerator,
    PlannerConfig,
    QueryPlanningError
)
from .query_complexity_analyzer import QueryComplexityAnalyzer
from .query_template_matcher import QueryTemplateMatcher
from .query_plan_generator import QueryPlanGenerator
from .enhanced_template_manager import EnhancedTemplateManager


class ComprehensiveQueryPlannerService(IQueryPlannerService):
    """
    Implementação completa do serviço de planejamento que coordena
    análise de complexidade, matching de templates e geração de planos
    """
    
    def __init__(
        self,
        config: Optional[PlannerConfig] = None,
        complexity_analyzer: Optional[IQueryComplexityAnalyzer] = None,
        template_matcher: Optional[IQueryTemplateMatcher] = None,
        plan_generator: Optional[IQueryPlanGenerator] = None
    ):
        """
        Inicializa o serviço de planejamento
        
        Args:
            config: Configuração do planejador
            complexity_analyzer: Analisador de complexidade (opcional)
            template_matcher: Matcher de templates (opcional)
            plan_generator: Gerador de planos (opcional)
        """
        self.config = config or PlannerConfig()
        self.logger = logging.getLogger(__name__)
        
        # Inicializar componentes
        self._complexity_analyzer = complexity_analyzer or QueryComplexityAnalyzer()
        self._template_matcher = template_matcher or QueryTemplateMatcher()
        self._plan_generator = plan_generator or QueryPlanGenerator()
        
        # NEW: Enhanced Template Manager para biblioteca expandida
        self._enhanced_template_manager = EnhancedTemplateManager()
        
        # Cache de planos (se habilitado)
        self._plan_cache = {} if self.config.cache_plans else None
        
        self.logger.info("ComprehensiveQueryPlannerService inicializado")
    
    def should_decompose_query(self, query: str) -> bool:
        """
        Determina se uma query deve ser decomposta baseado na complexidade
        """
        try:
            analysis = self._complexity_analyzer.analyze_complexity(query)
            
            # Verificar se atende ao threshold de complexidade
            meets_threshold = analysis.complexity_score >= self.config.complexity_threshold_decompose
            
            # Verificar se tem estratégia recomendada
            has_strategy = analysis.recommended_strategy is not None
            
            # Verificar se é um nível que se beneficia de decomposição
            benefits_from_decomposition = analysis.complexity_level in [
                QueryComplexityLevel.COMPLEX,
                QueryComplexityLevel.VERY_COMPLEX
            ]
            
            should_decompose = meets_threshold and has_strategy and benefits_from_decomposition
            
            self.logger.info(
                f"Query decomposition decision: {should_decompose} "
                f"(score: {analysis.complexity_score:.1f}, "
                f"level: {analysis.complexity_level.value}, "
                f"strategy: {analysis.recommended_strategy})"
            )
            
            return should_decompose
            
        except Exception as e:
            self.logger.error(f"Erro ao determinar se query deve ser decomposta: {e}")
            # Em caso de erro, não decompor (fallback seguro)
            return False
    
    def create_execution_plan(self, query: str) -> QueryPlan:
        """
        Cria plano de execução completo para a query
        """
        try:
            # Verificar cache primeiro
            if self._plan_cache is not None:
                cache_key = self._generate_cache_key(query)
                if cache_key in self._plan_cache:
                    self.logger.info(f"Plano encontrado no cache para query")
                    return self._plan_cache[cache_key]
            
            # 1. Analisar complexidade
            self.logger.info("Iniciando análise de complexidade")
            analysis = self._complexity_analyzer.analyze_complexity(query)
            
            if not self.should_decompose_query(query):
                raise QueryPlanningError(
                    f"Query não atende critérios para decomposição "
                    f"(score: {analysis.complexity_score:.1f}, "
                    f"threshold: {self.config.complexity_threshold_decompose})"
                )
            
            # 2. Tentar Enhanced Template Manager primeiro (Checkpoint 6)
            self.logger.info("Tentando Enhanced Template Manager (biblioteca expandida)")
            enhanced_template = self._enhanced_template_manager.select_best_template(query, analysis)
            
            if enhanced_template:
                self.logger.info(
                    f"Enhanced template selecionado: '{enhanced_template.name}' "
                    f"({enhanced_template.category.value})"
                )
                
                # Gerar etapas usando template aprimorado
                enhanced_steps = self._enhanced_template_manager.generate_steps_from_template(
                    enhanced_template, query, analysis
                )
                
                if enhanced_steps:
                    # Criar plano com etapas aprimoradas
                    plan = self._create_plan_from_enhanced_steps(
                        query, analysis, enhanced_template, enhanced_steps
                    )
                    self.logger.info(f"Plano criado com Enhanced Template Manager ({len(enhanced_steps)} etapas)")
                else:
                    # Fallback para template manager original
                    self.logger.warning("Enhanced template não gerou etapas, usando template manager original")
                    plan = self._fallback_to_original_templates(query, analysis)
            else:
                # Fallback para template manager original
                self.logger.info("Nenhum enhanced template encontrado, usando template manager original")
                plan = self._fallback_to_original_templates(query, analysis)
            
            # 3. Otimizar plano se habilitado
            if self.config.enable_query_optimization:
                self.logger.info("Otimizando plano gerado")
                plan = self._plan_generator.optimize_plan(plan)
            
            # 4. Validar plano
            self.logger.info("Validando plano final")
            warnings = self._plan_generator.validate_plan(plan)
            if warnings:
                self.logger.warning(f"Plano tem {len(warnings)} avisos: {warnings}")
                # Adicionar warnings ao metadata
                if plan.metadata is None:
                    plan.metadata = {}
                plan.metadata['validation_warnings'] = warnings
            
            # 5. Verificar limites
            if len(plan.steps) > self.config.max_steps_per_plan:
                raise QueryPlanningError(
                    f"Plano excede limite máximo de etapas "
                    f"({len(plan.steps)} > {self.config.max_steps_per_plan})"
                )
            
            # 6. Adicionar ao cache se habilitado
            if self._plan_cache is not None:
                self._plan_cache[cache_key] = plan
                # Limpar cache se muito grande (LRU simples)
                if len(self._plan_cache) > 100:
                    oldest_key = next(iter(self._plan_cache))
                    del self._plan_cache[oldest_key]
            
            self.logger.info(
                f"Plano criado com sucesso: {plan.plan_id} "
                f"({len(plan.steps)} etapas, estratégia: {plan.strategy.value})"
            )
            
            return plan
            
        except Exception as e:
            self.logger.error(f"Erro na criação do plano de execução: {e}")
            raise QueryPlanningError(f"Falha na criação do plano: {str(e)}")
    
    def get_complexity_analysis(self, query: str) -> ComplexityAnalysis:
        """
        Obtém análise de complexidade detalhada
        """
        try:
            return self._complexity_analyzer.analyze_complexity(query)
        except Exception as e:
            self.logger.error(f"Erro na análise de complexidade: {e}")
            raise QueryPlanningError(f"Falha na análise de complexidade: {str(e)}")
    
    def suggest_alternative_strategies(self, query: str) -> List[DecompositionStrategy]:
        """
        Sugere estratégias alternativas de decomposição
        """
        try:
            analysis = self.get_complexity_analysis(query)
            matching_templates = self._template_matcher.find_matching_templates(query, analysis)
            
            # Extrair estratégias dos templates compatíveis
            strategies = []
            seen_strategies = set()
            
            for template_match in matching_templates:
                strategy = template_match['strategy']
                if strategy not in seen_strategies:
                    strategies.append(strategy)
                    seen_strategies.add(strategy)
            
            # Adicionar estratégia recomendada pela análise se não estiver na lista
            if analysis.recommended_strategy and analysis.recommended_strategy not in seen_strategies:
                strategies.insert(0, analysis.recommended_strategy)
            
            # Limitar a 5 estratégias
            return strategies[:5]
            
        except Exception as e:
            self.logger.error(f"Erro ao sugerir estratégias alternativas: {e}")
            return []
    
    def estimate_execution_time(self, plan: QueryPlan) -> float:
        """
        Estima tempo de execução de um plano
        """
        try:
            if plan.estimated_total_time:
                return plan.estimated_total_time
            
            # Estimativa baseada no número e tipo de etapas
            base_time = 2.0  # segundos por etapa
            total_time = 0.0
            
            for step in plan.steps:
                step_time = base_time
                
                # Ajustar baseado no tipo de etapa
                if step.step_type.value == "CALCULATE":
                    step_time *= 1.5  # Cálculos são mais demorados
                elif step.step_type.value == "AGGREGATE":
                    step_time *= 1.3  # Agregações são moderadamente demoradas
                elif step.step_type.value == "FILTER":
                    step_time *= 0.8  # Filtros são mais rápidos
                
                total_time += step_time
            
            return total_time
            
        except Exception as e:
            self.logger.error(f"Erro na estimativa de tempo: {e}")
            return 30.0  # Fallback: 30 segundos
    
    def get_plan_statistics(self) -> dict:
        """
        Retorna estatísticas do planejador
        """
        cache_stats = {}
        if self._plan_cache is not None:
            cache_stats = {
                "cache_size": len(self._plan_cache),
                "cache_enabled": True
            }
        else:
            cache_stats = {"cache_enabled": False}
        
        return {
            "config": {
                "complexity_threshold": self.config.complexity_threshold_decompose,
                "max_steps_per_plan": self.config.max_steps_per_plan,
                "enable_optimization": self.config.enable_query_optimization
            },
            "cache": cache_stats,
            "components": {
                "complexity_analyzer": type(self._complexity_analyzer).__name__,
                "template_matcher": type(self._template_matcher).__name__,
                "plan_generator": type(self._plan_generator).__name__
            }
        }
    
    def clear_cache(self) -> bool:
        """
        Limpa cache de planos
        """
        if self._plan_cache is not None:
            cache_size = len(self._plan_cache)
            self._plan_cache.clear()
            self.logger.info(f"Cache limpo: {cache_size} planos removidos")
            return True
        return False
    
    def _generate_cache_key(self, query: str) -> str:
        """
        Gera chave de cache baseada na query
        """
        import hashlib
        return hashlib.md5(query.lower().strip().encode()).hexdigest()
    
    def diagnose_query(self, query: str) -> dict:
        """
        Diagnóstica uma query fornecendo informações detalhadas
        """
        try:
            # Análise de complexidade
            analysis = self.get_complexity_analysis(query)
            
            # Templates compatíveis
            matching_templates = self._template_matcher.find_matching_templates(query, analysis)
            
            # Padrões detectados
            patterns = self._complexity_analyzer.detect_patterns(query)
            
            # Decisão de decomposição
            should_decompose = self.should_decompose_query(query)
            
            diagnosis = {
                "query": query,
                "complexity": {
                    "score": analysis.complexity_score,
                    "level": analysis.complexity_level.value,
                    "factors": analysis.complexity_factors,
                    "recommended_strategy": analysis.recommended_strategy.value if analysis.recommended_strategy else None,
                    "should_decompose": analysis.should_decompose,
                    "decomposition_benefit": analysis.estimated_decomposition_benefit
                },
                "patterns_detected": patterns,
                "templates": [
                    {
                        "name": t['template'].name,
                        "strategy": t['strategy'].value,
                        "compatibility_score": t['compatibility_score'],
                        "description": t['template'].description
                    }
                    for t in matching_templates[:3]  # Top 3 templates
                ],
                "decision": {
                    "should_decompose": should_decompose,
                    "threshold_met": analysis.complexity_score >= self.config.complexity_threshold_decompose,
                    "has_strategy": analysis.recommended_strategy is not None,
                    "has_compatible_templates": len(matching_templates) > 0
                },
                "alternative_strategies": [s.value for s in self.suggest_alternative_strategies(query)]
            }
            
            return diagnosis
            
        except Exception as e:
            self.logger.error(f"Erro no diagnóstico da query: {e}")
            return {
                "error": str(e),
                "query": query
            }
    
    def _fallback_to_original_templates(self, query: str, analysis: ComplexityAnalysis) -> QueryPlan:
        """
        Fallback para o template manager original quando Enhanced não encontra templates
        """
        self.logger.info("Usando template manager original como fallback")
        matching_templates = self._template_matcher.find_matching_templates(query, analysis)
        
        if not matching_templates:
            self.logger.warning("Nenhum template encontrado, usando geração direta")
            return self._plan_generator.generate_plan(query, analysis)
        else:
            # Usar o melhor template encontrado
            best_template = matching_templates[0]
            self.logger.info(
                f"Usando template original '{best_template['template'].name}' "
                f"(compatibilidade: {best_template['compatibility_score']:.2f})"
            )
            
            # Gerar plano com estratégia do template
            return self._plan_generator.generate_plan(
                query, 
                analysis, 
                best_template['strategy']
            )
    
    def _create_plan_from_enhanced_steps(
        self, 
        query: str, 
        analysis: ComplexityAnalysis,
        template: "EnhancedDecompositionTemplate",
        steps: List["QueryStep"]
    ) -> QueryPlan:
        """
        Cria QueryPlan a partir de etapas geradas pelo Enhanced Template Manager
        """
        import uuid
        from datetime import datetime
        
        # Gerar ID único para o plano
        plan_id = f"enhanced_plan_{uuid.uuid4().hex[:8]}_{int(datetime.now().timestamp())}"
        
        # Estimar tempo de execução
        estimated_time = len(steps) * 2.5  # Etapas mais complexas
        
        # Criar plano
        plan = QueryPlan(
            plan_id=plan_id,
            original_query=query,
            complexity_score=analysis.complexity_score,
            complexity_level=analysis.complexity_level,
            strategy=template.strategy,
            steps=steps,
            estimated_total_time=estimated_time,
            metadata={
                "generated_at": datetime.now().isoformat(),
                "generator": "enhanced_template_manager",
                "template_id": template.template_id,
                "template_name": template.name,
                "template_category": template.category.value,
                "patterns_used": analysis.patterns_detected,
                "complexity_factors": analysis.complexity_factors,
                "optimization_hints": template.optimization_hints,
                "template_examples": template.examples
            }
        )
        
        return plan
    
    def get_enhanced_template_recommendations(self, query: str) -> List[Dict[str, Any]]:
        """
        Obtém recomendações de templates da biblioteca aprimorada
        """
        try:
            analysis = self.get_complexity_analysis(query)
            return self._enhanced_template_manager.get_template_recommendations(
                query, analysis, max_recommendations=5
            )
        except Exception as e:
            self.logger.error(f"Erro ao obter recomendações: {e}")
            return []
    
    def get_enhanced_manager_statistics(self) -> Dict[str, Any]:
        """
        Obtém estatísticas do Enhanced Template Manager
        """
        try:
            return self._enhanced_template_manager.get_manager_statistics()
        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas: {e}")
            return {}