"""
Enhanced Template Manager
Gerenciador aprimorado de templates de decomposição
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from domain.entities.query_decomposition import (
    QueryStep,
    QueryStepType,
    DecompositionStrategy,
    ComplexityAnalysis,
    QueryPlan
)
from .enhanced_template_library import (
    EnhancedTemplateLibrary,
    EnhancedDecompositionTemplate,
    TemplateCategory
)


class EnhancedTemplateManager:
    """
    Gerenciador que coordena a seleção e aplicação de templates aprimorados
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.template_library = EnhancedTemplateLibrary()
        self.usage_statistics = {}
        self.template_performance_cache = {}
        
        self.logger.info("EnhancedTemplateManager inicializado")
    
    def select_best_template(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> Optional[EnhancedDecompositionTemplate]:
        """
        Seleciona o melhor template para uma query baseado na análise de complexidade
        """
        try:
            # Encontrar templates compatíveis
            matching_templates = self.template_library.find_matching_templates(
                query, analysis, min_score=0.6
            )
            
            if not matching_templates:
                self.logger.warning(f"Nenhum template encontrado para query: {query[:50]}...")
                return None
            
            # Selecionar o melhor baseado em score + performance histórica
            best_template = self._select_optimal_template(matching_templates, query)
            
            # Registrar uso
            self._record_template_usage(best_template.template_id, query)
            
            self.logger.info(
                f"Template selecionado: {best_template.template_id} "
                f"({best_template.category.value}) com score {matching_templates[0][1]:.2f}"
            )
            
            return best_template
            
        except Exception as e:
            self.logger.error(f"Erro na seleção de template: {e}")
            return None
    
    def generate_steps_from_template(
        self, 
        template: EnhancedDecompositionTemplate,
        query: str,
        analysis: ComplexityAnalysis
    ) -> List[QueryStep]:
        """
        Gera etapas de execução a partir de um template
        """
        try:
            # Extrair parâmetros da query
            extracted_params = self._extract_template_parameters(template, query)
            
            # Gerar etapas
            steps = []
            step_counter = 0
            
            for step_template in template.step_templates:
                # Verificar se etapa é condicional
                if self._should_include_step(step_template, extracted_params):
                    step_counter += 1
                    
                    # Criar etapa
                    step = self._create_step_from_template(
                        step_template, step_counter, extracted_params, template
                    )
                    
                    steps.append(step)
            
            # Otimizar dependências
            steps = self._optimize_step_dependencies(steps)
            
            self.logger.info(f"Geradas {len(steps)} etapas do template {template.template_id}")
            
            return steps
            
        except Exception as e:
            self.logger.error(f"Erro na geração de etapas: {e}")
            return []
    
    def _select_optimal_template(
        self, 
        matching_templates: List[Tuple[EnhancedDecompositionTemplate, float]],
        query: str
    ) -> EnhancedDecompositionTemplate:
        """
        Seleciona template ótimo considerando score e performance histórica
        """
        best_template = matching_templates[0][0]  # Fallback
        best_score = 0.0
        
        for template, compatibility_score in matching_templates:
            # Performance histórica
            perf_data = self.template_performance_cache.get(template.template_id, {})
            success_rate = perf_data.get('success_rate', 0.8)  # Default otimista
            avg_execution_time = perf_data.get('avg_execution_time', 10.0)
            
            # Score composto
            time_penalty = max(0, (avg_execution_time - 5.0) / 20.0)  # Penalidade por tempo
            performance_score = success_rate - time_penalty
            
            final_score = (compatibility_score * 0.7) + (performance_score * 0.3)
            
            if final_score > best_score:
                best_score = final_score
                best_template = template
        
        return best_template
    
    def _extract_template_parameters(
        self, 
        template: EnhancedDecompositionTemplate,
        query: str
    ) -> Dict[str, str]:
        """
        Extrai parâmetros da query usando os extractors do template
        """
        import re
        
        extracted = {}
        query_lower = query.lower()
        
        for param_name, pattern in template.parameter_extractors.items():
            match = re.search(pattern, query_lower)
            if match:
                # Usar primeiro grupo capturado ou match completo
                extracted[param_name] = match.group(1) if match.groups() else match.group(0)
        
        # Parâmetros padrão baseados em análise da query
        self._add_default_parameters(extracted, query_lower)
        
        return extracted
    
    def _add_default_parameters(self, params: Dict[str, str], query_lower: str):
        """
        Adiciona parâmetros padrão baseados em análise da query
        """
        # Parâmetros de sexo
        if 'sexo_code' not in params:
            if any(term in query_lower for term in ['mulher', 'feminino']):
                params['sexo_code'] = '3'
                params['sexo_filtro'] = 'Feminino'
            elif any(term in query_lower for term in ['homem', 'masculino']):
                params['sexo_code'] = '1'
                params['sexo_filtro'] = 'Masculino'
        
        # Padrões de doença respiratória
        if 'respiratory_pattern' not in params:
            if any(term in query_lower for term in ['respiratór', 'pneum', 'j90', 'j44']):
                params['respiratory_pattern'] = 'J%'
        
        # Operadores de idade
        if 'idade_op' not in params:
            if any(term in query_lower for term in ['mais de', 'acima de', 'maior que']):
                params['idade_op'] = '>='
                params['idade_valor'] = '50'  # Default
            elif any(term in query_lower for term in ['menos de', 'abaixo de', 'menor que']):
                params['idade_op'] = '<='
                params['idade_valor'] = '65'  # Default
        
        # Limites e rankings
        if 'limite' not in params:
            import re
            limit_match = re.search(r'(\d+)', query_lower)
            params['limite'] = limit_match.group(1) if limit_match else '10'
        
        if 'min_casos' not in params:
            params['min_casos'] = '5'  # Default mínimo
        
        # Filtros base
        if 'base_filters' not in params:
            if 'mort' in query_lower:
                params['base_filters'] = "MORTE = 1"
            else:
                params['base_filters'] = "1=1"  # Sem filtro
        
        # Critério de ranking
        if 'criterio_ranking' not in params:
            if 'custo' in query_lower:
                params['criterio_ranking'] = 'custo'
            elif 'morte' in query_lower or 'letalidad' in query_lower:
                params['criterio_ranking'] = 'mortes'
            else:
                params['criterio_ranking'] = 'casos'
        
        # Entidade de ranking
        if 'ranking_entity' not in params:
            if 'procediment' in query_lower:
                params['ranking_entity'] = 'PROC_REA'
            else:
                params['ranking_entity'] = 'CIDADE_RESIDENCIA_PACIENTE'
        
        # Filtros financeiros
        if 'financial_filters' not in params:
            params['financial_filters'] = "PROC_REA IS NOT NULL"
        
        # Filtros de condição
        if 'condition_filters' not in params:
            params['condition_filters'] = "DIAG_PRINC IS NOT NULL"
        
        # Filtros temporais
        if 'temporal_filters' not in params:
            params['temporal_filters'] = "DT_INTER >= '20200101'"  # Default últimos anos
    
    def _should_include_step(
        self, 
        step_template: Dict[str, Any], 
        params: Dict[str, str]
    ) -> bool:
        """
        Determina se uma etapa deve ser incluída baseada em condições
        """
        conditional = step_template.get('conditional')
        if not conditional:
            return True
        
        # Verificar condições específicas
        if conditional == 'morte_analysis':
            return 'mort' in ' '.join(params.values()).lower()
        elif conditional == 'financial_analysis':
            return 'custo' in ' '.join(params.values()).lower()
        elif conditional == 'temporal_analysis':
            return any(term in ' '.join(params.values()).lower() 
                      for term in ['tempo', 'período', 'trimest'])
        
        return True
    
    def _create_step_from_template(
        self,
        step_template: Dict[str, Any],
        step_id: int,
        params: Dict[str, str],
        template: EnhancedDecompositionTemplate
    ) -> QueryStep:
        """
        Cria uma QueryStep a partir de um template
        """
        # Substituir parâmetros no SQL
        sql_template = step_template['sql_template']
        sql_with_params = self._substitute_parameters(sql_template, params, step_id)
        
        # Resolver dependências
        dependencies = step_template.get('dependencies', [])
        resolved_deps = []
        for dep in dependencies:
            if isinstance(dep, int):
                resolved_deps.append(dep)
            elif dep == 'previous_step' and step_id > 1:
                resolved_deps.append(step_id - 1)
        
        return QueryStep(
            step_id=step_id,
            step_type=QueryStepType(step_template['step_type']),
            description=step_template['description'],
            sql_template=sql_with_params,
            depends_on_steps=resolved_deps,
            metadata={
                'template_id': template.template_id,
                'template_category': template.category.value,
                'original_template': step_template,
                'extracted_params': params
            }
        )
    
    def _substitute_parameters(
        self, 
        sql_template: str, 
        params: Dict[str, str],
        current_step_id: int
    ) -> str:
        """
        Substitui parâmetros no template SQL
        """
        sql = sql_template
        
        # Substituir parâmetros extraídos
        for param_name, param_value in params.items():
            placeholder = f"{{{param_name}}}"
            sql = sql.replace(placeholder, str(param_value))
        
        # Substituir referências de etapas anteriores
        sql = sql.replace('{previous_step}', str(current_step_id - 1))
        
        # Limpar espaços e formatação
        import re
        sql = re.sub(r'\s+', ' ', sql.strip())
        
        return sql
    
    def _optimize_step_dependencies(self, steps: List[QueryStep]) -> List[QueryStep]:
        """
        Otimiza dependências entre etapas
        """
        # Verificar e corrigir dependências circulares
        for i, step in enumerate(steps):
            # Remover dependências para etapas que não existem
            valid_deps = [dep for dep in step.depends_on_steps 
                         if dep <= len(steps) and dep != step.step_id]
            
            # Atualizar dependências
            step.depends_on_steps = valid_deps
        
        return steps
    
    def _record_template_usage(self, template_id: str, query: str):
        """
        Registra uso de template para estatísticas
        """
        if template_id not in self.usage_statistics:
            self.usage_statistics[template_id] = {
                'usage_count': 0,
                'last_used': None,
                'sample_queries': []
            }
        
        stats = self.usage_statistics[template_id]
        stats['usage_count'] += 1
        stats['last_used'] = datetime.now().isoformat()
        
        # Manter sample de queries (máximo 5)
        if len(stats['sample_queries']) < 5:
            stats['sample_queries'].append(query[:100])
    
    def update_template_performance(
        self, 
        template_id: str, 
        success: bool, 
        execution_time: float
    ):
        """
        Atualiza métricas de performance de um template
        """
        if template_id not in self.template_performance_cache:
            self.template_performance_cache[template_id] = {
                'success_count': 0,
                'failure_count': 0,
                'total_execution_time': 0.0,
                'execution_count': 0
            }
        
        perf = self.template_performance_cache[template_id]
        
        if success:
            perf['success_count'] += 1
        else:
            perf['failure_count'] += 1
        
        perf['total_execution_time'] += execution_time
        perf['execution_count'] += 1
        
        # Calcular métricas derivadas
        total_attempts = perf['success_count'] + perf['failure_count']
        perf['success_rate'] = perf['success_count'] / total_attempts
        perf['avg_execution_time'] = perf['total_execution_time'] / perf['execution_count']
    
    def get_template_recommendations(
        self, 
        query: str, 
        analysis: ComplexityAnalysis,
        max_recommendations: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Retorna recomendações de templates com explicação
        """
        matching_templates = self.template_library.find_matching_templates(
            query, analysis, min_score=0.4
        )
        
        recommendations = []
        
        for template, score in matching_templates[:max_recommendations]:
            perf_data = self.template_performance_cache.get(template.template_id, {})
            usage_data = self.usage_statistics.get(template.template_id, {})
            
            recommendation = {
                'template_id': template.template_id,
                'name': template.name,
                'category': template.category.value,
                'compatibility_score': round(score, 2),
                'description': template.description,
                'strategy': template.strategy.value,
                'estimated_steps': len(template.step_templates),
                'success_rate': perf_data.get('success_rate', 0.8),
                'avg_execution_time': perf_data.get('avg_execution_time', 10.0),
                'usage_count': usage_data.get('usage_count', 0),
                'optimization_hints': template.optimization_hints[:2],  # Top 2
                'examples': template.examples[:1]  # 1 exemplo
            }
            
            recommendations.append(recommendation)
        
        return recommendations
    
    def get_manager_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do gerenciador
        """
        library_stats = self.template_library.get_library_statistics()
        
        # Calcular estatísticas de uso
        total_usage = sum(stats['usage_count'] for stats in self.usage_statistics.values())
        most_used = max(self.usage_statistics.items(), 
                       key=lambda x: x[1]['usage_count']) if self.usage_statistics else None
        
        # Calcular estatísticas de performance
        avg_success_rate = 0.0
        if self.template_performance_cache:
            success_rates = [perf['success_rate'] for perf in self.template_performance_cache.values() 
                           if 'success_rate' in perf]
            avg_success_rate = sum(success_rates) / len(success_rates) if success_rates else 0.0
        
        return {
            'library_statistics': library_stats,
            'usage_statistics': {
                'total_template_usage': total_usage,
                'templates_used': len(self.usage_statistics),
                'most_used_template': most_used[0] if most_used else None,
                'most_used_count': most_used[1]['usage_count'] if most_used else 0
            },
            'performance_statistics': {
                'templates_with_performance_data': len(self.template_performance_cache),
                'average_success_rate': round(avg_success_rate, 2),
                'cache_size': len(self.template_performance_cache)
            }
        }