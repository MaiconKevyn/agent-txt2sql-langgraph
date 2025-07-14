"""
Result Aggregator Implementation
Implementação do agregador de resultados de múltiplas etapas
"""
import logging
from typing import List, Dict, Any

from domain.entities.query_decomposition import (
    QueryPlan,
    StepExecutionResult,
    QueryStepType
)
from application.services.execution_orchestrator_service import IResultAggregator


class ResultAggregator(IResultAggregator):
    """
    Implementação do agregador que combina resultados de múltiplas etapas
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def combine_step_results(
        self, 
        step_results: List[StepExecutionResult],
        plan: QueryPlan
    ) -> List[Dict[str, Any]]:
        """
        Combina resultados de múltiplas etapas em resultado final
        """
        if not step_results:
            self.logger.warning("Nenhum resultado de etapa para combinar")
            return []
        
        try:
            # Para decomposição sequencial, o resultado final é da última etapa bem-sucedida
            if plan.strategy.value in ['sequential_filtering', 'demographic_analysis']:
                return self._combine_sequential_results(step_results, plan)
            
            # Para ranking com detalhes, combinar resultados de diferentes etapas
            elif plan.strategy.value == 'ranking_with_details':
                return self._combine_ranking_with_details(step_results, plan)
            
            # Para análise temporal, formatar resultados temporais
            elif plan.strategy.value == 'temporal_breakdown':
                return self._combine_temporal_results(step_results, plan)
            
            # Para análise financeira, combinar dados financeiros
            elif plan.strategy.value == 'financial_aggregation':
                return self._combine_financial_results(step_results, plan)
            
            # Para classificação de diagnósticos, agrupar por categoria
            elif plan.strategy.value == 'diagnosis_classification':
                return self._combine_diagnosis_results(step_results, plan)
            
            # Para análise geográfica, organizar por localização
            elif plan.strategy.value == 'geographic_analysis':
                return self._combine_geographic_results(step_results, plan)
            
            # Fallback: usar última etapa bem-sucedida
            else:
                return self._get_final_step_results(step_results)
                
        except Exception as e:
            self.logger.error(f"Erro ao combinar resultados: {e}")
            # Fallback: retornar resultado da última etapa
            return self._get_final_step_results(step_results)
    
    def format_final_result(
        self,
        combined_results: List[Dict[str, Any]],
        original_query: str,
        plan: QueryPlan
    ) -> Dict[str, Any]:
        """
        Formata o resultado final para apresentação
        """
        try:
            # Calcular estatísticas básicas
            total_records = len(combined_results)
            
            # Determinar tipo de apresentação baseado na query
            presentation_type = self._determine_presentation_type(original_query, plan)
            
            # Formatar baseado no tipo
            if presentation_type == 'ranking':
                formatted_data = self._format_ranking_presentation(combined_results, original_query)
            elif presentation_type == 'temporal':
                formatted_data = self._format_temporal_presentation(combined_results, original_query)
            elif presentation_type == 'financial':
                formatted_data = self._format_financial_presentation(combined_results, original_query)
            elif presentation_type == 'diagnostic':
                formatted_data = self._format_diagnostic_presentation(combined_results, original_query)
            else:
                formatted_data = self._format_default_presentation(combined_results, original_query)
            
            return {
                "success": True,
                "total_records": total_records,
                "presentation_type": presentation_type,
                "data": formatted_data,
                "summary": self._generate_summary(combined_results, original_query, plan),
                "metadata": {
                    "strategy_used": plan.strategy.value,
                    "steps_executed": len(plan.steps),
                    "plan_id": plan.plan_id,
                    "original_query": original_query
                }
            }
            
        except Exception as e:
            self.logger.error(f"Erro ao formatar resultado final: {e}")
            return {
                "success": False,
                "total_records": len(combined_results),
                "data": combined_results,
                "error": str(e),
                "metadata": {
                    "plan_id": plan.plan_id,
                    "original_query": original_query
                }
            }
    
    def _combine_sequential_results(
        self, 
        step_results: List[StepExecutionResult],
        plan: QueryPlan
    ) -> List[Dict[str, Any]]:
        """Combina resultados de filtros sequenciais"""
        # Para filtros sequenciais, o resultado final é da última etapa
        successful_results = [r for r in step_results if r.success]
        if successful_results:
            return successful_results[-1].results
        return []
    
    def _combine_ranking_with_details(
        self, 
        step_results: List[StepExecutionResult],
        plan: QueryPlan
    ) -> List[Dict[str, Any]]:
        """Combina ranking principal com detalhamento"""
        ranking_results = []
        detail_results = []
        
        for result in step_results:
            if not result.success:
                continue
                
            # Identificar tipo de resultado baseado nas colunas
            if result.results and isinstance(result.results[0], dict):
                columns = list(result.results[0].keys())
                
                if any('total' in col.lower() or 'count' in col.lower() for col in columns):
                    ranking_results.extend(result.results)
                else:
                    detail_results.extend(result.results)
        
        # Combinar ranking com detalhes
        combined = []
        for rank_item in ranking_results:
            combined_item = rank_item.copy()
            # Adicionar detalhes se disponível
            if detail_results:
                combined_item['details'] = detail_results[:3]  # Top 3 detalhes
            combined.append(combined_item)
        
        return combined if combined else ranking_results
    
    def _combine_temporal_results(
        self, 
        step_results: List[StepExecutionResult],
        plan: QueryPlan
    ) -> List[Dict[str, Any]]:
        """Combina resultados de análise temporal"""
        # Para análise temporal, focamos nos resultados com cálculos de tempo
        for result in reversed(step_results):  # Última etapa primeiro
            if result.success and result.results:
                # Verificar se tem dados temporais
                first_row = result.results[0]
                if isinstance(first_row, dict):
                    columns = list(first_row.keys())
                    if any('tempo' in col.lower() or 'dias' in col.lower() for col in columns):
                        return result.results
        
        # Fallback: última etapa bem-sucedida
        return self._get_final_step_results(step_results)
    
    def _combine_financial_results(
        self, 
        step_results: List[StepExecutionResult],
        plan: QueryPlan
    ) -> List[Dict[str, Any]]:
        """Combina resultados de análise financeira"""
        # Procurar resultados com dados financeiros
        for result in reversed(step_results):
            if result.success and result.results:
                first_row = result.results[0]
                if isinstance(first_row, dict):
                    columns = list(first_row.keys())
                    if any('valor' in col.lower() or 'custo' in col.lower() for col in columns):
                        return result.results
        
        return self._get_final_step_results(step_results)
    
    def _combine_diagnosis_results(
        self, 
        step_results: List[StepExecutionResult],
        plan: QueryPlan
    ) -> List[Dict[str, Any]]:
        """Combina resultados de classificação de diagnósticos"""
        # Procurar resultados com categorias de diagnóstico
        for result in reversed(step_results):
            if result.success and result.results:
                first_row = result.results[0]
                if isinstance(first_row, dict):
                    columns = list(first_row.keys())
                    if any('cid' in col.lower() or 'diag' in col.lower() or 'categoria' in col.lower() for col in columns):
                        return result.results
        
        return self._get_final_step_results(step_results)
    
    def _combine_geographic_results(
        self, 
        step_results: List[StepExecutionResult],
        plan: QueryPlan
    ) -> List[Dict[str, Any]]:
        """Combina resultados de análise geográfica"""
        # Procurar resultados com dados geográficos
        for result in reversed(step_results):
            if result.success and result.results:
                first_row = result.results[0]
                if isinstance(first_row, dict):
                    columns = list(first_row.keys())
                    if any('cidade' in col.lower() or 'municipio' in col.lower() or 'estado' in col.lower() for col in columns):
                        return result.results
        
        return self._get_final_step_results(step_results)
    
    def _get_final_step_results(self, step_results: List[StepExecutionResult]) -> List[Dict[str, Any]]:
        """Retorna resultados da última etapa bem-sucedida"""
        for result in reversed(step_results):
            if result.success and result.results:
                return result.results
        return []
    
    def _determine_presentation_type(self, query: str, plan: QueryPlan) -> str:
        """Determina tipo de apresentação baseado na query e plano"""
        query_lower = query.lower()
        
        if any(term in query_lower for term in ['top', 'mais', 'maior', 'ranking']):
            return 'ranking'
        elif any(term in query_lower for term in ['tempo', 'média', 'período', 'dias']):
            return 'temporal'
        elif any(term in query_lower for term in ['custo', 'valor', 'financeiro', 'gasto']):
            return 'financial'
        elif any(term in query_lower for term in ['cid', 'diagnóstico', 'doença', 'categoria']):
            return 'diagnostic'
        else:
            return 'default'
    
    def _format_ranking_presentation(self, results: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """Formata apresentação de ranking"""
        if not results:
            return {"items": [], "total": 0}
        
        # Identificar coluna de valor para ranking
        first_row = results[0]
        value_column = None
        
        for col in first_row.keys():
            if any(term in col.lower() for term in ['total', 'count', 'casos', 'mortes']):
                value_column = col
                break
        
        formatted_items = []
        for i, item in enumerate(results, 1):
            formatted_item = {
                "rank": i,
                "item": item,
                "value": item.get(value_column, 0) if value_column else 0
            }
            formatted_items.append(formatted_item)
        
        return {
            "type": "ranking",
            "items": formatted_items,
            "total": len(formatted_items),
            "value_column": value_column
        }
    
    def _format_temporal_presentation(self, results: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """Formata apresentação temporal"""
        return {
            "type": "temporal",
            "items": results,
            "total": len(results)
        }
    
    def _format_financial_presentation(self, results: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """Formata apresentação financeira"""
        return {
            "type": "financial", 
            "items": results,
            "total": len(results)
        }
    
    def _format_diagnostic_presentation(self, results: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """Formata apresentação de diagnósticos"""
        return {
            "type": "diagnostic",
            "items": results,
            "total": len(results)
        }
    
    def _format_default_presentation(self, results: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """Formata apresentação padrão"""
        return {
            "type": "default",
            "items": results,
            "total": len(results)
        }
    
    def _generate_summary(
        self, 
        results: List[Dict[str, Any]], 
        query: str, 
        plan: QueryPlan
    ) -> str:
        """Gera resumo executivo dos resultados"""
        if not results:
            return "Nenhum resultado encontrado para a consulta."
        
        total = len(results)
        strategy = plan.strategy.value
        
        # Identificar tipo de resultado
        first_row = results[0]
        if isinstance(first_row, dict):
            columns = list(first_row.keys())
            
            # Resumo para ranking
            if any('total' in col.lower() or 'count' in col.lower() for col in columns):
                return f"Encontrados {total} resultados usando estratégia {strategy}. Dados organizados por ranking."
            
            # Resumo para temporal
            elif any('tempo' in col.lower() or 'dias' in col.lower() for col in columns):
                return f"Análise temporal concluída com {total} registros usando estratégia {strategy}."
            
            # Resumo para financeiro
            elif any('valor' in col.lower() or 'custo' in col.lower() for col in columns):
                return f"Análise financeira realizada com {total} itens usando estratégia {strategy}."
            
            # Resumo genérico
            else:
                return f"Consulta processada com sucesso: {total} registros encontrados usando estratégia {strategy}."
        
        return f"Consulta concluída: {total} resultados."