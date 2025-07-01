#!/usr/bin/env python3
"""
Teste comparativo entre LangChain SQL Agent vs Fallback Direto
Avalia performance, confiabilidade e qualidade dos resultados
"""

import time
import json
import logging
from typing import Dict, List, Any
from dataclasses import dataclass, asdict
from datetime import datetime

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TestResult:
    """Resultado de um teste individual"""
    query: str
    method: str  # 'langchain' ou 'direct'
    success: bool
    execution_time: float
    sql_generated: str
    result_count: int
    error_message: str = ""
    
@dataclass
class ComparisonMetrics:
    """Métricas de comparação entre métodos"""
    method: str
    total_tests: int
    success_count: int
    success_rate: float
    avg_execution_time: float
    avg_result_count: float
    errors: List[str]

class SQLMethodComparator:
    """Compara performance entre LangChain Agent e Fallback Direto"""
    
    def __init__(self):
        self.test_queries = [
            "Quantos pacientes existem?",
            "Qual é cidade com mais morte de homens?",
            "Quantas mortes de mulheres?", 
            "Qual a média de idade dos pacientes?",
            "Quantos casos de doenças respiratórias?",
            "Qual cidade tem mais casos?",
            "Quantos pacientes morreram em 2017?",
            "Qual o tempo médio de internação?",
            "Quantos casos em Porto Alegre?",
            "Top 5 cidades com mais casos"
        ]
        self.results: List[TestResult] = []
        
    def run_comparison_test(self) -> Dict[str, ComparisonMetrics]:
        """Executa teste comparativo completo"""
        logger.info("🚀 Iniciando teste comparativo SQL Methods")
        
        from src.application.container.dependency_injection import DependencyContainer
        from src.application.services.query_processing_service import QueryRequest
        
        # Inicializar container de dependências
        container = DependencyContainer()
        query_service = container.get_query_processing_service()
        
        # Testar cada query com ambos os métodos
        for query in self.test_queries:
            logger.info(f"🔍 Testando: {query}")
            
            # Teste com LangChain Agent (método primário)
            langchain_result = self._test_langchain_method(query_service, query)
            self.results.append(langchain_result)
            
            # Teste com Fallback Direto (forçado)
            direct_result = self._test_direct_method(query_service, query)
            self.results.append(direct_result)
            
            # Pausa entre testes
            time.sleep(1)
        
        # Calcular métricas
        metrics = self._calculate_metrics()
        
        # Salvar resultados
        self._save_results(metrics)
        
        return metrics
    
    def _test_langchain_method(self, service, query: str) -> TestResult:
        """Testa método LangChain Agent"""
        start_time = time.time()
        
        try:
            # Forçar uso do LangChain Agent
            request = QueryRequest(user_query=query)
            result = service._process_with_langchain_agent(request, start_time)
            
            return TestResult(
                query=query,
                method="langchain", 
                success=result.success,
                execution_time=result.execution_time,
                sql_generated=result.sql_query,
                result_count=result.row_count,
                error_message=result.error_message or ""
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            return TestResult(
                query=query,
                method="langchain",
                success=False,
                execution_time=execution_time,
                sql_generated="",
                result_count=0,
                error_message=str(e)
            )
    
    def _test_direct_method(self, service, query: str) -> TestResult:
        """Testa método Fallback Direto"""
        start_time = time.time()
        
        try:
            # Forçar uso do método direto
            request = QueryRequest(user_query=query)
            result = service._process_with_direct_llm(request, start_time)
            
            return TestResult(
                query=query,
                method="direct",
                success=result.success,
                execution_time=result.execution_time,
                sql_generated=result.sql_query,
                result_count=result.row_count,
                error_message=result.error_message or ""
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            return TestResult(
                query=query,
                method="direct",
                success=False,
                execution_time=execution_time,
                sql_generated="",
                result_count=0,
                error_message=str(e)
            )
    
    def _calculate_metrics(self) -> Dict[str, ComparisonMetrics]:
        """Calcula métricas comparativas"""
        langchain_results = [r for r in self.results if r.method == "langchain"]
        direct_results = [r for r in self.results if r.method == "direct"]
        
        langchain_metrics = self._get_method_metrics("langchain", langchain_results)
        direct_metrics = self._get_method_metrics("direct", direct_results)
        
        return {
            "langchain": langchain_metrics,
            "direct": direct_metrics
        }
    
    def _get_method_metrics(self, method: str, results: List[TestResult]) -> ComparisonMetrics:
        """Calcula métricas para um método específico"""
        total = len(results)
        success_count = sum(1 for r in results if r.success)
        success_rate = (success_count / total) * 100 if total > 0 else 0
        
        successful_results = [r for r in results if r.success]
        avg_time = sum(r.execution_time for r in successful_results) / len(successful_results) if successful_results else 0
        avg_results = sum(r.result_count for r in successful_results) / len(successful_results) if successful_results else 0
        
        errors = [r.error_message for r in results if not r.success and r.error_message]
        
        return ComparisonMetrics(
            method=method,
            total_tests=total,
            success_count=success_count,
            success_rate=success_rate,
            avg_execution_time=avg_time,
            avg_result_count=avg_results,
            errors=errors
        )
    
    def _save_results(self, metrics: Dict[str, ComparisonMetrics]):
        """Salva resultados do teste"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Salvar dados detalhados
        detailed_results = {
            "timestamp": timestamp,
            "test_queries": self.test_queries,
            "detailed_results": [asdict(r) for r in self.results],
            "summary_metrics": {k: asdict(v) for k, v in metrics.items()}
        }
        
        with open(f"sql_methods_comparison_{timestamp}.json", "w", encoding="utf-8") as f:
            json.dump(detailed_results, f, indent=2, ensure_ascii=False)
        
        # Relatório resumido
        self._print_summary_report(metrics)
    
    def _print_summary_report(self, metrics: Dict[str, ComparisonMetrics]):
        """Imprime relatório resumido"""
        print("\n" + "="*60)
        print("📊 RELATÓRIO COMPARATIVO SQL METHODS")
        print("="*60)
        
        for method_name, metric in metrics.items():
            print(f"\n🔧 MÉTODO: {method_name.upper()}")
            print(f"   ✅ Taxa de Sucesso: {metric.success_rate:.1f}% ({metric.success_count}/{metric.total_tests})")
            print(f"   ⏱️ Tempo Médio: {metric.avg_execution_time:.2f}s")
            print(f"   📊 Resultados Médios: {metric.avg_result_count:.1f} registros")
            
            if metric.errors:
                print(f"   ❌ Principais Erros:")
                for i, error in enumerate(metric.errors[:3], 1):
                    print(f"      {i}. {error[:80]}...")
        
        # Recomendação
        langchain = metrics["langchain"]
        direct = metrics["direct"]
        
        print(f"\n🎯 RECOMENDAÇÃO:")
        if direct.success_rate > langchain.success_rate:
            print("   ✅ Use o MÉTODO DIRETO como padrão")
            print(f"   📈 Melhoria: +{direct.success_rate - langchain.success_rate:.1f}% confiabilidade")
        elif langchain.success_rate > direct.success_rate:
            print("   ✅ Use o LANGCHAIN AGENT como padrão")
            print(f"   📈 Melhoria: +{langchain.success_rate - direct.success_rate:.1f}% confiabilidade")
        else:
            print("   ⚖️ Ambos métodos têm performance similar")
        
        print("="*60)

def main():
    """Função principal"""
    comparator = SQLMethodComparator()
    metrics = comparator.run_comparison_test()
    
    print(f"\n✅ Teste concluído! Resultados salvos em sql_methods_comparison_*.json")

if __name__ == "__main__":
    main()