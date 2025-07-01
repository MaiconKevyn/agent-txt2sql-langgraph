#!/usr/bin/env python3
"""
Teste específico para analisar falhas do LangChain SQL Agent vs sucesso do Fallback
Foca em medir quando o Agent falha e o Fallback consegue resolver
"""

import time
import json
import logging
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import subprocess
import sys

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class AgentFallbackResult:
    """Resultado específico do teste Agent vs Fallback"""
    query: str
    agent_failed: bool
    agent_error: str
    fallback_succeeded: bool
    fallback_result_count: int
    total_execution_time: float
    final_success: bool
    failure_recovery: bool  # True se Agent falhou mas Fallback recuperou

class AgentFallbackAnalyzer:
    """Analisa especificamente falhas do Agent e recuperação via Fallback"""
    
    def __init__(self):
        # Conjunto diversificado de perguntas para testar diferentes cenários
        self.test_queries = [
            # Queries simples - contagem básica
            "Quantos pacientes existem no banco?",
            "Qual o total de registros na base de dados?",
            "Quantos casos temos registrados?",
            
            # Queries com filtros demográficos
            "Quantos homens morreram?",
            "Quantas mulheres existem na base?",
            "Qual a quantidade de óbitos masculinos?",
            "Quantas mortes de pacientes do sexo feminino?",
            
            # Queries geográficas
            "Qual cidade tem mais casos?",
            "Quantos casos em Porto Alegre?",
            "Qual é a cidade com mais mortes de homens?",
            "Top 5 cidades com mais pacientes",
            "Quantos casos na cidade de São Paulo?",
            
            # Queries por categoria de doença
            "Quantos casos de doenças respiratórias?",
            "Quantas mortes por neoplasias?",
            "Casos de doenças infecciosas no total",
            "Óbitos por doenças do aparelho circulatório",
            
            # Queries estatísticas
            "Qual a média de idade dos pacientes?",
            "Idade média dos que morreram?",
            "Qual a média de idade por cidade?",
            
            # Queries temporais
            "Quantos casos em 2017?",
            "Mortes em janeiro de 2017",
            "Casos entre abril e julho de 2017",
            "Quantos pacientes internaram em 2017?",
            
            # Queries complexas - tempo de internação
            "Qual o tempo médio de internação?",
            "Tempo médio de internação por doenças respiratórias",
            "Média de dias internados por cidade",
            
            # Queries com múltiplos filtros
            "Quantos homens morreram em Porto Alegre?",
            "Mulheres com doenças respiratórias em 2017",
            "Óbitos masculinos por neoplasias",
            "Casos de mulheres acima de 60 anos",
            
            # Queries que podem confundir o Agent
            "Me diga quantos são os pacientes",
            "Mostre o número total de registros",
            "Preciso saber quantos casos existem",
            "Qual é o count da tabela sus_data?",
            
            # Queries com linguagem natural variada
            "Gostaria de saber quantos óbitos tivemos",
            "Poderia me informar o total de mortes?",
            "Quantas pessoas faleceram na base?",
            "Número de pacientes que não sobreviveram",
            
            # Queries específicas que historicamente falham
            "Liste as top 3 cidades com mais casos",
            "Ranking das 5 cidades com mais mortes",
            "Quais cidades têm mais de 100 casos?",
            "Cidades ordenadas por número de pacientes"
        ]
        
        self.results: List[AgentFallbackResult] = []
        
    def run_agent_fallback_analysis(self) -> Dict[str, Any]:
        """Executa análise completa de falhas do Agent vs Fallback"""
        logger.info("🔍 Iniciando análise Agent vs Fallback")
        logger.info(f"📊 Total de queries a testar: {len(self.test_queries)}")
        
        # Executar cada query e capturar comportamento
        for i, query in enumerate(self.test_queries, 1):
            logger.info(f"🔄 Testando ({i}/{len(self.test_queries)}): {query}")
            
            result = self._test_single_query(query)
            self.results.append(result)
            
            # Log resultado imediato
            status = "✅ SUCCESS" if result.final_success else "❌ FAILED"
            recovery = "🔄 RECOVERED" if result.failure_recovery else ""
            logger.info(f"   {status} {recovery}")
            
            # Pausa para evitar sobrecarregar o sistema
            time.sleep(2)
        
        # Calcular estatísticas
        stats = self._calculate_statistics()
        
        # Salvar resultados
        self._save_detailed_results(stats)
        
        # Relatório
        self._print_analysis_report(stats)
        
        return stats
    
    def _test_single_query(self, query: str) -> AgentFallbackResult:
        """Testa uma query específica e analisa o comportamento"""
        start_time = time.time()
        
        try:
            # Executar via CLI e capturar output completo
            cmd = [sys.executable, "txt2sql_agent_clean.py", "--query", query]
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=60,  # 1 minuto timeout
                encoding='utf-8'
            )
            
            output = result.stdout + result.stderr
            execution_time = time.time() - start_time
            
            # Analisar output para detectar falhas do Agent e sucesso do Fallback
            analysis = self._analyze_execution_output(output)
            
            return AgentFallbackResult(
                query=query,
                agent_failed=analysis['agent_failed'],
                agent_error=analysis['agent_error'],
                fallback_succeeded=analysis['fallback_succeeded'],
                fallback_result_count=analysis['result_count'],
                total_execution_time=execution_time,
                final_success=analysis['final_success'],
                failure_recovery=analysis['agent_failed'] and analysis['fallback_succeeded']
            )
            
        except subprocess.TimeoutExpired:
            return AgentFallbackResult(
                query=query,
                agent_failed=True,
                agent_error="Timeout após 60 segundos",
                fallback_succeeded=False,
                fallback_result_count=0,
                total_execution_time=60.0,
                final_success=False,
                failure_recovery=False
            )
        except Exception as e:
            return AgentFallbackResult(
                query=query,
                agent_failed=True,
                agent_error=str(e),
                fallback_succeeded=False,
                fallback_result_count=0,
                total_execution_time=time.time() - start_time,
                final_success=False,
                failure_recovery=False
            )
    
    def _analyze_execution_output(self, output: str) -> Dict[str, Any]:
        """Analisa o output da execução para detectar padrões"""
        analysis = {
            'agent_failed': False,
            'agent_error': '',
            'fallback_succeeded': False,
            'result_count': 0,
            'final_success': False
        }
        
        # Detectar falha do LangChain Agent
        if "LangChain agent failed" in output or "OUTPUT_PARSING_FAILURE" in output:
            analysis['agent_failed'] = True
            
            # Extrair tipo de erro
            if "OUTPUT_PARSING_FAILURE" in output:
                analysis['agent_error'] = "Output parsing error"
            elif "An output parsing error occurred" in output:
                analysis['agent_error'] = "LangChain parsing error"
            elif "iteration limit" in output:
                analysis['agent_error'] = "Iteration limit exceeded"
            elif "time limit" in output:
                analysis['agent_error'] = "Time limit exceeded"
            else:
                analysis['agent_error'] = "Generic LangChain failure"
        
        # Detectar ativação do fallback
        if "🔄 Attempting fallback method" in output or "🎯 Using direct LLM fallback method" in output:
            # Detectar se o fallback teve sucesso
            if "✅ Query returned" in output or "📊 SQL executed successfully" in output:
                analysis['fallback_succeeded'] = True
                
                # Extrair contagem de resultados
                import re
                result_pattern = r"✅ Query returned (\d+) rows"
                match = re.search(result_pattern, output)
                if match:
                    analysis['result_count'] = int(match.group(1))
        
        # Detectar sucesso final
        if "✅ Resultado:" in output or "registros encontrados" in output:
            analysis['final_success'] = True
            
            # Se não houve falha do agent, então o agent teve sucesso
            if not analysis['agent_failed']:
                analysis['fallback_succeeded'] = False  # Não precisou do fallback
        
        return analysis
    
    def _calculate_statistics(self) -> Dict[str, Any]:
        """Calcula estatísticas detalhadas"""
        total_queries = len(self.results)
        
        # Contadores básicos
        agent_failures = sum(1 for r in self.results if r.agent_failed)
        fallback_recoveries = sum(1 for r in self.results if r.failure_recovery)
        final_successes = sum(1 for r in self.results if r.final_success)
        
        # Análise de erros do Agent
        agent_errors = {}
        for r in self.results:
            if r.agent_failed and r.agent_error:
                agent_errors[r.agent_error] = agent_errors.get(r.agent_error, 0) + 1
        
        # Casos onde Agent falhou mas sistema ainda funcionou
        successful_recoveries = sum(1 for r in self.results if r.agent_failed and r.final_success)
        
        # Casos onde nem Agent nem Fallback funcionaram
        total_failures = sum(1 for r in self.results if not r.final_success)
        
        # Tempos de execução
        avg_time = sum(r.total_execution_time for r in self.results) / total_queries
        
        return {
            'total_queries': total_queries,
            'agent_failures': agent_failures,
            'agent_failure_rate': (agent_failures / total_queries) * 100,
            'fallback_recoveries': fallback_recoveries,
            'recovery_success_rate': (fallback_recoveries / agent_failures) * 100 if agent_failures > 0 else 0,
            'final_success_count': final_successes,
            'final_success_rate': (final_successes / total_queries) * 100,
            'successful_recoveries': successful_recoveries,
            'total_failures': total_failures,
            'avg_execution_time': avg_time,
            'agent_error_breakdown': agent_errors,
            'fallback_effectiveness': (fallback_recoveries / agent_failures) * 100 if agent_failures > 0 else 0
        }
    
    def _save_detailed_results(self, stats: Dict[str, Any]):
        """Salva resultados detalhados"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        detailed_data = {
            'timestamp': timestamp,
            'test_info': {
                'total_queries': len(self.test_queries),
                'test_categories': [
                    'Contagem básica', 'Filtros demográficos', 'Queries geográficas',
                    'Categorias de doença', 'Estatísticas', 'Queries temporais',
                    'Tempo de internação', 'Múltiplos filtros', 'Linguagem natural'
                ]
            },
            'summary_statistics': stats,
            'detailed_results': [asdict(r) for r in self.results],
            'failed_queries': [
                {'query': r.query, 'error': r.agent_error} 
                for r in self.results if r.agent_failed
            ],
            'recovered_queries': [
                {'query': r.query, 'result_count': r.fallback_result_count}
                for r in self.results if r.failure_recovery
            ]
        }
        
        filename = f"agent_fallback_analysis_{timestamp}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(detailed_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📄 Resultados salvos em: {filename}")
    
    def _print_analysis_report(self, stats: Dict[str, Any]):
        """Imprime relatório de análise"""
        print("\n" + "="*70)
        print("🔍 ANÁLISE: FALHAS DO LANGCHAIN AGENT vs RECUPERAÇÃO FALLBACK")
        print("="*70)
        
        print(f"\n📊 ESTATÍSTICAS GERAIS:")
        print(f"   Total de queries testadas: {stats['total_queries']}")
        print(f"   Sucesso final: {stats['final_success_count']}/{stats['total_queries']} ({stats['final_success_rate']:.1f}%)")
        print(f"   Tempo médio de execução: {stats['avg_execution_time']:.2f}s")
        
        print(f"\n❌ FALHAS DO LANGCHAIN AGENT:")
        print(f"   Agent falhou: {stats['agent_failures']}/{stats['total_queries']} ({stats['agent_failure_rate']:.1f}%)")
        
        print(f"\n   Tipos de erro do Agent:")
        for error_type, count in stats['agent_error_breakdown'].items():
            percentage = (count / stats['agent_failures']) * 100 if stats['agent_failures'] > 0 else 0
            print(f"      • {error_type}: {count} ({percentage:.1f}%)")
        
        print(f"\n🔄 EFETIVIDADE DO FALLBACK:")
        print(f"   Fallback recuperou: {stats['fallback_recoveries']}/{stats['agent_failures']} ({stats['recovery_success_rate']:.1f}%)")
        print(f"   Recuperações bem-sucedidas: {stats['successful_recoveries']}")
        print(f"   Efetividade do fallback: {stats['fallback_effectiveness']:.1f}%")
        
        print(f"\n💡 INSIGHTS:")
        if stats['agent_failure_rate'] > 30:
            print("   ⚠️ Agent tem alta taxa de falha - considere usar fallback como método primário")
        
        if stats['recovery_success_rate'] > 80:
            print("   ✅ Fallback é muito efetivo - sistema de recuperação funciona bem")
        
        if stats['final_success_rate'] > 90:
            print("   🎯 Sistema geral é confiável apesar das falhas do Agent")
        
        # Queries mais problemáticas
        failed_queries = [r for r in self.results if r.agent_failed and not r.final_success]
        if failed_queries:
            print(f"\n❌ QUERIES QUE FALHARAM COMPLETAMENTE ({len(failed_queries)}):")
            for i, r in enumerate(failed_queries[:5], 1):
                print(f"   {i}. {r.query}")
                print(f"      Erro: {r.agent_error}")
        
        # Queries que o fallback salvou
        recovered_queries = [r for r in self.results if r.failure_recovery]
        if recovered_queries:
            print(f"\n✅ QUERIES SALVAS PELO FALLBACK ({len(recovered_queries)}):")
            for i, r in enumerate(recovered_queries[:5], 1):
                print(f"   {i}. {r.query}")
                print(f"      Resultados: {r.fallback_result_count} registros")
        
        print("\n" + "="*70)
        
        # Recomendação final
        if stats['agent_failure_rate'] > 50:
            print("🚨 RECOMENDAÇÃO: Agent falha muito - use fallback como método primário")
        elif stats['recovery_success_rate'] > 90:
            print("✅ RECOMENDAÇÃO: Sistema atual com fallback funciona bem")
        else:
            print("⚖️ RECOMENDAÇÃO: Considere otimizar tanto Agent quanto Fallback")

def main():
    """Função principal"""
    print("🚀 Iniciando análise de falhas Agent vs Fallback...")
    
    analyzer = AgentFallbackAnalyzer()
    stats = analyzer.run_agent_fallback_analysis()
    
    print(f"\n✅ Análise concluída!")
    print(f"📈 Taxa de falha do Agent: {stats['agent_failure_rate']:.1f}%")
    print(f"🔄 Taxa de recuperação do Fallback: {stats['recovery_success_rate']:.1f}%")
    print(f"🎯 Taxa de sucesso final: {stats['final_success_rate']:.1f}%")

if __name__ == "__main__":
    main()