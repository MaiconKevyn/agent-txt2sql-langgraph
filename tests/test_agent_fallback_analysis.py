#!/usr/bin/env python3
"""
REFATORADO: Teste para analisar Direct LLM Primário vs LangChain Fallback
Mede quantas vezes o método primário funciona vs quando precisa de fallback
"""

import time
import json
import logging
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import subprocess
import sys
import os

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass  
class NewArchitectureResult:
    """Resultado do teste com nova arquitetura (Direct Primary + LangChain Fallback)"""
    query: str
    direct_primary_success: bool  # Direct LLM como primário funcionou?
    langchain_fallback_activated: bool  # LangChain fallback foi chamado?
    langchain_fallback_success: bool  # LangChain fallback funcionou?
    total_execution_time: float
    final_success: bool
    method_used: str  # "direct_primary" ou "langchain_fallback" 
    sql_extracted: bool
    result_count: int

class NewArchitectureAnalyzer:
    """Analisa Direct LLM Primário vs LangChain Fallback na nova arquitetura"""
    
    def __init__(self):
        # Conjunto diversificado de perguntas para testar diferentes cenários
        self.test_queries = [
            # Queries simples - contagem básica
            "Quantos pacientes existem?",
            "Qual o total de registros?",
            "Quantos casos temos?",
            
            # Queries com filtros demográficos
            "Quantos homens morreram?",
            "Quantas mulheres na base?",
            "Óbitos masculinos total",
            "Mortes femininas",
            
            # Queries geográficas
            "Qual cidade tem mais casos?",
            "Casos em Porto Alegre",
            "Cidade com mais mortes de homens",
            "Top 5 cidades",
            
            # Queries por categoria de doença
            "Casos de doenças respiratórias",
            "Mortes por neoplasias",
            "Doenças infecciosas total",
            "Óbitos circulatórios",
            
            # Queries estatísticas
            "Média de idade",
            "Idade média mortes",
            
            # Queries temporais
            "Casos em 2017",
            "Tempo médio internação",
            
            # Queries complexas
            "Homens mortos Porto Alegre",
            "Top 3 cidades casos"
        ]
        
        self.results: List[NewArchitectureResult] = []
        
    def run_new_architecture_analysis(self) -> Dict[str, Any]:
        """Executa análise da nova arquitetura: Direct Primary vs LangChain Fallback"""
        logger.info("🔍 NOVA ARQUITETURA: Análise Direct Primary vs LangChain Fallback")
        logger.info(f"📊 Total de queries a testar: {len(self.test_queries)}")
        
        # Executar cada query e capturar comportamento
        for i, query in enumerate(self.test_queries, 1):
            logger.info(f"🔄 Testando ({i}/{len(self.test_queries)}): {query}")
            
            result = self._test_single_query_new_arch(query)
            self.results.append(result)
            
            # Log resultado imediato
            status = "✅ SUCCESS" if result.final_success else "❌ FAILED"
            method_info = f"🎯 {result.method_used}"
            if result.langchain_fallback_activated:
                method_info += " + 🔄 FALLBACK"
            logger.info(f"   {status} {method_info}")
            
            # Pausa para evitar sobrecarregar o sistema
            time.sleep(1)
        
        # Calcular estatísticas
        stats = self._calculate_new_architecture_statistics()
        
        # Salvar resultados
        self._save_new_architecture_results(stats)
        
        # Relatório
        self._print_new_architecture_report(stats)
        
        return stats
    
    def _test_single_query_new_arch(self, query: str) -> NewArchitectureResult:
        """Testa uma query específica com nova arquitetura"""
        start_time = time.time()
        
        try:
            # Mudar para diretório raiz para executar o script
            original_cwd = os.getcwd()
            project_root = "/home/maiconkevyn/PycharmProjects/txt2sql_claude"
            os.chdir(project_root)
            
            # Executar via CLI e capturar output completo
            cmd = [sys.executable, "txt2sql_agent_clean.py", "--query", query]
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=45,  # 45 segundos timeout
                encoding='utf-8'
            )
            
            # Voltar ao diretório original
            os.chdir(original_cwd)
            
            output = result.stdout + result.stderr
            execution_time = time.time() - start_time
            
            # Analisar output para nova arquitetura
            analysis = self._analyze_new_architecture_output(output)
            
            return NewArchitectureResult(
                query=query,
                direct_primary_success=analysis['direct_primary_success'],
                langchain_fallback_activated=analysis['langchain_fallback_activated'],
                langchain_fallback_success=analysis['langchain_fallback_success'],
                total_execution_time=execution_time,
                final_success=analysis['final_success'],
                method_used=analysis['method_used'],
                sql_extracted=analysis['sql_extracted'],
                result_count=analysis['result_count']
            )
            
        except subprocess.TimeoutExpired:
            os.chdir(original_cwd) if 'original_cwd' in locals() else None
            return NewArchitectureResult(
                query=query,
                direct_primary_success=False,
                langchain_fallback_activated=False,
                langchain_fallback_success=False,
                total_execution_time=45.0,
                final_success=False,
                method_used="timeout",
                sql_extracted=False,
                result_count=0
            )
        except Exception as e:
            os.chdir(original_cwd) if 'original_cwd' in locals() else None
            return NewArchitectureResult(
                query=query,
                direct_primary_success=False,
                langchain_fallback_activated=False,
                langchain_fallback_success=False,
                total_execution_time=time.time() - start_time,
                final_success=False,
                method_used="error",
                sql_extracted=False,
                result_count=0
            )
    
    def _analyze_new_architecture_output(self, output: str) -> Dict[str, Any]:
        """Analisa output da nova arquitetura (Direct Primary + LangChain Fallback)"""
        analysis = {
            'direct_primary_success': False,
            'langchain_fallback_activated': False,
            'langchain_fallback_success': False,
            'final_success': False,
            'method_used': 'unknown',
            'sql_extracted': False,
            'result_count': 0
        }
        
        # Detectar se Direct LLM primário foi usado com sucesso
        if "🎯 Using direct LLM as primary method" in output:
            analysis['method_used'] = 'direct_primary'
            
            # Se não há fallback ativado, então Direct foi bem-sucedido
            if "🔄 Attempting LangChain fallback" not in output:
                analysis['direct_primary_success'] = True
        
        # Detectar se LangChain fallback foi ativado
        if "🔄 Attempting LangChain fallback" in output:
            analysis['langchain_fallback_activated'] = True
            analysis['direct_primary_success'] = False  # Direct falhou
            analysis['method_used'] = 'langchain_fallback'
            
            # Verificar se LangChain fallback teve sucesso
            if "📊 SQL executed successfully" in output:
                analysis['langchain_fallback_success'] = True
        
        # Detectar extração de SQL
        if any(phrase in output for phrase in [
            "🔧 Extracted SQL from direct response:",
            "🔧 Extracted SQL:",
            "📊 SQL executed successfully"
        ]):
            analysis['sql_extracted'] = True
        
        # Detectar sucesso final
        if "✅ Resultado:" in output:
            analysis['final_success'] = True
            
            # Extrair contagem de resultados
            import re
            result_match = re.search(r"✅ Resultado: (\d+) registros", output)
            if result_match:
                analysis['result_count'] = int(result_match.group(1))
        
        return analysis
    
    def _calculate_new_architecture_statistics(self) -> Dict[str, Any]:
        """Calcula estatísticas da nova arquitetura"""
        total_queries = len(self.results)
        
        # Contadores para nova arquitetura
        direct_primary_successes = sum(1 for r in self.results if r.direct_primary_success)
        langchain_fallback_activations = sum(1 for r in self.results if r.langchain_fallback_activated)
        langchain_fallback_successes = sum(1 for r in self.results if r.langchain_fallback_success)
        final_successes = sum(1 for r in self.results if r.final_success)
        
        # Eficiência do método primário
        primary_efficiency = (direct_primary_successes / total_queries) * 100 if total_queries > 0 else 0
        
        # Eficiência do fallback quando ativado
        fallback_efficiency = (langchain_fallback_successes / langchain_fallback_activations) * 100 if langchain_fallback_activations > 0 else 0
        
        # Tempo médio
        avg_time = sum(r.total_execution_time for r in self.results) / total_queries if total_queries > 0 else 0
        
        # Análise por método usado
        method_breakdown = {}
        for r in self.results:
            method_breakdown[r.method_used] = method_breakdown.get(r.method_used, 0) + 1
        
        return {
            'total_queries': total_queries,
            'direct_primary_successes': direct_primary_successes,
            'direct_primary_success_rate': primary_efficiency,
            'langchain_fallback_activations': langchain_fallback_activations,
            'langchain_fallback_activation_rate': (langchain_fallback_activations / total_queries) * 100,
            'langchain_fallback_successes': langchain_fallback_successes,
            'fallback_efficiency': fallback_efficiency,
            'final_success_count': final_successes,
            'final_success_rate': (final_successes / total_queries) * 100,
            'avg_execution_time': avg_time,
            'method_breakdown': method_breakdown,
            'system_efficiency': (final_successes / total_queries) * 100  # Overall efficiency
        }
    
    def _save_new_architecture_results(self, stats: Dict[str, Any]):
        """Salva resultados da nova arquitetura"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        detailed_data = {
            'timestamp': timestamp,
            'architecture': 'direct_primary_langchain_fallback',
            'test_info': {
                'total_queries': len(self.test_queries),
                'test_categories': [
                    'Contagem básica', 'Filtros demográficos', 'Queries geográficas',
                    'Categorias de doença', 'Estatísticas', 'Temporais', 'Complexas'
                ]
            },
            'summary_statistics': stats,
            'detailed_results': [asdict(r) for r in self.results],
            'successful_direct_queries': [
                r.query for r in self.results if r.direct_primary_success
            ],
            'fallback_activated_queries': [
                r.query for r in self.results if r.langchain_fallback_activated
            ],
            'failed_queries': [
                r.query for r in self.results if not r.final_success
            ]
        }
        
        filename = f"new_architecture_analysis_{timestamp}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(detailed_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📄 Resultados salvos em: {filename}")
    
    def _print_new_architecture_report(self, stats: Dict[str, Any]):
        """Imprime relatório da nova arquitetura"""
        print("\n" + "="*70)
        print("🎯 ANÁLISE: NOVA ARQUITETURA - DIRECT PRIMARY vs LANGCHAIN FALLBACK")
        print("="*70)
        
        print(f"\n📊 ESTATÍSTICAS GERAIS:")
        print(f"   Total de queries testadas: {stats['total_queries']}")
        print(f"   Sucesso final: {stats['final_success_count']}/{stats['total_queries']} ({stats['final_success_rate']:.1f}%)")
        print(f"   Tempo médio de execução: {stats['avg_execution_time']:.2f}s")
        
        print(f"\n🎯 PERFORMANCE DO MÉTODO PRIMÁRIO (Direct LLM):")
        print(f"   Sucessos diretos: {stats['direct_primary_successes']}/{stats['total_queries']} ({stats['direct_primary_success_rate']:.1f}%)")
        
        print(f"\n🔄 ATIVAÇÃO DO FALLBACK (LangChain):")
        print(f"   Fallback ativado: {stats['langchain_fallback_activations']}/{stats['total_queries']} ({stats['langchain_fallback_activation_rate']:.1f}%)")
        if stats['langchain_fallback_activations'] > 0:
            print(f"   Eficiência do fallback: {stats['fallback_efficiency']:.1f}%")
            print(f"   Sucessos do fallback: {stats['langchain_fallback_successes']}/{stats['langchain_fallback_activations']}")
        
        print(f"\n📈 ANÁLISE POR MÉTODO:")
        for method, count in stats['method_breakdown'].items():
            percentage = (count / stats['total_queries']) * 100
            print(f"   • {method}: {count} ({percentage:.1f}%)")
        
        print(f"\n🎯 EFICIÊNCIA GERAL DO SISTEMA:")
        print(f"   Taxa de sucesso total: {stats['system_efficiency']:.1f}%")
        
        print(f"\n💡 INSIGHTS:")
        if stats['direct_primary_success_rate'] > 80:
            print("   ✅ Método primário é muito eficiente - arquitetura ideal")
        if stats['langchain_fallback_activation_rate'] < 20:
            print("   ✅ Baixa dependência de fallback - sistema estável")
        if stats['system_efficiency'] > 95:
            print("   🎯 Sistema altamente confiável")
        
        # Queries que usaram fallback
        fallback_queries = [r for r in self.results if r.langchain_fallback_activated]
        if fallback_queries:
            print(f"\n🔄 QUERIES QUE USARAM FALLBACK ({len(fallback_queries)}):")
            for i, r in enumerate(fallback_queries[:5], 1):
                status = "✅" if r.langchain_fallback_success else "❌"
                print(f"   {i}. {r.query} {status}")
        
        print("\n" + "="*70)
        
        # Conclusão
        if stats['direct_primary_success_rate'] > 90:
            print("🎉 EXCELENTE: Direct LLM primário funciona muito bem!")
        elif stats['direct_primary_success_rate'] > 70:
            print("✅ BOM: Direct LLM primário é eficiente")
        else:
            print("⚠️ ATENÇÃO: Direct LLM primário precisa otimização")
    
    def _analyze_execution_output_legacy(self, output: str) -> Dict[str, Any]:
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
    """Função principal - NOVA ARQUITETURA"""
    print("🚀 NOVA ARQUITETURA: Análise Direct Primary vs LangChain Fallback")
    
    analyzer = NewArchitectureAnalyzer()
    stats = analyzer.run_new_architecture_analysis()
    
    print(f"\n✅ Análise concluída!")
    print(f"🎯 Sucesso método primário: {stats['direct_primary_success_rate']:.1f}%")
    print(f"🔄 Ativação de fallback: {stats['langchain_fallback_activation_rate']:.1f}%")
    print(f"📊 Eficiência geral: {stats['system_efficiency']:.1f}%")

if __name__ == "__main__":
    main()