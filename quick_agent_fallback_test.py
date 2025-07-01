#!/usr/bin/env python3
"""
Teste rápido focado para analisar falhas do LangChain SQL Agent vs sucesso do Fallback
Versão otimizada com menos queries mas representativas
"""

import time
import json
import subprocess
import sys
from datetime import datetime

class QuickAgentTest:
    """Teste rápido e focado"""
    
    def __init__(self):
        # Conjunto menor mas representativo
        self.test_queries = [
            # Contagem básica (historicamente falha)
            "Quantos pacientes existem?",
            "Qual o total de registros?",
            
            # Filtros demográficos (falha parsing)
            "Quantos homens morreram?",
            "Quantas mulheres na base?",
            
            # Geográficas (complexas)
            "Qual cidade tem mais casos?",
            "Quantos casos em Porto Alegre?",
            
            # Estatísticas (parsing issues)
            "Qual a média de idade?",
            "Tempo médio de internação?",
            
            # Complexas (alta chance de falha)
            "Top 5 cidades com mais mortes",
            "Ranking de cidades por casos"
        ]
    
    def run_quick_test(self):
        """Executa teste rápido"""
        print("🚀 Teste Rápido: Agent vs Fallback")
        print(f"📊 Testando {len(self.test_queries)} queries representativas\n")
        
        results = {
            'agent_failures': 0,
            'fallback_recoveries': 0,
            'total_successes': 0,
            'details': []
        }
        
        for i, query in enumerate(self.test_queries, 1):
            print(f"🔄 ({i}/{len(self.test_queries)}) {query}")
            
            # Executar query
            try:
                cmd = [sys.executable, "txt2sql_agent_clean.py", "--query", query]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                output = result.stdout + result.stderr
                
                # Analisar resultado
                agent_failed = "LangChain agent failed" in output or "OUTPUT_PARSING_FAILURE" in output
                fallback_used = "🔄 Attempting fallback method" in output
                final_success = "✅ Resultado:" in output or "registros encontrados" in output
                
                # Contabilizar
                if agent_failed:
                    results['agent_failures'] += 1
                    print("   ❌ Agent falhou")
                
                if fallback_used:
                    results['fallback_recoveries'] += 1
                    print("   🔄 Fallback ativado")
                
                if final_success:
                    results['total_successes'] += 1
                    print("   ✅ Sucesso final")
                else:
                    print("   💥 Falha completa")
                
                results['details'].append({
                    'query': query,
                    'agent_failed': agent_failed,
                    'fallback_used': fallback_used,
                    'final_success': final_success
                })
                
            except subprocess.TimeoutExpired:
                print("   ⏰ Timeout")
                results['details'].append({
                    'query': query,
                    'agent_failed': True,
                    'fallback_used': False,
                    'final_success': False
                })
            
            print()  # Linha em branco
        
        # Relatório final
        self._print_quick_report(results)
        
        # Salvar dados
        with open(f"quick_test_{datetime.now().strftime('%H%M%S')}.json", "w") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
    
    def _print_quick_report(self, results):
        """Imprime relatório rápido"""
        total = len(self.test_queries)
        
        print("="*50)
        print("📊 RELATÓRIO RÁPIDO - AGENT vs FALLBACK")
        print("="*50)
        
        print(f"Total de queries: {total}")
        print(f"Agent falhou: {results['agent_failures']}/{total} ({results['agent_failures']/total*100:.1f}%)")
        print(f"Fallback ativado: {results['fallback_recoveries']}/{total} ({results['fallback_recoveries']/total*100:.1f}%)")
        print(f"Sucesso final: {results['total_successes']}/{total} ({results['total_successes']/total*100:.1f}%)")
        
        # Efetividade do fallback
        if results['agent_failures'] > 0:
            fallback_effectiveness = (results['fallback_recoveries'] / results['agent_failures']) * 100
            print(f"Efetividade do Fallback: {fallback_effectiveness:.1f}%")
        
        print("\n🎯 CONCLUSÃO:")
        if results['agent_failures'] > total * 0.5:
            print("❌ Agent falha muito - fallback é essencial")
        if results['fallback_recoveries'] > results['agent_failures'] * 0.8:
            print("✅ Fallback é muito efetivo")
        if results['total_successes'] > total * 0.9:
            print("🎯 Sistema geral funciona bem")
        
        print("="*50)

if __name__ == "__main__":
    tester = QuickAgentTest()
    tester.run_quick_test()