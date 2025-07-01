#!/usr/bin/env python3
"""
Teste para validar a extração SQL após inversão: Direct Method como primário
"""

import subprocess
import sys
import time
import re
from typing import Dict, List

def test_direct_method_extraction():
    """Testa especificamente a extração de SQL pelo método direto"""
    
    test_queries = [
        # Queries básicas - teste de extração simples
        "Quantos pacientes existem?",
        "Qual o total de registros?",
        
        # Queries com filtros - teste extração com WHERE
        "Quantos homens morreram?",
        "Quantas mulheres na base?",
        
        # Queries geográficas - teste extração com GROUP BY  
        "Qual cidade tem mais casos?",
        "Top 5 cidades com mais mortes",
        
        # Queries estatísticas - teste extração de funções agregadas
        "Qual a média de idade?",
        "Tempo médio de internação?",
        
        # Queries complexas - teste extração multi-linha
        "Quantos casos de doenças respiratórias?",
        "Homens com mais de 60 anos em Porto Alegre"
    ]
    
    print("🔍 TESTE DE VALIDAÇÃO: Extração SQL Método Direto")
    print("="*55)
    
    successful_extractions = 0
    total_queries = len(test_queries)
    extraction_details = []
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n({i:2d}/10) {query}")
        print("-" * 50)
        
        try:
            # Executar query e capturar logs detalhados
            cmd = [sys.executable, "txt2sql_agent_clean.py", "--query", query]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=40)
            output = result.stdout + result.stderr
            
            # Analisar extração SQL
            analysis = analyze_sql_extraction(output, query)
            extraction_details.append(analysis)
            
            if analysis['sql_extracted']:
                successful_extractions += 1
                print(f"✅ SQL Extraído: {analysis['sql_query'][:60]}...")
            else:
                print(f"❌ Falha na extração: {analysis['error']}")
                
            print(f"📊 Método usado: {analysis['method']}")
            print(f"⏱️ Tempo: {analysis['execution_time']:.1f}s")
            
        except subprocess.TimeoutExpired:
            print("⏰ Timeout - Falha na extração")
            extraction_details.append({
                'query': query,
                'sql_extracted': False,
                'error': 'Timeout',
                'method': 'unknown',
                'execution_time': 40.0
            })
    
    # Relatório final
    print_extraction_report(successful_extractions, total_queries, extraction_details)
    
    return extraction_details

def analyze_sql_extraction(output: str, query: str) -> Dict:
    """Analisa se o SQL foi extraído corretamente"""
    
    analysis = {
        'query': query,
        'sql_extracted': False,
        'sql_query': '',
        'method': 'unknown',
        'error': '',
        'execution_time': 0.0
    }
    
    # Detectar método usado
    if "🎯 Using direct LLM fallback method" in output:
        analysis['method'] = 'direct_primary'
    elif "🤖 Calling LangChain agent" in output and "LangChain agent failed" not in output:
        analysis['method'] = 'langchain_success'
    elif "LangChain agent failed" in output:
        analysis['method'] = 'langchain_failed_direct_fallback'
    
    # Detectar extração SQL bem-sucedida
    if "🔧 Extracted SQL from direct response:" in output:
        analysis['sql_extracted'] = True
        # Extrair o SQL do log
        sql_match = re.search(r"🔧 Extracted SQL from direct response: (.+?)\.\.\.?", output)
        if sql_match:
            analysis['sql_query'] = sql_match.group(1).strip()
    elif "🔧 Extracted SQL:" in output:
        analysis['sql_extracted'] = True
        sql_match = re.search(r"🔧 Extracted SQL: (.+?)\.\.\.?", output)
        if sql_match:
            analysis['sql_query'] = sql_match.group(1).strip()
    
    # Detectar se query foi executada com sucesso
    if "📊 SQL executed successfully" in output:
        analysis['sql_extracted'] = True
    
    # Detectar erros
    if "❌ SQL execution failed" in output:
        analysis['error'] = 'SQL execution failed'
    elif "OUTPUT_PARSING_FAILURE" in output:
        analysis['error'] = 'LangChain parsing failure'
    elif "Timeout" in output:
        analysis['error'] = 'Query timeout'
    
    # Extrair tempo de execução
    time_match = re.search(r"⏱️ (?:Query completed|Direct method completed) in ([\d.]+)s", output)
    if time_match:
        analysis['execution_time'] = float(time_match.group(1))
    
    return analysis

def print_extraction_report(successful: int, total: int, details: List[Dict]):
    """Imprime relatório de extração"""
    
    print("\n" + "="*55)
    print("📊 RELATÓRIO DE VALIDAÇÃO DA EXTRAÇÃO")
    print("="*55)
    
    success_rate = (successful / total) * 100
    print(f"Taxa de Extração: {successful}/{total} ({success_rate:.1f}%)")
    
    # Análise por método
    methods = {}
    for detail in details:
        method = detail['method']
        methods[method] = methods.get(method, 0) + 1
    
    print(f"\nMétodos utilizados:")
    for method, count in methods.items():
        print(f"  • {method}: {count} queries")
    
    # Queries que falharam
    failed_queries = [d for d in details if not d['sql_extracted']]
    if failed_queries:
        print(f"\n❌ Queries que falharam na extração ({len(failed_queries)}):")
        for detail in failed_queries:
            print(f"  • {detail['query'][:40]}... - {detail['error']}")
    
    # Tempo médio
    avg_time = sum(d['execution_time'] for d in details) / len(details)
    print(f"\n⏱️ Tempo médio de execução: {avg_time:.1f}s")
    
    # Conclusão
    print(f"\n🎯 VALIDAÇÃO:")
    if success_rate >= 90:
        print("✅ Extração funciona muito bem")
    elif success_rate >= 75:
        print("⚠️ Extração funciona bem, mas pode melhorar")
    else:
        print("❌ Problemas na extração - necessita correção")
    
    print("="*55)

if __name__ == "__main__":
    test_direct_method_extraction()