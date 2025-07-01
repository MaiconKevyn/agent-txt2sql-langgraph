#!/usr/bin/env python3
"""
Teste simples para contar falhas do Agent vs sucessos do Fallback
"""

import subprocess
import sys

# Queries de teste representativas
queries = [
    "Quantos pacientes existem?",
    "Quantos homens morreram?", 
    "Quantas mulheres na base?",
    "Qual cidade tem mais casos?",
    "Quantos casos em Porto Alegre?",
    "Qual a média de idade?",
    "Top 5 cidades com mais mortes",
    "Quantos casos de doenças respiratórias?",
    "Tempo médio de internação?",
    "Ranking de cidades por casos"
]

def test_single_query(query):
    """Testa uma query e retorna status"""
    try:
        cmd = [sys.executable, "txt2sql_agent_clean.py", "--query", query]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
        output = result.stdout + result.stderr
        
        agent_failed = "LangChain agent failed" in output
        fallback_used = "🔄 Attempting fallback method" in output  
        final_success = "✅ Resultado:" in output
        
        return {
            'agent_failed': agent_failed,
            'fallback_used': fallback_used, 
            'final_success': final_success
        }
    except:
        return {
            'agent_failed': True,
            'fallback_used': False,
            'final_success': False
        }

def main():
    print("🔍 Teste de Falhas: Agent vs Fallback")
    print("="*40)
    
    results = []
    
    for i, query in enumerate(queries, 1):
        print(f"({i:2d}/10) {query[:35]:35}", end=" ")
        
        status = test_single_query(query)
        results.append(status)
        
        # Status visual
        if status['agent_failed']:
            print("❌", end="")
        else:
            print("✅", end="")
            
        if status['fallback_used']:
            print("🔄", end="")
            
        if status['final_success']:
            print("✅")
        else:
            print("💥")
    
    # Estatísticas finais
    print("\n" + "="*40)
    print("📊 ESTATÍSTICAS FINAIS")
    
    agent_failures = sum(1 for r in results if r['agent_failed'])
    fallback_uses = sum(1 for r in results if r['fallback_used'])
    final_successes = sum(1 for r in results if r['final_success'])
    
    print(f"Agent falhou: {agent_failures}/10 ({agent_failures*10:.0f}%)")
    print(f"Fallback usado: {fallback_uses}/10 ({fallback_uses*10:.0f}%)")
    print(f"Sucesso final: {final_successes}/10 ({final_successes*10:.0f}%)")
    
    if agent_failures > 0:
        recovery_rate = (fallback_uses / agent_failures) * 100
        print(f"Taxa de recuperação: {recovery_rate:.0f}%")
    
    print("\n🎯 CONCLUSÃO:")
    if agent_failures > 5:
        print("❗ Agent é muito instável - fallback essencial")
    if fallback_uses >= agent_failures * 0.8:
        print("✅ Fallback funciona bem quando necessário")
    if final_successes >= 8:
        print("🎯 Sistema geral é confiável")

if __name__ == "__main__":
    main()