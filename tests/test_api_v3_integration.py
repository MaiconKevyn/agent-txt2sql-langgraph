#!/usr/bin/env python3
"""
Teste da Integração API com LangGraph V3
Valida se a API está usando corretamente o orquestrador V3
"""

import sys
import os
import time
import json
import requests
from datetime import datetime
import subprocess
import signal
from threading import Thread
import atexit

# Global para controlar o servidor
server_process = None

def start_api_server():
    """Inicia o servidor API em background"""
    global server_process
    try:
        print("🚀 Iniciando servidor API...")
        server_process = subprocess.Popen(
            [sys.executable, "api_server.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid
        )
        
        # Aguarda o servidor inicializar
        time.sleep(8)
        
        # Verifica se o servidor está rodando
        try:
            response = requests.get("http://localhost:8000/health", timeout=5)
            if response.status_code == 200:
                print("✅ Servidor API iniciado com sucesso!")
                return True
            else:
                print(f"❌ Servidor API retornou status: {response.status_code}")
                return False
        except requests.RequestException as e:
            print(f"❌ Erro ao conectar com servidor API: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao iniciar servidor API: {e}")
        return False

def stop_api_server():
    """Para o servidor API"""
    global server_process
    if server_process:
        try:
            print("🛑 Parando servidor API...")
            # Mata todo o grupo de processos
            os.killpg(os.getpgid(server_process.pid), signal.SIGTERM)
            server_process.wait(timeout=5)
            print("✅ Servidor API parado")
        except Exception as e:
            print(f"⚠️  Erro ao parar servidor: {e}")
            try:
                # Force kill se necessário
                os.killpg(os.getpgid(server_process.pid), signal.SIGKILL)
            except:
                pass

# Registra cleanup no exit
atexit.register(stop_api_server)

def test_api_endpoints():
    """Testa todos os endpoints da API"""
    base_url = "http://localhost:8000"
    
    print("\n🧪 TESTANDO ENDPOINTS DA API...")
    
    # 1. Health Check
    print("\n1. 🏥 Testando Health Check...")
    try:
        response = requests.get(f"{base_url}/health", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("   ✅ Health Check: SUCCESS")
            print(f"   📊 Status: {data.get('status', 'unknown')}")
            
            # Verifica se está usando V3
            orchestrator_info = data.get('orchestrator', {})
            if orchestrator_info.get('version') == '3.0':
                print("   🚀 Usando Orquestrador V3: ✅")
            else:
                print("   ⚠️  Não está usando V3!")
                
        else:
            print(f"   ❌ Health Check falhou: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Erro no health check: {e}")
    
    # 2. Query Endpoint - Pergunta específica
    print("\n2. 🧪 Testando Query Endpoint...")
    query_data = {"question": "Em qual cidade morrem mais homens?"}
    
    try:
        print(f"   📝 Pergunta: \"{query_data['question']}\"")
        response = requests.post(
            f"{base_url}/query", 
            json=query_data, 
            timeout=30,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            data = response.json()
            print("   ✅ Query Endpoint: SUCCESS")
            print(f"   🎯 Sucesso: {data.get('success', False)}")
            print(f"   ⏱️  Tempo: {data.get('execution_time', 0):.2f}s")
            
            if data.get('sql_query'):
                print(f"   🗄️  SQL: {data['sql_query'][:60]}...")
            
            if data.get('results'):
                print(f"   📊 Resultados: {len(data['results'])} linhas")
            
            # Verifica metadata V3
            metadata = data.get('metadata', {})
            if metadata.get('orchestrator_v3'):
                print("   🚀 Resposta do Orquestrador V3: ✅")
                model_info = metadata.get('current_model', {})
                print(f"   🧠 Modelo: {model_info.get('model_name', 'unknown')}")
            
            return data.get('success', False)
        else:
            print(f"   ❌ Query falhou: {response.status_code}")
            try:
                error_data = response.json()
                print(f"   📄 Erro: {error_data}")
            except:
                print(f"   📄 Resposta: {response.text[:200]}...")
            return False
            
    except Exception as e:
        print(f"   ❌ Erro na query: {e}")
        return False

def test_api_performance():
    """Testa performance da API"""
    print("\n⚡ TESTANDO PERFORMANCE DA API...")
    
    base_url = "http://localhost:8000"
    test_queries = [
        "Quantos pacientes existem?",
        "Qual a média de idade?",
        "Pacientes de Porto Alegre"
    ]
    
    times = []
    successes = 0
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n   {i}. Testando: \"{query}\"")
        
        start_time = time.time()
        try:
            response = requests.post(
                f"{base_url}/query", 
                json={"question": query}, 
                timeout=20
            )
            
            execution_time = time.time() - start_time
            times.append(execution_time)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    successes += 1
                    print(f"      ✅ Sucesso em {execution_time:.2f}s")
                else:
                    print(f"      ❌ Falhou em {execution_time:.2f}s: {data.get('error_message', 'unknown')}")
            else:
                print(f"      ❌ HTTP {response.status_code} em {execution_time:.2f}s")
                
        except Exception as e:
            execution_time = time.time() - start_time
            times.append(execution_time)
            print(f"      💥 Exceção em {execution_time:.2f}s: {e}")
    
    # Estatísticas
    if times:
        avg_time = sum(times) / len(times)
        max_time = max(times)
        min_time = min(times)
        
        print(f"\n   📊 Estatísticas:")
        print(f"      Taxa de sucesso: {successes}/{len(test_queries)} ({successes/len(test_queries)*100:.1f}%)")
        print(f"      Tempo médio: {avg_time:.2f}s")
        print(f"      Tempo mínimo: {min_time:.2f}s")
        print(f"      Tempo máximo: {max_time:.2f}s")
        
        return successes == len(test_queries) and avg_time < 10.0
    
    return False

def test_migration_stats():
    """Testa endpoint de estatísticas de migração"""
    print("\n📈 TESTANDO MIGRATION STATS...")
    
    try:
        response = requests.get("http://localhost:8000/migration-stats", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("   ✅ Migration Stats: SUCCESS")
            
            orchestrator_info = data.get('orchestrator_info', {})
            print(f"   🔧 Versão: {orchestrator_info.get('version', 'unknown')}")
            print(f"   🌍 Ambiente: {orchestrator_info.get('environment', 'unknown')}")
            
            total_stats = data.get('total_statistics', {})
            print(f"   📊 Total queries: {total_stats.get('total_queries', 0)}")
            print(f"   ✅ Taxa de sucesso: {total_stats.get('success_rate', 0):.1%}")
            
            return True
        else:
            print(f"   ❌ Migration stats falhou: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Erro migration stats: {e}")
        return False

def main():
    print("🔗 TESTE DE INTEGRAÇÃO API + LANGGRAPH V3")
    print("=" * 60)
    
    start_time = datetime.now()
    print(f"🕒 Iniciado em: {start_time}")
    
    # Iniciar servidor
    if not start_api_server():
        print("❌ Não foi possível iniciar o servidor API")
        return False
    
    try:
        # Executar testes
        results = []
        
        # Teste dos endpoints
        endpoint_success = test_api_endpoints()
        results.append(("Endpoints", endpoint_success))
        
        # Teste de performance
        performance_success = test_api_performance()
        results.append(("Performance", performance_success))
        
        # Teste migration stats
        stats_success = test_migration_stats()
        results.append(("Migration Stats", stats_success))
        
        # Resumo
        print("\n" + "=" * 60)
        print("📋 RESUMO DOS TESTES DE INTEGRAÇÃO:")
        
        total_success = 0
        for test_name, success in results:
            status = "✅ PASSOU" if success else "❌ FALHOU"
            print(f"   {test_name}: {status}")
            if success:
                total_success += 1
        
        success_rate = total_success / len(results)
        print(f"\n   Taxa de sucesso geral: {total_success}/{len(results)} ({success_rate:.1%})")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"   Duração total: {duration:.2f}s")
        
        overall_success = success_rate == 1.0
        
        if overall_success:
            print("\n🏆 ✅ INTEGRAÇÃO API + V3 FUNCIONANDO PERFEITAMENTE!")
            print("🚀 SISTEMA PRONTO PARA PRODUÇÃO!")
        else:
            print("\n⚠️  ❌ ALGUNS TESTES FALHARAM")
            print("🔧 Revise os erros antes de colocar em produção")
        
        return overall_success
        
    finally:
        # Garantir que o servidor seja parado
        stop_api_server()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)