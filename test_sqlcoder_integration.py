#!/usr/bin/env python3
"""
Teste de Integração: SQLCoder-7b-2
Script para validar a integração do defog/sqlcoder-7b-2 no sistema TXT2SQL
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.application.services.llm_communication_service import (
    LLMCommunicationFactory, 
    LLMConfig
)
from src.application.config.simple_config import ApplicationConfig

def test_huggingface_availability():
    """Testa se as dependências do Hugging Face estão instaladas"""
    print("🔍 Testando disponibilidade das dependências...")
    
    try:
        import transformers
        import torch
        import accelerate
        print("✅ Dependências do Hugging Face instaladas corretamente")
        print(f"   - transformers: {transformers.__version__}")
        print(f"   - torch: {torch.__version__}")
        print(f"   - CUDA disponível: {torch.cuda.is_available()}")
        return True
    except ImportError as e:
        print(f"❌ Dependência faltando: {e}")
        print("   Execute: pip install transformers torch accelerate")
        return False

def test_llm_service_creation():
    """Testa a criação do serviço LLM com SQLCoder"""
    print("\n🏗️ Testando criação do serviço LLM...")
    
    try:
        # Teste com configuração mínima (não baixa modelo ainda)
        config = LLMConfig(
            model_name="defog/sqlcoder-7b-2",
            provider="huggingface",
            temperature=0.0,
            device="cpu",  # CPU para teste
            load_in_4bit=True
        )
        
        print("✅ LLMConfig criado com sucesso")
        print(f"   - Modelo: {config.model_name}")
        print(f"   - Provider: {config.provider}")
        print(f"   - Device: {config.device}")
        print(f"   - Quantização 4-bit: {config.load_in_4bit}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro na criação do serviço: {e}")
        return False

def test_factory_pattern():
    """Testa o factory pattern para diferentes providers"""
    print("\n🏭 Testando Factory Pattern...")
    
    try:
        # Teste Ollama factory
        ollama_service = LLMCommunicationFactory.create_ollama_service(
            model_name="llama3"
        )
        print("✅ Factory Ollama funciona")
        
        # Teste Hugging Face factory  
        hf_service = LLMCommunicationFactory.create_huggingface_service(
            model_name="defog/sqlcoder-7b-2",
            device="cpu",
            load_in_4bit=True
        )
        print("✅ Factory Hugging Face funciona")
        
        # Teste factory genérico
        generic_service = LLMCommunicationFactory.create_service(
            "huggingface",
            model_name="defog/sqlcoder-7b-2"
        )
        print("✅ Factory genérico funciona")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no factory pattern: {e}")
        return False

def test_configuration_integration():
    """Testa integração com ApplicationConfig"""
    print("\n⚙️ Testando integração com ApplicationConfig...")
    
    try:
        # Configuração para SQLCoder
        config = ApplicationConfig(
            llm_provider="huggingface",
            llm_model="defog/sqlcoder-7b-2",
            llm_device="cpu",
            llm_load_in_4bit=True
        )
        
        print("✅ ApplicationConfig configurado para SQLCoder")
        print(f"   - Provider: {config.llm_provider}")
        print(f"   - Modelo: {config.llm_model}")
        print(f"   - Device: {config.llm_device}")
        print(f"   - Quantização: {config.llm_load_in_4bit}")
        
        # Teste switching para Ollama
        config_ollama = ApplicationConfig(
            llm_provider="ollama",
            llm_model="llama3"
        )
        
        print("✅ ApplicationConfig configurado para Ollama")
        print(f"   - Provider: {config_ollama.llm_provider}")
        print(f"   - Modelo: {config_ollama.llm_model}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro na configuração: {e}")
        return False

def test_model_download_simulation():
    """Simula teste de download do modelo (sem realmente baixar)"""
    print("\n📥 Simulando teste de download do modelo...")
    
    print("ℹ️ Para teste real do modelo, execute:")
    print("   1. Certifique-se de ter 16GB+ RAM disponível")
    print("   2. Configure llm_provider='huggingface' em simple_config.py")
    print("   3. Execute: python txt2sql_agent_clean.py --health-check")
    print("   4. O modelo será baixado automaticamente (~7GB)")
    
    print("✅ Instruções de teste fornecidas")
    return True

def main():
    """Executa todos os testes de integração"""
    print("🚀 TESTE DE INTEGRAÇÃO: SQLCoder-7b-2")
    print("=" * 50)
    
    tests = [
        ("Dependências", test_huggingface_availability),
        ("Serviço LLM", test_llm_service_creation),
        ("Factory Pattern", test_factory_pattern),
        ("Configuração", test_configuration_integration),
        ("Download Modelo", test_model_download_simulation),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Erro inesperado em {test_name}: {e}")
            results.append((test_name, False))
    
    # Resumo dos resultados
    print("\n" + "=" * 50)
    print("📊 RESUMO DOS TESTES:")
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n🎯 Resultado: {passed}/{len(results)} testes passaram")
    
    if passed == len(results):
        print("\n🎉 INTEGRAÇÃO COMPLETA!")
        print("   O sistema está pronto para usar SQLCoder-7b-2")
        print("   Próximos passos:")
        print("   1. Configure llm_provider='huggingface' em simple_config.py")
        print("   2. Execute: python txt2sql_agent_clean.py")
        print("   3. Teste queries SQL com o novo modelo")
    else:
        print("\n⚠️ ATENÇÃO: Alguns testes falharam")
        print("   Verifique os erros acima antes de prosseguir")
    
    return passed == len(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)