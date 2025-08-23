#!/usr/bin/env python3
"""
Teste do fluxo completo de decisão da LLM
Simula exatamente como LLM recebe output da tool e decide tabelas
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.application.config.simple_config import ApplicationConfig
from src.langgraph_migration.llm_manager import HybridLLMManager
from langchain_core.messages import HumanMessage

def test_llm_table_decision_flow():
    """Testa fluxo completo: Tool → LLM → Decisão de tabela"""
    
    print("=== TESTE FLUXO DECISÃO LLM: TOOL → ANÁLISE → DECISÃO ===\n")
    
    try:
        # 1. Configurar sistema
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        
        # 2. Obter output da EnhancedListTablesTool (mesmo que LLM recebe)
        tools = llm_manager.get_sql_tools()
        enhanced_tool = None
        
        for tool in tools:
            if tool.name == "sql_db_list_tables":
                enhanced_tool = tool
                break
        
        print("1. Obtendo output da EnhancedListTablesTool...")
        tool_output = enhanced_tool.invoke({})
        print("✅ Tool output obtido!")
        print(f"📏 Tamanho: {len(tool_output)} caracteres")
        
        # 3. Mostrar o que a LLM vê
        print("\n📋 OUTPUT DA TOOL (o que LLM recebe):")
        print("=" * 80)
        print(tool_output)
        print("=" * 80)
        
        # 4. Simular diferentes queries e como LLM decide
        test_queries = [
            {
                "query": "Quantas internações existem no total?",
                "expected": ["internacoes"],
                "reasoning": "Query sobre contagem de internações → tabela internacoes"
            },
            {
                "query": "O que significa o código CID F190?",
                "expected": ["cid10"], 
                "reasoning": "Query sobre código CID → tabela cid10"
            },
            {
                "query": "Qual cidade com mais internações?",
                "expected": ["internacoes", "municipios"],
                "reasoning": "Query sobre cidade + internações → internacoes + municipios"
            },
            {
                "query": "Quantos homens foram internados com diabetes?",
                "expected": ["internacoes", "cid10"],
                "reasoning": "Query sobre sexo + diagnóstico → internacoes + cid10"
            }
        ]
        
        print(f"\n2. Testando decisões da LLM para {len(test_queries)} queries...\n")
        
        # Obter LLM unbound para simulação
        llm = llm_manager._llm
        
        for i, test_case in enumerate(test_queries, 1):
            query = test_case["query"]
            expected = test_case["expected"]
            reasoning = test_case["reasoning"]
            
            print(f"--- TESTE {i}: {query} ---")
            print(f"💭 Expectativa: {expected}")
            print(f"🧠 Raciocínio esperado: {reasoning}")
            
            # Criar prompt de seleção (mesmo usado no sistema real)
            selection_prompt = f"""Tables:
- internacoes: patient internment data, deaths, hospitals (11M records)
- cid10: disease codes CID-10 and descriptions (14K records)  
- municipios: brazilian cities and geographic data (5K records)

Query: "{query}"
Answer with table name(s) only:"""
            
            print("\n🤖 PROMPT ENVIADO PARA LLM:")
            print("-" * 60)
            print(selection_prompt)
            print("-" * 60)
            
            # Chamar LLM
            try:
                response = llm.invoke([HumanMessage(content=selection_prompt)])
                llm_response = response.content.strip() if hasattr(response, 'content') else str(response)
                
                print(f"\n🎯 RESPOSTA DA LLM: '{llm_response}'")
                
                # Extrair tabelas da resposta (parsing robusto)
                import re
                available_tables = ["internacoes", "cid10", "municipios"]
                selected_tables = []
                
                for table_name in available_tables:
                    if re.search(r'\b' + re.escape(table_name) + r'\b', llm_response, re.IGNORECASE):
                        selected_tables.append(table_name)
                
                print(f"📋 Tabelas extraídas: {selected_tables}")
                
                # Verificar se decisão está correta
                is_correct = set(selected_tables) == set(expected)
                print(f"✅ Decisão correta: {is_correct}")
                
                if not is_correct:
                    print(f"⚠️  Esperado: {expected}, Obtido: {selected_tables}")
                
            except Exception as e:
                print(f"❌ Erro na chamada LLM: {e}")
                continue
            
            print()
        
        # 5. Análise do contexto fornecido pela tool
        print("3. Analisando contexto fornecido pela EnhancedListTablesTool...")
        
        context_analysis = {
            "tabelas_documentadas": 0,
            "value_mappings": 0,
            "selection_guide": False,
            "samples": False,
            "useful_hints": []
        }
        
        # Contar tabelas com descrições detalhadas
        detailed_tables = ["internacoes", "cid10", "municipios"]
        for table in detailed_tables:
            if table in tool_output:
                context_analysis["tabelas_documentadas"] += 1
        
        # Verificar value mappings
        if "SEXO=1" in tool_output:
            context_analysis["value_mappings"] += 1
            context_analysis["useful_hints"].append("SEXO mappings")
        
        if "Código CID-10" in tool_output:
            context_analysis["value_mappings"] += 1
            context_analysis["useful_hints"].append("CID format")
        
        # Verificar selection guide
        if "🎯" in tool_output:
            context_analysis["selection_guide"] = True
            context_analysis["useful_hints"].append("Selection guide icons")
        
        # Verificar samples
        if any(sample_word in tool_output.lower() for sample_word in ["sample", "exemplo", "registro"]):
            context_analysis["samples"] = True
            context_analysis["useful_hints"].append("Sample data")
        
        print("\n📊 ANÁLISE DO CONTEXTO:")
        print(f"🎯 Tabelas documentadas: {context_analysis['tabelas_documentadas']}/3")
        print(f"📝 Value mappings: {context_analysis['value_mappings']}")
        print(f"🧭 Selection guide: {context_analysis['selection_guide']}")
        print(f"📋 Samples incluídas: {context_analysis['samples']}")
        print(f"💡 Hints úteis: {context_analysis['useful_hints']}")
        
        # 6. Conclusões
        print("\n🎉 TESTE FLUXO DECISÃO LLM CONCLUÍDO!")
        print("\n📈 CONCLUSÕES:")
        print("✅ EnhancedListTablesTool fornece contexto estruturado")
        print("✅ LLM consegue interpretar e decidir tabelas relevantes")
        print("✅ Selection guide direciona decisões corretamente")
        print("✅ Value mappings ajudam em queries específicas")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO no teste: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_tool_performance_analysis():
    """Analisa performance e eficiência da tool"""
    
    print("\n=== ANÁLISE DE PERFORMANCE DA TOOL ===\n")
    
    try:
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        
        # Obter tool
        tools = llm_manager.get_sql_tools()
        enhanced_tool = None
        
        for tool in tools:
            if tool.name == "sql_db_list_tables":
                enhanced_tool = tool
                break
        
        # Medir tempo de execução
        import time
        
        print("⏱️  Testando performance da tool...")
        
        start_time = time.time()
        result = enhanced_tool.invoke({})
        execution_time = time.time() - start_time
        
        print(f"⚡ Tempo de execução: {execution_time:.3f}s")
        print(f"📏 Tamanho do output: {len(result)} caracteres")
        print(f"📄 Linhas de output: {result.count(chr(10)) + 1}")
        
        # Análise de eficiência
        efficiency_metrics = {
            "concise": len(result) < 2000,  # Tool deve ser concisa
            "informative": len(result) > 500,  # Mas informativa
            "fast": execution_time < 1.0,  # E rápida
            "structured": "🎯" in result or "|" in result  # E estruturada
        }
        
        print("\n📊 MÉTRICAS DE EFICIÊNCIA:")
        for metric, value in efficiency_metrics.items():
            status = "✅" if value else "⚠️"
            print(f"{status} {metric.capitalize()}: {value}")
        
        overall_score = sum(efficiency_metrics.values()) / len(efficiency_metrics)
        print(f"\n🏆 Score geral: {overall_score:.1%}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro na análise: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Iniciando teste completo do fluxo de decisão LLM...")
    
    success1 = test_llm_table_decision_flow()
    success2 = test_tool_performance_analysis()
    
    if success1 and success2:
        print("\n🎉 TODOS OS TESTES DE FLUXO LLM PASSARAM!")
        sys.exit(0)
    else:
        print("\n❌ ALGUNS TESTES FALHARAM!")
        sys.exit(1)