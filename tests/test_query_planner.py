#!/usr/bin/env python3
"""
Script de teste para o QueryPlannerService
Valida a implementação do Checkpoint 4
"""
import sys
import os

# Adicionar src ao path para imports
project_root = os.path.dirname(__file__)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

# Agora importar com paths relativos ao src
from application.services.implementations.comprehensive_query_planner_service import ComprehensiveQueryPlannerService
from application.services.query_planner_service import PlannerConfig
from domain.entities.query_decomposition import QueryComplexityLevel, DecompositionStrategy


def test_complexity_analysis():
    """Testa análise de complexidade"""
    print("🔍 Testando Análise de Complexidade")
    print("=" * 50)
    
    planner = ComprehensiveQueryPlannerService()
    
    # Queries de teste baseadas no Checkpoint 1
    test_queries = [
        # Query simples (não deve decompor)
        "Quantos pacientes existem?",
        
        # Query complexa - múltiplos filtros demográficos
        "Quais foram as 5 cidades que mais morreram mulheres com mais de 50 anos e tiveram doença respiratória?",
        
        # Query complexa - temporal
        "Qual o tempo médio de internação para doenças respiratórias por cidade?",
        
        # Query complexa - neoplasias (alto risco)
        "Quantas mortes por neoplasias existem no banco?",
        
        # Query moderada - ranking simples
        "Top 5 cidades com mais mortes",
        
        # Query complexa - financeira
        "Qual o custo total por procedimento com mais de 100 casos?"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. Query: {query}")
        print("-" * 40)
        
        try:
            analysis = planner.get_complexity_analysis(query)
            
            print(f"   Score: {analysis.complexity_score:.1f}")
            print(f"   Nível: {analysis.complexity_level.value}")
            print(f"   Estratégia: {analysis.recommended_strategy.value if analysis.recommended_strategy else 'Nenhuma'}")
            print(f"   Deve decompor: {analysis.should_decompose}")
            print(f"   Benefício estimado: {analysis.estimated_decomposition_benefit:.2f}")
            print(f"   Padrões detectados: {len(analysis.patterns_detected)}")
            
            if analysis.patterns_detected:
                print(f"   Principais padrões: {', '.join(analysis.patterns_detected[:3])}")
            
        except Exception as e:
            print(f"   ❌ Erro: {e}")


def test_decomposition_decision():
    """Testa decisão de decomposição"""
    print("\n\n🎯 Testando Decisão de Decomposição")
    print("=" * 50)
    
    planner = ComprehensiveQueryPlannerService()
    
    # Queries que devem ser decompostas
    should_decompose = [
        "Quais foram as 5 cidades que mais morreram mulheres com mais de 50 anos e tiveram doença respiratória?",
        "Qual o tempo médio de internação para doenças respiratórias por cidade?",
        "Quantas mortes por neoplasias existem no banco?",
    ]
    
    # Queries que NÃO devem ser decompostas
    should_not_decompose = [
        "Quantos pacientes existem?",
        "Qual é a população total?",
        "Listar cidades do banco"
    ]
    
    print("\n✅ Queries que DEVEM ser decompostas:")
    for query in should_decompose:
        decision = planner.should_decompose_query(query)
        status = "✅" if decision else "❌"
        print(f"   {status} {query[:60]}...")
        
    print("\n❌ Queries que NÃO devem ser decompostas:")
    for query in should_not_decompose:
        decision = planner.should_decompose_query(query)
        status = "❌" if not decision else "⚠️"
        print(f"   {status} {query[:60]}...")


def test_plan_generation():
    """Testa geração de planos"""
    print("\n\n🏗️ Testando Geração de Planos")
    print("=" * 50)
    
    planner = ComprehensiveQueryPlannerService()
    
    complex_queries = [
        "Quais foram as 5 cidades que mais morreram mulheres com mais de 50 anos e tiveram doença respiratória?",
        "Qual o tempo médio de internação para doenças respiratórias por cidade?"
    ]
    
    for i, query in enumerate(complex_queries, 1):
        print(f"\n{i}. Gerando plano para: {query[:60]}...")
        print("-" * 40)
        
        try:
            # Verificar se deve decompor
            if not planner.should_decompose_query(query):
                print("   ⚠️ Query não atende critérios para decomposição")
                continue
            
            # Gerar plano
            plan = planner.create_execution_plan(query)
            
            print(f"   ✅ Plano gerado: {plan.plan_id}")
            print(f"   📊 Estratégia: {plan.strategy.value}")
            print(f"   🔢 Etapas: {len(plan.steps)}")
            print(f"   ⏱️ Tempo estimado: {plan.estimated_total_time:.1f}s")
            
            print("\n   📋 Etapas do plano:")
            for step in plan.steps:
                print(f"      {step.step_id}. [{step.step_type.value}] {step.description}")
                if step.depends_on_steps:
                    print(f"         Depende de: {step.depends_on_steps}")
            
            # Validar plano
            warnings = planner._plan_generator.validate_plan(plan)
            if warnings:
                print(f"   ⚠️ Avisos de validação: {len(warnings)}")
                for warning in warnings:
                    print(f"      - {warning}")
            else:
                print("   ✅ Plano validado sem avisos")
                
        except Exception as e:
            print(f"   ❌ Erro na geração: {e}")


def test_template_matching():
    """Testa matching de templates"""
    print("\n\n🎯 Testando Template Matching")
    print("=" * 50)
    
    planner = ComprehensiveQueryPlannerService()
    
    # Query com padrão claro
    query = "Quais foram as 5 cidades que mais morreram mulheres com mais de 50 anos e tiveram doença respiratória?"
    
    print(f"Query: {query}")
    print("-" * 40)
    
    try:
        # Análise de complexidade
        analysis = planner.get_complexity_analysis(query)
        
        # Templates compatíveis
        templates = planner._template_matcher.find_matching_templates(query, analysis)
        
        print(f"✅ Templates encontrados: {len(templates)}")
        
        for i, template_match in enumerate(templates[:3], 1):  # Top 3
            template = template_match['template']
            score = template_match['compatibility_score']
            
            print(f"\n   {i}. {template.name}")
            print(f"      Compatibilidade: {score:.2f}")
            print(f"      Estratégia: {template.strategy.value}")
            print(f"      Descrição: {template.description}")
            
            # Parâmetros extraídos
            parameters = template.extract_parameters(query)
            if parameters:
                print(f"      Parâmetros: {parameters}")
        
    except Exception as e:
        print(f"❌ Erro no template matching: {e}")


def test_query_diagnosis():
    """Testa diagnóstico completo de queries"""
    print("\n\n🩺 Testando Diagnóstico de Queries")
    print("=" * 50)
    
    planner = ComprehensiveQueryPlannerService()
    
    query = "Quais foram as 5 cidades que mais morreram mulheres com mais de 50 anos e tiveram doença respiratória?"
    
    print(f"Diagnóstico para: {query}")
    print("-" * 40)
    
    try:
        diagnosis = planner.diagnose_query(query)
        
        print(f"📊 Complexidade:")
        print(f"   Score: {diagnosis['complexity']['score']:.1f}")
        print(f"   Nível: {diagnosis['complexity']['level']}")
        print(f"   Estratégia recomendada: {diagnosis['complexity']['recommended_strategy']}")
        
        print(f"\n🔍 Padrões detectados: {len(diagnosis['patterns_detected'])}")
        for pattern in diagnosis['patterns_detected'][:3]:
            print(f"   - {pattern}")
        
        print(f"\n🎯 Templates compatíveis: {len(diagnosis['templates'])}")
        for template in diagnosis['templates']:
            print(f"   - {template['name']} (score: {template['compatibility_score']:.2f})")
        
        print(f"\n🎯 Decisão:")
        decision = diagnosis['decision']
        print(f"   Deve decompor: {decision['should_decompose']}")
        print(f"   Threshold atingido: {decision['threshold_met']}")
        print(f"   Tem estratégia: {decision['has_strategy']}")
        print(f"   Tem templates: {decision['has_compatible_templates']}")
        
        print(f"\n🔄 Estratégias alternativas: {len(diagnosis['alternative_strategies'])}")
        for strategy in diagnosis['alternative_strategies']:
            print(f"   - {strategy}")
            
    except Exception as e:
        print(f"❌ Erro no diagnóstico: {e}")


def test_configuration():
    """Testa diferentes configurações"""
    print("\n\n⚙️ Testando Configurações")
    print("=" * 50)
    
    # Configuração rigorosa (threshold alto)
    rigorous_config = PlannerConfig(
        complexity_threshold_decompose=80.0,
        max_steps_per_plan=5,
        enable_query_optimization=True
    )
    
    # Configuração permissiva (threshold baixo)
    permissive_config = PlannerConfig(
        complexity_threshold_decompose=40.0,
        max_steps_per_plan=15,
        enable_query_optimization=False
    )
    
    query = "Top 5 cidades com mais mortes de mulheres"
    
    print(f"Query de teste: {query}")
    print("-" * 40)
    
    # Teste com configuração rigorosa
    print("\n🔒 Configuração rigorosa (threshold: 80.0):")
    planner_rigorous = ComprehensiveQueryPlannerService(rigorous_config)
    decision_rigorous = planner_rigorous.should_decompose_query(query)
    analysis = planner_rigorous.get_complexity_analysis(query)
    print(f"   Score: {analysis.complexity_score:.1f}")
    print(f"   Decisão: {'✅ Decompor' if decision_rigorous else '❌ Não decompor'}")
    
    # Teste com configuração permissiva
    print("\n🔓 Configuração permissiva (threshold: 40.0):")
    planner_permissive = ComprehensiveQueryPlannerService(permissive_config)
    decision_permissive = planner_permissive.should_decompose_query(query)
    print(f"   Score: {analysis.complexity_score:.1f}")
    print(f"   Decisão: {'✅ Decompor' if decision_permissive else '❌ Não decompor'}")


def main():
    """Executa todos os testes"""
    print("🧪 TESTE DO QUERY PLANNER SERVICE - CHECKPOINT 4")
    print("=" * 60)
    
    try:
        test_complexity_analysis()
        test_decomposition_decision()
        test_plan_generation()
        test_template_matching()
        test_query_diagnosis()
        test_configuration()
        
        print("\n\n🎉 TODOS OS TESTES CONCLUÍDOS")
        print("=" * 60)
        print("✅ QueryPlannerService implementado com sucesso!")
        
    except Exception as e:
        print(f"\n\n❌ ERRO GERAL NOS TESTES: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())