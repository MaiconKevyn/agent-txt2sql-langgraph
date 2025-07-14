#!/usr/bin/env python3
"""
Script de teste para Enhanced Template Library - Checkpoint 6
Valida a biblioteca expandida de templates para padrões comuns
"""
import sys
import os
import time

# Adicionar src ao path para imports
project_root = os.path.dirname(__file__)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

from application.services.implementations.enhanced_template_library import (
    EnhancedTemplateLibrary, TemplateCategory
)
from application.services.implementations.enhanced_template_manager import EnhancedTemplateManager
from application.services.implementations.comprehensive_query_planner_service import ComprehensiveQueryPlannerService
from domain.entities.query_decomposition import ComplexityAnalysis, QueryComplexityLevel, DecompositionStrategy


def test_enhanced_template_library():
    """Testa a biblioteca de templates aprimorada"""
    print("🔬 Testando Enhanced Template Library")
    print("=" * 60)
    
    library = EnhancedTemplateLibrary()
    
    # Verificar inicialização
    stats = library.get_library_statistics()
    print(f"✅ Biblioteca inicializada:")
    print(f"   Total de templates: {stats['total_templates']}")
    print(f"   Categorias: {list(stats['categories'].keys())}")
    print(f"   Templates por categoria:")
    for category, count in stats['categories'].items():
        print(f"     - {category}: {count} templates")
    
    # Testar busca de templates
    test_queries = [
        "Mulheres com doenças respiratórias que morreram em Porto Alegre",
        "Análise de neoplasias por faixa etária e letalidade",
        "Tendência trimestral de pneumonias nos últimos 3 anos",
        "Custo total por procedimento com mais de 100 casos",
        "Top 5 cidades com mais mortes por região"
    ]
    
    print(f"\n🔍 Testando compatibilidade com {len(test_queries)} queries:")
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n   {i}. Query: {query[:50]}...")
        
        # Mock analysis para teste
        mock_analysis = ComplexityAnalysis(
            query=query,
            complexity_score=75.0,
            complexity_level=QueryComplexityLevel.COMPLEX,
            patterns_detected=["demographic_filter", "geographic_analysis"],
            complexity_factors={"multiple_filters": 25.0, "geographic_grouping": 20.0},
            recommended_strategy=DecompositionStrategy.SEQUENTIAL_FILTERING,
            analysis_metadata={}
        )
        
        matching_templates = library.find_matching_templates(query, mock_analysis, min_score=0.4)
        print(f"      Matches: {len(matching_templates)} templates")
        
        if matching_templates:
            best_template, score = matching_templates[0]
            print(f"      Melhor: {best_template.name} (score: {score:.2f})")
            print(f"      Categoria: {best_template.category.value}")
            print(f"      Estratégia: {best_template.strategy.value}")
        else:
            print(f"      ⚠️ Nenhum template compatível encontrado")
    
    return len(stats['categories']) >= 8  # Deve ter pelo menos 8 categorias


def test_enhanced_template_manager():
    """Testa o gerenciador de templates aprimorado"""
    print("\n\n🎯 Testando Enhanced Template Manager")
    print("=" * 60)
    
    manager = EnhancedTemplateManager()
    
    # Testar seleção de template
    test_query = "Mulheres idosas com doenças respiratórias por cidade"
    
    mock_analysis = ComplexityAnalysis(
        query=test_query,
        complexity_score=80.0,
        complexity_level=QueryComplexityLevel.COMPLEX,
        patterns_detected=["respiratory_diseases", "demographic_filters"],
        complexity_factors={"age_filter": 20.0, "sex_filter": 15.0, "geographic_grouping": 25.0},
        recommended_strategy=DecompositionStrategy.DEMOGRAPHIC_ANALYSIS,
        analysis_metadata={}
    )
    
    print(f"Query de teste: {test_query}")
    
    # Selecionar melhor template
    selected_template = manager.select_best_template(test_query, mock_analysis)
    
    if selected_template:
        print(f"✅ Template selecionado:")
        print(f"   ID: {selected_template.template_id}")
        print(f"   Nome: {selected_template.name}")
        print(f"   Categoria: {selected_template.category.value}")
        print(f"   Descrição: {selected_template.description}")
        print(f"   Estratégia: {selected_template.strategy.value}")
        print(f"   Etapas no template: {len(selected_template.step_templates)}")
        
        # Gerar etapas
        print(f"\n🏗️ Gerando etapas do template:")
        steps = manager.generate_steps_from_template(selected_template, test_query, mock_analysis)
        
        if steps:
            print(f"✅ {len(steps)} etapas geradas:")
            for step in steps:
                print(f"   {step.step_id}. {step.description}")
                print(f"      Tipo: {step.step_type.value}")
                print(f"      Dependências: {step.depends_on_steps}")
        else:
            print(f"❌ Falha na geração de etapas")
            return False
        
        # Testar recomendações
        print(f"\n📋 Testando recomendações:")
        recommendations = manager.get_template_recommendations(test_query, mock_analysis)
        print(f"✅ {len(recommendations)} recomendações obtidas")
        
        for i, rec in enumerate(recommendations[:3], 1):
            print(f"   {i}. {rec['name']} (score: {rec['compatibility_score']})")
            print(f"      Categoria: {rec['category']}")
            print(f"      Taxa de sucesso: {rec['success_rate']:.1%}")
        
        return True
    else:
        print(f"❌ Nenhum template selecionado")
        return False


def test_integration_with_planner():
    """Testa integração com ComprehensiveQueryPlannerService"""
    print("\n\n🔗 Testando Integração com Query Planner")
    print("=" * 60)
    
    planner = ComprehensiveQueryPlannerService()
    
    test_queries = [
        "Análise de mortes por doenças respiratórias em mulheres por cidade",
        "Custo total e efetividade de procedimentos de alta complexidade", 
        "Tendência de neoplasias por faixa etária nos últimos anos"
    ]
    
    success_count = 0
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. Query: {query}")
        
        try:
            # Verificar se deve decompor
            should_decompose = planner.should_decompose_query(query)
            print(f"   Deve decompor: {should_decompose}")
            
            if should_decompose:
                # Tentar criar plano
                start_time = time.time()
                plan = planner.create_execution_plan(query)
                execution_time = time.time() - start_time
                
                print(f"   ✅ Plano criado em {execution_time:.2f}s:")
                print(f"      ID: {plan.plan_id}")
                print(f"      Etapas: {len(plan.steps)}")
                print(f"      Estratégia: {plan.strategy.value}")
                print(f"      Score complexidade: {plan.complexity_score:.1f}")
                
                # Verificar se usou Enhanced Template Manager
                if plan.metadata and plan.metadata.get('generator') == 'enhanced_template_manager':
                    print(f"      🎯 Enhanced Template usado: {plan.metadata.get('template_name')}")
                    print(f"      Categoria: {plan.metadata.get('template_category')}")
                else:
                    print(f"      📋 Template original usado")
                
                success_count += 1
            else:
                print(f"   ⚠️ Query não atendeu critérios para decomposição")
            
        except Exception as e:
            print(f"   ❌ Erro: {e}")
    
    print(f"\n📊 Resultado: {success_count}/{len(test_queries)} queries processadas com sucesso")
    return success_count > 0


def test_template_categories():
    """Testa cobertura das categorias de templates"""
    print("\n\n📂 Testando Cobertura de Categorias")
    print("=" * 60)
    
    library = EnhancedTemplateLibrary()
    
    # Verificar se todas as categorias esperadas estão presentes
    expected_categories = [
        TemplateCategory.RESPIRATORY_ANALYSIS,
        TemplateCategory.NEOPLASIA_ANALYSIS, 
        TemplateCategory.DEMOGRAPHIC_COMPLEX,
        TemplateCategory.TEMPORAL_ADVANCED,
        TemplateCategory.GEOGRAPHIC_CORRELATION,
        TemplateCategory.FINANCIAL_ANALYSIS,
        TemplateCategory.MULTI_CONDITION,
        TemplateCategory.RANKING_DETAILED
    ]
    
    print(f"Categorias esperadas: {len(expected_categories)}")
    
    missing_categories = []
    found_categories = []
    
    for category in expected_categories:
        templates = library.get_templates_by_category(category)
        if templates:
            found_categories.append(category)
            print(f"✅ {category.value}: {len(templates)} templates")
            
            # Mostrar exemplo de template
            example = templates[0]
            print(f"   Exemplo: {example.name}")
            print(f"   Padrões: {len(example.query_patterns)} padrões regex")
            print(f"   Etapas: {len(example.step_templates)} etapas")
        else:
            missing_categories.append(category)
            print(f"❌ {category.value}: Nenhum template encontrado")
    
    coverage = len(found_categories) / len(expected_categories)
    print(f"\n📈 Cobertura: {coverage:.1%} ({len(found_categories)}/{len(expected_categories)})")
    
    if missing_categories:
        print(f"⚠️ Categorias faltantes:")
        for cat in missing_categories:
            print(f"   - {cat.value}")
    
    return coverage >= 0.8  # 80% de cobertura mínima


def test_enhanced_statistics():
    """Testa estatísticas do sistema aprimorado"""
    print("\n\n📊 Testando Estatísticas do Sistema")
    print("=" * 60)
    
    manager = EnhancedTemplateManager()
    
    # Simular alguns usos
    test_queries = [
        "Mortes por doenças respiratórias em mulheres",
        "Análise de custos por procedimento", 
        "Neoplasias por faixa etária"
    ]
    
    for query in test_queries:
        mock_analysis = ComplexityAnalysis(
            query=query,
            complexity_score=70.0,
            complexity_level=QueryComplexityLevel.COMPLEX,
            patterns_detected=["test_pattern"],
            complexity_factors={"test_factor": 30.0},
            recommended_strategy=DecompositionStrategy.SEQUENTIAL_FILTERING,
            analysis_metadata={}
        )
        
        template = manager.select_best_template(query, mock_analysis)
        if template:
            # Simular performance
            manager.update_template_performance(template.template_id, True, 5.2)
    
    # Obter estatísticas
    stats = manager.get_manager_statistics()
    
    print(f"📋 Estatísticas da Biblioteca:")
    lib_stats = stats.get('library_statistics', {})
    print(f"   Total de templates: {lib_stats.get('total_templates', 0)}")
    print(f"   Categorias: {len(lib_stats.get('categories', {}))}")
    
    print(f"\n🎯 Estatísticas de Uso:")
    usage_stats = stats.get('usage_statistics', {})
    print(f"   Templates usados: {usage_stats.get('templates_used', 0)}")
    print(f"   Uso total: {usage_stats.get('total_template_usage', 0)}")
    
    print(f"\n⚡ Estatísticas de Performance:")
    perf_stats = stats.get('performance_statistics', {})
    print(f"   Templates com dados: {perf_stats.get('templates_with_performance_data', 0)}")
    print(f"   Taxa média de sucesso: {perf_stats.get('average_success_rate', 0):.1%}")
    
    return stats.get('library_statistics', {}).get('total_templates', 0) > 0


def main():
    """Executa todos os testes do Checkpoint 6"""
    print("🧪 TESTE DA ENHANCED TEMPLATE LIBRARY - CHECKPOINT 6")
    print("=" * 70)
    
    tests = [
        ("Biblioteca de Templates", test_enhanced_template_library),
        ("Gerenciador de Templates", test_enhanced_template_manager), 
        ("Integração com Planner", test_integration_with_planner),
        ("Cobertura de Categorias", test_template_categories),
        ("Estatísticas do Sistema", test_enhanced_statistics)
    ]
    
    results = []
    
    try:
        for test_name, test_func in tests:
            try:
                print(f"\n{'='*20} {test_name} {'='*20}")
                start_time = time.time()
                result = test_func()
                execution_time = time.time() - start_time
                
                results.append((test_name, result, execution_time))
                status = "✅ PASSOU" if result else "❌ FALHOU"
                print(f"\n{status}: {test_name} ({execution_time:.2f}s)")
                
            except Exception as e:
                print(f"\n❌ ERRO em {test_name}: {e}")
                results.append((test_name, False, 0))
                import traceback
                traceback.print_exc()
        
        # Resumo final
        print("\n\n🎉 RESUMO DOS TESTES - CHECKPOINT 6")
        print("=" * 70)
        
        passed = sum(1 for _, result, _ in results if result)
        total = len(results)
        
        for test_name, result, exec_time in results:
            status = "✅" if result else "❌"
            print(f"{status} {test_name} ({exec_time:.2f}s)")
        
        print(f"\n📊 Total: {passed}/{total} testes passaram")
        print(f"⏱️ Tempo total: {sum(t for _, _, t in results):.2f}s")
        
        if passed == total:
            print("\n🎉 TODOS OS TESTES PASSARAM!")
            print("✅ Enhanced Template Library implementada com sucesso!")
            print("🎯 Checkpoint 6 concluído - Biblioteca expandida pronta!")
            return 0
        else:
            print(f"\n⚠️ {total - passed} testes falharam")
            print("❌ Enhanced Template Library precisa de ajustes")
            return 1
        
    except Exception as e:
        print(f"\n\n❌ ERRO GERAL NOS TESTES: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())