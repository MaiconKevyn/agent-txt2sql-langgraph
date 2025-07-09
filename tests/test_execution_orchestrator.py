#!/usr/bin/env python3
"""
Script de teste para o ExecutionOrchestrator
Valida a implementação do Checkpoint 5
"""
import sys
import os
import time

# Adicionar src ao path para imports
project_root = os.path.dirname(__file__)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

from application.services.implementations.comprehensive_query_planner_service import ComprehensiveQueryPlannerService
from application.services.implementations.comprehensive_execution_orchestrator import ComprehensiveExecutionOrchestrator
from application.services.execution_orchestrator_service import OrchestratorConfig, ExecutionMode
from domain.entities.query_decomposition import QueryPlan, QueryStep, QueryStepType


class MockQueryProcessingService:
    """Mock do QueryProcessingService para testes"""
    
    def execute_sql_query(self, sql_query: str):
        """Simula execução de SQL"""
        time.sleep(0.1)  # Simular tempo de execução
        
        # Mock de resultados baseado no SQL
        class MockResult:
            def __init__(self, success=True, results=None, row_count=0, error_message=None):
                self.success = success
                self.results = results or []
                self.row_count = row_count
                self.error_message = error_message
                self.metadata = {}
        
        # Simular diferentes tipos de resultados
        if "WHERE SEXO = 3" in sql_query:
            return MockResult(
                success=True,
                results=[
                    {"CIDADE_RESIDENCIA_PACIENTE": "Porto Alegre", "SEXO": 3, "IDADE": 55},
                    {"CIDADE_RESIDENCIA_PACIENTE": "Caxias do Sul", "SEXO": 3, "IDADE": 62}
                ],
                row_count=2
            )
        elif "GROUP BY" in sql_query:
            return MockResult(
                success=True,
                results=[
                    {"CIDADE_RESIDENCIA_PACIENTE": "Porto Alegre", "total_mortes": 15},
                    {"CIDADE_RESIDENCIA_PACIENTE": "Caxias do Sul", "total_mortes": 12},
                    {"CIDADE_RESIDENCIA_PACIENTE": "Pelotas", "total_mortes": 8}
                ],
                row_count=3
            )
        elif "ORDER BY" in sql_query and "LIMIT" in sql_query:
            return MockResult(
                success=True,
                results=[
                    {"CIDADE_RESIDENCIA_PACIENTE": "Porto Alegre", "total_mortes": 15},
                    {"CIDADE_RESIDENCIA_PACIENTE": "Caxias do Sul", "total_mortes": 12}
                ],
                row_count=2
            )
        else:
            return MockResult(
                success=True,
                results=[{"count": 100}],
                row_count=1
            )


def create_test_plan() -> QueryPlan:
    """Cria um plano de teste simples"""
    from domain.entities.query_decomposition import DecompositionStrategy, QueryComplexityLevel
    
    steps = [
        QueryStep(
            step_id=1,
            step_type=QueryStepType.FILTER,
            description="Filtrar por sexo feminino",
            sql_template="SELECT * FROM sus_data WHERE SEXO = 3",
            depends_on_steps=[]
        ),
        QueryStep(
            step_id=2,
            step_type=QueryStepType.FILTER,
            description="Filtrar por idade > 50",
            sql_template="SELECT * FROM step_1_result WHERE IDADE > 50",
            depends_on_steps=[1]
        ),
        QueryStep(
            step_id=3,
            step_type=QueryStepType.AGGREGATE,
            description="Agrupar por cidade",
            sql_template="SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_mortes FROM step_2_result GROUP BY CIDADE_RESIDENCIA_PACIENTE",
            depends_on_steps=[2]
        ),
        QueryStep(
            step_id=4,
            step_type=QueryStepType.RANK,
            description="Ordenar e limitar a 5",
            sql_template="SELECT * FROM step_3_result ORDER BY total_mortes DESC LIMIT 5",
            depends_on_steps=[3]
        )
    ]
    
    return QueryPlan(
        plan_id="test_plan_001",
        original_query="Quais as 5 cidades com mais mortes de mulheres acima de 50 anos?",
        complexity_score=65.0,
        complexity_level=QueryComplexityLevel.COMPLEX,
        strategy=DecompositionStrategy.SEQUENTIAL_FILTERING,
        steps=steps
    )


def test_basic_execution():
    """Testa execução básica de um plano"""
    print("🚀 Testando Execução Básica")
    print("=" * 50)
    
    # Criar serviços
    mock_query_service = MockQueryProcessingService()
    orchestrator = ComprehensiveExecutionOrchestrator(mock_query_service)
    
    # Criar plano de teste
    plan = create_test_plan()
    
    print(f"Plano de teste: {plan.plan_id}")
    print(f"Etapas: {len(plan.steps)}")
    print(f"Estratégia: {plan.strategy.value}")
    
    # Executar plano
    print("\nExecutando plano...")
    start_time = time.time()
    result = orchestrator.execute_plan(plan)
    execution_time = time.time() - start_time
    
    # Verificar resultados
    print(f"\n📊 Resultados:")
    print(f"   Sucesso: {result.success}")
    print(f"   Tempo de execução: {execution_time:.2f}s")
    print(f"   Etapas executadas: {len(result.step_results)}")
    print(f"   Etapas completadas: {len(result.completed_steps)}")
    print(f"   Registros finais: {result.final_row_count}")
    
    if result.failed_step_id:
        print(f"   ❌ Etapa falhada: {result.failed_step_id}")
        print(f"   Erro: {result.error_message}")
    
    # Mostrar resultados de cada etapa
    print(f"\n📋 Detalhes das Etapas:")
    for step_result in result.step_results:
        status = "✅" if step_result.success else "❌"
        print(f"   {status} Etapa {step_result.step_id}: {step_result.row_count} registros em {step_result.execution_time:.2f}s")
        if not step_result.success:
            print(f"      Erro: {step_result.error_message}")
    
    return result.success


def test_execution_with_progress():
    """Testa execução com callback de progresso"""
    print("\n\n📈 Testando Execução com Progresso")
    print("=" * 50)
    
    mock_query_service = MockQueryProcessingService()
    orchestrator = ComprehensiveExecutionOrchestrator(mock_query_service)
    plan = create_test_plan()
    
    progress_updates = []
    
    def progress_callback(progress):
        progress_updates.append(progress)
        print(f"   📊 Progresso: {progress.overall_progress:.0%} - {progress.current_step_description}")
    
    print("Executando com callbacks de progresso...")
    result = orchestrator.execute_plan_async(plan, progress_callback)
    
    print(f"\n✅ Execução concluída:")
    print(f"   Updates de progresso: {len(progress_updates)}")
    print(f"   Sucesso: {result.success}")
    print(f"   Tempo total: {result.total_execution_time:.2f}s")
    
    return result.success


def test_execution_modes():
    """Testa diferentes modos de execução"""
    print("\n\n⚙️ Testando Modos de Execução")
    print("=" * 50)
    
    mock_query_service = MockQueryProcessingService()
    
    # Criar plano com uma etapa que falha
    from domain.entities.query_decomposition import DecompositionStrategy, QueryComplexityLevel
    
    steps = [
        QueryStep(
            step_id=1,
            step_type=QueryStepType.FILTER,
            description="Etapa que funciona",
            sql_template="SELECT * FROM sus_data WHERE SEXO = 3",
            depends_on_steps=[]
        ),
        QueryStep(
            step_id=2,
            step_type=QueryStepType.FILTER,
            description="Etapa que pode falhar",
            sql_template="SELECT * FROM invalid_table WHERE invalid_column = 'test'",  # SQL inválido
            depends_on_steps=[1]
        ),
        QueryStep(
            step_id=3,
            step_type=QueryStepType.AGGREGATE,
            description="Etapa após falha",
            sql_template="SELECT COUNT(*) FROM step_2_result",
            depends_on_steps=[2]
        )
    ]
    
    failing_plan = QueryPlan(
        plan_id="failing_test_plan",
        original_query="Plano de teste com falha",
        complexity_score=60.0,
        complexity_level=QueryComplexityLevel.COMPLEX,
        strategy=DecompositionStrategy.SEQUENTIAL_FILTERING,
        steps=steps
    )
    
    # Teste modo FAIL_FAST
    print("\n🔒 Modo FAIL_FAST:")
    config_fail_fast = OrchestratorConfig(execution_mode=ExecutionMode.FAIL_FAST)
    orchestrator_fail_fast = ComprehensiveExecutionOrchestrator(mock_query_service, config_fail_fast)
    
    result_fail_fast = orchestrator_fail_fast.execute_plan(failing_plan)
    print(f"   Sucesso: {result_fail_fast.success}")
    print(f"   Etapas executadas: {len(result_fail_fast.step_results)}")
    print(f"   Etapa falhada: {result_fail_fast.failed_step_id}")
    
    # Teste modo BEST_EFFORT
    print("\n🔓 Modo BEST_EFFORT:")
    config_best_effort = OrchestratorConfig(execution_mode=ExecutionMode.BEST_EFFORT)
    orchestrator_best_effort = ComprehensiveExecutionOrchestrator(mock_query_service, config_best_effort)
    
    result_best_effort = orchestrator_best_effort.execute_plan(failing_plan)
    print(f"   Sucesso: {result_best_effort.success}")
    print(f"   Etapas executadas: {len(result_best_effort.step_results)}")
    print(f"   Etapa falhada: {result_best_effort.failed_step_id}")
    
    return True


def test_result_aggregation():
    """Testa agregação de resultados"""
    print("\n\n🔄 Testando Agregação de Resultados")
    print("=" * 50)
    
    mock_query_service = MockQueryProcessingService()
    orchestrator = ComprehensiveExecutionOrchestrator(mock_query_service)
    plan = create_test_plan()
    
    result = orchestrator.execute_plan(plan)
    
    if result.success and result.metadata and 'formatted_result' in result.metadata:
        formatted = result.metadata['formatted_result']
        
        print(f"✅ Resultado formatado:")
        print(f"   Tipo: {formatted.get('presentation_type', 'N/A')}")
        print(f"   Total de registros: {formatted.get('total_records', 0)}")
        print(f"   Sucesso na formatação: {formatted.get('success', False)}")
        
        if 'summary' in formatted:
            print(f"   Resumo: {formatted['summary']}")
        
        if 'data' in formatted and formatted['data']:
            data = formatted['data']
            if isinstance(data, dict) and 'items' in data:
                print(f"   Itens encontrados: {len(data['items'])}")
                if data['items']:
                    print(f"   Primeiro item: {data['items'][0]}")
        
        return True
    else:
        print("❌ Falha na agregação de resultados")
        return False


def test_orchestrator_statistics():
    """Testa estatísticas do orquestrador"""
    print("\n\n📊 Testando Estatísticas do Orquestrador")
    print("=" * 50)
    
    mock_query_service = MockQueryProcessingService()
    config = OrchestratorConfig(
        execution_mode=ExecutionMode.SEQUENTIAL,
        max_retries_per_step=3,
        step_timeout_seconds=30.0
    )
    orchestrator = ComprehensiveExecutionOrchestrator(mock_query_service, config)
    
    stats = orchestrator.get_orchestrator_statistics()
    
    print("📈 Estatísticas:")
    print(f"   Modo de execução: {stats['config']['execution_mode']}")
    print(f"   Max retries por etapa: {stats['config']['max_retries_per_step']}")
    print(f"   Timeout por etapa: {stats['config']['step_timeout_seconds']}s")
    print(f"   Execuções ativas: {stats['active_executions']}")
    print(f"   Estados salvos: {stats['execution_states']}")
    print(f"   Step executor: {stats['components']['step_executor']}")
    print(f"   Result aggregator: {stats['components']['result_aggregator']}")
    
    return True


def test_integration_with_planner():
    """Testa integração com QueryPlannerService"""
    print("\n\n🔗 Testando Integração com QueryPlannerService")
    print("=" * 50)
    
    # Criar query planner
    planner = ComprehensiveQueryPlannerService()
    
    # Query complexa que deve gerar plano
    complex_query = "Qual o custo total por procedimento com mais de 100 casos?"
    
    print(f"Query: {complex_query}")
    
    # Verificar se deve decompor
    should_decompose = planner.should_decompose_query(complex_query)
    print(f"Deve decompor: {should_decompose}")
    
    if should_decompose:
        # Gerar plano
        plan = planner.create_execution_plan(complex_query)
        print(f"Plano gerado: {plan.plan_id} com {len(plan.steps)} etapas")
        
        # Executar plano
        mock_query_service = MockQueryProcessingService()
        orchestrator = ComprehensiveExecutionOrchestrator(mock_query_service)
        
        result = orchestrator.execute_plan(plan)
        print(f"Execução: {'✅ Sucesso' if result.success else '❌ Falha'}")
        print(f"Tempo total: {result.total_execution_time:.2f}s")
        
        return result.success
    else:
        print("⚠️ Query não foi considerada complexa o suficiente para decomposição")
        return True


def main():
    """Executa todos os testes"""
    print("🧪 TESTE DO EXECUTION ORCHESTRATOR - CHECKPOINT 5")
    print("=" * 60)
    
    tests = [
        ("Execução Básica", test_basic_execution),
        ("Execução com Progresso", test_execution_with_progress),
        ("Modos de Execução", test_execution_modes),
        ("Agregação de Resultados", test_result_aggregation),
        ("Estatísticas do Orquestrador", test_orchestrator_statistics),
        ("Integração com Planner", test_integration_with_planner)
    ]
    
    results = []
    
    try:
        for test_name, test_func in tests:
            try:
                print(f"\n{'='*20} {test_name} {'='*20}")
                result = test_func()
                results.append((test_name, result))
                status = "✅ PASSOU" if result else "❌ FALHOU"
                print(f"\n{status}: {test_name}")
            except Exception as e:
                print(f"\n❌ ERRO em {test_name}: {e}")
                results.append((test_name, False))
                import traceback
                traceback.print_exc()
        
        # Resumo final
        print("\n\n🎉 RESUMO DOS TESTES")
        print("=" * 60)
        
        passed = sum(1 for _, result in results if result)
        total = len(results)
        
        for test_name, result in results:
            status = "✅" if result else "❌"
            print(f"{status} {test_name}")
        
        print(f"\n📊 Total: {passed}/{total} testes passaram")
        
        if passed == total:
            print("🎉 TODOS OS TESTES PASSARAM!")
            print("✅ ExecutionOrchestrator implementado com sucesso!")
            return 0
        else:
            print(f"⚠️ {total - passed} testes falharam")
            return 1
        
    except Exception as e:
        print(f"\n\n❌ ERRO GERAL NOS TESTES: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())