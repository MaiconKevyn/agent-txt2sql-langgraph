#!/usr/bin/env python3
"""
Demo: Como a Métrica Execution Accuracy (EX) Funciona

Este script demonstra detalhadamente como a métrica EX executa queries
no banco e compara resultados, mostrando todo o processo passo a passo.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from evaluation.database_evaluator import DatabaseTextToSQLEvaluator
from evaluation.metrics.execution_accuracy import ExecutionAccuracyMetric
from evaluation.metrics.base_metrics import EvaluationContext


def demonstrate_ex_step_by_step():
    """Demonstra como EX funciona passo a passo"""
    print("🔍 DEMONSTRAÇÃO: Como Funciona a Execution Accuracy (EX)")
    print("="*70)

    # Criar conexão de banco
    evaluator = DatabaseTextToSQLEvaluator()

    if not evaluator.db_connection:
        print("❌ Sem conexão de banco - demonstração limitada")
        return

    print("✅ Conexão de banco estabelecida")

    # Casos de teste
    test_cases = [
        {
            "name": "Resultado IGUAL - EX = 1.0",
            "description": "Ambas queries retornam o mesmo resultado",
            "gt_sql": "SELECT COUNT(*) FROM mortes",
            "pred_sql": "SELECT COUNT(*) AS total_mortes FROM mortes",
            "expected_ex": 1.0
        },
        {
            "name": "Resultado DIFERENTE - EX = 0.0",
            "description": "Queries retornam resultados diferentes",
            "gt_sql": "SELECT COUNT(*) FROM mortes",
            "pred_sql": "SELECT COUNT(*) FROM internacoes",
            "expected_ex": 0.0
        },
        {
            "name": "Query INVÁLIDA - EX = 0.0",
            "description": "Query predita tem erro de sintaxe",
            "gt_sql": "SELECT COUNT(*) FROM mortes",
            "pred_sql": "SELECT COUNT(*) FROM tabela_inexistente",
            "expected_ex": 0.0
        }
    ]

    for i, case in enumerate(test_cases, 1):
        print(f"\n📊 CASO {i}: {case['name']}")
        print("-" * 50)
        print(f"Descrição: {case['description']}")
        print(f"GT SQL:    {case['gt_sql']}")
        print(f"Pred SQL:  {case['pred_sql']}")

        # Executar manualmente para mostrar processo
        print(f"\n🔄 PROCESSO DE EXECUÇÃO:")

        # Passo 1: Executar Ground Truth
        print("1️⃣ Executando Ground Truth SQL...")
        try:
            gt_result, gt_error = evaluator.db_connection.execute_query(case['gt_sql'])
            if gt_error:
                print(f"   ❌ Erro GT: {gt_error}")
            else:
                print(f"   ✅ GT executado com sucesso: {len(gt_result)} linhas")
                print(f"   📄 Resultado GT: {gt_result[:3]}...")  # Primeiras 3 linhas
        except Exception as e:
            print(f"   ❌ Exceção GT: {e}")
            gt_result, gt_error = None, str(e)

        # Passo 2: Executar Predicted
        print("2️⃣ Executando Predicted SQL...")
        try:
            pred_result, pred_error = evaluator.db_connection.execute_query(case['pred_sql'])
            if pred_error:
                print(f"   ❌ Erro Pred: {pred_error}")
            else:
                print(f"   ✅ Pred executado com sucesso: {len(pred_result)} linhas")
                print(f"   📄 Resultado Pred: {pred_result[:3]}...")  # Primeiras 3 linhas
        except Exception as e:
            print(f"   ❌ Exceção Pred: {e}")
            pred_result, pred_error = None, str(e)

        # Passo 3: Comparar resultados
        print("3️⃣ Comparando resultados...")
        if gt_error or pred_error:
            results_match = False
            print("   ❌ Não é possível comparar (erro na execução)")
        elif gt_result is None or pred_result is None:
            results_match = False
            print("   ❌ Resultado nulo")
        else:
            # Normalizar e comparar
            from collections import Counter

            # Converter para formato comparável
            gt_tuples = [tuple(row) for row in gt_result]
            pred_tuples = [tuple(row) for row in pred_result]

            gt_counter = Counter(gt_tuples)
            pred_counter = Counter(pred_tuples)

            results_match = gt_counter == pred_counter

            if results_match:
                print("   ✅ Resultados IDÊNTICOS")
            else:
                print("   ❌ Resultados DIFERENTES")
                print(f"      GT Counter:   {gt_counter}")
                print(f"      Pred Counter: {pred_counter}")

        # Passo 4: Score final
        ex_score = 1.0 if results_match else 0.0
        print(f"4️⃣ Score EX: {ex_score}")

        # Verificar se está conforme esperado
        status = "✅" if ex_score == case['expected_ex'] else "❌"
        print(f"   Resultado esperado: {case['expected_ex']} {status}")

        # Usar a métrica oficial para verificar
        print(f"\n🧪 VERIFICAÇÃO COM MÉTRICA OFICIAL:")
        context = EvaluationContext(
            question_id=f"demo_{i}",
            question=f"Teste {i}",
            ground_truth_sql=case['gt_sql'],
            predicted_sql=case['pred_sql'],
            database_connection=evaluator.db_connection
        )

        ex_metric = ExecutionAccuracyMetric()
        official_result = ex_metric.evaluate(context)
        print(f"   Score oficial: {official_result.score}")
        print(f"   Is correct: {official_result.is_correct}")

        if official_result.error_message:
            print(f"   Erro: {official_result.error_message}")


def show_ex_advantages():
    """Mostra as vantagens da métrica EX"""
    print(f"\n\n💡 VANTAGENS DA EXECUTION ACCURACY (EX)")
    print("="*70)

    advantages = [
        {
            "title": "🎯 Avaliação Semântica",
            "description": "Foca no que realmente importa: o resultado correto",
            "example": "SELECT COUNT(*) vs SELECT COUNT(*) AS total → Mesmo resultado = Score 1.0"
        },
        {
            "title": "🔄 Independente de Sintaxe",
            "description": "Diferentes sintaxes que produzem mesmo resultado são aceitas",
            "example": "SELECT * FROM users WHERE age > 18 vs SELECT * FROM users WHERE 18 < age"
        },
        {
            "title": "📊 Mais Realista",
            "description": "Espelha o que realmente importa para o usuário final",
            "example": "Usuário quer dados corretos, não sintaxe específica"
        },
        {
            "title": "🔍 Detecta Erros Lógicos",
            "description": "Identifica queries sintaticamente corretas mas logicamente erradas",
            "example": "SELECT * FROM wrong_table → Sintaxe OK, mas resultado errado"
        },
        {
            "title": "🛡️ Robusto a Variações",
            "description": "Tolera diferentes formas de expressar a mesma consulta",
            "example": "Ordem de colunas, aliases, formatação não afetam"
        }
    ]

    for adv in advantages:
        print(f"\n{adv['title']}")
        print(f"   {adv['description']}")
        print(f"   Exemplo: {adv['example']}")


def show_ex_limitations():
    """Mostra limitações da métrica EX"""
    print(f"\n\n⚠️  LIMITAÇÕES DA EXECUTION ACCURACY (EX)")
    print("="*70)

    limitations = [
        {
            "title": "🐌 Performance",
            "description": "Requer execução real no banco - mais lenta que EM/CM",
            "impact": "Pode ser problemática para avaliações em larga escala"
        },
        {
            "title": "🔗 Dependência de Banco",
            "description": "Precisa de conexão ativa e banco configurado",
            "impact": "Não funciona offline ou sem infraestrutura"
        },
        {
            "title": "💾 Estado do Banco",
            "description": "Resultados podem variar se dados mudarem",
            "impact": "Precisa de dados consistentes para reprodutibilidade"
        },
        {
            "title": "⏱️ Timeout Issues",
            "description": "Queries muito complexas podem exceder timeout",
            "impact": "Queries otimizadas podem ser penalizadas injustamente"
        },
        {
            "title": "🔒 Permissions",
            "description": "Requer permissões adequadas no banco",
            "impact": "Configuração mais complexa que métricas sintáticas"
        }
    ]

    for lim in limitations:
        print(f"\n❌ {lim['title']}")
        print(f"   {lim['description']}")
        print(f"   Impacto: {lim['impact']}")


def main():
    """Função principal"""
    print("🚀 DEMONSTRAÇÃO COMPLETA: EXECUTION ACCURACY (EX)")
    print("Como a métrica EX executa queries no banco e compara resultados")

    demonstrate_ex_step_by_step()
    show_ex_advantages()
    show_ex_limitations()

    print(f"\n\n🎯 RESUMO: COMO EX FUNCIONA")
    print("="*70)
    print("1️⃣ Executa Ground Truth SQL no banco → Resultado A")
    print("2️⃣ Executa Predicted SQL no banco → Resultado B")
    print("3️⃣ Normaliza ambos os resultados (tipos, ordem)")
    print("4️⃣ Compara Resultado A == Resultado B")
    print("5️⃣ Score = 1.0 se iguais, 0.0 se diferentes")
    print("\n💡 É a métrica mais confiável para correção semântica!")


if __name__ == "__main__":
    main()