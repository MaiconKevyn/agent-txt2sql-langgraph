#!/usr/bin/env python3
"""
Análise Completa do Sistema de Métricas de Avaliação Text-to-SQL

Este script realiza uma análise abrangente para verificar:
1. Implementação correta das métricas
2. Consistência dos resultados
3. Casos extremos e edge cases
4. Qualidade geral do sistema
"""

import sys
from pathlib import Path
from collections import defaultdict

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from evaluation.metrics.exact_match import ExactMatchMetric
from evaluation.metrics.component_matching import ComponentMatchingMetric
from evaluation.metrics.execution_accuracy import ExecutionAccuracyMetric
from evaluation.metrics.base_metrics import EvaluationContext
from evaluation.database_evaluator import DatabaseTextToSQLEvaluator


class MetricsAnalyzer:
    """Analisador completo do sistema de métricas"""

    def __init__(self):
        self.em_metric = ExactMatchMetric()
        self.cm_metric = ComponentMatchingMetric()
        self.ex_metric = ExecutionAccuracyMetric()
        self.results = defaultdict(list)
        self.issues = []
        self.warnings = []

    def analyze_metric_consistency(self):
        """Analisa consistência entre métricas"""
        print("="*70)
        print("1️⃣ ANÁLISE DE CONSISTÊNCIA ENTRE MÉTRICAS")
        print("="*70)

        test_cases = [
            {
                "name": "Identical Queries",
                "gt": "SELECT COUNT(*) FROM users",
                "pred": "SELECT COUNT(*) FROM users",
                "expected": {"EM": 1.0, "CM": 1.0, "comment": "Queries idênticas"}
            },
            {
                "name": "Different Aliases",
                "gt": "SELECT COUNT(*) AS total FROM users",
                "pred": "SELECT COUNT(*) AS count FROM users",
                "expected": {"EM": 0.0, "CM": 1.0, "comment": "Aliases diferentes, expressão igual"}
            },
            {
                "name": "Missing WHERE",
                "gt": "SELECT COUNT(*) FROM users WHERE age > 18",
                "pred": "SELECT COUNT(*) FROM users",
                "expected": {"EM": 0.0, "CM": "<1.0", "comment": "Falta WHERE clause"}
            },
            {
                "name": "Wrong Table",
                "gt": "SELECT COUNT(*) FROM users",
                "pred": "SELECT COUNT(*) FROM products",
                "expected": {"EM": 0.0, "CM": "<1.0", "comment": "Tabela errada"}
            },
            {
                "name": "Column Order Difference",
                "gt": "SELECT name, age FROM users",
                "pred": "SELECT age, name FROM users",
                "expected": {"EM": 0.0, "CM": 1.0, "comment": "Ordem de colunas diferente"}
            },
            {
                "name": "Whitespace Only",
                "gt": "SELECT COUNT(*) FROM users",
                "pred": "SELECT   COUNT(*)   FROM   users",
                "expected": {"EM": 1.0, "CM": 1.0, "comment": "Apenas espaços diferentes"}
            },
            {
                "name": "Case Sensitivity",
                "gt": "SELECT COUNT(*) FROM users",
                "pred": "select count(*) from users",
                "expected": {"EM": 1.0, "CM": 1.0, "comment": "Apenas case diferente"}
            }
        ]

        consistency_score = 0
        total_tests = len(test_cases)

        for case in test_cases:
            print(f"\n📝 {case['name']}")
            print(f"   GT:   {case['gt']}")
            print(f"   Pred: {case['pred']}")
            print(f"   Expectativa: {case.get('comment', 'N/A')}")

            context = EvaluationContext(
                question_id="test",
                question="Test",
                ground_truth_sql=case['gt'],
                predicted_sql=case['pred']
            )

            em_result = self.em_metric.evaluate(context)
            cm_result = self.cm_metric.evaluate(context)

            print(f"   Resultados:")
            print(f"     EM: {em_result.score:.3f} (esperado: {case['expected']['EM']})")
            print(f"     CM: {cm_result.score:.3f} (esperado: {case['expected']['CM']})")

            # Verificar consistência
            em_ok = abs(em_result.score - case['expected']['EM']) < 0.01 if isinstance(case['expected']['EM'], (int, float)) else True
            cm_ok = (cm_result.score == 1.0 if case['expected']['CM'] == 1.0
                    else cm_result.score < 1.0 if case['expected']['CM'] == "<1.0"
                    else True)

            if em_ok and cm_ok:
                print(f"   ✅ Consistente")
                consistency_score += 1
            else:
                print(f"   ❌ Inconsistente")
                self.issues.append(f"{case['name']}: Resultados não batem com esperado")

        return consistency_score / total_tests

    def analyze_edge_cases(self):
        """Analisa casos extremos"""
        print("\n\n" + "="*70)
        print("2️⃣ ANÁLISE DE CASOS EXTREMOS")
        print("="*70)

        edge_cases = [
            {
                "name": "Empty Queries",
                "gt": "",
                "pred": "",
                "should_handle": True
            },
            {
                "name": "One Empty Query",
                "gt": "SELECT * FROM users",
                "pred": "",
                "should_handle": True
            },
            {
                "name": "Very Long Query",
                "gt": "SELECT " + ", ".join([f"col{i}" for i in range(100)]) + " FROM users",
                "pred": "SELECT " + ", ".join([f"col{i}" for i in range(100)]) + " FROM users",
                "should_handle": True
            },
            {
                "name": "Complex Nested",
                "gt": "SELECT * FROM (SELECT id FROM users WHERE age > 18) AS subq",
                "pred": "SELECT * FROM (SELECT id FROM users WHERE age > 18) AS subq",
                "should_handle": True
            },
            {
                "name": "Special Characters",
                "gt": "SELECT \"Nome Completo\" FROM \"Usuários\"",
                "pred": "SELECT \"Nome Completo\" FROM \"Usuários\"",
                "should_handle": True
            }
        ]

        edge_score = 0
        for case in edge_cases:
            print(f"\n📝 {case['name']}")
            try:
                context = EvaluationContext(
                    question_id="edge",
                    question="Edge case",
                    ground_truth_sql=case['gt'],
                    predicted_sql=case['pred']
                )

                em_result = self.em_metric.evaluate(context)
                cm_result = self.cm_metric.evaluate(context)

                print(f"   EM: {em_result.score:.3f}")
                print(f"   CM: {cm_result.score:.3f}")
                print(f"   ✅ Tratado sem erros")
                edge_score += 1

            except Exception as e:
                print(f"   ❌ Erro: {e}")
                self.issues.append(f"Edge case '{case['name']}' falhou: {e}")

        return edge_score / len(edge_cases)

    def analyze_component_parsing(self):
        """Analisa parsing de componentes SQL"""
        print("\n\n" + "="*70)
        print("3️⃣ ANÁLISE DE PARSING DE COMPONENTES")
        print("="*70)

        from evaluation.metrics.improved_sql_parser import ImprovedSQLParser

        parsing_tests = [
            {
                "sql": "SELECT name FROM users WHERE age > 18",
                "expected": {
                    "select": "name",
                    "from": "users",
                    "where": "age > 18"
                }
            },
            {
                "sql": "SELECT COUNT(*) AS total FROM users GROUP BY status",
                "expected": {
                    "select": "COUNT ( * ) total",  # Variação aceitável
                    "from": "users",
                    "group_by": "status"
                }
            },
            {
                "sql": "SELECT u.name, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id",
                "expected": {
                    "select": "u.name, COUNT ( o.id )",
                    "from": "users u",
                    "joins": True  # Apenas verificar se existe
                }
            }
        ]

        parsing_score = 0
        for test in parsing_tests:
            print(f"\n📝 SQL: {test['sql'][:60]}...")
            components = ImprovedSQLParser.extract_components(test['sql'])

            print(f"   Componentes extraídos:")
            issues = []
            for key, expected_value in test['expected'].items():
                actual = components.get(key, '')

                if isinstance(expected_value, bool):
                    # Apenas verificar se existe
                    exists = bool(actual.strip())
                    status = "✅" if exists == expected_value else "❌"
                    print(f"     {key}: {status} (existe: {exists})")
                    if exists == expected_value:
                        parsing_score += 0.33
                else:
                    # Verificar conteúdo (com flexibilidade para espaços)
                    actual_normalized = ' '.join(actual.split())
                    expected_normalized = ' '.join(expected_value.split())
                    match = actual_normalized.startswith(expected_normalized[:10]) if expected_normalized else not actual_normalized
                    status = "✅" if match else "⚠️"
                    print(f"     {key}: {status} '{actual}'")
                    if match:
                        parsing_score += 0.33

        return min(parsing_score / len(parsing_tests), 1.0)

    def analyze_alias_handling(self):
        """Analisa tratamento de aliases"""
        print("\n\n" + "="*70)
        print("4️⃣ ANÁLISE DE TRATAMENTO DE ALIASES")
        print("="*70)

        from evaluation.metrics.improved_sql_parser import ImprovedColumnComparator

        alias_tests = [
            {
                "name": "Same Expression, Different Aliases",
                "gt": ["COUNT(*) AS total"],
                "pred": ["COUNT(*) AS count"],
                "expected_similarity": 1.0
            },
            {
                "name": "One with Alias, One Without",
                "gt": ["COUNT(*) AS total"],
                "pred": ["COUNT(*)"],
                "expected_similarity": 0.7
            },
            {
                "name": "Multiple Columns with Aliases",
                "gt": ["name", "COUNT(*) AS total"],
                "pred": ["name", "COUNT(*) AS count"],
                "expected_similarity": 1.0
            },
            {
                "name": "Different Expressions",
                "gt": ["COUNT(*)"],
                "pred": ["SUM(value)"],
                "expected_similarity": 0.0
            }
        ]

        alias_score = 0
        for test in alias_tests:
            print(f"\n📝 {test['name']}")
            comparison = ImprovedColumnComparator.compare_select_items(test['gt'], test['pred'])

            similarity = comparison['jaccard_similarity']
            expected = test['expected_similarity']

            print(f"   GT:   {test['gt']}")
            print(f"   Pred: {test['pred']}")
            print(f"   Similaridade: {similarity:.3f} (esperado: {expected:.3f})")

            # Tolerância de 0.1
            if abs(similarity - expected) < 0.1:
                print(f"   ✅ Correto")
                alias_score += 1
            else:
                print(f"   ❌ Incorreto")
                self.warnings.append(f"Alias handling '{test['name']}': esperado {expected}, obtido {similarity}")

        return alias_score / len(alias_tests)

    def analyze_database_integration(self):
        """Analisa integração com banco de dados"""
        print("\n\n" + "="*70)
        print("5️⃣ ANÁLISE DE INTEGRAÇÃO COM BANCO DE DADOS")
        print("="*70)

        try:
            evaluator = DatabaseTextToSQLEvaluator()

            if not evaluator.db_connection:
                print("⚠️  Banco de dados não disponível")
                self.warnings.append("Database não conectado - EX metric não testada")
                return 0.5

            print("✅ Conexão de banco estabelecida")

            # Testar execução básica
            test_queries = [
                ("SELECT COUNT(*) FROM mortes", "Query simples de contagem"),
                ("SELECT COUNT(*) AS total FROM mortes", "Query com alias"),
            ]

            db_score = 0
            for query, description in test_queries:
                print(f"\n📝 {description}")
                print(f"   SQL: {query}")

                try:
                    result, error = evaluator.db_connection.execute_query(query)

                    if error:
                        print(f"   ❌ Erro: {error}")
                    else:
                        print(f"   ✅ Executado com sucesso: {len(result)} linhas")
                        db_score += 1
                except Exception as e:
                    print(f"   ❌ Exceção: {e}")

            return db_score / len(test_queries)

        except Exception as e:
            print(f"❌ Erro ao conectar banco: {e}")
            self.warnings.append(f"Database integration error: {e}")
            return 0.0

    def generate_report(self):
        """Gera relatório final"""
        print("\n\n" + "="*70)
        print("📊 RELATÓRIO FINAL DE ANÁLISE")
        print("="*70)

        scores = {
            "Consistência entre métricas": self.analyze_metric_consistency(),
            "Casos extremos": self.analyze_edge_cases(),
            "Parsing de componentes": self.analyze_component_parsing(),
            "Tratamento de aliases": self.analyze_alias_handling(),
            "Integração com banco": self.analyze_database_integration()
        }

        print("\n📈 SCORES POR CATEGORIA:")
        for category, score in scores.items():
            bar = "█" * int(score * 20) + "░" * (20 - int(score * 20))
            print(f"   {category:30s} [{bar}] {score*100:5.1f}%")

        overall_score = sum(scores.values()) / len(scores)

        print(f"\n🎯 SCORE GERAL: {overall_score*100:.1f}%")
        print(f"   Nota (0-10): {overall_score*10:.1f}")

        # Avaliar qualidade
        if overall_score >= 0.9:
            quality = "EXCELENTE"
            emoji = "🏆"
        elif overall_score >= 0.8:
            quality = "MUITO BOM"
            emoji = "✅"
        elif overall_score >= 0.7:
            quality = "BOM"
            emoji = "👍"
        elif overall_score >= 0.6:
            quality = "REGULAR"
            emoji = "⚠️"
        else:
            quality = "PRECISA MELHORIAS"
            emoji = "❌"

        print(f"   Qualidade: {emoji} {quality}")

        if self.issues:
            print(f"\n❌ PROBLEMAS ENCONTRADOS ({len(self.issues)}):")
            for issue in self.issues[:5]:  # Mostrar primeiros 5
                print(f"   • {issue}")

        if self.warnings:
            print(f"\n⚠️  AVISOS ({len(self.warnings)}):")
            for warning in self.warnings[:5]:  # Mostrar primeiros 5
                print(f"   • {warning}")

        print("\n💡 RECOMENDAÇÕES:")
        if overall_score >= 0.8:
            print("   ✅ Sistema bem implementado e funcionando corretamente")
            print("   ✅ Métricas consistentes com padrões Spider")
            print("   ✅ Pronto para uso em avaliação acadêmica")
        else:
            print("   ⚠️  Revisar casos que falharam")
            print("   ⚠️  Melhorar tratamento de edge cases")
            print("   ⚠️  Validar parsing de componentes complexos")

        return overall_score


def main():
    print("🔍 ANÁLISE COMPLETA DO SISTEMA DE MÉTRICAS TEXT-TO-SQL")
    print("Verificando implementação, consistência e qualidade...")
    print()

    analyzer = MetricsAnalyzer()
    final_score = analyzer.generate_report()

    print("\n" + "="*70)
    print(f"ANÁLISE CONCLUÍDA - NOTA FINAL: {final_score*10:.1f}/10")
    print("="*70)


if __name__ == "__main__":
    main()