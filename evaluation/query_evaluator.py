#!/usr/bin/env python3
"""
Query Evaluator - Executa queries no banco e compara resultados

Este script é responsável por executar as queries (esperadas e geradas) no banco
de dados e comparar os resultados, fornecendo métricas de equivalência.

Autor: Claude Code Assistant
Data: 2025-07-13
"""

import sys
import argparse
from pathlib import Path
from typing import List, Dict, Any

from utils import (
    EvaluationConfig, QueryEvaluation, FileManager, 
    DatabaseManager, DataProcessor
)


class QueryEvaluator:
    """Avaliador de queries SQL"""
    
    def __init__(self, config: EvaluationConfig):
        """
        Inicializa o avaliador
        
        Args:
            config: Configuração de avaliação
        """
        self.config = config
        self.file_manager = FileManager(config.output_dir)
        self.db_manager = DatabaseManager(config.database_path)
        self.evaluations = []
        
        # Validar conexão com banco
        if not self.db_manager.validate_connection():
            raise ConnectionError(f"Não foi possível conectar ao banco: {config.database_path}")
        
        print(f"✅ Conexão com banco validada: {config.database_path}")
    
    def evaluate_single_query(self, model_result: Dict[str, Any]) -> QueryEvaluation:
        """
        Avalia uma única query comparando esperada vs gerada
        
        Args:
            model_result: Resultado do modelo contendo queries
            
        Returns:
            Avaliação da query
        """
        query_id = model_result["query_id"]
        expected_query = model_result["expected_query"]
        generated_query = model_result["generated_query"]
        
        print(f"  🔍 {query_id}: {model_result['question']}")
        
        # Executar query esperada
        expected_result = self.db_manager.execute_query(expected_query)
        if not expected_result['success']:
            print(f"    ❌ Erro na query esperada: {expected_result['error']}")
        
        # Executar query gerada (se existe)
        generated_result = {'success': False, 'data': [], 'columns': [], 'row_count': 0, 'error': 'No query generated'}
        if generated_query and generated_query.strip():
            generated_result = self.db_manager.execute_query(generated_query)
            if not generated_result['success']:
                print(f"    ❌ Erro na query gerada: {generated_result['error']}")
        
        # Comparar resultados
        comparison = DataProcessor.compare_query_results(expected_result, generated_result)
        
        # Calcular similaridade SQL
        sql_similarity = DataProcessor.calculate_sql_similarity(expected_query, generated_query)
        
        # Status da comparação
        if comparison['exact_match']:
            print(f"    ✅ Match exato | SQL similarity: {sql_similarity:.3f}")
        elif comparison['semantic_equivalence']:
            print(f"    🟡 Semanticamente equivalente | SQL similarity: {sql_similarity:.3f}")
        elif expected_result['success'] and generated_result['success']:
            print(f"    ❌ Dados diferentes | SQL similarity: {sql_similarity:.3f}")
            # Mostrar os primeiros resultados para debug
            exp_preview = str(expected_result['data'][:3]) if expected_result['data'] else "[]"
            gen_preview = str(generated_result['data'][:3]) if generated_result['data'] else "[]"
            print(f"      Expected: {exp_preview}...")
            print(f"      Generated: {gen_preview}...")
        else:
            print(f"    ❌ Erro de execução | SQL similarity: {sql_similarity:.3f}")
        
        # Criar avaliação
        evaluation = QueryEvaluation(
            query_id=query_id,
            model_name=model_result["model_name"],
            model_key=model_result["model_key"],
            difficulty=model_result["difficulty"],
            question=model_result["question"],
            expected_query=expected_query,
            generated_query=generated_query,
            
            # Resultados de execução
            expected_success=expected_result['success'],
            generated_success=generated_result['success'],
            expected_error=expected_result['error'],
            generated_error=generated_result['error'],
            expected_rows=expected_result['row_count'],
            generated_rows=generated_result['row_count'],
            expected_columns=expected_result['columns'],
            generated_columns=generated_result['columns'],
            expected_data=expected_result['data'],
            generated_data=generated_result['data'],
            
            # Resultados de comparação
            exact_match=comparison['exact_match'],
            structure_match=comparison['structure_match'],
            sql_similarity=sql_similarity,
            semantic_equivalence=comparison['semantic_equivalence'],
            
            # Metadados
            execution_time=model_result.get("execution_time", 0.0)
        )
        
        return evaluation
    
    def evaluate_model_results(self, model_results: List[Dict[str, Any]], model_key: str) -> List[QueryEvaluation]:
        """
        Avalia resultados de um modelo específico
        
        Args:
            model_results: Lista de resultados do modelo
            model_key: Chave do modelo
            
        Returns:
            Lista de avaliações
        """
        if not model_results:
            return []
        
        model_name = model_results[0]["model_name"]
        print(f"\n🔍 Avaliando modelo: {model_name}")
        print("=" * 60)
        
        model_evaluations = []
        for result in model_results:
            if result["model_key"] == model_key:
                evaluation = self.evaluate_single_query(result)
                model_evaluations.append(evaluation)
        
        # Estatísticas do modelo
        total = len(model_evaluations)
        exact_matches = sum(1 for e in model_evaluations if e.exact_match)
        semantic_matches = sum(1 for e in model_evaluations if e.semantic_equivalence)
        execution_successes = sum(1 for e in model_evaluations if e.expected_success and e.generated_success)
        
        print(f"\n📊 Resumo {model_name}:")
        print(f"  Queries avaliadas: {total}")
        print(f"  Matches exatos: {exact_matches} ({exact_matches/total*100:.1f}%)")
        print(f"  Equivalência semântica: {semantic_matches} ({semantic_matches/total*100:.1f}%)")
        print(f"  Execuções bem-sucedidas: {execution_successes} ({execution_successes/total*100:.1f}%)")
        
        return model_evaluations
    
    def evaluate_all_results(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Avalia todos os resultados de modelos
        
        Args:
            input_data: Dados de entrada do model_runner
            
        Returns:
            Dados de avaliação consolidados
        """
        print(f"🎯 Iniciando avaliação de queries")
        print(f"📋 Modelos testados: {len(input_data['model_summaries'])}")
        print("=" * 80)
        
        all_evaluations = []
        model_summaries = {}
        
        # Agrupar resultados por modelo
        results_by_model = {}
        for result in input_data["detailed_results"]:
            model_key = result["model_key"]
            if model_key not in results_by_model:
                results_by_model[model_key] = []
            results_by_model[model_key].append(result)
        
        # Avaliar cada modelo
        for model_key, model_results in results_by_model.items():
            model_evaluations = self.evaluate_model_results(model_results, model_key)
            all_evaluations.extend(model_evaluations)
            
            if model_evaluations:
                # Calcular métricas agregadas
                total = len(model_evaluations)
                exact_matches = sum(1 for e in model_evaluations if e.exact_match)
                semantic_matches = sum(1 for e in model_evaluations if e.semantic_equivalence)
                structure_matches = sum(1 for e in model_evaluations if e.structure_match)
                execution_successes = sum(1 for e in model_evaluations if e.expected_success and e.generated_success)
                avg_sql_similarity = sum(e.sql_similarity for e in model_evaluations) / total
                
                # Métricas por dificuldade
                metrics_by_difficulty = {}
                for difficulty in ["easy", "medium", "hard"]:
                    diff_evals = [e for e in model_evaluations if e.difficulty == difficulty]
                    if diff_evals:
                        diff_total = len(diff_evals)
                        diff_exact = sum(1 for e in diff_evals if e.exact_match)
                        diff_semantic = sum(1 for e in diff_evals if e.semantic_equivalence)
                        diff_sql_sim = sum(e.sql_similarity for e in diff_evals) / diff_total
                        
                        metrics_by_difficulty[difficulty] = {
                            "total": diff_total,
                            "exact_matches": diff_exact,
                            "exact_match_rate": diff_exact / diff_total,
                            "semantic_matches": diff_semantic,
                            "semantic_match_rate": diff_semantic / diff_total,
                            "avg_sql_similarity": diff_sql_sim
                        }
                
                model_summaries[model_key] = {
                    "model_name": model_evaluations[0].model_name,
                    "total_queries": total,
                    "exact_matches": exact_matches,
                    "exact_match_rate": exact_matches / total,
                    "semantic_matches": semantic_matches,
                    "semantic_match_rate": semantic_matches / total,
                    "structure_matches": structure_matches,
                    "structure_match_rate": structure_matches / total,
                    "execution_successes": execution_successes,
                    "execution_success_rate": execution_successes / total,
                    "avg_sql_similarity": avg_sql_similarity,
                    "metrics_by_difficulty": metrics_by_difficulty
                }
        
        # Preparar output
        output_data = {
            "metadata": {
                "timestamp": self.config.timestamp,
                "input_file": input_data.get("metadata", {}).get("timestamp", "unknown"),
                "database_path": self.config.database_path,
                "models_evaluated": list(results_by_model.keys()),
                "total_evaluations": len(all_evaluations)
            },
            "evaluation_summaries": model_summaries,
            "detailed_evaluations": [
                {
                    "query_id": e.query_id,
                    "model_name": e.model_name,
                    "model_key": e.model_key,
                    "difficulty": e.difficulty,
                    "question": e.question,
                    "expected_query": e.expected_query,
                    "generated_query": e.generated_query,
                    "expected_success": e.expected_success,
                    "generated_success": e.generated_success,
                    "expected_error": e.expected_error,
                    "generated_error": e.generated_error,
                    "expected_rows": e.expected_rows,
                    "generated_rows": e.generated_rows,
                    "expected_columns": e.expected_columns,
                    "generated_columns": e.generated_columns,
                    "expected_data": e.expected_data,
                    "generated_data": e.generated_data,
                    "exact_match": e.exact_match,
                    "structure_match": e.structure_match,
                    "sql_similarity": e.sql_similarity,
                    "semantic_equivalence": e.semantic_equivalence,
                    "execution_time": e.execution_time
                }
                for e in all_evaluations
            ]
        }
        
        return output_data
    
    def save_results(self, results: Dict[str, Any], input_timestamp: str) -> str:
        """
        Salva resultados de avaliação
        
        Args:
            results: Resultados para salvar
            input_timestamp: Timestamp do arquivo de entrada
            
        Returns:
            Caminho do arquivo salvo
        """
        filename = f"evaluation_results_{input_timestamp}.json"
        file_path = self.file_manager.save_json(results, filename)
        
        print(f"\n✅ Avaliação salva em: {file_path}")
        return file_path
    
    def print_summary(self, results: Dict[str, Any]):
        """Imprime resumo da avaliação"""
        print("\n" + "=" * 80)
        print("📊 RESUMO DA AVALIAÇÃO DE QUERIES")
        print("=" * 80)
        
        summaries = results["evaluation_summaries"]
        
        if not summaries:
            print("Nenhum resultado disponível.")
            return
        
        # Ranking por equivalência semântica
        print("\n🏆 RANKING POR EQUIVALÊNCIA SEMÂNTICA:")
        print("-" * 50)
        
        by_semantic = sorted(summaries.items(),
                           key=lambda x: x[1]["semantic_match_rate"],
                           reverse=True)
        
        for i, (model_key, summary) in enumerate(by_semantic, 1):
            rate = summary["semantic_match_rate"]
            count = summary["semantic_matches"]
            total = summary["total_queries"]
            print(f"{i}. {summary['model_name']}: {rate:.1%} ({count}/{total})")
        
        # Ranking por matches exatos
        print("\n🎯 RANKING POR MATCHES EXATOS:")
        print("-" * 50)
        
        by_exact = sorted(summaries.items(),
                         key=lambda x: x[1]["exact_match_rate"],
                         reverse=True)
        
        for i, (model_key, summary) in enumerate(by_exact, 1):
            rate = summary["exact_match_rate"]
            count = summary["exact_matches"]
            total = summary["total_queries"]
            print(f"{i}. {summary['model_name']}: {rate:.1%} ({count}/{total})")
        
        # Ranking por similaridade SQL
        print("\n📝 RANKING POR SIMILARIDADE SQL:")
        print("-" * 50)
        
        by_similarity = sorted(summaries.items(),
                             key=lambda x: x[1]["avg_sql_similarity"],
                             reverse=True)
        
        for i, (model_key, summary) in enumerate(by_similarity, 1):
            similarity = summary["avg_sql_similarity"]
            print(f"{i}. {summary['model_name']}: {similarity:.3f}")


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description="Avaliador de queries SQL")
    parser.add_argument("--input", required=True,
                       help="Arquivo de resultados do model_runner")
    parser.add_argument("--database", default="../sus_database.db",
                       help="Caminho para o banco de dados")
    parser.add_argument("--output-dir", default="results",
                       help="Diretório de saída")
    
    args = parser.parse_args()
    
    try:
        # Carregar dados de entrada
        file_manager = FileManager(args.output_dir)
        input_data = file_manager.load_json(args.input)
        
        # Extrair timestamp do arquivo de entrada
        input_timestamp = input_data.get("metadata", {}).get("timestamp", "unknown")
        
        # Criar configuração
        config = EvaluationConfig(
            database_path=args.database,
            output_dir=args.output_dir,
            timestamp=input_timestamp  # Usar mesmo timestamp
        )
        
        # Executar avaliação
        evaluator = QueryEvaluator(config)
        results = evaluator.evaluate_all_results(input_data)
        
        # Salvar e mostrar resultados
        evaluator.save_results(results, input_timestamp)
        evaluator.print_summary(results)
        
        print(f"\n🎉 Avaliação concluída! Use o próximo script para análise detalhada:")
        print(f"python analysis_reporter.py --input evaluation_results_{input_timestamp}.json")
        
    except FileNotFoundError as e:
        print(f"❌ Arquivo não encontrado: {e}")
        sys.exit(1)
    except ConnectionError as e:
        print(f"❌ Erro de conexão: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Erro durante avaliação: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()