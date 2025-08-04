#!/usr/bin/env python3
"""
Pipeline Simplificado de Avaliação

Este script executa todo o pipeline de avaliação em um único comando:
1. Executa modelos LLM (model_runner.py)
2. Avalia queries no banco (query_evaluator.py)  
3. Gera análise consolidada (analysis_reporter.py)
4. Produz um resultado final unificado

Uso: python run_evaluation.py --models ollama_llama3 mistral
"""

import sys
import json
import time
import argparse
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent))

from utils import FileManager


class EvaluationPipeline:
    """Pipeline simplificado de avaliação"""
    
    def __init__(self, output_dir: str = "results"):
        """Inicializa pipeline"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.file_manager = FileManager(str(self.output_dir))
        self.timestamp = time.strftime("%Y%m%d_%H%M%S")
        
    def run_stage(self, script_name: str, args: List[str]) -> Optional[str]:
        """
        Executa um estágio do pipeline
        
        Args:
            script_name: Nome do script Python
            args: Argumentos para o script
            
        Returns:
            Caminho do arquivo de saída ou None se falhou
        """
        print(f"\n{'='*60}")
        print(f"🚀 Executando: {script_name}")
        print(f"{'='*60}")
        
        try:
            # Montar comando
            cmd = [sys.executable, script_name] + args
            
            # Executar com output em tempo real
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=Path(__file__).parent,
                bufsize=1,
                universal_newlines=True
            )
            
            output_lines = []
            
            # Ler e mostrar output em tempo real
            while True:
                line = process.stdout.readline()
                if not line:
                    break
                print(line.rstrip())  # Mostrar em tempo real
                output_lines.append(line.rstrip())
            
            # Aguardar processo terminar
            return_code = process.wait()
            
            if return_code != 0:
                print(f"❌ Falha em {script_name} (código: {return_code})")
                return None
                
            print(f"✅ {script_name} concluído com sucesso")
            
            # Extrair arquivo de saída do output
            for line in reversed(output_lines):
                if '.json' in line and ('results' in line or 'evaluation' in line or 'analysis' in line):
                    # Extrair caminho do arquivo
                    if 'Resultados salvos em:' in line:
                        return line.split(': ')[-1].strip()
                    elif 'Análise salva em:' in line:
                        return line.split(': ')[-1].strip()
                        
            return None
            
        except Exception as e:
            print(f"❌ Erro executando {script_name}: {e}")
            return None
    
    def consolidate_results(self, model_results_file: str, 
                          evaluation_results_file: str, 
                          analysis_results_file: str) -> Dict[str, Any]:
        """
        Consolida todos os resultados em uma estrutura unificada
        
        Args:
            model_results_file: Arquivo com resultados dos modelos
            evaluation_results_file: Arquivo com avaliação de queries
            analysis_results_file: Arquivo com análise estatística
            
        Returns:
            Resultados consolidados
        """
        print(f"\n{'='*60}")
        print("📊 Consolidando resultados...")
        print(f"{'='*60}")
        
        try:
            # Carregar arquivos
            with open(model_results_file, 'r', encoding='utf-8') as f:
                model_data = json.load(f)
                
            with open(evaluation_results_file, 'r', encoding='utf-8') as f:
                eval_data = json.load(f)
                
            with open(analysis_results_file, 'r', encoding='utf-8') as f:
                analysis_data = json.load(f)
            
            # Extrair métricas principais por modelo
            model_performance = {}
            
            for model_key, summary in model_data.get("model_summaries", {}).items():
                # Buscar métricas de avaliação para este modelo
                eval_metrics = {}
                for eval_result in eval_data.get("evaluations", []):
                    if eval_result.get("model_key") == model_key:
                        metrics = eval_result.get("equivalence_metrics", {})
                        eval_metrics = {
                            "exact_match_rate": metrics.get("exact_match", 0),
                            "semantic_match_rate": metrics.get("semantic_equivalence", 0),
                            "functional_match_rate": metrics.get("functional_equivalence", 0),
                            "data_match_rate": metrics.get("data_equivalence", 0)
                        }
                        break
                
                model_performance[model_key] = {
                    "model_name": summary.get("model_name"),
                    "provider": summary.get("provider"),
                    "queries_generated": summary.get("queries_generated", 0),
                    "generation_rate": summary.get("generation_rate", 0),
                    "success_rate": summary.get("success_rate", 0),
                    "avg_execution_time": summary.get("avg_execution_time", 0),
                    **eval_metrics
                }
            
            # Organizar resultados detalhados por query
            detailed_results = []
            
            # Agrupar por query_id
            queries_dict = {}
            
            # Adicionar resultados dos modelos
            for result in model_data.get("detailed_results", []):
                query_id = result["query_id"]
                if query_id not in queries_dict:
                    queries_dict[query_id] = {
                        "query_id": query_id,
                        "question": result["question"],
                        "difficulty": result["difficulty"],
                        "expected_query": result["expected_query"],
                        "model_results": {}
                    }
                
                queries_dict[query_id]["model_results"][result["model_key"]] = {
                    "generated_query": result["generated_query"],
                    "execution_time": result["execution_time"],
                    "success": result["success"],
                    "query_generated": result["query_generated"],
                    "error_message": result.get("error_message", "")
                }
            
            # Adicionar métricas de equivalência
            for eval_result in eval_data.get("evaluations", []):
                for query_eval in eval_result.get("query_results", []):
                    query_id = query_eval["query_id"]
                    model_key = eval_result["model_key"]
                    
                    if query_id in queries_dict and model_key in queries_dict[query_id]["model_results"]:
                        queries_dict[query_id]["model_results"][model_key].update({
                            "result_match": query_eval.get("equivalence_metrics", {}).get("data_equivalence", False),
                            "exact_match": query_eval.get("equivalence_metrics", {}).get("exact_match", False),
                            "semantic_match": query_eval.get("equivalence_metrics", {}).get("semantic_equivalence", False),
                            "functional_match": query_eval.get("equivalence_metrics", {}).get("functional_equivalence", False),
                            "expected_rows": query_eval.get("expected_result", {}).get("row_count", 0),
                            "generated_rows": query_eval.get("generated_result", {}).get("row_count", 0)
                        })
            
            detailed_results = list(queries_dict.values())
            
            # Calcular métricas globais
            total_queries = len(detailed_results)
            models_tested = list(model_performance.keys())
            
            # Encontrar melhor modelo (por data_match_rate)
            best_model = None
            best_score = 0
            for model_key, perf in model_performance.items():
                score = perf.get("data_match_rate", 0)
                if score > best_score:
                    best_score = score
                    best_model = model_key
            
            # Estrutura consolidada
            consolidated = {
                "metadata": {
                    "timestamp": self.timestamp,
                    "pipeline_version": "1.0",
                    "total_queries": total_queries,
                    "models_tested": models_tested,
                    "evaluation_date": time.strftime("%Y-%m-%d %H:%M:%S")
                },
                "model_performance": model_performance,
                "detailed_results": detailed_results,
                "summary": {
                    "best_model": best_model,
                    "best_model_name": model_performance.get(best_model, {}).get("model_name", "N/A") if best_model else "N/A",
                    "best_score": best_score,
                    "total_queries": total_queries,
                    "models_count": len(models_tested),
                    "avg_generation_rate": sum(p.get("generation_rate", 0) for p in model_performance.values()) / len(model_performance) if model_performance else 0,
                    "avg_success_rate": sum(p.get("success_rate", 0) for p in model_performance.values()) / len(model_performance) if model_performance else 0
                },
                "original_files": {
                    "model_results": model_results_file,
                    "evaluation_results": evaluation_results_file,
                    "analysis_results": analysis_results_file
                }
            }
            
            print(f"✅ Resultados consolidados:")
            print(f"  📊 {total_queries} queries avaliadas")
            print(f"  🤖 {len(models_tested)} modelos testados")
            print(f"  🏆 Melhor modelo: {consolidated['summary']['best_model_name']} ({best_score:.1%})")
            
            return consolidated
            
        except Exception as e:
            print(f"❌ Erro consolidando resultados: {e}")
            raise
    
    def save_consolidated_results(self, consolidated_data: Dict[str, Any]) -> str:
        """Salva resultados consolidados"""
        filename = f"evaluation_summary_{self.timestamp}.json"
        file_path = self.file_manager.save_json(consolidated_data, filename)
        
        print(f"\n✅ Resultado consolidado salvo em: {file_path}")
        return file_path
    
    def print_summary(self, consolidated_data: Dict[str, Any]):
        """Imprime resumo final"""
        print(f"\n{'='*80}")
        print("🎯 RESUMO FINAL DA AVALIAÇÃO")
        print(f"{'='*80}")
        
        summary = consolidated_data["summary"]
        performance = consolidated_data["model_performance"]
        
        print(f"\n📊 Estatísticas Gerais:")
        print(f"  Queries avaliadas: {summary['total_queries']}")
        print(f"  Modelos testados: {summary['models_count']}")
        print(f"  Taxa média de geração: {summary['avg_generation_rate']:.1%}")
        print(f"  Taxa média de sucesso: {summary['avg_success_rate']:.1%}")
        
        print(f"\n🏆 Ranking dos Modelos (por precisão):")
        print("-" * 50)
        
        # Ordenar por data_match_rate
        sorted_models = sorted(
            performance.items(),
            key=lambda x: x[1].get("data_match_rate", 0),
            reverse=True
        )
        
        for i, (model_key, perf) in enumerate(sorted_models, 1):
            name = perf.get("model_name", model_key)
            rate = perf.get("data_match_rate", 0)
            generated = perf.get("queries_generated", 0)
            total = summary["total_queries"]
            time_avg = perf.get("avg_execution_time", 0)
            
            print(f"{i}. {name}")
            print(f"   Precisão: {rate:.1%} | Geradas: {generated}/{total} | Tempo: {time_avg:.1f}s")
        
        print(f"\n🎉 Avaliação completa! Melhor modelo: {summary['best_model_name']}")
    
    def run_full_pipeline(self, models: List[str], ground_truth: str = "ground_truth_improved.json") -> str:
        """
        Executa pipeline completo
        
        Args:
            models: Lista de modelos para testar
            ground_truth: Arquivo ground truth
            
        Returns:
            Caminho do arquivo de resultado consolidado
        """
        print(f"🚀 Iniciando pipeline completo de avaliação")
        print(f"📋 Modelos: {', '.join(models)}")
        print(f"📄 Ground truth: {ground_truth}")
        
        # Estágio 1: Executar modelos
        model_args = ["--models"] + models + ["--ground-truth", ground_truth, "--output-dir", str(self.output_dir)]
        model_results_file = self.run_stage("scripts/model_runner.py", model_args)
        
        if not model_results_file:
            raise RuntimeError("Falha no estágio 1 (model_runner)")
        
        # Estágio 2: Avaliar queries
        eval_args = ["--input", model_results_file, "--output-dir", str(self.output_dir)]
        evaluation_results_file = self.run_stage("scripts/query_evaluator.py", eval_args)
        
        if not evaluation_results_file:
            raise RuntimeError("Falha no estágio 2 (query_evaluator)")
        
        # Estágio 3: Gerar análise
        analysis_args = ["--input", evaluation_results_file, "--output-dir", str(self.output_dir)]
        analysis_results_file = self.run_stage("scripts/analysis_reporter.py", analysis_args)
        
        if not analysis_results_file:
            raise RuntimeError("Falha no estágio 3 (analysis_reporter)")
        
        # Consolidar resultados
        consolidated_data = self.consolidate_results(
            model_results_file, 
            evaluation_results_file, 
            analysis_results_file
        )
        
        # Salvar resultado final
        final_file = self.save_consolidated_results(consolidated_data)
        
        # Mostrar resumo
        self.print_summary(consolidated_data)
        
        return final_file


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description="Pipeline simplificado de avaliação Text2SQL")
    parser.add_argument("--models", nargs="+", required=True,
                       help="Modelos para testar (ex: ollama_llama3 mistral qwen3)  # python run_evaluation.py --models ollama_llama3 mistral")
    parser.add_argument("--ground-truth", default="ground_truth_improved.json",
                       help="Arquivo ground truth  # python run_evaluation.py --models ollama_llama3 --ground-truth custom_queries.json")
    parser.add_argument("--output-dir", default="results",
                       help="Diretório de saída  # python run_evaluation.py --models ollama_llama3 --output-dir my_results")
    
    args = parser.parse_args()
    
    try:
        pipeline = EvaluationPipeline(args.output_dir)
        result_file = pipeline.run_full_pipeline(args.models, args.ground_truth)
        
        print(f"\n🎉 Pipeline concluído com sucesso!")
        print(f"📄 Resultado final: {result_file}")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro durante execução do pipeline: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()