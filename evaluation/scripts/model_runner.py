#!/usr/bin/env python3
"""
Model Runner - Executa modelos LLM e extrai queries SQL

Este script é responsável APENAS por executar os modelos LLM contra o ground truth
e extrair as queries SQL geradas. Não executa queries no banco nem faz análises.

Autor: Claude Code Assistant
Data: 2025-07-13
"""

import sys
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

from utils import EvaluationConfig, ModelResult, FileManager, ConfigManager
from src.application.config.simple_config import ApplicationConfig, OrchestratorConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator


class ModelRunner:
    """Executor de modelos LLM para extração de queries"""
    
    def __init__(self, config: EvaluationConfig):
        """
        Inicializa o runner
        
        Args:
            config: Configuração de avaliação
        """
        self.config = config
        self.file_manager = FileManager(config.output_dir)
        self.ground_truth = ConfigManager.load_ground_truth(config.ground_truth_file)
        self.available_models = ConfigManager.get_available_models()
        self.results = []
        
        print(f"📋 Ground truth carregado: {len(self.ground_truth)} queries")
        print(f"📂 Diretório de saída: {config.output_dir}")
    
    def create_model_config(self, model_key: str) -> ApplicationConfig:
        """
        Cria configuração para um modelo específico
        
        Args:
            model_key: Chave do modelo
            
        Returns:
            ApplicationConfig configurada
        """
        if model_key not in self.available_models:
            raise ValueError(f"Modelo '{model_key}' não encontrado")
        
        model_info = self.available_models[model_key]
        
        return ApplicationConfig(
            database_path=self.config.database_path,
            llm_provider=model_info["provider"],
            llm_model=model_info["model"],
            llm_temperature=0.0,
            llm_timeout=120
        )
    
    def test_single_query(self, orchestrator: Text2SQLOrchestrator, 
                         query_data: Dict[str, Any], model_info: Dict[str, str]) -> ModelResult:
        """
        Testa uma única query com um modelo
        
        Args:
            orchestrator: Orquestrador configurado
            query_data: Dados da query do ground truth
            model_info: Informações do modelo
            
        Returns:
            Resultado do teste
        """
        question = query_data["question"]
        expected_query = query_data["query"]
        
        print(f"  🔍 {query_data['id']}: {question}")
        
        start_time = time.time()
        
        try:
            # Processar pergunta para gerar SQL
            result = orchestrator.process_single_query(question)
            execution_time = time.time() - start_time
            
            # Verificar se query foi gerada
            query_generated = result.sql_query is not None and result.sql_query.strip() != ""
            generated_query = result.sql_query.strip() if result.sql_query else ""
            error_message = result.error_message if not result.success else ""
            
            print(f"    ✅ SQL gerada: {query_generated} | Tempo: {execution_time:.2f}s")
            if not query_generated:
                print(f"    ❌ Erro: {error_message}")
            
            return ModelResult(
                model_name=model_info["name"],
                model_key=model_info["key"],
                provider=model_info["provider"],
                query_id=query_data["id"],
                question=question,
                difficulty=query_data["difficulty"],
                expected_query=expected_query,
                generated_query=generated_query,
                execution_time=execution_time,
                success=result.success and query_generated,
                error_message=error_message,
                query_generated=query_generated
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)
            
            print(f"    ❌ Erro na geração: {error_msg}")
            
            return ModelResult(
                model_name=model_info["name"],
                model_key=model_info["key"],
                provider=model_info["provider"],
                query_id=query_data["id"],
                question=question,
                difficulty=query_data["difficulty"],
                expected_query=expected_query,
                generated_query="",
                execution_time=execution_time,
                success=False,
                error_message=error_msg,
                query_generated=False
            )
    
    def run_model(self, model_key: str) -> List[ModelResult]:
        """
        Executa um modelo específico contra todas as queries
        
        Args:
            model_key: Chave do modelo
            
        Returns:
            Lista de resultados
        """
        if model_key not in self.available_models:
            print(f"❌ Modelo '{model_key}' não encontrado")
            return []
        
        model_info = self.available_models[model_key].copy()
        model_info["key"] = model_key
        
        print(f"\n🚀 Testando modelo: {model_info['name']} ({model_info['provider']})")
        print("=" * 60)
        
        # Criar configuração e orquestrador
        try:
            config = self.create_model_config(model_key)
            orchestrator_config = OrchestratorConfig()
            orchestrator = Text2SQLOrchestrator(config, orchestrator_config)
            
            print(f"✅ Orquestrador inicializado para {model_info['name']}")
            
        except Exception as e:
            print(f"❌ Erro ao inicializar orquestrador: {e}")
            return []
        
        # Testar cada query
        model_results = []
        for query_data in self.ground_truth:
            result = self.test_single_query(orchestrator, query_data, model_info)
            model_results.append(result)
        
        # Calcular estatísticas básicas
        total = len(model_results)
        successful = sum(1 for r in model_results if r.success)
        generated = sum(1 for r in model_results if r.query_generated)
        avg_time = sum(r.execution_time for r in model_results) / total if total > 0 else 0
        
        print(f"\n📊 Resumo {model_info['name']}:")
        print(f"  Queries processadas: {total}")
        print(f"  SQL gerada: {generated} ({generated/total*100:.1f}%)")
        print(f"  Sucessos: {successful} ({successful/total*100:.1f}%)")
        print(f"  Tempo médio: {avg_time:.2f}s")
        
        return model_results
    
    def run_benchmark(self, models: List[str]) -> Dict[str, Any]:
        """
        Executa benchmark para múltiplos modelos
        
        Args:
            models: Lista de modelos para testar
            
        Returns:
            Resultados consolidados
        """
        print(f"🎯 Iniciando benchmark de modelos: {', '.join(models)}")
        print(f"📋 Total de queries: {len(self.ground_truth)}")
        print("=" * 80)
        
        all_results = []
        model_summaries = {}
        
        for model_key in models:
            model_results = self.run_model(model_key)
            all_results.extend(model_results)
            
            if model_results:
                # Calcular métricas do modelo
                total = len(model_results)
                successful = sum(1 for r in model_results if r.success)
                generated = sum(1 for r in model_results if r.query_generated)
                avg_time = sum(r.execution_time for r in model_results) / total
                
                model_summaries[model_key] = {
                    "model_name": model_results[0].model_name,
                    "provider": model_results[0].provider,
                    "total_queries": total,
                    "queries_generated": generated,
                    "generation_rate": generated / total,
                    "successful_queries": successful,
                    "success_rate": successful / total,
                    "avg_execution_time": avg_time
                }
        
        # Preparar output
        output_data = {
            "metadata": {
                "timestamp": self.config.timestamp,
                "ground_truth_file": self.config.ground_truth_file,
                "models_tested": models,
                "total_queries": len(self.ground_truth)
            },
            "model_summaries": model_summaries,
            "detailed_results": [
                {
                    "model_key": r.model_key,
                    "model_name": r.model_name,
                    "provider": r.provider,
                    "query_id": r.query_id,
                    "question": r.question,
                    "difficulty": r.difficulty,
                    "expected_query": r.expected_query,
                    "generated_query": r.generated_query,
                    "execution_time": r.execution_time,
                    "success": r.success,
                    "error_message": r.error_message,
                    "query_generated": r.query_generated
                }
                for r in all_results
            ]
        }
        
        return output_data
    
    def save_results(self, results: Dict[str, Any]) -> str:
        """
        Salva resultados em arquivo JSON
        
        Args:
            results: Resultados para salvar
            
        Returns:
            Caminho do arquivo salvo
        """
        filename = f"model_results_{self.config.timestamp}.json"
        file_path = self.file_manager.save_json(results, filename)
        
        print(f"\n✅ Resultados salvos em: {file_path}")
        return file_path
    
    def print_summary(self, results: Dict[str, Any]):
        """Imprime resumo dos resultados"""
        print("\n" + "=" * 80)
        print("📊 RESUMO DO BENCHMARK DE MODELOS")
        print("=" * 80)
        
        summaries = results["model_summaries"]
        
        if not summaries:
            print("Nenhum resultado disponível.")
            return
        
        # Ranking por taxa de geração
        print("\n🏆 RANKING POR TAXA DE GERAÇÃO DE SQL:")
        print("-" * 50)
        
        by_generation = sorted(summaries.items(), 
                             key=lambda x: x[1]["generation_rate"], 
                             reverse=True)
        
        for i, (model_key, summary) in enumerate(by_generation, 1):
            rate = summary["generation_rate"]
            count = summary["queries_generated"]
            total = summary["total_queries"]
            print(f"{i}. {summary['model_name']}: {rate:.1%} ({count}/{total})")
        
        # Ranking por taxa de sucesso
        print("\n🎯 RANKING POR TAXA DE SUCESSO:")
        print("-" * 50)
        
        by_success = sorted(summaries.items(),
                          key=lambda x: x[1]["success_rate"],
                          reverse=True)
        
        for i, (model_key, summary) in enumerate(by_success, 1):
            rate = summary["success_rate"]
            count = summary["successful_queries"]
            total = summary["total_queries"]
            print(f"{i}. {summary['model_name']}: {rate:.1%} ({count}/{total})")
        
        # Ranking por velocidade
        print("\n⚡ RANKING POR VELOCIDADE:")
        print("-" * 50)
        
        by_speed = sorted(summaries.items(),
                         key=lambda x: x[1]["avg_execution_time"])
        
        for i, (model_key, summary) in enumerate(by_speed, 1):
            time_avg = summary["avg_execution_time"]
            print(f"{i}. {summary['model_name']}: {time_avg:.2f}s")


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description="Executor de modelos LLM para Text2SQL")
    parser.add_argument("--models", nargs="+", 
                       help="Modelos para testar (ex: ollama_llama3 mistral)")
    parser.add_argument("--ground-truth", default="ground_truth_improved.json",
                       help="Arquivo ground truth")
    parser.add_argument("--output-dir", default="results",
                       help="Diretório de saída")
    parser.add_argument("--list-models", action="store_true",
                       help="Listar modelos disponíveis")
    parser.add_argument("--all-models", action="store_true",
                       help="Executar todos os modelos disponíveis")
    
    args = parser.parse_args()
    
    # Listar modelos disponíveis
    if args.list_models:
        available = ConfigManager.get_available_models()
        print("🤖 Modelos disponíveis:")
        for key, info in available.items():
            print(f"  {key}: {info['name']} ({info['provider']})")
        return
    
    # Validar argumentos
    if args.all_models:
        # Usar todos os modelos disponíveis
        available = ConfigManager.get_available_models()
        args.models = list(available.keys())
        print(f"🚀 Executando todos os modelos disponíveis: {', '.join(args.models)}")
    elif not args.models:
        print("❌ Especifique pelo menos um modelo com --models ou use --all-models")
        print("Use --list-models para ver modelos disponíveis")
        sys.exit(1)
    
    # Validar modelos
    valid_models = ConfigManager.validate_models(args.models)
    if not valid_models:
        print("❌ Nenhum modelo válido especificado")
        sys.exit(1)
    
    # Criar configuração
    config = EvaluationConfig(
        ground_truth_file=args.ground_truth,
        output_dir=args.output_dir
    )
    
    try:
        # Executar benchmark
        runner = ModelRunner(config)
        results = runner.run_benchmark(valid_models)
        
        # Salvar e mostrar resultados
        runner.save_results(results)
        runner.print_summary(results)
        
        print(f"\n🎉 Benchmark concluído! Use o próximo script para avaliar as queries:")
        print(f"python query_evaluator.py --input model_results_{config.timestamp}.json")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Benchmark interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro durante execução: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()