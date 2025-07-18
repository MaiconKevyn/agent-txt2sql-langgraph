#!/usr/bin/env python3
"""
Ground Truth Query Validator
Valida todas as queries do ground truth melhorado
"""

import json
import sqlite3
import pandas as pd
import time
from typing import Dict, List, Tuple
from pathlib import Path

class GroundTruthValidator:
    def __init__(self, database_path: str = "sus_database.db"):
        self.database_path = database_path
        self.connection = None
        self.validation_results = []
        
    def connect_database(self) -> bool:
        """Conecta ao banco de dados"""
        try:
            self.connection = sqlite3.connect(self.database_path)
            print(f"✅ Conectado ao banco: {self.database_path}")
            return True
        except Exception as e:
            print(f"❌ Erro ao conectar ao banco: {e}")
            return False
    
    def disconnect_database(self):
        """Desconecta do banco de dados"""
        if self.connection:
            self.connection.close()
    
    def validate_query(self, query_id: str, query: str) -> Dict:
        """Valida uma query individual"""
        result = {
            "id": query_id,
            "query": query,
            "valid": False,
            "execution_time": 0,
            "row_count": 0,
            "error": None,
            "sample_result": None
        }
        
        try:
            start_time = time.time()
            df = pd.read_sql_query(query, self.connection)
            execution_time = time.time() - start_time
            
            result.update({
                "valid": True,
                "execution_time": round(execution_time, 3),
                "row_count": len(df),
                "sample_result": df.head(3).to_dict('records') if not df.empty else []
            })
            
            print(f"✅ {query_id}: OK ({execution_time:.3f}s, {len(df)} rows)")
            
        except Exception as e:
            result["error"] = str(e)
            print(f"❌ {query_id}: ERRO - {str(e)}")
        
        return result
    
    def load_ground_truth(self, file_path: str) -> List[Dict]:
        """Carrega o arquivo de ground truth"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Erro ao carregar ground truth: {e}")
            return []
    
    def validate_all_queries(self, ground_truth_path: str) -> Dict:
        """Valida todas as queries do ground truth"""
        print("🔍 Iniciando validação do Ground Truth...")
        print("=" * 60)
        
        if not self.connect_database():
            return {}
        
        # Carregar ground truth
        ground_truth = self.load_ground_truth(ground_truth_path)
        if not ground_truth:
            return {}
        
        print(f"📋 Total de queries para validar: {len(ground_truth)}")
        print("-" * 60)
        
        # Validar cada query
        valid_queries = 0
        invalid_queries = 0
        total_time = 0
        
        for item in ground_truth:
            query_id = item.get("id", "unknown")
            query = item.get("query", "")
            difficulty = item.get("difficulty", "unknown")
            category = item.get("category", "unknown")
            
            print(f"📝 Validando {query_id} ({difficulty}, {category})")
            
            result = self.validate_query(query_id, query)
            result.update({
                "difficulty": difficulty,
                "category": category,
                "question": item.get("question", "")
            })
            
            self.validation_results.append(result)
            
            if result["valid"]:
                valid_queries += 1
                total_time += result["execution_time"]
            else:
                invalid_queries += 1
        
        # Estatísticas finais
        summary = {
            "total_queries": len(ground_truth),
            "valid_queries": valid_queries,
            "invalid_queries": invalid_queries,
            "success_rate": round((valid_queries / len(ground_truth)) * 100, 2),
            "total_execution_time": round(total_time, 3),
            "average_execution_time": round(total_time / valid_queries, 3) if valid_queries > 0 else 0
        }
        
        self.print_summary(summary)
        self.disconnect_database()
        
        return {
            "summary": summary,
            "results": self.validation_results
        }
    
    def print_summary(self, summary: Dict):
        """Imprime resumo da validação"""
        print("\n" + "=" * 60)
        print("📊 RESUMO DA VALIDAÇÃO")
        print("=" * 60)
        print(f"📋 Total de queries: {summary['total_queries']}")
        print(f"✅ Queries válidas: {summary['valid_queries']}")
        print(f"❌ Queries inválidas: {summary['invalid_queries']}")
        print(f"📈 Taxa de sucesso: {summary['success_rate']}%")
        print(f"⏱️ Tempo total de execução: {summary['total_execution_time']}s")
        print(f"⏱️ Tempo médio por query: {summary['average_execution_time']}s")
        
        # Estatísticas por dificuldade
        difficulty_stats = {}
        category_stats = {}
        
        for result in self.validation_results:
            difficulty = result["difficulty"]
            category = result["category"]
            
            if difficulty not in difficulty_stats:
                difficulty_stats[difficulty] = {"total": 0, "valid": 0}
            difficulty_stats[difficulty]["total"] += 1
            if result["valid"]:
                difficulty_stats[difficulty]["valid"] += 1
                
            if category not in category_stats:
                category_stats[category] = {"total": 0, "valid": 0}
            category_stats[category]["total"] += 1
            if result["valid"]:
                category_stats[category]["valid"] += 1
        
        print("\n📊 ESTATÍSTICAS POR DIFICULDADE:")
        for difficulty, stats in difficulty_stats.items():
            success_rate = (stats["valid"] / stats["total"]) * 100
            print(f"   {difficulty}: {stats['valid']}/{stats['total']} ({success_rate:.1f}%)")
        
        print("\n📊 ESTATÍSTICAS POR CATEGORIA:")
        for category, stats in sorted(category_stats.items()):
            success_rate = (stats["valid"] / stats["total"]) * 100
            print(f"   {category}: {stats['valid']}/{stats['total']} ({success_rate:.1f}%)")
        
        # Mostrar queries com erro
        invalid_queries = [r for r in self.validation_results if not r["valid"]]
        if invalid_queries:
            print("\n❌ QUERIES COM ERRO:")
            for result in invalid_queries:
                print(f"   {result['id']}: {result['error']}")
    
    def save_validation_report(self, output_path: str):
        """Salva relatório de validação"""
        validation_data = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "database_path": self.database_path,
            "summary": {
                "total_queries": len(self.validation_results),
                "valid_queries": len([r for r in self.validation_results if r["valid"]]),
                "invalid_queries": len([r for r in self.validation_results if not r["valid"]])
            },
            "results": self.validation_results
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(validation_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Relatório salvo em: {output_path}")

def main():
    """Função principal"""
    # Caminhos dos arquivos
    ground_truth_path = "evaluation/ground_truth_improved.json"
    database_path = "sus_database.db"
    output_path = "evaluation/ground_truth_validation_report.json"
    
    # Verificar se arquivos existem
    if not Path(ground_truth_path).exists():
        print(f"❌ Arquivo não encontrado: {ground_truth_path}")
        return
    
    if not Path(database_path).exists():
        print(f"❌ Database não encontrado: {database_path}")
        return
    
    # Executar validação
    validator = GroundTruthValidator(database_path)
    results = validator.validate_all_queries(ground_truth_path)
    
    if results:
        validator.save_validation_report(output_path)
        
        # Se há queries inválidas, mostrar detalhes
        invalid_count = results["summary"]["invalid_queries"]
        if invalid_count > 0:
            print(f"\n⚠️ ATENÇÃO: {invalid_count} queries precisam ser corrigidas!")
        else:
            print(f"\n🎉 SUCESSO: Todas as {results['summary']['total_queries']} queries são válidas!")

if __name__ == "__main__":
    main()