#!/usr/bin/env python3
"""
Utilities - Módulos utilitários compartilhados para o sistema de avaliação

Este módulo contém classes e funções compartilhadas entre os scripts de avaliação,
eliminando redundância e centralizando funcionalidades comuns.

Autor: Claude Code Assistant
Data: 2025-07-13
"""

import json
import sqlite3
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict
from contextlib import contextmanager


@dataclass
class EvaluationConfig:
    """Configuração centralizada para avaliação"""
    ground_truth_file: str = "ground_truth.json"
    database_path: str = "../sus_database.db"
    output_dir: str = "results"
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Ensure output directory exists
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)


@dataclass
class ModelResult:
    """Resultado de um modelo para uma query específica"""
    model_name: str
    model_key: str
    provider: str
    query_id: str
    question: str
    difficulty: str
    expected_query: str
    generated_query: str
    execution_time: float
    success: bool
    error_message: str = ""
    query_generated: bool = False


@dataclass
class QueryEvaluation:
    """Avaliação de uma query (execução + comparação)"""
    query_id: str
    model_name: str
    model_key: str
    difficulty: str
    question: str
    expected_query: str
    generated_query: str
    
    # Execution results
    expected_success: bool = False
    generated_success: bool = False
    expected_error: str = ""
    generated_error: str = ""
    expected_rows: int = 0
    generated_rows: int = 0
    expected_columns: List[str] = None
    generated_columns: List[str] = None
    expected_data: List = None
    generated_data: List = None
    
    # Comparison results  
    exact_match: bool = False
    structure_match: bool = False
    sql_similarity: float = 0.0
    semantic_equivalence: bool = False
    
    # Execution metadata
    execution_time: float = 0.0
    
    def __post_init__(self):
        if self.expected_columns is None:
            self.expected_columns = []
        if self.generated_columns is None:
            self.generated_columns = []
        if self.expected_data is None:
            self.expected_data = []
        if self.generated_data is None:
            self.generated_data = []


class DatabaseManager:
    """Gerenciador centralizado de conexões com banco de dados"""
    
    def __init__(self, database_path: str):
        """
        Inicializa o gerenciador de banco
        
        Args:
            database_path: Caminho para o banco SQLite
        """
        self.database_path = Path(database_path)
        if not self.database_path.exists():
            raise FileNotFoundError(f"Database not found: {database_path}")
        
        self._connection = None
    
    @contextmanager
    def get_connection(self):
        """Context manager para conexões seguras"""
        conn = sqlite3.connect(self.database_path)
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Executa uma query e retorna resultados estruturados
        
        Args:
            query: Query SQL para executar
            
        Returns:
            Dict com success, data, columns, error
        """
        start_time = time.time()
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                return {
                    'success': True,
                    'data': data,
                    'columns': columns,
                    'row_count': len(data),
                    'execution_time': time.time() - start_time,
                    'error': ''
                }
                
        except Exception as e:
            return {
                'success': False,
                'data': [],
                'columns': [],
                'row_count': 0,
                'execution_time': time.time() - start_time,
                'error': str(e)
            }
    
    def validate_connection(self) -> bool:
        """Valida se a conexão com o banco está funcionando"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return True
        except Exception:
            return False


class FileManager:
    """Gerenciador centralizado de arquivos"""
    
    def __init__(self, output_dir: str = "results"):
        """
        Inicializa o gerenciador de arquivos
        
        Args:
            output_dir: Diretório base para outputs
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save_json(self, data: Any, filename: str, pretty: bool = True) -> str:
        """
        Salva dados em JSON
        
        Args:
            data: Dados para salvar
            filename: Nome do arquivo
            pretty: Se deve formatar com indentação
            
        Returns:
            Caminho completo do arquivo salvo
        """
        file_path = self.output_dir / filename
        
        with open(file_path, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            else:
                json.dump(data, f, ensure_ascii=False, default=str)
        
        return str(file_path)
    
    def load_json(self, filename: str) -> Any:
        """
        Carrega dados de JSON
        
        Args:
            filename: Nome do arquivo ou caminho completo
            
        Returns:
            Dados carregados
        """
        # Try as full path first, then as relative to output_dir
        file_path = Path(filename)
        if not file_path.exists():
            file_path = self.output_dir / filename
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_output_path(self, filename: str) -> str:
        """Retorna caminho completo para um arquivo de output"""
        return str(self.output_dir / filename)


class DataProcessor:
    """Processador de dados compartilhado"""
    
    @staticmethod
    def calculate_sql_similarity(expected: str, generated: str) -> float:
        """
        Calcula similaridade entre duas queries SQL usando Jaccard
        
        Args:
            expected: Query esperada
            generated: Query gerada
            
        Returns:
            Score de similaridade (0-1)
        """
        if not expected or not generated:
            return 0.0
        
        # Normalizar queries
        expected_clean = ' '.join(expected.lower().split())
        generated_clean = ' '.join(generated.lower().split())
        
        # Comparação exata
        if expected_clean == generated_clean:
            return 1.0
        
        # Jaccard similarity
        expected_tokens = set(expected_clean.split())
        generated_tokens = set(generated_clean.split())
        
        if not expected_tokens:
            return 0.0
        
        intersection = expected_tokens.intersection(generated_tokens)
        union = expected_tokens.union(generated_tokens)
        
        return len(intersection) / len(union) if union else 0.0
    
    @staticmethod
    def normalize_data_for_comparison(data: List, columns: List[str]) -> str:
        """
        Normaliza dados para comparação consistente
        
        Args:
            data: Dados da query
            columns: Nomes das colunas
            
        Returns:
            Hash string dos dados normalizados
        """
        try:
            # Ordenar dados para garantir consistência
            sorted_data = sorted(data) if data else []
            normalized = {
                'columns': sorted(columns),
                'data': sorted_data,
                'row_count': len(data)
            }
            
            normalized_str = json.dumps(normalized, sort_keys=True, default=str)
            return hashlib.md5(normalized_str.encode()).hexdigest()
            
        except Exception:
            return str(data)
    
    @staticmethod
    def compare_query_results(expected_result: Dict, generated_result: Dict) -> Dict[str, Any]:
        """
        Compara resultados de duas queries
        
        Args:
            expected_result: Resultado da query esperada
            generated_result: Resultado da query gerada
            
        Returns:
            Dict com detalhes da comparação
        """
        # Se alguma falhou, não são equivalentes
        if not expected_result['success'] or not generated_result['success']:
            return {
                'exact_match': False,
                'structure_match': False,
                'semantic_equivalence': False,
                'reason': 'execution_error'
            }
        
        # Comparar estrutura
        structure_match = (
            expected_result['row_count'] == generated_result['row_count'] and
            set(expected_result['columns']) == set(generated_result['columns'])
        )
        
        # Comparar dados
        expected_hash = DataProcessor.normalize_data_for_comparison(
            expected_result['data'], expected_result['columns']
        )
        generated_hash = DataProcessor.normalize_data_for_comparison(
            generated_result['data'], generated_result['columns']
        )
        
        exact_match = expected_hash == generated_hash
        
        # Equivalência semântica: mesmo resultado mas estrutura diferente
        semantic_equivalence = exact_match or (
            expected_result['row_count'] == generated_result['row_count'] and
            expected_result['row_count'] > 0 and
            # Para valores únicos, comparar se são próximos
            DataProcessor._check_semantic_equivalence(
                expected_result['data'], generated_result['data']
            )
        )
        
        return {
            'exact_match': exact_match,
            'structure_match': structure_match,
            'semantic_equivalence': semantic_equivalence,
            'expected_rows': expected_result['row_count'],
            'generated_rows': generated_result['row_count'],
            'expected_columns': expected_result['columns'],
            'generated_columns': generated_result['columns']
        }
    
    @staticmethod
    def _check_semantic_equivalence(expected_data: List, generated_data: List) -> bool:
        """Verifica equivalência semântica entre dados"""
        if not expected_data or not generated_data:
            return len(expected_data) == len(generated_data)
        
        # Para valores únicos numéricos, verificar proximidade
        if len(expected_data) == 1 and len(generated_data) == 1:
            exp_val = expected_data[0]
            gen_val = generated_data[0]
            
            if isinstance(exp_val, (list, tuple)) and isinstance(gen_val, (list, tuple)):
                if len(exp_val) == 1 and len(gen_val) == 1:
                    try:
                        exp_num = float(exp_val[0])
                        gen_num = float(gen_val[0])
                        # Tolerância de 1% para valores numéricos
                        return abs(exp_num - gen_num) / abs(exp_num) < 0.01 if exp_num != 0 else gen_num == 0
                    except (ValueError, TypeError):
                        pass
        
        return False


class ConfigManager:
    """Gerenciador de configurações"""
    
    @staticmethod
    def load_ground_truth(file_path: str) -> List[Dict[str, Any]]:
        """Carrega arquivo ground truth"""
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    @staticmethod
    def get_available_models() -> Dict[str, Dict[str, str]]:
        """Retorna modelos disponíveis"""
        return {
            "ollama_llama3": {
                "provider": "ollama",
                "model": "llama3",
                "name": "Ollama Llama3"
            },
            "ollama_llama3.1": {
                "provider": "ollama", 
                "model": "llama3.1",
                "name": "Ollama Llama3.1"
            },
            "ollama_llama3.2": {
                "provider": "ollama",
                "model": "llama3.2", 
                "name": "Ollama Llama3.2"
            },
            "mistral": {
                "provider": "ollama",
                "model": "mistral",
                "name": "Mistral"
            },
            "qwen3": {
                "provider": "ollama",
                "model": "qwen3",
                "name": "Qwen 3"
            }
        }
    
    @staticmethod
    def validate_models(model_list: List[str]) -> List[str]:
        """Valida lista de modelos"""
        available = ConfigManager.get_available_models()
        valid_models = []
        
        for model in model_list:
            if model in available:
                valid_models.append(model)
            else:
                print(f"⚠️  Modelo '{model}' não encontrado. Modelos disponíveis: {list(available.keys())}")
        
        return valid_models