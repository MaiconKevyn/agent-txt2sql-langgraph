#!/usr/bin/env python3
"""
Utilities - Módulos utilitários compartilhados para o sistema de avaliação

Este módulo contém classes e funções compartilhadas entre os scripts de avaliação,
eliminando redundância e centralizando funcionalidades comuns.

Autor: Claude Code Assistant
Data: 2025-07-13
"""

import json
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from dataclasses import dataclass
from contextlib import contextmanager


@dataclass
class EvaluationConfig:
    """Configuração centralizada para avaliação"""
    ground_truth_file: str = "ground_truth_postgresql.json"
    database_path: str = "postgresql://postgres:1234@localhost:5432/sih_rs"
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
    data_equivalence: bool = False
    functional_equivalence: bool = False
    columns_match: bool = False
    confidence: float = 0.0
    
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
    """Gerenciador centralizado de conexões com banco PostgreSQL"""
    
    def __init__(self, database_path: str):
        """
        Inicializa o gerenciador de banco PostgreSQL
        
        Args:
            database_path: String de conexão PostgreSQL
        """
        self.database_path = database_path
        if not database_path.startswith('postgresql://'):
            raise ValueError(f"Apenas PostgreSQL é suportado. Use postgresql://... connection string")
        
        self._connection = None
    
    @contextmanager
    def get_connection(self):
        """Context manager para conexões seguras com PostgreSQL"""
        try:
            import psycopg2
            conn = psycopg2.connect(self.database_path)
            conn.autocommit = True
            try:
                yield conn
            finally:
                conn.close()
        except ImportError:
            raise ImportError("psycopg2 não instalado. Execute: pip install psycopg2-binary")
    
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
        """Valida se a conexão com o banco PostgreSQL está funcionando"""
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
    
    def get_next_version_filename(self, base_name: str, extension: str = ".json") -> str:
        """
        Gera o próximo nome de arquivo versionado
        
        Args:
            base_name: Nome base do arquivo (ex: "results")
            extension: Extensão do arquivo
            
        Returns:
            Nome do arquivo versionado (ex: "results_v1.json")
        """
        version = 1
        while True:
            filename = f"{base_name}_v{version}{extension}"
            file_path = self.output_dir / filename
            if not file_path.exists():
                return filename
            version += 1
    
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
    
    def save_json_versioned(self, data: Any, base_name: str, pretty: bool = True) -> str:
        """
        Salva dados em JSON com versionamento automático
        
        Args:
            data: Dados para salvar
            base_name: Nome base do arquivo (sem extensão)
            pretty: Se deve formatar com indentação
            
        Returns:
            Caminho completo do arquivo salvo
        """
        filename = self.get_next_version_filename(base_name)
        return self.save_json(data, filename, pretty)
    
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
                "model": "llama3.1:8b ",
                "name": "Ollama Llama3.1"
            },
            "mistral": {
                "provider": "ollama",
                "model": "mistral",
                "name": "Mistral"
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


class EnhancedDataComparator:
    """Enhanced data comparison engine with semantic understanding"""
    
    # Common value mappings for the dataset
    GENDER_MAPPINGS = {
        1: ["Masculino", "M", "MASCULINO", "masc", "male"],
        3: ["Feminino", "F", "FEMININO", "fem", "female"]
    }
    
    AGE_GROUP_MAPPINGS = {
        "Menor de 18": ["Menor", "MENOR", "menor", "criança", "child", "youth"],
        "18-64 anos": ["Adulto", "ADULTO", "adulto", "adult", "working age"],
        "Acima de 65": ["Idoso", "IDOSO", "idoso", "elderly", "senior", "65+"]
    }
    
    COLUMN_ALIASES = {
        "total": ["total_casos", "total_mortes", "total_mortos", "total_atendimentos", "count", "quantidade"],
        "sexo": ["SEXO", "gender", "genero"],
        "faixa_etaria": ["FAIXA_ETARIA", "age_group", "grupo_idade"],
        "diag_princ": ["DIAG_PRINC", "diagnostico", "diagnosis"]
    }
    
    @staticmethod
    def normalize_value(value, context_column=None):
        """
        Normalize a value based on context and known mappings
        
        Args:
            value: Value to normalize
            context_column: Column name for context
            
        Returns:
            Normalized value or original if no normalization needed
        """
        if value is None:
            return None
            
        # Handle numeric gender codes
        if context_column and any(alias in context_column.lower() for alias in ["sexo", "gender"]):
            if isinstance(value, (int, float)) and value in EnhancedDataComparator.GENDER_MAPPINGS:
                return value  # Keep original numeric for comparison
            elif isinstance(value, str):
                # Try to reverse map gender strings to numbers
                for code, labels in EnhancedDataComparator.GENDER_MAPPINGS.items():
                    if value.strip().lower() in [l.lower() for l in labels]:
                        return code
        
        # Handle age group labels
        if context_column and any(alias in context_column.lower() for alias in ["faixa", "age", "idade"]):
            if isinstance(value, str):
                value_clean = value.strip()
                for canonical, aliases in EnhancedDataComparator.AGE_GROUP_MAPPINGS.items():
                    if value_clean in aliases or value_clean == canonical:
                        return canonical
        
        # Normalize strings
        if isinstance(value, str):
            return value.strip().lower()
            
        return value
    
    @staticmethod
    def normalize_column_name(column_name):
        """
        Normalize column name to canonical form
        
        Args:
            column_name: Original column name
            
        Returns:
            Canonical column name
        """
        if not column_name:
            return column_name
            
        column_lower = column_name.lower().strip()
        
        # Find canonical form
        for canonical, aliases in EnhancedDataComparator.COLUMN_ALIASES.items():
            if column_lower == canonical or column_lower in [alias.lower() for alias in aliases]:
                return canonical
                
        return column_lower
    
    @staticmethod
    def compare_data_rows(expected_data, generated_data, expected_columns, generated_columns):
        """
        Compare data rows with semantic understanding
        
        Args:
            expected_data: Expected result rows
            generated_data: Generated result rows  
            expected_columns: Expected column names
            generated_columns: Generated column names
            
        Returns:
            Dict with comparison results
        """
        if len(expected_data) != len(generated_data):
            return {
                "data_match": False,
                "functional_match": False,
                "confidence": 0.0,
                "reason": "Different row counts"
            }
        
        if not expected_data:
            return {
                "data_match": True,
                "functional_match": True,
                "confidence": 1.0,
                "reason": "Both empty"
            }
        
        # Normalize column names for comparison
        exp_columns_norm = [EnhancedDataComparator.normalize_column_name(col) for col in expected_columns]
        gen_columns_norm = [EnhancedDataComparator.normalize_column_name(col) for col in generated_columns]
        
        # Create column mapping if structure is similar
        column_mapping = {}
        if len(expected_columns) == len(generated_columns):
            for i, exp_col in enumerate(exp_columns_norm):
                gen_col = gen_columns_norm[i]
                column_mapping[i] = i  # Positional mapping first
                
                # Try to find better mapping by name
                for j, gen_col_check in enumerate(gen_columns_norm):
                    if exp_col == gen_col_check:
                        column_mapping[i] = j
                        break
        
        # Compare each row
        matches = 0
        functional_matches = 0
        total_comparisons = 0
        
        for exp_row, gen_row in zip(expected_data, generated_data):
            if len(exp_row) != len(gen_row):
                continue
                
            row_matches = 0
            row_functional_matches = 0
            
            for i, exp_val in enumerate(exp_row):
                total_comparisons += 1
                gen_idx = column_mapping.get(i, i)
                
                if gen_idx >= len(gen_row):
                    continue
                    
                gen_val = gen_row[gen_idx]
                
                # Get column context for normalization
                exp_col = expected_columns[i] if i < len(expected_columns) else None
                
                # Exact match
                if exp_val == gen_val:
                    matches += 1
                    functional_matches += 1
                    row_matches += 1
                    row_functional_matches += 1
                    continue
                
                # Semantic comparison
                exp_norm = EnhancedDataComparator.normalize_value(exp_val, exp_col)
                gen_norm = EnhancedDataComparator.normalize_value(gen_val, exp_col)
                
                if exp_norm == gen_norm:
                    functional_matches += 1
                    row_functional_matches += 1
                elif isinstance(exp_val, (int, float)) and isinstance(gen_val, (int, float)):
                    # Numeric tolerance
                    if abs(exp_val - gen_val) < 0.01:
                        functional_matches += 1
                        row_functional_matches += 1
        
        if total_comparisons == 0:
            confidence = 0.0
        else:
            data_match_rate = matches / total_comparisons
            functional_match_rate = functional_matches / total_comparisons
            confidence = max(data_match_rate, functional_match_rate)
        
        return {
            "data_match": matches == total_comparisons,
            "functional_match": functional_matches == total_comparisons,
            "confidence": confidence,
            "match_rate": matches / total_comparisons if total_comparisons > 0 else 0,
            "functional_match_rate": functional_matches / total_comparisons if total_comparisons > 0 else 0,
            "reason": f"Matched {matches}/{total_comparisons} exact, {functional_matches}/{total_comparisons} functional"
        }
    
    @staticmethod
    def enhanced_compare_query_results(expected_result: Dict, generated_result: Dict) -> Dict[str, Any]:
        """
        Enhanced comparison with semantic understanding
        
        Args:
            expected_result: Expected query result
            generated_result: Generated query result
            
        Returns:
            Enhanced comparison results
        """
        # Basic validation
        if not expected_result['success'] or not generated_result['success']:
            return {
                'exact_match': False,
                'structure_match': False,
                'semantic_equivalence': False,
                'data_equivalence': False,
                'functional_equivalence': False,
                'confidence': 0.0,
                'reason': 'execution_error',
                'expected_rows': expected_result.get('row_count', 0),
                'generated_rows': generated_result.get('row_count', 0),
                'expected_columns': expected_result.get('columns', []),
                'generated_columns': generated_result.get('columns', [])
            }
        
        # Structure comparison
        structure_match = (
            expected_result['row_count'] == generated_result['row_count'] and
            len(expected_result['columns']) == len(generated_result['columns'])
        )
        
        # Column comparison
        exp_columns_norm = [EnhancedDataComparator.normalize_column_name(col) 
                           for col in expected_result['columns']]
        gen_columns_norm = [EnhancedDataComparator.normalize_column_name(col) 
                           for col in generated_result['columns']]
        
        columns_match = set(exp_columns_norm) == set(gen_columns_norm)
        
        # Data comparison
        data_comparison = EnhancedDataComparator.compare_data_rows(
            expected_result['data'],
            generated_result['data'], 
            expected_result['columns'],
            generated_result['columns']
        )
        
        # Original exact match for backward compatibility
        expected_hash = DataProcessor.normalize_data_for_comparison(
            expected_result['data'], expected_result['columns']
        )
        generated_hash = DataProcessor.normalize_data_for_comparison(
            generated_result['data'], generated_result['columns']
        )
        exact_match = expected_hash == generated_hash
        
        # Enhanced equivalence detection
        semantic_equivalence = (
            exact_match or 
            (data_comparison['functional_match'] and columns_match) or
            (data_comparison['confidence'] > 0.95 and structure_match)
        )
        
        data_equivalence = data_comparison['data_match'] or data_comparison['confidence'] > 0.9
        functional_equivalence = data_comparison['functional_match'] or data_comparison['confidence'] > 0.8
        
        # Detailed evaluation checkpoints
        checkpoints = EnhancedDataComparator._generate_evaluation_checkpoints(
            expected_result, generated_result, data_comparison, 
            exact_match, semantic_equivalence, data_equivalence, functional_equivalence
        )

        return {
            'exact_match': exact_match,
            'structure_match': structure_match,
            'semantic_equivalence': semantic_equivalence,
            'data_equivalence': data_equivalence,
            'functional_equivalence': functional_equivalence,
            'columns_match': columns_match,
            'confidence': data_comparison['confidence'],
            'match_details': data_comparison,
            'reason': data_comparison.get('reason', 'compared'),
            'expected_rows': expected_result['row_count'],
            'generated_rows': generated_result['row_count'],
            'expected_columns': expected_result['columns'],
            'generated_columns': generated_result['columns'],
            'evaluation_checkpoints': checkpoints
        }
    
    @staticmethod
    def _generate_evaluation_checkpoints(expected_result, generated_result, data_comparison, 
                                       exact_match, semantic_equivalence, data_equivalence, functional_equivalence):
        """
        Generate detailed evaluation checkpoints for transparency
        
        Returns:
            Dict with detailed checkpoint analysis
        """
        checkpoints = {
            "execution": {
                "expected_success": expected_result['success'],
                "generated_success": generated_result['success'],
                "passed": expected_result['success'] and generated_result['success'],
                "details": "Both queries executed successfully" if expected_result['success'] and generated_result['success'] 
                          else f"Expected: {expected_result.get('error', 'N/A')}, Generated: {generated_result.get('error', 'N/A')}"
            },
            "row_count": {
                "expected": expected_result['row_count'],
                "generated": generated_result['row_count'],
                "passed": expected_result['row_count'] == generated_result['row_count'],
                "details": f"Expected {expected_result['row_count']} rows, got {generated_result['row_count']}"
            },
            "column_structure": {
                "expected_count": len(expected_result['columns']),
                "generated_count": len(generated_result['columns']),
                "passed": len(expected_result['columns']) == len(generated_result['columns']),
                "details": f"Expected {len(expected_result['columns'])} columns, got {len(generated_result['columns'])}"
            },
            "column_semantics": {
                "expected_columns": expected_result['columns'],
                "generated_columns": generated_result['columns'],
                "normalized_expected": [EnhancedDataComparator.normalize_column_name(col) for col in expected_result['columns']],
                "normalized_generated": [EnhancedDataComparator.normalize_column_name(col) for col in generated_result['columns']],
                "passed": set([EnhancedDataComparator.normalize_column_name(col) for col in expected_result['columns']]) == 
                         set([EnhancedDataComparator.normalize_column_name(col) for col in generated_result['columns']]),
                "details": "Column names are semantically equivalent" if set([EnhancedDataComparator.normalize_column_name(col) for col in expected_result['columns']]) == set([EnhancedDataComparator.normalize_column_name(col) for col in generated_result['columns']]) else "Column names differ semantically"
            },
            "data_exactness": {
                "passed": exact_match,
                "confidence": 1.0 if exact_match else data_comparison.get('match_rate', 0.0),
                "details": "Data matches exactly" if exact_match else f"Exact match rate: {data_comparison.get('match_rate', 0.0):.3f}"
            },
            "data_functional": {
                "passed": functional_equivalence,
                "confidence": data_comparison.get('functional_match_rate', 0.0),
                "details": f"Functional match rate: {data_comparison.get('functional_match_rate', 0.0):.3f}"
            },
            "business_logic": {
                "passed": semantic_equivalence,
                "confidence": data_comparison['confidence'],
                "details": f"Answers the same business question with confidence {data_comparison['confidence']:.3f}"
            }
        }
        
        # Overall assessment
        level = ("exact" if exact_match else 
                "functional" if functional_equivalence else 
                "semantic" if semantic_equivalence else 
                "data" if data_equivalence else "failed")
                
        checkpoints["overall"] = {
            "passed": functional_equivalence or semantic_equivalence,
            "level": level,
            "confidence": data_comparison['confidence'],
            "summary": f"Query evaluation {level} with {data_comparison['confidence']:.1%} confidence"
        }
        
        return checkpoints