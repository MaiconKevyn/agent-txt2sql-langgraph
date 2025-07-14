"""
Query Template Matcher Implementation  
Implementação do matcher de templates de decomposição
"""
import re
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from domain.entities.query_decomposition import (
    ComplexityAnalysis,
    DecompositionStrategy,
    QueryParameters
)
from application.services.query_planner_service import IQueryTemplateMatcher, TemplateMismatchError


@dataclass
class DecompositionTemplate:
    """Template para decomposição de queries"""
    name: str
    strategy: DecompositionStrategy
    pattern_regex: str
    parameter_extractors: Dict[str, str]  # param_name -> regex
    step_templates: List[Dict[str, Any]]
    compatibility_factors: Dict[str, float]
    min_confidence: float = 0.6
    description: str = ""
    
    def extract_parameters(self, query: str) -> QueryParameters:
        """Extrai parâmetros da query usando os extractors"""
        query_lower = query.lower()
        parameters = {}
        
        for param_name, extractor_regex in self.parameter_extractors.items():
            match = re.search(extractor_regex, query_lower)
            if match:
                if match.groups():
                    parameters[param_name] = match.group(1)
                else:
                    parameters[param_name] = match.group(0)
        
        return parameters
    
    def calculate_compatibility(self, query: str, analysis: ComplexityAnalysis) -> float:
        """Calcula score de compatibilidade (0.0 a 1.0)"""
        score = 0.0
        
        # 1. Pattern match score (40% do peso)
        if re.search(self.pattern_regex, query.lower()):
            score += 0.4
        
        # 2. Strategy alignment score (30% do peso)  
        if analysis.recommended_strategy == self.strategy:
            score += 0.3
        
        # 3. Complexity factors score (30% do peso)
        factor_score = 0.0
        for factor, weight in self.compatibility_factors.items():
            if factor in analysis.complexity_factors:
                factor_score += weight
        
        score += min(factor_score, 0.3)
        
        return min(score, 1.0)


class QueryTemplateMatcher(IQueryTemplateMatcher):
    """
    Implementação do matcher de templates baseado nos padrões do Checkpoint 1
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._templates = self._initialize_templates()
    
    def _initialize_templates(self) -> List[DecompositionTemplate]:
        """
        Inicializa templates baseados nos padrões identificados no Checkpoint 1
        """
        return [
            # Template 1: Filtros Demográficos Sequenciais
            DecompositionTemplate(
                name="demographic_sequential_filtering",
                strategy=DecompositionStrategy.DEMOGRAPHIC_ANALYSIS,
                pattern_regex=r"(mulher|homem|feminino|masculino).*(anos?|idade).*(mort|óbit|cidad|municip)",
                parameter_extractors={
                    "sexo": r"(mulher|feminino|sexo.*3|sexo.*f)",
                    "idade_op": r"(mais de|menos de|acima de|abaixo de|maior|menor)",
                    "idade_valor": r"(?:mais de|menos de|acima de|abaixo de|maior que|menor que)\s+(\d+)",
                    "limite": r"(?:top\s+|primeiros?\s+)?(\d+)(?:\s+cidades?|\s+municípios?)?",
                    "doenca_tipo": r"(respiratória|cardíaca|digestiva|neurológica|J\d+|I\d+|K\d+|G\d+)"
                },
                step_templates=[
                    {
                        "step_type": "FILTER",
                        "description": "Filtrar por sexo",
                        "sql_template": "SELECT * FROM sus_data WHERE SEXO = {sexo_code}"
                    },
                    {
                        "step_type": "FILTER", 
                        "description": "Filtrar por idade",
                        "sql_template": "SELECT * FROM {previous_table} WHERE IDADE {idade_op} {idade_valor}"
                    },
                    {
                        "step_type": "FILTER",
                        "description": "Filtrar por morte",
                        "sql_template": "SELECT * FROM {previous_table} WHERE MORTE = 1"
                    },
                    {
                        "step_type": "FILTER",
                        "description": "Filtrar por tipo de doença",
                        "sql_template": "SELECT * FROM {previous_table} WHERE DIAG_PRINC LIKE '{doenca_pattern}'"
                    },
                    {
                        "step_type": "AGGREGATE",
                        "description": "Agrupar por cidade e contar",
                        "sql_template": "SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_mortes FROM {previous_table} GROUP BY CIDADE_RESIDENCIA_PACIENTE"
                    },
                    {
                        "step_type": "RANK",
                        "description": "Ordenar e limitar resultado",
                        "sql_template": "SELECT * FROM {previous_table} ORDER BY total_mortes DESC LIMIT {limite}"
                    }
                ],
                compatibility_factors={
                    "multiple_demographic_filters": 0.8,
                    "respiratory_disease_complex": 0.6,
                    "multiple_criteria_with_ranking": 0.7
                },
                description="Template para filtros demográficos sequenciais"
            ),
            
            # Template 2: Análise Temporal Complexa
            DecompositionTemplate(
                name="temporal_analysis",
                strategy=DecompositionStrategy.TEMPORAL_BREAKDOWN,
                pattern_regex=r"(tempo.*internaç|média.*internaç|período|201\d|202\d)",
                parameter_extractors={
                    "ano_inicio": r"(201\d|202\d)",
                    "mes_inicio": r"(janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)",
                    "agregacao": r"(média|tempo médio|duração|dias)",
                    "grupo": r"(cidade|município|estado|diagnóstico|CID)"
                },
                step_templates=[
                    {
                        "step_type": "CALCULATE",
                        "description": "Calcular tempo de internação",
                        "sql_template": """SELECT *, 
                            JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                            JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2)) 
                            as tempo_internacao_dias
                        FROM sus_data"""
                    },
                    {
                        "step_type": "FILTER",
                        "description": "Filtrar período se especificado",
                        "sql_template": "SELECT * FROM {previous_table} WHERE DT_INTER >= '{data_inicio}' AND DT_INTER <= '{data_fim}'"
                    },
                    {
                        "step_type": "FILTER",
                        "description": "Filtrar casos válidos (com alta)",
                        "sql_template": "SELECT * FROM {previous_table} WHERE DT_SAIDA IS NOT NULL AND DT_SAIDA != '' AND tempo_internacao_dias >= 0"
                    },
                    {
                        "step_type": "AGGREGATE",
                        "description": "Calcular média por grupo",
                        "sql_template": "SELECT {grupo_coluna}, AVG(tempo_internacao_dias) as tempo_medio_dias, COUNT(*) as casos FROM {previous_table} GROUP BY {grupo_coluna}"
                    },
                    {
                        "step_type": "RANK",
                        "description": "Ordenar por tempo médio",
                        "sql_template": "SELECT * FROM {previous_table} ORDER BY tempo_medio_dias DESC"
                    }
                ],
                compatibility_factors={
                    "temporal_calculation": 0.9,
                    "temporal_period_analysis": 0.7,
                    "trend_analysis": 0.6
                },
                description="Template para análises temporais com cálculos de internação"
            ),
            
            # Template 3: Ranking com Detalhamento
            DecompositionTemplate(
                name="ranking_with_details",
                strategy=DecompositionStrategy.RANKING_WITH_DETAILS,
                pattern_regex=r"(qual.*mais|maior.*qual|principal.*qual|primeiro.*qual)",
                parameter_extractors={
                    "entidade_principal": r"qual\s+(médico|cidade|município|hospital|estado)",
                    "metrica_ranking": r"(mais consultas|mais mortes|mais casos|maior número|maior volume)",
                    "detalhamento": r"qual.*?(especialidade|diagnóstico|categoria|tipo|CID)",
                    "limite": r"(\d+)(?:\s+primeiros?|\s+principais?|\s+maiores?)?"
                },
                step_templates=[
                    {
                        "step_type": "AGGREGATE",
                        "description": "Calcular ranking da entidade principal",
                        "sql_template": "SELECT {entidade_coluna}, COUNT(*) as total FROM sus_data GROUP BY {entidade_coluna}"
                    },
                    {
                        "step_type": "RANK",
                        "description": "Identificar top entidade",
                        "sql_template": "SELECT {entidade_coluna} FROM {previous_table} ORDER BY total DESC LIMIT 1"
                    },
                    {
                        "step_type": "FILTER",
                        "description": "Filtrar dados da entidade vencedora",
                        "sql_template": "SELECT * FROM sus_data WHERE {entidade_coluna} = '{entidade_vencedora}'"
                    },
                    {
                        "step_type": "AGGREGATE",
                        "description": "Analisar detalhamento",
                        "sql_template": "SELECT {detalhamento_coluna}, COUNT(*) as frequencia FROM {previous_table} GROUP BY {detalhamento_coluna}"
                    },
                    {
                        "step_type": "RANK",
                        "description": "Ranking do detalhamento",
                        "sql_template": "SELECT * FROM {previous_table} ORDER BY frequencia DESC LIMIT {limite}"
                    }
                ],
                compatibility_factors={
                    "ranking_with_details": 0.9,
                    "multiple_questions": 0.8,
                    "comparison_analysis": 0.6
                },
                description="Template para queries que pedem ranking seguido de detalhamento"
            ),
            
            # Template 4: Análise de Neoplasias (Alto Risco)
            DecompositionTemplate(
                name="neoplasia_safe_analysis",
                strategy=DecompositionStrategy.DIAGNOSIS_CLASSIFICATION,
                pattern_regex=r"(neoplasia|cancer|tumor|oncolog|C\d+)",
                parameter_extractors={
                    "tipo_neoplasia": r"(cancer|tumor|neoplasia|oncolog)",
                    "localizacao": r"(pulmão|mama|próstata|cólon|fígado|estômago)",
                    "estadio": r"(inicial|avançado|metástase|estadio\s+\d+)",
                    "grupo_idade": r"(criança|jovem|adulto|idoso|\d+\s+anos?)"
                },
                step_templates=[
                    {
                        "step_type": "FILTER",
                        "description": "Filtrar diagnósticos de neoplasias (simples)",
                        "sql_template": "SELECT * FROM sus_data WHERE DIAG_PRINC LIKE 'C%'"
                    },
                    {
                        "step_type": "FILTER", 
                        "description": "Filtrar mortes se solicitado",
                        "sql_template": "SELECT * FROM {previous_table} WHERE MORTE = 1"
                    },
                    {
                        "step_type": "AGGREGATE",
                        "description": "Contagem simples por categoria",
                        "sql_template": "SELECT SUBSTR(DIAG_PRINC, 1, 3) as categoria_cid, COUNT(*) as total FROM {previous_table} GROUP BY categoria_cid"
                    },
                    {
                        "step_type": "RANK",
                        "description": "Ordenar por frequência",
                        "sql_template": "SELECT * FROM {previous_table} ORDER BY total DESC"
                    }
                ],
                compatibility_factors={
                    "neoplasia_queries": 1.0,
                    "multiple_cid_categories": 0.7
                },
                min_confidence=0.8,  # Alta confiança necessária
                description="Template seguro para análises de neoplasias (evita timeouts)"
            ),
            
            # Template 5: Análise Geográfica
            DecompositionTemplate(
                name="geographic_analysis",
                strategy=DecompositionStrategy.GEOGRAPHIC_ANALYSIS,
                pattern_regex=r"(cidade|município|estado|região|raio|proximidade)",
                parameter_extractors={
                    "nivel_geografico": r"(cidade|município|estado|região)",
                    "cidade_referencia": r"(?:perto de|próximo a|em|de)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
                    "raio_km": r"(\d+)\s*km",
                    "top_n": r"(?:top\s+|primeiros?\s+)?(\d+)"
                },
                step_templates=[
                    {
                        "step_type": "FILTER",
                        "description": "Filtrar dados geográficos válidos",
                        "sql_template": "SELECT * FROM sus_data WHERE CIDADE_RESIDENCIA_PACIENTE IS NOT NULL AND CIDADE_RESIDENCIA_PACIENTE != ''"
                    },
                    {
                        "step_type": "AGGREGATE",
                        "description": "Agrupar por nível geográfico",
                        "sql_template": "SELECT {nivel_coluna}, COUNT(*) as total FROM {previous_table} GROUP BY {nivel_coluna}"
                    },
                    {
                        "step_type": "RANK",
                        "description": "Ranking geográfico",
                        "sql_template": "SELECT * FROM {previous_table} ORDER BY total DESC LIMIT {top_n}"
                    }
                ],
                compatibility_factors={
                    "geographic_proximity": 0.8,
                    "coordinate_analysis": 0.9,
                    "multiple_criteria_with_ranking": 0.6
                },
                description="Template para análises geográficas"
            ),
            
            # Template 6: Análise Financeira
            DecompositionTemplate(
                name="financial_analysis",
                strategy=DecompositionStrategy.FINANCIAL_AGGREGATION,
                pattern_regex=r"(custo|valor|gasto|financeiro|reais?|R\$)",
                parameter_extractors={
                    "tipo_valor": r"(custo|valor|gasto|financeiro)",
                    "agregacao": r"(total|média|máximo|mínimo|soma)",
                    "grupo": r"(procedimento|diagnóstico|cidade|estado|especialidade)",
                    "filtro_valor": r"(mais de|acima de|maior que)\s+(\d+)"
                },
                step_templates=[
                    {
                        "step_type": "FILTER",
                        "description": "Filtrar registros com valores válidos",
                        "sql_template": "SELECT * FROM sus_data WHERE VAL_TOT IS NOT NULL AND VAL_TOT > 0"
                    },
                    {
                        "step_type": "AGGREGATE",
                        "description": "Calcular agregação financeira",
                        "sql_template": "SELECT {grupo_coluna}, {agregacao_func}(VAL_TOT) as valor_calculado, COUNT(*) as casos FROM {previous_table} GROUP BY {grupo_coluna}"
                    },
                    {
                        "step_type": "FILTER",
                        "description": "Aplicar filtros de volume se necessário",
                        "sql_template": "SELECT * FROM {previous_table} WHERE casos >= {min_casos}"
                    },
                    {
                        "step_type": "RANK",
                        "description": "Ordenar por valor",
                        "sql_template": "SELECT * FROM {previous_table} ORDER BY valor_calculado DESC"
                    }
                ],
                compatibility_factors={
                    "financial_aggregation": 0.9,
                    "volume_threshold": 0.7
                },
                description="Template para análises financeiras com agregações"
            )
        ]
    
    def find_matching_templates(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> List[Dict[str, Any]]:
        """
        Encontra templates compatíveis com a query e análise
        """
        matches = []
        
        for template in self._templates:
            compatibility_score = template.calculate_compatibility(query, analysis)
            
            if compatibility_score >= template.min_confidence:
                parameters = template.extract_parameters(query)
                
                matches.append({
                    "template": template,
                    "compatibility_score": compatibility_score,
                    "parameters": parameters,
                    "strategy": template.strategy,
                    "confidence": compatibility_score
                })
        
        # Ordenar por score de compatibilidade
        matches.sort(key=lambda x: x["compatibility_score"], reverse=True)
        
        if not matches:
            self.logger.warning(f"Nenhum template compatível encontrado para query: {query[:50]}...")
            
        return matches
    
    def extract_parameters(
        self, 
        query: str, 
        template: Dict[str, Any]
    ) -> QueryParameters:
        """
        Extrai parâmetros específicos para um template
        """
        if "template" not in template:
            raise TemplateMismatchError("Template inválido - objeto 'template' não encontrado")
        
        template_obj = template["template"]
        return template_obj.extract_parameters(query)
    
    def get_best_template(
        self, 
        query: str, 
        analysis: ComplexityAnalysis
    ) -> Optional[Dict[str, Any]]:
        """
        Retorna o melhor template para a query
        """
        matches = self.find_matching_templates(query, analysis)
        return matches[0] if matches else None
    
    def get_template_by_name(self, name: str) -> Optional[DecompositionTemplate]:
        """
        Busca template pelo nome
        """
        for template in self._templates:
            if template.name == name:
                return template
        return None