"""
Enhanced Template Library for Query Decomposition
Biblioteca expandida de templates para decomposição de queries
"""
import re
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from domain.entities.query_decomposition import (
    QueryStep,
    QueryStepType,
    DecompositionStrategy,
    ComplexityAnalysis
)


class TemplateCategory(Enum):
    """Categorias de templates"""
    RESPIRATORY_ANALYSIS = "respiratory_analysis"
    NEOPLASIA_ANALYSIS = "neoplasia_analysis"
    DEMOGRAPHIC_COMPLEX = "demographic_complex"
    TEMPORAL_ADVANCED = "temporal_advanced"
    GEOGRAPHIC_CORRELATION = "geographic_correlation"
    FINANCIAL_ANALYSIS = "financial_analysis"
    MULTI_CONDITION = "multi_condition"
    RANKING_DETAILED = "ranking_detailed"


@dataclass
class EnhancedDecompositionTemplate:
    """Template aprimorado de decomposição"""
    template_id: str
    name: str
    category: TemplateCategory
    description: str
    query_patterns: List[str]  # Regex patterns que ativam este template
    compatibility_score_threshold: float
    strategy: DecompositionStrategy
    step_templates: List[Dict[str, Any]]
    parameter_extractors: Dict[str, str]  # Regexes para extrair parâmetros
    optimization_hints: List[str]
    examples: List[str]
    metadata: Dict[str, Any]


class EnhancedTemplateLibrary:
    """
    Biblioteca expandida de templates de decomposição
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.templates = self._initialize_templates()
        self.template_cache = {}
        
        self.logger.info(f"EnhancedTemplateLibrary inicializada com {len(self.templates)} templates")
    
    def _initialize_templates(self) -> List[EnhancedDecompositionTemplate]:
        """Inicializa biblioteca completa de templates"""
        templates = []
        
        # 1. Templates de Análise Respiratória
        templates.extend(self._create_respiratory_templates())
        
        # 2. Templates de Análise de Neoplasias
        templates.extend(self._create_neoplasia_templates())
        
        # 3. Templates Demográficos Complexos
        templates.extend(self._create_demographic_complex_templates())
        
        # 4. Templates Temporais Avançados
        templates.extend(self._create_temporal_advanced_templates())
        
        # 5. Templates de Correlação Geográfica
        templates.extend(self._create_geographic_correlation_templates())
        
        # 6. Templates de Análise Financeira
        templates.extend(self._create_financial_analysis_templates())
        
        # 7. Templates Multi-Condição
        templates.extend(self._create_multi_condition_templates())
        
        # 8. Templates de Ranking Detalhado
        templates.extend(self._create_ranking_detailed_templates())
        
        return templates
    
    def _create_respiratory_templates(self) -> List[EnhancedDecompositionTemplate]:
        """Cria templates especializados para análise respiratória"""
        return [
            EnhancedDecompositionTemplate(
                template_id="respiratory_demographic_analysis",
                name="Análise Demográfica de Doenças Respiratórias",
                category=TemplateCategory.RESPIRATORY_ANALYSIS,
                description="Análise detalhada de doenças respiratórias por demografia",
                query_patterns=[
                    r"(mulher|feminino|homem|masculino).*(respiratór|pulmão|j90|j44|pneum)",
                    r"(doença.*(respiratór|pulmonar)).*(idade|anos|idoso)",
                    r"(j\d+|pneum|asma|dpoc).*(mort|óbit).*(cidad|municip)"
                ],
                compatibility_score_threshold=0.75,
                strategy=DecompositionStrategy.DEMOGRAPHIC_ANALYSIS,
                step_templates=[
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Filtrar diagnósticos respiratórios específicos",
                        "sql_template": """
                            SELECT * FROM sus_data 
                            WHERE DIAG_PRINC LIKE '{respiratory_pattern}' 
                            AND DIAG_PRINC IS NOT NULL AND DIAG_PRINC != ''
                        """,
                        "dependencies": []
                    },
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Aplicar filtros demográficos",
                        "sql_template": """
                            SELECT * FROM step_1_result 
                            WHERE SEXO = {sexo_code} 
                            AND IDADE {idade_op} {idade_valor}
                        """,
                        "dependencies": [1]
                    },
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Filtrar por desfecho (mortes)",
                        "sql_template": """
                            SELECT * FROM step_2_result 
                            WHERE MORTE = 1
                        """,
                        "dependencies": [2],
                        "conditional": "morte_analysis"
                    },
                    {
                        "step_type": QueryStepType.AGGREGATE,
                        "description": "Agregar por localização e calcular estatísticas",
                        "sql_template": """
                            SELECT 
                                CIDADE_RESIDENCIA_PACIENTE,
                                COUNT(*) as total_casos,
                                AVG(IDADE) as idade_media,
                                MIN(IDADE) as idade_min,
                                MAX(IDADE) as idade_max,
                                COUNT(DISTINCT DIAG_PRINC) as diagnosticos_distintos
                            FROM step_{previous_step}_result 
                            GROUP BY CIDADE_RESIDENCIA_PACIENTE
                            HAVING COUNT(*) >= {min_casos}
                        """,
                        "dependencies": [3]
                    },
                    {
                        "step_type": QueryStepType.RANK,
                        "description": "Ordenar por impacto e limitar resultados",
                        "sql_template": """
                            SELECT * FROM step_4_result 
                            ORDER BY total_casos DESC, idade_media DESC
                            LIMIT {limite}
                        """,
                        "dependencies": [4]
                    }
                ],
                parameter_extractors={
                    "respiratory_pattern": r"(j\d+|pneum|asma|dpoc|respiratór)",
                    "sexo_code": r"(mulher|feminino|homem|masculino)",
                    "idade_op": r"(mais de|menos de|acima de|abaixo de)",
                    "idade_valor": r"(\d+)\s*anos?",
                    "limite": r"(\d+)\s*(cidade|município|resultado)",
                    "min_casos": r"mais de\s*(\d+)|acima de\s*(\d+)"
                },
                optimization_hints=[
                    "Use índices em DIAG_PRINC, SEXO, IDADE",
                    "Considere particionamento por CIDADE_RESIDENCIA_PACIENTE",
                    "Cache resultados de diagnósticos respiratórios frequentes"
                ],
                examples=[
                    "Mulheres com doenças respiratórias que morreram por cidade",
                    "Homens acima de 60 anos com J90 nas 5 principais cidades",
                    "Mortes por pneumonia em idosos por município"
                ],
                metadata={
                    "medical_focus": "respiratory_diseases",
                    "cid_codes": ["J00-J99"],
                    "demographic_dimensions": ["age", "sex", "geography"]
                }
            ),
            
            EnhancedDecompositionTemplate(
                template_id="respiratory_temporal_trend",
                name="Tendência Temporal de Doenças Respiratórias",
                category=TemplateCategory.RESPIRATORY_ANALYSIS,
                description="Análise de tendências temporais em doenças respiratórias",
                query_patterns=[
                    r"(trend|tendênc|evoluç|variaç).*(respiratór|pulmão|pneum)",
                    r"(trimest|mensal|anual).*(j\d+|respiratór|pneum)",
                    r"(aument|diminu|cresc).*(respiratór|pulmão).*(tempo|período)"
                ],
                compatibility_score_threshold=0.80,
                strategy=DecompositionStrategy.TEMPORAL_BREAKDOWN,
                step_templates=[
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Filtrar casos respiratórios com datas válidas",
                        "sql_template": """
                            SELECT * FROM sus_data 
                            WHERE DIAG_PRINC LIKE 'J%'
                            AND DT_INTER IS NOT NULL AND DT_INTER != ''
                            AND LENGTH(DT_INTER) = 8
                        """,
                        "dependencies": []
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Extrair componentes temporais",
                        "sql_template": """
                            SELECT *,
                                SUBSTR(DT_INTER, 1, 4) as ano,
                                SUBSTR(DT_INTER, 5, 2) as mes,
                                CASE 
                                    WHEN SUBSTR(DT_INTER, 5, 2) IN ('01','02','03') THEN 'Q1'
                                    WHEN SUBSTR(DT_INTER, 5, 2) IN ('04','05','06') THEN 'Q2'
                                    WHEN SUBSTR(DT_INTER, 5, 2) IN ('07','08','09') THEN 'Q3'
                                    ELSE 'Q4'
                                END as trimestre,
                                ano || '-' || trimestre as periodo_trimestral
                            FROM step_1_result
                        """,
                        "dependencies": [1]
                    },
                    {
                        "step_type": QueryStepType.AGGREGATE,
                        "description": "Agregar por período temporal",
                        "sql_template": """
                            SELECT 
                                periodo_trimestral,
                                ano,
                                trimestre,
                                COUNT(*) as casos_total,
                                COUNT(CASE WHEN MORTE = 1 THEN 1 END) as mortes_total,
                                ROUND(COUNT(CASE WHEN MORTE = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as taxa_mortalidade,
                                AVG(IDADE) as idade_media
                            FROM step_2_result 
                            GROUP BY periodo_trimestral, ano, trimestre
                            ORDER BY ano, trimestre
                        """,
                        "dependencies": [2]
                    }
                ],
                parameter_extractors={
                    "periodo_tipo": r"(trimest|mensal|anual|semanal)",
                    "anos_analise": r"(\d{4})\s*(?:a|até|-)?\s*(\d{4})?",
                    "metrica_foco": r"(casos|mortes|taxa|incidênc)"
                },
                optimization_hints=[
                    "Use índices compostos em (DT_INTER, DIAG_PRINC)",
                    "Considere materializar views para períodos temporais",
                    "Cache agregações trimestrais para consultas frequentes"
                ],
                examples=[
                    "Tendência trimestral de pneumonias nos últimos 3 anos",
                    "Evolução mensal de mortes por doenças respiratórias",
                    "Variação anual de casos de J90 por região"
                ],
                metadata={
                    "temporal_focus": "trend_analysis",
                    "aggregation_levels": ["monthly", "quarterly", "yearly"],
                    "metrics": ["cases", "deaths", "mortality_rate"]
                }
            )
        ]
    
    def _create_neoplasia_templates(self) -> List[EnhancedDecompositionTemplate]:
        """Cria templates especializados para análise de neoplasias"""
        return [
            EnhancedDecompositionTemplate(
                template_id="neoplasia_comprehensive_analysis",
                name="Análise Abrangente de Neoplasias",
                category=TemplateCategory.NEOPLASIA_ANALYSIS,
                description="Análise detalhada de neoplasias por tipo, estágio e demografia",
                query_patterns=[
                    r"(neoplas|cancer|tumor|malign).*(tipo|categor|classif)",
                    r"(c\d+|cancer|tumor).*(mort|óbit).*(idade|demogr)",
                    r"(onco|cancer|tumor).*(distribu|geogr|região)"
                ],
                compatibility_score_threshold=0.85,
                strategy=DecompositionStrategy.DIAGNOSIS_CLASSIFICATION,
                step_templates=[
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Filtrar diagnósticos de neoplasias",
                        "sql_template": """
                            SELECT * FROM sus_data 
                            WHERE DIAG_PRINC LIKE 'C%'
                            AND DIAG_PRINC IS NOT NULL 
                            AND DIAG_PRINC != ''
                            AND LENGTH(DIAG_PRINC) >= 3
                        """,
                        "dependencies": []
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Classificar tipos de neoplasias",
                        "sql_template": """
                            SELECT *,
                                SUBSTR(DIAG_PRINC, 1, 3) as categoria_neoplasia,
                                CASE 
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C00' AND 'C14' THEN 'Lábio, cavidade oral e faringe'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C15' AND 'C26' THEN 'Órgãos digestivos'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C30' AND 'C39' THEN 'Órgãos respiratórios'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C40' AND 'C41' THEN 'Osso e cartilagem'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C43' AND 'C44' THEN 'Pele'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C45' AND 'C49' THEN 'Tecidos moles'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C50' AND 'C50' THEN 'Mama'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C51' AND 'C58' THEN 'Órgãos genitais femininos'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C60' AND 'C63' THEN 'Órgãos genitais masculinos'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C64' AND 'C68' THEN 'Vias urinárias'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C69' AND 'C72' THEN 'Olho, encéfalo e SNC'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C73' AND 'C75' THEN 'Glândulas endócrinas'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C76' AND 'C80' THEN 'Localizações mal definidas'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 3) BETWEEN 'C81' AND 'C96' THEN 'Tecidos linfáticos e hematopoéticos'
                                    ELSE 'Outras neoplasias'
                                END as tipo_anatomico,
                                CASE 
                                    WHEN IDADE < 18 THEN 'Pediátrico (0-17)'
                                    WHEN IDADE BETWEEN 18 AND 39 THEN 'Adulto jovem (18-39)'
                                    WHEN IDADE BETWEEN 40 AND 59 THEN 'Adulto (40-59)'
                                    WHEN IDADE BETWEEN 60 AND 79 THEN 'Idoso (60-79)'
                                    ELSE 'Muito idoso (80+)'
                                END as faixa_etaria_oncologica
                            FROM step_1_result
                        """,
                        "dependencies": [1]
                    },
                    {
                        "step_type": QueryStepType.AGGREGATE,
                        "description": "Agregar por tipo anatômico e demografia",
                        "sql_template": """
                            SELECT 
                                tipo_anatomico,
                                faixa_etaria_oncologica,
                                CASE WHEN SEXO = 1 THEN 'Masculino' ELSE 'Feminino' END as sexo_desc,
                                COUNT(*) as total_casos,
                                COUNT(CASE WHEN MORTE = 1 THEN 1 END) as total_mortes,
                                ROUND(COUNT(CASE WHEN MORTE = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as taxa_letalidade,
                                AVG(IDADE) as idade_media,
                                COUNT(DISTINCT CIDADE_RESIDENCIA_PACIENTE) as cidades_afetadas
                            FROM step_2_result 
                            GROUP BY tipo_anatomico, faixa_etaria_oncologica, sexo_desc
                            HAVING COUNT(*) >= {min_casos}
                        """,
                        "dependencies": [2]
                    },
                    {
                        "step_type": QueryStepType.RANK,
                        "description": "Ranking por impacto e letalidade",
                        "sql_template": """
                            SELECT * FROM step_3_result 
                            ORDER BY total_casos DESC, taxa_letalidade DESC
                            LIMIT {limite}
                        """,
                        "dependencies": [3]
                    }
                ],
                parameter_extractors={
                    "categoria_foco": r"(c\d+|mama|próstat|pulmão|cólon)",
                    "idade_grupo": r"(criança|jovem|adulto|idoso|pediátr)",
                    "sexo_foco": r"(homem|mulher|masculino|feminino)",
                    "limite": r"(\d+)\s*(princip|maior|top)",
                    "min_casos": r"mais de\s*(\d+)|mínimo\s*(\d+)"
                },
                optimization_hints=[
                    "Use índices em (DIAG_PRINC, IDADE, SEXO, MORTE)",
                    "Considere particionamento por categoria de neoplasia",
                    "Cache classificações anatômicas para melhor performance"
                ],
                examples=[
                    "Análise de neoplasias de mama por faixa etária",
                    "Distribuição de cânceres por tipo anatômico e letalidade",
                    "Perfil demográfico de neoplasias do sistema digestivo"
                ],
                metadata={
                    "medical_focus": "oncology",
                    "cid_codes": ["C00-C97"],
                    "anatomical_classification": "ICD-10_chapter_II",
                    "demographic_stratification": "age_sex_geography"
                }
            )
        ]
    
    def _create_demographic_complex_templates(self) -> List[EnhancedDecompositionTemplate]:
        """Cria templates para análises demográficas complexas"""
        return [
            EnhancedDecompositionTemplate(
                template_id="multi_demographic_stratification",
                name="Estratificação Demográfica Múltipla",
                category=TemplateCategory.DEMOGRAPHIC_COMPLEX,
                description="Análise com múltiplas dimensões demográficas simultâneas",
                query_patterns=[
                    r"(mulher|homem).*(idade|anos).*(mort|óbit).*(cidad|município)",
                    r"(demogr|perfil).*(idade|sexo|região).*(doença|diagnóst)",
                    r"(estratific|segment|grupo).*(população|pacient).*(caracterís)"
                ],
                compatibility_score_threshold=0.75,
                strategy=DecompositionStrategy.DEMOGRAPHIC_ANALYSIS,
                step_templates=[
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Criar segmentação demográfica avançada",
                        "sql_template": """
                            SELECT *,
                                CASE 
                                    WHEN IDADE < 1 THEN 'Neonato (0-11m)'
                                    WHEN IDADE BETWEEN 1 AND 4 THEN 'Primeira infância (1-4a)'
                                    WHEN IDADE BETWEEN 5 AND 9 THEN 'Segunda infância (5-9a)'
                                    WHEN IDADE BETWEEN 10 AND 14 THEN 'Pré-adolescente (10-14a)'
                                    WHEN IDADE BETWEEN 15 AND 19 THEN 'Adolescente (15-19a)'
                                    WHEN IDADE BETWEEN 20 AND 39 THEN 'Adulto jovem (20-39a)'
                                    WHEN IDADE BETWEEN 40 AND 59 THEN 'Adulto (40-59a)'
                                    WHEN IDADE BETWEEN 60 AND 79 THEN 'Idoso (60-79a)'
                                    ELSE 'Muito idoso (80+a)'
                                END as faixa_etaria_detalhada,
                                CASE WHEN SEXO = 1 THEN 'Masculino' ELSE 'Feminino' END as sexo_desc,
                                CASE 
                                    WHEN IDADE < 18 THEN 'Menor'
                                    WHEN IDADE BETWEEN 18 AND 64 THEN 'Adulto'
                                    ELSE 'Idoso'
                                END as categoria_etaria_legal,
                                SUBSTR(DIAG_PRINC, 1, 1) as capitulo_cid
                            FROM sus_data
                            WHERE {base_filters}
                        """,
                        "dependencies": []
                    },
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Aplicar filtros demográficos específicos",
                        "sql_template": """
                            SELECT * FROM step_1_result 
                            WHERE sexo_desc = '{sexo_filtro}'
                            AND faixa_etaria_detalhada IN ({faixas_etarias})
                            AND capitulo_cid IN ({capitulos_cid})
                        """,
                        "dependencies": [1]
                    },
                    {
                        "step_type": QueryStepType.AGGREGATE,
                        "description": "Agregar por múltiplas dimensões demográficas",
                        "sql_template": """
                            SELECT 
                                CIDADE_RESIDENCIA_PACIENTE,
                                faixa_etaria_detalhada,
                                sexo_desc,
                                capitulo_cid,
                                COUNT(*) as total_casos,
                                COUNT(CASE WHEN MORTE = 1 THEN 1 END) as total_mortes,
                                ROUND(AVG(IDADE), 1) as idade_media,
                                ROUND(COUNT(CASE WHEN MORTE = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as taxa_letalidade,
                                COUNT(DISTINCT DIAG_PRINC) as diagnosticos_distintos
                            FROM step_2_result 
                            GROUP BY CIDADE_RESIDENCIA_PACIENTE, faixa_etaria_detalhada, sexo_desc, capitulo_cid
                            HAVING COUNT(*) >= {min_casos}
                        """,
                        "dependencies": [2]
                    },
                    {
                        "step_type": QueryStepType.RANK,
                        "description": "Ranking por impacto demográfico",
                        "sql_template": """
                            SELECT 
                                CIDADE_RESIDENCIA_PACIENTE,
                                SUM(total_casos) as casos_cidade,
                                SUM(total_mortes) as mortes_cidade,
                                ROUND(SUM(total_mortes) * 100.0 / SUM(total_casos), 2) as taxa_letalidade_cidade,
                                COUNT(*) as segmentos_demograficos
                            FROM step_3_result 
                            GROUP BY CIDADE_RESIDENCIA_PACIENTE
                            ORDER BY casos_cidade DESC, taxa_letalidade_cidade DESC
                            LIMIT {limite}
                        """,
                        "dependencies": [3]
                    }
                ],
                parameter_extractors={
                    "sexo_filtro": r"(mulher|feminino|homem|masculino)",
                    "faixas_etarias": r"(criança|jovem|adulto|idoso|adolesc)",
                    "capitulos_cid": r"([a-z]\d*|respiratór|cardiac|neoplas)",
                    "base_filters": r"(diagnóst|doença|mort)",
                    "limite": r"(\d+)\s*(princip|cidade|município)",
                    "min_casos": r"mais de\s*(\d+)|acima de\s*(\d+)"
                },
                optimization_hints=[
                    "Use índices compostos em (CIDADE_RESIDENCIA_PACIENTE, IDADE, SEXO)",
                    "Considere vistas materializadas para segmentações demográficas",
                    "Cache agregações por capítulo CID para consultas frequentes"
                ],
                examples=[
                    "Perfil demográfico completo de mortes por cidade",
                    "Segmentação etária e sexual de doenças cardiovasculares",
                    "Análise multi-dimensional de população SUS por município"
                ],
                metadata={
                    "demographic_dimensions": ["age_detailed", "sex", "geography", "disease_chapter"],
                    "segmentation_type": "multi_dimensional",
                    "aggregation_levels": ["individual", "segment", "city", "region"]
                }
            )
        ]
    
    def _create_temporal_advanced_templates(self) -> List[EnhancedDecompositionTemplate]:
        """Cria templates para análises temporais avançadas"""
        return [
            EnhancedDecompositionTemplate(
                template_id="temporal_cohort_analysis",
                name="Análise de Coorte Temporal",
                category=TemplateCategory.TEMPORAL_ADVANCED,
                description="Análise de coortes com follow-up temporal e métricas de sobrevivência",
                query_patterns=[
                    r"(coorte|cohort|acompanha|seguiment).*(tempo|período|sobreviv)",
                    r"(tempo.*(intern|hospitaliz|tratament)).*(média|median|distribu)",
                    r"(evoluç|progr|desfech).*(tempo|período|prazo)"
                ],
                compatibility_score_threshold=0.80,
                strategy=DecompositionStrategy.TEMPORAL_BREAKDOWN,
                step_templates=[
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Filtrar casos com dados temporais válidos",
                        "sql_template": """
                            SELECT * FROM sus_data 
                            WHERE DT_INTER IS NOT NULL AND DT_INTER != ''
                            AND DT_SAIDA IS NOT NULL AND DT_SAIDA != ''
                            AND LENGTH(DT_INTER) = 8 AND LENGTH(DT_SAIDA) = 8
                            AND DT_SAIDA >= DT_INTER
                            AND {temporal_filters}
                        """,
                        "dependencies": []
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Calcular métricas temporais avançadas",
                        "sql_template": """
                            SELECT *,
                                -- Cálculo de tempo de internação
                                JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                                JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2)) 
                                as tempo_internacao_dias,
                                
                                -- Período de entrada (trimestre)
                                SUBSTR(DT_INTER, 1, 4) as ano_internacao,
                                CASE 
                                    WHEN SUBSTR(DT_INTER, 5, 2) IN ('01','02','03') THEN 'Q1'
                                    WHEN SUBSTR(DT_INTER, 5, 2) IN ('04','05','06') THEN 'Q2'
                                    WHEN SUBSTR(DT_INTER, 5, 2) IN ('07','08','09') THEN 'Q3'
                                    ELSE 'Q4'
                                END as trimestre_internacao,
                                
                                -- Classificação de tempo de internação
                                CASE 
                                    WHEN JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                                         JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2)) <= 1 
                                    THEN 'Muito curta (≤1 dia)'
                                    WHEN JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                                         JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2)) BETWEEN 2 AND 7 
                                    THEN 'Curta (2-7 dias)'
                                    WHEN JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                                         JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2)) BETWEEN 8 AND 30 
                                    THEN 'Média (8-30 dias)'
                                    WHEN JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                                         JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2)) > 30 
                                    THEN 'Longa (>30 dias)'
                                    ELSE 'Indeterminada'
                                END as categoria_tempo_internacao
                            FROM step_1_result
                            WHERE JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                                  JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2)) >= 0
                            AND JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                                JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2)) <= 365
                        """,
                        "dependencies": [1]
                    },
                    {
                        "step_type": QueryStepType.AGGREGATE,
                        "description": "Agregar métricas de coorte por período",
                        "sql_template": """
                            SELECT 
                                ano_internacao,
                                trimestre_internacao,
                                categoria_tempo_internacao,
                                COUNT(*) as total_casos,
                                COUNT(CASE WHEN MORTE = 1 THEN 1 END) as total_mortes,
                                ROUND(AVG(tempo_internacao_dias), 2) as tempo_medio_internacao,
                                ROUND(MIN(tempo_internacao_dias), 2) as tempo_min_internacao,
                                ROUND(MAX(tempo_internacao_dias), 2) as tempo_max_internacao,
                                ROUND(COUNT(CASE WHEN MORTE = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as taxa_letalidade,
                                AVG(IDADE) as idade_media_coorte,
                                COUNT(DISTINCT CIDADE_RESIDENCIA_PACIENTE) as cidades_origem
                            FROM step_2_result 
                            GROUP BY ano_internacao, trimestre_internacao, categoria_tempo_internacao
                            HAVING COUNT(*) >= {min_casos}
                        """,
                        "dependencies": [2]
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Calcular indicadores de tendência temporal",
                        "sql_template": """
                            SELECT *,
                                -- Calcular tendência de letalidade ao longo do tempo
                                LAG(taxa_letalidade) OVER (
                                    PARTITION BY categoria_tempo_internacao 
                                    ORDER BY ano_internacao, trimestre_internacao
                                ) as taxa_letalidade_anterior,
                                
                                taxa_letalidade - LAG(taxa_letalidade) OVER (
                                    PARTITION BY categoria_tempo_internacao 
                                    ORDER BY ano_internacao, trimestre_internacao
                                ) as variacao_letalidade,
                                
                                -- Calcular tendência de tempo médio
                                LAG(tempo_medio_internacao) OVER (
                                    PARTITION BY categoria_tempo_internacao 
                                    ORDER BY ano_internacao, trimestre_internacao
                                ) as tempo_medio_anterior,
                                
                                tempo_medio_internacao - LAG(tempo_medio_internacao) OVER (
                                    PARTITION BY categoria_tempo_internacao 
                                    ORDER BY ano_internacao, trimestre_internacao
                                ) as variacao_tempo_medio
                                
                            FROM step_3_result
                        """,
                        "dependencies": [3]
                    }
                ],
                parameter_extractors={
                    "temporal_filters": r"(201\d|202\d|ano|trimest)",
                    "periodo_analise": r"(\d{4})\s*(?:a|até|-)?\s*(\d{4})?",
                    "metrica_foco": r"(sobreviv|letalidad|tempo|internação)",
                    "min_casos": r"mais de\s*(\d+)|mínimo\s*(\d+)"
                },
                optimization_hints=[
                    "Use índices em (DT_INTER, DT_SAIDA, MORTE)",
                    "Considere particionamento temporal para grandes volumes",
                    "Cache cálculos de JULIANDAY para melhor performance"
                ],
                examples=[
                    "Análise de coorte de internações por trimestre com sobrevivência",
                    "Evolução temporal de tempo de internação por tipo de alta",
                    "Métricas de follow-up de pacientes por período de entrada"
                ],
                metadata={
                    "temporal_focus": "cohort_analysis",
                    "metrics": ["length_of_stay", "mortality_rate", "temporal_trends"],
                    "analysis_type": "longitudinal"
                }
            )
        ]
    
    def _create_geographic_correlation_templates(self) -> List[EnhancedDecompositionTemplate]:
        """Cria templates para correlações geográficas"""
        return [
            EnhancedDecompositionTemplate(
                template_id="geographic_disease_correlation",
                name="Correlação Geográfica de Doenças",
                category=TemplateCategory.GEOGRAPHIC_CORRELATION,
                description="Análise de correlações entre localização geográfica e padrões de doença",
                query_patterns=[
                    r"(região|geogr|cidad|municip).*(doença|diagnóst|mort)",
                    r"(distribu|mapa|localiz).*(doença|incidênc|prevalênc)",
                    r"(cluster|agrupam|concentr).*(geogr|region|espacial)"
                ],
                compatibility_score_threshold=0.75,
                strategy=DecompositionStrategy.GEOGRAPHIC_ANALYSIS,
                step_templates=[
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Filtrar dados com localização válida",
                        "sql_template": """
                            SELECT * FROM sus_data 
                            WHERE CIDADE_RESIDENCIA_PACIENTE IS NOT NULL 
                            AND CIDADE_RESIDENCIA_PACIENTE != ''
                            AND TRIM(CIDADE_RESIDENCIA_PACIENTE) != ''
                            AND {disease_filters}
                        """,
                        "dependencies": []
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Enriquecer com dados geográficos",
                        "sql_template": """
                            SELECT *,
                                -- Normalizar nomes de cidades
                                UPPER(TRIM(CIDADE_RESIDENCIA_PACIENTE)) as cidade_normalizada,
                                
                                -- Classificar por tamanho de cidade (estimativa baseada em volume de dados)
                                CASE 
                                    WHEN CIDADE_RESIDENCIA_PACIENTE IN ('PORTO ALEGRE', 'CAXIAS DO SUL', 'PELOTAS', 'CANOAS', 'SANTA MARIA') 
                                    THEN 'Metrópole'
                                    WHEN CIDADE_RESIDENCIA_PACIENTE IN ('NOVO HAMBURGO', 'SAO LEOPOLDO', 'RIO GRANDE', 'ALVORADA', 'GRAVATAI')
                                    THEN 'Grande'
                                    ELSE 'Média/Pequena'
                                END as porte_cidade,
                                
                                -- Classificar região (baseado em conhecimento de RS)
                                CASE 
                                    WHEN CIDADE_RESIDENCIA_PACIENTE IN ('PORTO ALEGRE', 'CANOAS', 'ALVORADA', 'CACHOEIRINHA', 'GRAVATAI', 'VIAMAO')
                                    THEN 'Região Metropolitana'
                                    WHEN CIDADE_RESIDENCIA_PACIENTE IN ('CAXIAS DO SUL', 'BENTO GONCALVES', 'FARROUPILHA', 'FLORES DA CUNHA')
                                    THEN 'Serra Gaúcha'
                                    WHEN CIDADE_RESIDENCIA_PACIENTE IN ('PELOTAS', 'RIO GRANDE', 'JAGUARAO', 'SAGUENAY')
                                    THEN 'Região Sul'
                                    WHEN CIDADE_RESIDENCIA_PACIENTE IN ('SANTA MARIA', 'URUGUAIANA', 'ALEGRETE', 'SANTIAGO')
                                    THEN 'Região Central/Fronteira'
                                    ELSE 'Interior'
                                END as regiao_geografica
                            FROM step_1_result
                        """,
                        "dependencies": [1]
                    },
                    {
                        "step_type": QueryStepType.AGGREGATE,
                        "description": "Agregar por localização e calcular métricas epidemiológicas",
                        "sql_template": """
                            SELECT 
                                cidade_normalizada,
                                porte_cidade,
                                regiao_geografica,
                                COUNT(*) as total_casos,
                                COUNT(CASE WHEN MORTE = 1 THEN 1 END) as total_mortes,
                                ROUND(COUNT(CASE WHEN MORTE = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as taxa_letalidade,
                                COUNT(DISTINCT DIAG_PRINC) as diagnosticos_distintos,
                                AVG(IDADE) as idade_media,
                                COUNT(CASE WHEN SEXO = 1 THEN 1 END) as casos_masculino,
                                COUNT(CASE WHEN SEXO = 3 THEN 1 END) as casos_feminino,
                                -- Concentração de diagnósticos específicos
                                COUNT(CASE WHEN DIAG_PRINC LIKE 'J%' THEN 1 END) as casos_respiratorios,
                                COUNT(CASE WHEN DIAG_PRINC LIKE 'I%' THEN 1 END) as casos_cardiovasculares,
                                COUNT(CASE WHEN DIAG_PRINC LIKE 'C%' THEN 1 END) as casos_neoplasias
                            FROM step_2_result 
                            GROUP BY cidade_normalizada, porte_cidade, regiao_geografica
                            HAVING COUNT(*) >= {min_casos}
                        """,
                        "dependencies": [2]
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Calcular indicadores de correlação geográfica",
                        "sql_template": """
                            SELECT *,
                                -- Índice de concentração de casos por porte de cidade
                                ROUND(total_casos * 100.0 / SUM(total_casos) OVER (PARTITION BY porte_cidade), 2) as prop_casos_no_porte,
                                
                                -- Índice de letalidade relativa
                                ROUND(taxa_letalidade / AVG(taxa_letalidade) OVER (), 2) as indice_letalidade_relativa,
                                
                                -- Diversidade diagnóstica
                                ROUND(diagnosticos_distintos * 100.0 / total_casos, 2) as indice_diversidade_diagnostica,
                                
                                -- Prevalência de condições específicas
                                ROUND(casos_respiratorios * 100.0 / total_casos, 2) as prev_respiratorios,
                                ROUND(casos_cardiovasculares * 100.0 / total_casos, 2) as prev_cardiovasculares,
                                ROUND(casos_neoplasias * 100.0 / total_casos, 2) as prev_neoplasias,
                                
                                -- Ranking dentro da região
                                ROW_NUMBER() OVER (PARTITION BY regiao_geografica ORDER BY total_casos DESC) as rank_casos_regiao,
                                ROW_NUMBER() OVER (PARTITION BY regiao_geografica ORDER BY taxa_letalidade DESC) as rank_letalidade_regiao
                                
                            FROM step_3_result
                        """,
                        "dependencies": [3]
                    },
                    {
                        "step_type": QueryStepType.RANK,
                        "description": "Ranking final por correlação geográfica",
                        "sql_template": """
                            SELECT * FROM step_4_result 
                            ORDER BY 
                                CASE '{order_by}' 
                                    WHEN 'casos' THEN total_casos 
                                    WHEN 'letalidade' THEN taxa_letalidade 
                                    WHEN 'diversidade' THEN indice_diversidade_diagnostica
                                    ELSE total_casos 
                                END DESC
                            LIMIT {limite}
                        """,
                        "dependencies": [4]
                    }
                ],
                parameter_extractors={
                    "disease_filters": r"(diagnóst|doença|j\d+|i\d+|c\d+)",
                    "regiao_foco": r"(metropolit|serra|sul|central|interior)",
                    "metrica_ordem": r"(casos|mortes|letalidad|diversidad)",
                    "min_casos": r"mais de\s*(\d+)|acima de\s*(\d+)",
                    "limite": r"(\d+)\s*(princip|cidade|município)",
                    "order_by": r"(casos|letalidade|diversidade|concentração)"
                },
                optimization_hints=[
                    "Use índices em (CIDADE_RESIDENCIA_PACIENTE, DIAG_PRINC)",
                    "Considere normalização de nomes de cidades em tabela separada",
                    "Cache agregações geográficas para consultas frequentes"
                ],
                examples=[
                    "Correlação entre porte de cidade e diversidade diagnóstica",
                    "Mapa de letalidade por região geográfica no RS",
                    "Clusters de doenças respiratórias por localização"
                ],
                metadata={
                    "geographic_focus": "municipality_analysis",
                    "correlation_types": ["disease_geography", "mortality_location", "demographic_spatial"],
                    "regional_classification": "rio_grande_do_sul"
                }
            )
        ]
    
    def _create_financial_analysis_templates(self) -> List[EnhancedDecompositionTemplate]:
        """Cria templates para análises financeiras"""
        return [
            EnhancedDecompositionTemplate(
                template_id="comprehensive_cost_analysis",
                name="Análise Abrangente de Custos",
                category=TemplateCategory.FINANCIAL_ANALYSIS,
                description="Análise detalhada de custos por procedimento, complexidade e desfecho",
                query_patterns=[
                    r"(custo|valor|gasto|financ).*(procediment|tratament)",
                    r"(mais caro|maior custo|alto valor).*(procediment|diagnóst)",
                    r"(análise.*(econôm|financ|custo)).*(sus|saúde)"
                ],
                compatibility_score_threshold=0.80,
                strategy=DecompositionStrategy.FINANCIAL_AGGREGATION,
                step_templates=[
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Filtrar registros com dados financeiros válidos",
                        "sql_template": """
                            SELECT * FROM sus_data 
                            WHERE VAL_TOT IS NOT NULL 
                            AND VAL_TOT > 0
                            AND PROC_REA IS NOT NULL 
                            AND PROC_REA != ''
                            AND {financial_filters}
                        """,
                        "dependencies": []
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Enriquecer com classificações de custo e complexidade",
                        "sql_template": """
                            SELECT *,
                                -- Classificação de valor
                                CASE 
                                    WHEN VAL_TOT <= 100 THEN 'Baixo custo (≤R$ 100)'
                                    WHEN VAL_TOT BETWEEN 101 AND 500 THEN 'Custo moderado (R$ 101-500)'
                                    WHEN VAL_TOT BETWEEN 501 AND 2000 THEN 'Alto custo (R$ 501-2000)'
                                    WHEN VAL_TOT BETWEEN 2001 AND 10000 THEN 'Muito alto custo (R$ 2001-10000)'
                                    ELSE 'Custo excepcional (>R$ 10000)'
                                END as categoria_custo,
                                
                                -- Classificação de complexidade por procedimento
                                CASE 
                                    WHEN SUBSTR(PROC_REA, 1, 2) IN ('01', '02', '03') THEN 'Atenção básica'
                                    WHEN SUBSTR(PROC_REA, 1, 2) IN ('04', '05', '06') THEN 'Média complexidade'
                                    WHEN SUBSTR(PROC_REA, 1, 2) IN ('07', '08', '09') THEN 'Alta complexidade'
                                    ELSE 'Complexidade indeterminada'
                                END as complexidade_procedimento,
                                
                                -- Custo por dia de internação
                                CASE 
                                    WHEN DT_SAIDA IS NOT NULL AND DT_SAIDA != '' AND DT_INTER IS NOT NULL AND DT_INTER != ''
                                    THEN ROUND(VAL_TOT / GREATEST(1, 
                                        JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                                        JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2))
                                    ), 2)
                                    ELSE VAL_TOT
                                END as custo_por_dia,
                                
                                -- Classificação de desfecho financeiro
                                CASE 
                                    WHEN MORTE = 1 THEN 'Óbito'
                                    ELSE 'Alta'
                                END as desfecho
                            FROM step_1_result
                        """,
                        "dependencies": [1]
                    },
                    {
                        "step_type": QueryStepType.AGGREGATE,
                        "description": "Agregar custos por procedimento e características",
                        "sql_template": """
                            SELECT 
                                PROC_REA,
                                complexidade_procedimento,
                                categoria_custo,
                                desfecho,
                                COUNT(*) as total_casos,
                                ROUND(SUM(VAL_TOT), 2) as custo_total,
                                ROUND(AVG(VAL_TOT), 2) as custo_medio,
                                ROUND(MIN(VAL_TOT), 2) as custo_minimo,
                                ROUND(MAX(VAL_TOT), 2) as custo_maximo,
                                ROUND(AVG(custo_por_dia), 2) as custo_medio_por_dia,
                                AVG(IDADE) as idade_media_pacientes,
                                COUNT(CASE WHEN SEXO = 1 THEN 1 END) as casos_masculino,
                                COUNT(CASE WHEN SEXO = 3 THEN 1 END) as casos_feminino,
                                COUNT(DISTINCT CIDADE_RESIDENCIA_PACIENTE) as cidades_atendidas
                            FROM step_2_result 
                            GROUP BY PROC_REA, complexidade_procedimento, categoria_custo, desfecho
                            HAVING COUNT(*) >= {min_casos}
                        """,
                        "dependencies": [2]
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Calcular métricas de eficiência e impacto financeiro",
                        "sql_template": """
                            SELECT *,
                                -- Eficiência por caso
                                ROUND(custo_total / total_casos, 2) as custo_por_caso,
                                
                                -- Proporção do custo total
                                ROUND(custo_total * 100.0 / SUM(custo_total) OVER (), 2) as prop_custo_total,
                                
                                -- Índice de custo relativo
                                ROUND(custo_medio / AVG(custo_medio) OVER (PARTITION BY complexidade_procedimento), 2) as indice_custo_relativo,
                                
                                -- Variabilidade de custo
                                ROUND((custo_maximo - custo_minimo) / custo_medio * 100, 2) as coef_variacao_custo,
                                
                                -- Taxa de efetividade (sobrevivência vs custo)
                                ROUND(
                                    CASE WHEN desfecho = 'Alta' THEN total_casos * 100.0 / (custo_total / 1000) ELSE 0 END, 
                                    2
                                ) as taxa_efetividade,
                                
                                -- Ranking de custo dentro da complexidade
                                ROW_NUMBER() OVER (PARTITION BY complexidade_procedimento ORDER BY custo_total DESC) as rank_custo_complexidade
                                
                            FROM step_3_result
                        """,
                        "dependencies": [3]
                    },
                    {
                        "step_type": QueryStepType.RANK,
                        "description": "Ranking final por critério financeiro",
                        "sql_template": """
                            SELECT * FROM step_4_result 
                            WHERE complexidade_procedimento IN ({complexidades_filtro})
                            ORDER BY 
                                CASE '{criterio_ranking}' 
                                    WHEN 'custo_total' THEN custo_total
                                    WHEN 'custo_medio' THEN custo_medio 
                                    WHEN 'efetividade' THEN taxa_efetividade
                                    WHEN 'variabilidade' THEN coef_variacao_custo
                                    ELSE custo_total 
                                END DESC
                            LIMIT {limite}
                        """,
                        "dependencies": [4]
                    }
                ],
                parameter_extractors={
                    "financial_filters": r"(procediment|diagnóst|alta.complex|média.complex)",
                    "complexidades_filtro": r"(básica|média|alta|todas)",
                    "criterio_ranking": r"(custo|efetividad|variabilidad|volume)",
                    "min_casos": r"mais de\s*(\d+)|acima de\s*(\d+)",
                    "limite": r"(\d+)\s*(princip|procediment|maior)"
                },
                optimization_hints=[
                    "Use índices em (PROC_REA, VAL_TOT, MORTE)",
                    "Considere particionamento por complexidade de procedimento",
                    "Cache agregações financeiras para relatórios gerenciais"
                ],
                examples=[
                    "Análise de custo-efetividade de procedimentos de alta complexidade",
                    "Ranking de procedimentos por custo total e variabilidade",
                    "Impacto financeiro de desfechos por tipo de procedimento"
                ],
                metadata={
                    "financial_focus": "cost_effectiveness",
                    "metrics": ["total_cost", "average_cost", "cost_per_day", "effectiveness_ratio"],
                    "complexity_levels": ["basic", "medium", "high"]
                }
            )
        ]
    
    def _create_multi_condition_templates(self) -> List[EnhancedDecompositionTemplate]:
        """Cria templates para análises multi-condição"""
        return [
            EnhancedDecompositionTemplate(
                template_id="multi_condition_interaction",
                name="Interação entre Múltiplas Condições",
                category=TemplateCategory.MULTI_CONDITION,
                description="Análise de interações entre múltiplas condições médicas",
                query_patterns=[
                    r"(comorbidad|múltip|várias).*(doença|diagnóst|condição)",
                    r"(associaç|correlaç|interact).*(diagnóst|doença)",
                    r"(combinaç|conjunto).*(diagnóst|condição|doença)"
                ],
                compatibility_score_threshold=0.85,
                strategy=DecompositionStrategy.DIAGNOSIS_CLASSIFICATION,
                step_templates=[
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Filtrar casos com diagnósticos válidos",
                        "sql_template": """
                            SELECT * FROM sus_data 
                            WHERE DIAG_PRINC IS NOT NULL AND DIAG_PRINC != ''
                            AND LENGTH(DIAG_PRINC) >= 3
                            AND {condition_filters}
                        """,
                        "dependencies": []
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Classificar sistemas e condições",
                        "sql_template": """
                            SELECT *,
                                -- Classificação por sistema corporal
                                CASE 
                                    WHEN SUBSTR(DIAG_PRINC, 1, 1) = 'I' THEN 'Cardiovascular'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 1) = 'J' THEN 'Respiratório'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 1) = 'C' THEN 'Neoplasias'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 1) = 'K' THEN 'Digestivo'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 1) = 'N' THEN 'Genitourinário'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 1) = 'E' THEN 'Endócrino'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 1) = 'F' THEN 'Mental'
                                    WHEN SUBSTR(DIAG_PRINC, 1, 1) = 'G' THEN 'Neurológico'
                                    ELSE 'Outros'
                                END as sistema_corporal,
                                
                                -- Identificar condições crônicas comuns
                                CASE 
                                    WHEN DIAG_PRINC LIKE 'I10%' OR DIAG_PRINC LIKE 'I11%' OR DIAG_PRINC LIKE 'I12%' THEN 'Hipertensão'
                                    WHEN DIAG_PRINC LIKE 'E10%' OR DIAG_PRINC LIKE 'E11%' OR DIAG_PRINC LIKE 'E14%' THEN 'Diabetes'
                                    WHEN DIAG_PRINC LIKE 'J44%' OR DIAG_PRINC LIKE 'J45%' THEN 'DPOC/Asma'
                                    WHEN DIAG_PRINC LIKE 'I20%' OR DIAG_PRINC LIKE 'I21%' OR DIAG_PRINC LIKE 'I25%' THEN 'Doença Coronária'
                                    WHEN DIAG_PRINC LIKE 'N18%' OR DIAG_PRINC LIKE 'N19%' THEN 'Doença Renal Crônica'
                                    WHEN DIAG_PRINC LIKE 'F32%' OR DIAG_PRINC LIKE 'F33%' THEN 'Depressão'
                                    ELSE 'Outras condições'
                                END as condicao_cronica,
                                
                                -- Indicadores de gravidade
                                CASE 
                                    WHEN MORTE = 1 THEN 'Fatal'
                                    WHEN VAL_TOT > 5000 THEN 'Alto custo'
                                    WHEN IDADE > 65 THEN 'Idade avançada'
                                    ELSE 'Padrão'
                                END as indicador_gravidade
                            FROM step_1_result
                        """,
                        "dependencies": [1]
                    },
                    {
                        "step_type": QueryStepType.AGGREGATE,
                        "description": "Agregar por combinações de sistemas e condições",
                        "sql_template": """
                            SELECT 
                                sistema_corporal,
                                condicao_cronica,
                                indicador_gravidade,
                                COUNT(*) as total_casos,
                                COUNT(CASE WHEN MORTE = 1 THEN 1 END) as total_mortes,
                                ROUND(COUNT(CASE WHEN MORTE = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as taxa_letalidade,
                                AVG(IDADE) as idade_media,
                                COUNT(CASE WHEN SEXO = 1 THEN 1 END) as casos_masculino,
                                COUNT(CASE WHEN SEXO = 3 THEN 1 END) as casos_feminino,
                                ROUND(AVG(VAL_TOT), 2) as custo_medio,
                                COUNT(DISTINCT CIDADE_RESIDENCIA_PACIENTE) as cidades_origem,
                                COUNT(DISTINCT DIAG_PRINC) as diagnosticos_especificos
                            FROM step_2_result 
                            GROUP BY sistema_corporal, condicao_cronica, indicador_gravidade
                            HAVING COUNT(*) >= {min_casos}
                        """,
                        "dependencies": [2]
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Calcular métricas de interação entre condições",
                        "sql_template": """
                            SELECT *,
                                -- Índice de comorbidade por sistema
                                ROW_NUMBER() OVER (PARTITION BY sistema_corporal ORDER BY total_casos DESC) as rank_sistema,
                                
                                -- Concentração de casos por condição crônica
                                ROUND(total_casos * 100.0 / SUM(total_casos) OVER (PARTITION BY condicao_cronica), 2) as prop_casos_condicao,
                                
                                -- Índice de severidade
                                ROUND(
                                    (taxa_letalidade * 0.4) + 
                                    (CASE WHEN custo_medio > AVG(custo_medio) OVER () THEN 30 ELSE 0 END) +
                                    (CASE WHEN idade_media > 65 THEN 30 ELSE 0 END), 
                                    2
                                ) as indice_severidade,
                                
                                -- Diversidade geográfica
                                ROUND(cidades_origem * 100.0 / total_casos, 2) as indice_dispersao_geografica,
                                
                                -- Complexidade diagnóstica
                                ROUND(diagnosticos_especificos * 100.0 / total_casos, 2) as indice_complexidade_diagnostica
                                
                            FROM step_3_result
                        """,
                        "dependencies": [3]
                    }
                ],
                parameter_extractors={
                    "condition_filters": r"(cardiov|respir|neoplas|digest|renal|mental)",
                    "sistemas_foco": r"(cardiov|respir|digest|neurolog|endocrin)",
                    "gravidade_filtro": r"(grave|severo|fatal|crônic)",
                    "min_casos": r"mais de\s*(\d+)|acima de\s*(\d+)"
                },
                optimization_hints=[
                    "Use índices em (DIAG_PRINC, IDADE, SEXO, MORTE, VAL_TOT)",
                    "Considere vistas materializadas para classificações de sistema",
                    "Cache mapping de CID para sistemas corporais"
                ],
                examples=[
                    "Interação entre diabetes e doenças cardiovasculares",
                    "Comorbidades mais frequentes em idosos",
                    "Padrões de múltiplas condições crônicas por região"
                ],
                metadata={
                    "medical_focus": "comorbidity_analysis",
                    "body_systems": ["cardiovascular", "respiratory", "digestive", "neurological", "endocrine"],
                    "interaction_types": ["system_overlap", "chronic_conditions", "severity_patterns"]
                }
            )
        ]
    
    def _create_ranking_detailed_templates(self) -> List[EnhancedDecompositionTemplate]:
        """Cria templates para rankings detalhados"""
        return [
            EnhancedDecompositionTemplate(
                template_id="comprehensive_ranking_analysis",
                name="Análise de Ranking Abrangente",
                category=TemplateCategory.RANKING_DETAILED,
                description="Rankings detalhados com múltiplas métricas e contexto",
                query_patterns=[
                    r"(ranking|top|maior|princip|primeiro).*(cidade|município|procediment)",
                    r"(comparaç|ordem|classific).*(cidade|município|diagnóst)",
                    r"(\d+).*(maior|princip|primeiro).*(cidade|município|região)"
                ],
                compatibility_score_threshold=0.70,
                strategy=DecompositionStrategy.RANKING_WITH_DETAILS,
                step_templates=[
                    {
                        "step_type": QueryStepType.FILTER,
                        "description": "Filtrar dados base para ranking",
                        "sql_template": """
                            SELECT * FROM sus_data 
                            WHERE {ranking_entity} IS NOT NULL 
                            AND {ranking_entity} != ''
                            AND {ranking_filters}
                        """,
                        "dependencies": []
                    },
                    {
                        "step_type": QueryStepType.AGGREGATE,
                        "description": "Calcular métricas principais para ranking",
                        "sql_template": """
                            SELECT 
                                {ranking_entity},
                                COUNT(*) as total_casos,
                                COUNT(CASE WHEN MORTE = 1 THEN 1 END) as total_mortes,
                                ROUND(COUNT(CASE WHEN MORTE = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as taxa_letalidade,
                                AVG(IDADE) as idade_media,
                                ROUND(AVG(VAL_TOT), 2) as custo_medio,
                                SUM(VAL_TOT) as custo_total,
                                COUNT(CASE WHEN SEXO = 1 THEN 1 END) as casos_masculino,
                                COUNT(CASE WHEN SEXO = 3 THEN 1 END) as casos_feminino,
                                COUNT(DISTINCT DIAG_PRINC) as diagnosticos_distintos,
                                COUNT(DISTINCT PROC_REA) as procedimentos_distintos
                            FROM step_1_result 
                            GROUP BY {ranking_entity}
                            HAVING COUNT(*) >= {min_casos}
                        """,
                        "dependencies": [1]
                    },
                    {
                        "step_type": QueryStepType.CALCULATE,
                        "description": "Enriquecer com métricas contextuais",
                        "sql_template": """
                            SELECT *,
                                -- Rankings específicos
                                ROW_NUMBER() OVER (ORDER BY total_casos DESC) as rank_casos,
                                ROW_NUMBER() OVER (ORDER BY total_mortes DESC) as rank_mortes,
                                ROW_NUMBER() OVER (ORDER BY taxa_letalidade DESC) as rank_letalidade,
                                ROW_NUMBER() OVER (ORDER BY custo_total DESC) as rank_custo_total,
                                ROW_NUMBER() OVER (ORDER BY diagnosticos_distintos DESC) as rank_diversidade,
                                
                                -- Percentis
                                PERCENT_RANK() OVER (ORDER BY total_casos) as percentil_casos,
                                PERCENT_RANK() OVER (ORDER BY taxa_letalidade) as percentil_letalidade,
                                PERCENT_RANK() OVER (ORDER BY custo_medio) as percentil_custo,
                                
                                -- Proporções
                                ROUND(total_casos * 100.0 / SUM(total_casos) OVER (), 2) as prop_casos_total,
                                ROUND(custo_total * 100.0 / SUM(custo_total) OVER (), 2) as prop_custo_total,
                                
                                -- Índices compostos
                                ROUND(
                                    (total_casos * 0.4) + 
                                    (total_mortes * 0.3) + 
                                    (custo_total / 1000 * 0.3), 
                                    2
                                ) as indice_impacto_geral,
                                
                                -- Classificação de perfil
                                CASE 
                                    WHEN taxa_letalidade > 15 AND custo_medio > 2000 THEN 'Alto risco e custo'
                                    WHEN taxa_letalidade > 15 THEN 'Alto risco'
                                    WHEN custo_medio > 2000 THEN 'Alto custo'
                                    WHEN total_casos > 100 THEN 'Alto volume'
                                    ELSE 'Padrão'
                                END as perfil_entidade
                                
                            FROM step_2_result
                        """,
                        "dependencies": [2]
                    },
                    {
                        "step_type": QueryStepType.RANK,
                        "description": "Ranking final ordenado com contexto",
                        "sql_template": """
                            SELECT 
                                rank_casos as posicao,
                                {ranking_entity} as entidade,
                                total_casos,
                                total_mortes,
                                taxa_letalidade,
                                custo_total,
                                custo_medio,
                                diagnosticos_distintos,
                                perfil_entidade,
                                indice_impacto_geral,
                                ROUND(prop_casos_total, 1) || '%' as participacao_casos,
                                ROUND(prop_custo_total, 1) || '%' as participacao_custo
                            FROM step_3_result 
                            ORDER BY 
                                CASE '{criterio_ranking}' 
                                    WHEN 'casos' THEN total_casos
                                    WHEN 'mortes' THEN total_mortes 
                                    WHEN 'letalidade' THEN taxa_letalidade
                                    WHEN 'custo' THEN custo_total
                                    WHEN 'impacto' THEN indice_impacto_geral
                                    ELSE total_casos 
                                END DESC
                            LIMIT {limite}
                        """,
                        "dependencies": [3]
                    }
                ],
                parameter_extractors={
                    "ranking_entity": r"(cidade|município|procediment|diagnóst)",
                    "ranking_filters": r"(mort|todos|específic)",
                    "criterio_ranking": r"(casos|mortes|letalidad|custo|impacto)",
                    "limite": r"(\d+)",
                    "min_casos": r"mais de\s*(\d+)|acima de\s*(\d+)"
                },
                optimization_hints=[
                    "Use índices na entidade de ranking principal",
                    "Considere particionamento para grandes volumes",
                    "Cache rankings frequentes em tabelas materialized"
                ],
                examples=[
                    "Top 10 cidades por total de casos com contexto detalhado",
                    "Ranking de procedimentos por impacto geral no sistema",
                    "Classificação de municípios por perfil de risco e custo"
                ],
                metadata={
                    "ranking_focus": "comprehensive_analysis",
                    "metrics": ["volume", "mortality", "cost", "diversity", "impact"],
                    "context_levels": ["absolute", "relative", "percentile", "profile"]
                }
            )
        ]
    
    def find_matching_templates(
        self, 
        query: str, 
        analysis: ComplexityAnalysis,
        min_score: float = 0.5
    ) -> List[Tuple[EnhancedDecompositionTemplate, float]]:
        """
        Encontra templates que correspondem à query
        """
        matching_templates = []
        query_lower = query.lower()
        
        for template in self.templates:
            # Calcular score de compatibilidade
            pattern_matches = 0
            total_patterns = len(template.query_patterns)
            
            for pattern in template.query_patterns:
                if re.search(pattern, query_lower):
                    pattern_matches += 1
            
            # Score baseado em padrões + análise de complexidade
            pattern_score = pattern_matches / total_patterns if total_patterns > 0 else 0
            
            # Boost se a estratégia recomendada coincide
            strategy_boost = 0.2 if analysis.recommended_strategy == template.strategy else 0
            
            # Score final
            final_score = pattern_score + strategy_boost
            
            if final_score >= min_score and final_score >= template.compatibility_score_threshold:
                matching_templates.append((template, final_score))
        
        # Ordenar por score decrescente
        matching_templates.sort(key=lambda x: x[1], reverse=True)
        
        self.logger.info(f"Encontrados {len(matching_templates)} templates compatíveis para query")
        
        return matching_templates
    
    def get_template_by_id(self, template_id: str) -> Optional[EnhancedDecompositionTemplate]:
        """Recupera template por ID"""
        for template in self.templates:
            if template.template_id == template_id:
                return template
        return None
    
    def get_templates_by_category(self, category: TemplateCategory) -> List[EnhancedDecompositionTemplate]:
        """Recupera templates por categoria"""
        return [t for t in self.templates if t.category == category]
    
    def get_library_statistics(self) -> Dict[str, Any]:
        """Retorna estatísticas da biblioteca"""
        category_counts = {}
        for template in self.templates:
            category = template.category.value
            category_counts[category] = category_counts.get(category, 0) + 1
        
        return {
            "total_templates": len(self.templates),
            "categories": category_counts,
            "cache_size": len(self.template_cache),
            "strategies_covered": list(set(t.strategy for t in self.templates))
        }