"""
Table Selection Service - Intelligent Table Selection using LLM

🎯 OBJETIVO:
Utilizar inteligência da LLM para decidir qual(is) tabela(s) são mais adequadas
para responder uma pergunta específica, baseando-se em descrições detalhadas das tabelas.

🔄 POSIÇÃO NO FLUXO:
User Query → Query Classification → **Table Selection** → Schema Context → SQL Generation

📥 ENTRADAS:
- User query (pergunta do usuário)
- Table descriptions (descrições das tabelas disponíveis)

📤 SAÍDAS:
- Lista de tabelas relevantes para a pergunta
- Justificativa da seleção (para debugging)
- Confiança da seleção

🧠 ESTRATÉGIA:
1. Apresentar descrições concisas de cada tabela para a LLM
2. LLM analisa a pergunta e decide quais tabelas são relevantes
3. Retorna lista ordenada por relevância
4. Fallback para todas as tabelas se incerto
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import logging
import json
import re

from .llm_communication_service import ILLMCommunicationService, LLMResponse


class TableRelevance(Enum):
    """Nível de relevância da tabela para a pergunta"""
    ESSENTIAL = "essential"      # Tabela essencial para responder
    HELPFUL = "helpful"          # Tabela útil mas não essencial  
    UNNECESSARY = "unnecessary"  # Tabela não necessária


@dataclass
class TableDescription:
    """Descrição detalhada de uma tabela"""
    name: str
    title: str
    description: str
    main_use_cases: List[str]
    key_columns: List[str]
    sample_questions: List[str]
    record_count: int


@dataclass
class TableSelectionResult:
    """Resultado da seleção de tabelas"""
    selected_tables: List[str]
    relevance_scores: Dict[str, TableRelevance]
    justification: str
    confidence: float
    fallback_used: bool = False


class ITableSelectionService(ABC):
    """Interface para seleção inteligente de tabelas"""
    
    @abstractmethod
    def select_tables_for_query(self, user_query: str) -> TableSelectionResult:
        """Seleciona as tabelas mais adequadas para uma pergunta"""
        pass
    
    @abstractmethod
    def get_available_tables(self) -> List[TableDescription]:
        """Retorna lista de tabelas disponíveis com descrições"""
        pass


class SUSTableSelectionService(ITableSelectionService):
    """Serviço de seleção de tabelas para dados SUS usando LLM"""
    
    def __init__(self, llm_service: ILLMCommunicationService):
        """
        Initialize table selection service
        
        Args:
            llm_service: LLM communication service for intelligent selection
        """
        self._llm_service = llm_service
        self._table_descriptions = self._initialize_table_descriptions()
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Setup logging"""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _initialize_table_descriptions(self) -> List[TableDescription]:
        """Inicializa descrições detalhadas das tabelas SUS"""
        return [
            TableDescription(
                name="sus_data",
                title="Dados Principais de Hospitalização SUS",
                description="Tabela principal contendo todos os registros de internações hospitalares do SUS. Contém informações demográficas, diagnósticos, procedimentos, custos e desfechos.",
                main_use_cases=[
                    "Estatísticas gerais de pacientes (contagens, médias, totais)",
                    "Análises demográficas (idade, sexo, localização)",
                    "Consultas sobre mortalidade e óbitos",
                    "Análises de custos e procedimentos",
                    "Consultas temporais (por ano, mês, período)",
                    "Análises geográficas (por cidade, estado)",
                    "Tempo de internação e permanência hospitalar",
                    "Códigos CID específicos (diagnósticos individuais)"
                ],
                key_columns=[
                    "DIAG_PRINC (código CID-10 do diagnóstico)",
                    "IDADE, SEXO (dados demográficos)",
                    "CIDADE_RESIDENCIA_PACIENTE, UF_RESIDENCIA_PACIENTE (localização)",
                    "MORTE (indicador de óbito: 0=não, 1=sim)",
                    "VAL_TOT (valor total do procedimento)",
                    "DT_INTER, DT_SAIDA (datas de internação e alta)",
                    "UTI_MES_TO (dias em UTI)",
                    "PROC_REA (código do procedimento realizado)"
                ],
                sample_questions=[
                    "Quantos pacientes existem no total?",
                    "Qual a idade média dos pacientes?",
                    "Quantas mortes ocorreram em Porto Alegre?",
                    "Qual o custo total de internações por estado?",
                    "Quantos casos de diagnóstico J44 tivemos?",
                    "Qual o tempo médio de internação?",
                    "Quantos homens morreram em 2017?",
                    "Qual cidade tem mais internações?"
                ],
                record_count=24485
            ),

            TableDescription(
                name="cid_detalhado",
                title="Códigos CID-10 Específicos",
                description="Códigos CID-10 individuais e específicos com suas descrições detalhadas. Usado para lookup de códigos específicos e suas definições.",
                main_use_cases=[
                    "Explicação de códigos CID específicos",
                    "Busca por descrição de diagnósticos",
                    "Lookup de significado de códigos",
                    "Validação de códigos CID",
                    "Pesquisa semântica por diagnósticos"
                ],
                key_columns=[
                    "codigo (código CID específico: I200, J441, etc.)",
                    "descricao (descrição detalhada do diagnóstico)"
                ],
                sample_questions=[
                    "O que significa o código CID I200?",
                    "Qual a descrição do diagnóstico J441?",
                    "Que código CID corresponde à pneumonia?",
                    "Explique o que é o código CID C780?",
                    "Buscar códigos relacionados a diabetes"
                ],
                record_count=1000  # Estimado
            ),
            
        ]
    
    def get_available_tables(self) -> List[TableDescription]:
        """Retorna lista de tabelas disponíveis"""
        return self._table_descriptions
    
    def select_tables_for_query(self, user_query: str) -> TableSelectionResult:
        """
        Seleciona tabelas usando inteligência da LLM
        
        Args:
            user_query: Pergunta do usuário
            
        Returns:
            TableSelectionResult com tabelas selecionadas e justificativa
        """
        self.logger.info(f"🎯 Selecting tables for query: {user_query}")
        
        try:
            # Criar prompt para seleção de tabelas
            selection_prompt = self._create_table_selection_prompt(user_query)
            
            # Chamar LLM para decidir
            llm_response = self._llm_service.send_prompt(selection_prompt)
            self.logger.info(f"🤖 LLM table selection response received")
            self.logger.info(f"📄 Raw LLM response: {llm_response.content[:500]}...")
            
            # Processar resposta da LLM
            selection_result = self._parse_llm_response(llm_response.content, user_query)
            
            self.logger.info(f"✅ Selected tables: {selection_result.selected_tables}")
            self.logger.info(f"📊 Confidence: {selection_result.confidence:.2f}")
            
            return selection_result
            
        except Exception as e:
            self.logger.error(f"❌ Error in table selection: {str(e)}")
            
            # Fallback: retornar apenas tabelas suportadas
            return TableSelectionResult(
                selected_tables=["sus_data", "cid_detalhado"],
                relevance_scores={
                    "sus_data": TableRelevance.ESSENTIAL,
                    "cid_detalhado": TableRelevance.HELPFUL
                },
                justification=f"Fallback devido a erro: {str(e)}",
                confidence=0.5,
                fallback_used=True
            )
    
    def _create_table_selection_prompt(self, user_query: str) -> str:
        """Cria prompt para seleção inteligente de tabelas"""
        
        # Montar descrições das tabelas
        tables_info = ""
        for table in self._table_descriptions:
            tables_info += f"""
        **TABELA: {table.name}**
        - Título: {table.title}
        - Descrição: {table.description}
        - Principais usos: {'; '.join(table.main_use_cases)}
        - Colunas chave: {'; '.join(table.key_columns)}
        - Exemplos de perguntas: {'; '.join(table.sample_questions[:3])}
        - Registros: {table.record_count:,}
        
        """
        
        prompt = f"""Você é um especialista em análise de dados SUS e precisa selecionar as tabelas mais adequadas para responder uma pergunta específica.

        TABELAS DISPONÍVEIS:
        {tables_info}
        
        PERGUNTA DO USUÁRIO: "{user_query}"
    
        INSTRUÇÕES:
        1. Analise cuidadosamente a pergunta do usuário
        2. Determine quais tabelas são ESSENCIAIS, ÚTEIS ou DESNECESSÁRIAS
        3. Selecione APENAS as tabelas realmente necessárias (evite incluir todas)
        4. Prefira o menor conjunto de tabelas que pode responder completamente a pergunta
        
        CRITÉRIOS DE SELEÇÃO:
        - **sus_data**: Para estatísticas, contagens, dados demográficos, mortalidade, custos, análises temporais/geográficas
        - **cid_detalhado**: Para explicação de códigos CID ESPECÍFICOS (o que significa I200, J441, etc.) E para mostrar DIAGNÓSTICOS ESPECÍFICOS com descrições legíveis
        
        REGRAS IMPORTANTES:
        - Para perguntas sobre "diagnóstico mais comum", "qual diagnóstico", "diagnósticos específicos": SEMPRE USE cid_detalhado + sus_data
        - Para explicar "o que significa código X": USE cid_detalhado apenas
        - Para estatísticas simples sem necessidade de descrição: USE sus_data apenas
        - SEMPRE prefira mostrar diagnósticos com descrições legíveis usando cid_detalhado
        
        RESPONDA EXATAMENTE NO FORMATO JSON:
        {{
          "selected_tables": ["tabela1", "tabela2"],
          "relevance_scores": {{
            "tabela1": "essential",
            "tabela2": "helpful",
            "tabela3": "unnecessary"
          }},
          "justification": "Explicação clara da seleção",
          "confidence": 0.95
        }}
        
        IMPORTANTE:
        - Use APENAS os nomes: sus_data, cid_detalhado
        - Confidence entre 0.0 e 1.0
        - Relevance: "essential", "helpful", ou "unnecessary"
        - Seja seletivo - NÃO inclua tabelas desnecessárias"""

        return prompt
    
    def _parse_llm_response(self, response: str, user_query: str) -> TableSelectionResult:
        """Parse da resposta da LLM para extrair seleção de tabelas"""
        
        try:
            # Procurar JSON na resposta usando uma abordagem mais robusta
            json_str = self._extract_complete_json(response)
            if not json_str:
                raise ValueError("No JSON found in response")
            
            self.logger.info(f"🔍 Extracted JSON: {json_str[:200]}...")
            
            # Parse JSON
            selection_data = json.loads(json_str)
            
            # Validar e extrair dados
            selected_tables = selection_data.get("selected_tables", [])
            relevance_scores_raw = selection_data.get("relevance_scores", {})
            justification = selection_data.get("justification", "")
            confidence = float(selection_data.get("confidence", 0.8))
            
            # Converter relevance scores para enum
            relevance_scores = {}
            for table, relevance in relevance_scores_raw.items():
                try:
                    relevance_scores[table] = TableRelevance(relevance.lower())
                except ValueError:
                    relevance_scores[table] = TableRelevance.HELPFUL
            
            # Validar tabelas selecionadas
            valid_tables = ["sus_data", "cid_detalhado"]
            selected_tables = [t for t in selected_tables if t in valid_tables]
            
            # Se nenhuma tabela válida foi selecionada, usar sus_data como padrão
            if not selected_tables:
                selected_tables = ["sus_data"]
                relevance_scores["sus_data"] = TableRelevance.ESSENTIAL
                justification += " (Default to sus_data due to empty selection)"
            
            return TableSelectionResult(
                selected_tables=selected_tables,
                relevance_scores=relevance_scores,
                justification=justification,
                confidence=min(max(confidence, 0.0), 1.0),  # Clamp to 0-1
                fallback_used=False
            )
            
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to parse LLM response: {str(e)}")
            self.logger.info(f"📄 Raw response: {response}")
            
            # Fallback inteligente baseado em análise simples da pergunta
            return self._fallback_table_selection(user_query)
    
    def _extract_complete_json(self, response: str) -> str:
        """Extrai JSON completo da resposta, tratando JSONs aninhados"""
        # Encontrar início do JSON
        start = response.find('{')
        if start == -1:
            return ""
        
        # Contar chaves para encontrar o JSON completo
        brace_count = 0
        in_string = False
        escape_next = False
        
        for i, char in enumerate(response[start:], start):
            if escape_next:
                escape_next = False
                continue
                
            if char == '\\':
                escape_next = True
                continue
                
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
                
            if not in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        # Encontrou o final do JSON
                        return response[start:i+1]
        
        # Se não encontrou o final, retorna do início até o final da string
        return response[start:]
    
    def _fallback_table_selection(self, user_query: str) -> TableSelectionResult:
        """Fallback inteligente para seleção de tabelas"""
        query_lower = user_query.lower()
        selected_tables = []
        relevance_scores = {}
        
        # Regras de fallback baseadas em padrões
        if any(word in query_lower for word in ["quantos", "total", "média", "custo", "idade", "sexo", "morte", "cidade", "estado", "tempo"]):
            selected_tables.append("sus_data")
            relevance_scores["sus_data"] = TableRelevance.ESSENTIAL
        
        if any(word in query_lower for word in ["significa", "código", "diagnóstico", "cid", "o que é", "mais comum", "qual diagnóstico"]):
            selected_tables.append("cid_detalhado")
            relevance_scores["cid_detalhado"] = TableRelevance.ESSENTIAL
        
        # Se nenhuma regra se aplicou, usar sus_data
        if not selected_tables:
            selected_tables = ["sus_data"]
            relevance_scores["sus_data"] = TableRelevance.ESSENTIAL
        
        return TableSelectionResult(
            selected_tables=selected_tables,
            relevance_scores=relevance_scores,
            justification=f"Fallback selection based on keyword analysis: {query_lower}",
            confidence=0.6,
            fallback_used=True
        )


class TableSelectionFactory:
    """Factory para criação de serviços de seleção de tabelas"""
    
    @staticmethod
    def create_sus_service(llm_service: ILLMCommunicationService) -> ITableSelectionService:
        """Cria serviço de seleção para dados SUS"""
        return SUSTableSelectionService(llm_service)
    
    @staticmethod
    def create_service(
        schema_type: str, 
        llm_service: ILLMCommunicationService
    ) -> ITableSelectionService:
        """Cria serviço baseado no tipo de schema"""
        if schema_type.lower() == "sus":
            return SUSTableSelectionService(llm_service)
        else:
            raise ValueError(f"Unsupported schema type: {schema_type}")