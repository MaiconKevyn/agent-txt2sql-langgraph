"""
Query Classification Service - Single Responsibility: Classify user queries by intent
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
import re
import time
import logging

from .llm_communication_service import ILLMCommunicationService
from .error_handling_service import IErrorHandlingService, ErrorCategory


class QueryType(Enum):
    """Types of user queries"""
    DATABASE_QUERY = "database_query"          # Requires SQL execution
    CONVERSATIONAL_QUERY = "conversational_query"  # General questions, explanations
    AMBIGUOUS_QUERY = "ambiguous_query"        # Unclear intent


@dataclass
class QueryClassification:
    """Result of query classification"""
    query_type: QueryType
    confidence_score: float
    reasoning: str
    detected_patterns: List[str]
    suggested_reroute: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class IQueryClassificationService(ABC):
    """Interface for query classification service"""
    
    @abstractmethod
    def classify_query(self, user_query: str) -> QueryClassification:
        """
        Classify user query by intent
        
        Args:
            user_query: User's natural language query
            
        Returns:
            Classification result with type and confidence
        """
        pass
    
    @abstractmethod
    def is_database_query(self, user_query: str) -> bool:
        """Quick check if query requires database access"""
        pass
    
    @abstractmethod
    def is_conversational_query(self, user_query: str) -> bool:
        """Quick check if query is conversational/explanatory"""
        pass


class QueryClassificationService(IQueryClassificationService):
    """
    Service for classifying user queries to determine routing strategy
    
    Uses combination of pattern matching and LLM-based classification
    for SUS healthcare domain queries.
    """
    
    def __init__(
        self,
        llm_service: ILLMCommunicationService,
        error_service: IErrorHandlingService,
        confidence_threshold: float = 0.7
    ):
        """
        Initialize query classification service
        
        Args:
            llm_service: LLM communication service for intelligent classification
            error_service: Error handling service
            confidence_threshold: Minimum confidence for classification
        """
        self._llm_service = llm_service
        self._error_service = error_service
        self._confidence_threshold = confidence_threshold
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Initialize pattern databases
        self._setup_patterns()
        
        self.logger.info("QueryClassificationService inicializado")
    
    def _setup_patterns(self) -> None:
        """Setup pattern databases for quick classification"""
        
        # Patterns that indicate DATABASE queries (statistical, counting, data retrieval)
        self._database_patterns = [
            # Counting patterns
            r'\b(quantos?|quantas?|quantidade|número|total)\b',
            r'\b(count|sum|soma|somar)\b',
            
            # Statistical patterns  
            r'\b(média|mediana|estatística|percentual|proporção|distribuição)\b',
            r'\b(maior|menor|máximo|mínimo|ranking|top|primeiro|último)\b',
            r'\b(comparar|comparação|diferença|versus|vs)\b',
            
            # Temporal patterns
            r'\b(ano|anos|mês|meses|período|durante|entre|desde|até|quando)\b',
            r'\b(20\d{2}|19\d{2})\b',  # Years
            r'\b(janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\b',
            
            # Geographic patterns
            r'\b(estado|cidade|município|região|local|onde|Porto Alegre|Rio Grande do Sul|RS)\b',
            
            # Medical data patterns
            r'\b(pacientes?|casos?|internações?|mortes?|óbitos?|alta|UTI)\b',
            r'\b(diagnóstico|doença|condição|CID|procedimento|custo|valor)\b',
            r'\b(sexo|idade|masculino|feminino|homens|mulheres)\b',
            
            # List/show patterns
            r'\b(listar|mostrar|exibir|apresentar|relacionar)\b',
            r'\b(dados|informações|registros|resultados)\b'
        ]
        
        # Patterns that indicate CONVERSATIONAL queries (explanations, definitions)
        self._conversational_patterns = [
            # Question words for explanation
            r'\b(o que é|que significa|significado|definição|explique|explica)\b',
            r'\b(como funciona|para que serve|qual o objetivo)\b',
            r'\b(diferença entre|diferenças|distinção)\b',
            
            # CID explanation patterns
            r'\bCID[- ]?([A-Z]\d{2}(\.\d{1,2})?)\b',  # CID codes like J90, J90.1
            r'\b(código|classificação) (CID|internacional)\b',
            
            # Medical terminology explanations
            r'\b(sintomas?|causas?|tratamento|diagnóstico|prognóstico)\b',
            r'\b(indica|indicação|classificado|categorizado)\b',
            
            # General SUS questions
            r'\b(SUS|Sistema Único|política|programa|protocolo)\b',
            r'\b(significa|quer dizer|conceito|definir)\b',
            
            # Help/guidance patterns
            r'\b(como|ajuda|orientação|dúvida|esclarecer)\b',
            r'\b(exemplo|exemplos|ilustração|demonstração)\b'
        ]
        
        # Ambiguous patterns (could be either)
        self._ambiguous_patterns = [
            r'\b(informações sobre|dados sobre|sobre)\b',
            r'\b(relatório|análise|estudo|pesquisa)\b',
            r'\b(tendência|evolução|comportamento)\b'
        ]
    
    def classify_query(self, user_query: str) -> QueryClassification:
        """
        Classify user query using pattern matching + LLM validation
        """
        start_time = time.time()
        
        try:
            self.logger.info(f"🔍 Classificando query: {user_query[:50]}...")
            
            # Step 1: Pattern-based quick classification
            pattern_result = self._classify_by_patterns(user_query)
            
            # Step 2: If patterns are clear enough, use them
            if pattern_result.confidence_score >= self._confidence_threshold:
                self.logger.info(f"✅ Classificação por padrões: {pattern_result.query_type.value} (confiança: {pattern_result.confidence_score:.2f})")
                pattern_result.metadata = {"method": "pattern_based", "processing_time": time.time() - start_time}
                return pattern_result
            
            # Step 3: Use LLM for ambiguous cases
            self.logger.info("🤖 Usando LLM para classificação avançada...")
            llm_result = self._classify_by_llm(user_query, pattern_result)
            
            # Step 4: Combine pattern and LLM results
            final_result = self._combine_classifications(pattern_result, llm_result)
            final_result.metadata = {
                "method": "hybrid_pattern_llm",
                "processing_time": time.time() - start_time,
                "pattern_confidence": pattern_result.confidence_score,
                "llm_confidence": llm_result.confidence_score
            }
            
            self.logger.info(f"✅ Classificação final: {final_result.query_type.value} (confiança: {final_result.confidence_score:.2f})")
            return final_result
            
        except Exception as e:
            error_info = self._error_service.handle_error(e, ErrorCategory.QUERY_PROCESSING)
            self.logger.error(f"❌ Erro na classificação: {error_info.message}")
            
            # Fallback: default to database query for safety
            return QueryClassification(
                query_type=QueryType.DATABASE_QUERY,
                confidence_score=0.3,
                reasoning=f"Erro na classificação, assumindo query de banco: {error_info.message}",
                detected_patterns=[],
                metadata={"method": "error_fallback", "error": error_info.message}
            )
    
    def _classify_by_patterns(self, user_query: str) -> QueryClassification:
        """Classify query using pattern matching"""
        query_lower = user_query.lower()
        
        # Count pattern matches
        db_matches = []
        conv_matches = []
        amb_matches = []
        
        for pattern in self._database_patterns:
            matches = re.findall(pattern, query_lower, re.IGNORECASE)
            if matches:
                db_matches.extend(matches)
        
        for pattern in self._conversational_patterns:
            matches = re.findall(pattern, query_lower, re.IGNORECASE)
            if matches:
                conv_matches.extend(matches)
        
        for pattern in self._ambiguous_patterns:
            matches = re.findall(pattern, query_lower, re.IGNORECASE)
            if matches:
                amb_matches.extend(matches)
        
        # Calculate scores
        db_score = len(db_matches) * 0.3
        conv_score = len(conv_matches) * 0.3
        amb_score = len(amb_matches) * 0.1
        
        # Normalize scores
        total_score = db_score + conv_score + amb_score
        if total_score > 0:
            db_confidence = db_score / total_score
            conv_confidence = conv_score / total_score
        else:
            db_confidence = conv_confidence = 0.5
        
        # Special rules for high-confidence classification
        
        # Strong conversational indicators
        if re.search(r'\b(o que é|que significa|explique|CID[- ]?[A-Z]\d{2})\b', query_lower, re.IGNORECASE):
            return QueryClassification(
                query_type=QueryType.CONVERSATIONAL_QUERY,
                confidence_score=0.9,
                reasoning="Pergunta explicativa detectada",
                detected_patterns=conv_matches,
                suggested_reroute="conversational_direct"
            )
        
        # Strong database indicators
        if re.search(r'\b(quantos?|quantas?|média|total|listar)\b', query_lower, re.IGNORECASE):
            return QueryClassification(
                query_type=QueryType.DATABASE_QUERY,
                confidence_score=0.9,
                reasoning="Pergunta estatística/quantitativa detectada",
                detected_patterns=db_matches,
                suggested_reroute="sql_processing"
            )
        
        # Determine primary classification
        if db_confidence > conv_confidence and db_confidence > 0.6:
            query_type = QueryType.DATABASE_QUERY
            confidence = min(db_confidence, 0.8)
            reasoning = f"Padrões de banco detectados: {db_matches[:3]}"
        elif conv_confidence > db_confidence and conv_confidence > 0.6:
            query_type = QueryType.CONVERSATIONAL_QUERY
            confidence = min(conv_confidence, 0.8)
            reasoning = f"Padrões conversacionais detectados: {conv_matches[:3]}"
        else:
            query_type = QueryType.AMBIGUOUS_QUERY
            confidence = 0.5
            reasoning = "Padrões ambíguos ou insuficientes para classificação"
        
        return QueryClassification(
            query_type=query_type,
            confidence_score=confidence,
            reasoning=reasoning,
            detected_patterns=db_matches + conv_matches + amb_matches
        )
    
    def _classify_by_llm(self, user_query: str, pattern_result: QueryClassification) -> QueryClassification:
        """Use LLM for intelligent classification"""
        
        classification_prompt = f"""
Você é um especialista em classificação de queries para sistema SUS brasileiro.

QUERY DO USUÁRIO: "{user_query}"

ANÁLISE PRÉVIA POR PADRÕES:
- Tipo detectado: {pattern_result.query_type.value}
- Confiança: {pattern_result.confidence_score:.2f}
- Padrões: {pattern_result.detected_patterns[:5]}

CLASSIFIQUE ESTA QUERY EM UMA DAS CATEGORIAS:

1. DATABASE_QUERY: Requer busca/cálculo em banco de dados
   - Estatísticas (quantos, média, total, distribuição)
   - Listagens de dados (pacientes, diagnósticos, cidades)
   - Comparações numéricas
   - Análises temporais/geográficas
   - Exemplos: "Quantos pacientes em 2017?", "Média de idade", "Top 5 cidades"

2. CONVERSATIONAL_QUERY: Pergunta explicativa/conceitual
   - Explicações de códigos CID (O que é J90?)
   - Definições médicas ou do SUS
   - Conceitos e significados
   - Exemplos: "O que significa CID J90?", "Explique hipertensão", "Para que serve UTI?"

3. AMBIGUOUS_QUERY: Não está claro o que o usuário quer
   - Queries muito vagas
   - Podem ser interpretadas de múltiplas formas
   - Exemplos: "Fale sobre diabetes", "Dados de saúde"

RESPONDA APENAS COM:
TIPO: [DATABASE_QUERY|CONVERSATIONAL_QUERY|AMBIGUOUS_QUERY]
CONFIANÇA: [0.0-1.0]
RAZÃO: [Breve explicação da classificação]
"""
        
        try:
            llm_response = self._llm_service.send_prompt(classification_prompt)
            
            # Parse LLM response
            response_text = llm_response.content.strip()
            
            # Extract classification
            tipo_match = re.search(r'TIPO:\s*(\w+)', response_text)
            confianca_match = re.search(r'CONFIANÇA:\s*([\d.]+)', response_text)
            razao_match = re.search(r'RAZÃO:\s*(.+)', response_text)
            
            if tipo_match and confianca_match:
                tipo_str = tipo_match.group(1).upper()
                confianca = float(confianca_match.group(1))
                razao = razao_match.group(1) if razao_match else "Classificação por LLM"
                
                # Map to enum
                tipo_mapping = {
                    "DATABASE_QUERY": QueryType.DATABASE_QUERY,
                    "CONVERSATIONAL_QUERY": QueryType.CONVERSATIONAL_QUERY,
                    "AMBIGUOUS_QUERY": QueryType.AMBIGUOUS_QUERY
                }
                
                query_type = tipo_mapping.get(tipo_str, QueryType.AMBIGUOUS_QUERY)
                
                return QueryClassification(
                    query_type=query_type,
                    confidence_score=min(confianca, 1.0),
                    reasoning=f"LLM: {razao}",
                    detected_patterns=["llm_classification"]
                )
            
            # Fallback parsing
            if "DATABASE" in response_text.upper():
                return QueryClassification(
                    query_type=QueryType.DATABASE_QUERY,
                    confidence_score=0.7,
                    reasoning="LLM indicou DATABASE_QUERY",
                    detected_patterns=["llm_database"]
                )
            elif "CONVERSATIONAL" in response_text.upper():
                return QueryClassification(
                    query_type=QueryType.CONVERSATIONAL_QUERY,
                    confidence_score=0.7,
                    reasoning="LLM indicou CONVERSATIONAL_QUERY",
                    detected_patterns=["llm_conversational"]
                )
            else:
                return QueryClassification(
                    query_type=QueryType.AMBIGUOUS_QUERY,
                    confidence_score=0.5,
                    reasoning="LLM response unclear",
                    detected_patterns=["llm_ambiguous"]
                )
                
        except Exception as e:
            self.logger.error(f"Erro na classificação por LLM: {e}")
            return QueryClassification(
                query_type=QueryType.AMBIGUOUS_QUERY,
                confidence_score=0.3,
                reasoning=f"Erro LLM: {str(e)}",
                detected_patterns=["llm_error"]
            )
    
    def _combine_classifications(
        self, 
        pattern_result: QueryClassification, 
        llm_result: QueryClassification
    ) -> QueryClassification:
        """Combine pattern and LLM classifications"""
        
        # If both agree and have good confidence, use higher confidence
        if pattern_result.query_type == llm_result.query_type:
            combined_confidence = max(pattern_result.confidence_score, llm_result.confidence_score)
            return QueryClassification(
                query_type=pattern_result.query_type,
                confidence_score=min(combined_confidence * 1.1, 1.0),  # Boost for agreement
                reasoning=f"Acordo entre padrões e LLM: {pattern_result.reasoning} | {llm_result.reasoning}",
                detected_patterns=pattern_result.detected_patterns + llm_result.detected_patterns
            )
        
        # If they disagree, prefer higher confidence
        if llm_result.confidence_score > pattern_result.confidence_score:
            return QueryClassification(
                query_type=llm_result.query_type,
                confidence_score=llm_result.confidence_score * 0.9,  # Slight penalty for disagreement
                reasoning=f"LLM override: {llm_result.reasoning} (vs padrões: {pattern_result.reasoning})",
                detected_patterns=llm_result.detected_patterns + pattern_result.detected_patterns
            )
        else:
            return QueryClassification(
                query_type=pattern_result.query_type,
                confidence_score=pattern_result.confidence_score * 0.9,
                reasoning=f"Padrões override: {pattern_result.reasoning} (vs LLM: {llm_result.reasoning})",
                detected_patterns=pattern_result.detected_patterns + llm_result.detected_patterns
            )
    
    def is_database_query(self, user_query: str) -> bool:
        """Quick check for database queries"""
        classification = self.classify_query(user_query)
        return classification.query_type == QueryType.DATABASE_QUERY
    
    def is_conversational_query(self, user_query: str) -> bool:
        """Quick check for conversational queries"""  
        classification = self.classify_query(user_query)
        return classification.query_type == QueryType.CONVERSATIONAL_QUERY
    
    def get_classification_stats(self) -> Dict[str, Any]:
        """Get classification statistics (for monitoring)"""
        return {
            "service_name": "QueryClassificationService",
            "confidence_threshold": self._confidence_threshold,
            "database_patterns_count": len(self._database_patterns),
            "conversational_patterns_count": len(self._conversational_patterns),
            "ambiguous_patterns_count": len(self._ambiguous_patterns),
            "llm_available": self._llm_service.is_available()
        }


class QueryClassificationFactory:
    """Factory for creating query classification services"""
    
    @staticmethod
    def create_service(
        llm_service: ILLMCommunicationService,
        error_service: IErrorHandlingService,
        confidence_threshold: float = 0.7
    ) -> IQueryClassificationService:
        """Create query classification service"""
        return QueryClassificationService(
            llm_service=llm_service,
            error_service=error_service,
            confidence_threshold=confidence_threshold
        )