"""
Serviço de comunicação com LLM especializado em conversação e domínio SUS.
"""

import json
import logging
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

import requests
from requests.exceptions import RequestException, Timeout

from src.domain.exceptions.custom_exceptions import (
    LLMCommunicationError,
    LLMTimeoutError,
    LLMUnavailableError
)


@dataclass
class ConversationalConfig:
    """Configuração especializada para LLM conversacional."""
    # model_name: str = "llama3.2:latest"
    model_name: str = "mistral"  # Use available model
    temperature: float = 0.8  # Mais criativo para conversação
    max_tokens: int = 1000
    timeout: int = 60
    max_retries: int = 3
    system_role: str = "assistant"
    stream: bool = False


class ConversationalLLMService:
    """
    Serviço especializado em comunicação com LLM para respostas conversacionais
    amigáveis no domínio SUS.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        config: Optional[ConversationalConfig] = None
    ):
        self.base_url = base_url.rstrip('/')
        self.config = config or ConversationalConfig()
        self.logger = logging.getLogger(__name__)
        # Prevent duplicate logs by disabling propagation to root logger
        self.logger.propagate = False
        
        # Endpoints
        self.chat_endpoint = f"{self.base_url}/api/chat"
        self.health_endpoint = f"{self.base_url}/api/tags"
        
        self.logger.info(
            f"ConversationalLLMService inicializado com modelo: {self.config.model_name}"
        )

    def is_available(self) -> bool:
        """Verifica se o serviço LLM conversacional está disponível."""
        try:
            response = requests.get(
                self.health_endpoint,
                timeout=5
            )
            return response.status_code == 200
        except RequestException as e:
            self.logger.warning(f"LLM conversacional indisponível: {e}")
            return False

    def generate_conversational_response(
        self,
        user_query: str,
        sql_query: str,
        sql_results: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Gera resposta conversacional amigável baseada nos resultados SQL.
        
        Args:
            user_query: Pergunta original do usuário
            sql_query: Query SQL executada
            sql_results: Resultados da query SQL
            context: Contexto adicional da conversação
            
        Returns:
            Resposta em linguagem natural amigável
        """
        prompt = self._build_conversational_prompt(
            user_query, sql_query, sql_results, context
        )
        
        return self._call_llm(prompt)

    def _build_conversational_prompt(
        self,
        user_query: str,
        sql_query: str,
        sql_results: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Constrói prompt especializado para respostas conversacionais."""
        
        # Contexto SUS especializado
        sus_context = """
        Você é um assistente especialista em dados do Sistema Único de Saúde (SUS) brasileiro.
        Sua função é transformar resultados de consultas SQL em respostas conversacionais
        amigáveis e informativas para profissionais de saúde e gestores públicos.
        
        CARACTERÍSTICAS DA SUA RESPOSTA:
        - Linguagem clara e profissional, mas amigável
        - Explicações contextualizadas sobre o SUS quando relevante
        - Interpretação dos dados com insights úteis
        - Formatação organizada e fácil de ler
        - Sugestões de ações ou análises adicionais quando apropriado
        
        CONHECIMENTO ESPECÍFICO DO SUS:
        - CNES: Cadastro Nacional de Estabelecimentos de Saúde
        - CID: Classificação Internacional de Doenças
        - SIGTAP: Sistema de Gerenciamento da Tabela de Procedimentos
        - Estados e municípios brasileiros
        - Terminologia médica em português brasileiro
        
        FORMATO DA RESPOSTA:
        Responda de forma DIRETA e CONCISA, em uma única frase ou parágrafo curto.
        
        INSTRUÇÕES ESPECÍFICAS:
        - Interprete a pergunta do usuário e responda exatamente o que foi solicitado
        - NÃO use headers como "Resumo Direto" ou "Dados Detalhados"
        - NÃO adicione explicações extras, contextualizações ou insights
        - Seja direto: exemplo "Foram encontrados 2.785 casos de doenças respiratórias nos registros do SUS."
        - Use linguagem natural e clara em português brasileiro
        - SEMPRE inclua os dados específicos dos resultados (nomes de cidades, números exatos, etc.). 
        - Se não foi especificado o numero cidades/municipios, use toda informação disponível.
        - Se perguntarem sobre "qual cidade", responda com o NOME da cidade, não apenas "a cidade"
        - Para resultados com ranking, mencione o primeiro item da lista como resposta principal
        """
        
        # Formatação dos resultados SQL
        results_text = self._format_sql_results(sql_results)
        
        prompt = f"""{sus_context}

    PERGUNTA DO USUÁRIO:
    {user_query}
    
    CONSULTA SQL EXECUTADA:
    {sql_query}
    
    RESULTADOS OBTIDOS:
    {results_text}
    
    CONTEXTO ADICIONAL:
    {json.dumps(context or {}, ensure_ascii=False, indent=2)}

    ⚠️ INSTRUÇÕES CRÍTICAS - LEIA COM ATENÇÃO:
    1. NUNCA invente cidades, números ou informações que não estão nos resultados
    2. Para perguntas como "qual cidade", responda EXATAMENTE com o nome da cidade que aparece em "1º lugar" nos resultados
    3.Quando perguntado sobre "quais cidades" liste exatamente o numero que pede com a informação correta.
    4. Utilize apenas os dados fornecidos para gerar a resposta. Não adicione informações externas ou suposições.
    
    Transforme estes dados em uma resposta conversacional amigável e informativa.
    Responda em português brasileiro, focando na utilidade prática da informação.
    """
        
        return prompt

    def _format_sql_results(self, sql_results: Any) -> str:
        """Formata os resultados SQL para inclusão no prompt."""
        if sql_results is None:
            return "Nenhum resultado encontrado."
        
        if isinstance(sql_results, (list, tuple)):
            if len(sql_results) == 0:
                return "Nenhum resultado encontrado."
            
            # Limita a quantidade de resultados para evitar prompts muito longos
            limited_results = sql_results[:20]
            
            # Handle structured results from query parser
            if isinstance(limited_results[0], dict):
                # Look for simple query results with a single value
                result_item = limited_results[0]
                if "result" in result_item:
                    result_value = result_item["result"]
                    return f"Resultado encontrado: {result_value}"
                
                # Handle complex query results
                formatted = []
                for item in limited_results:
                    if "final_answer_text" in item:
                        continue  # Skip metadata entries
                    if "city" in item and "count" in item:
                        formatted.append(f"{item.get('rank', '')}. {item['city']} - {item['count']}")
                    elif "result" in item:
                        formatted.append(f"Resultado: {item['result']}")
                    else:
                        # Fallback for other dict structures
                        formatted.append(str(item))
                
                if formatted:
                    result_text = "\n".join(formatted)
                    if len(sql_results) > 20:
                        result_text += f"\n... (mostrando 20 de {len(sql_results)} resultados)"
                    return result_text
            
            elif isinstance(limited_results[0], (list, tuple)):
                # Resultados tabulares - analisa se é uma consulta de ranking
                formatted = []
                for i, row in enumerate(limited_results):
                    if len(row) == 2:  # Provavelmente cidade/valor
                        if i == 0:
                            formatted.append(f"1º lugar: {row[0]} com {row[1]} casos")
                        else:
                            formatted.append(f"{i+1}º lugar: {row[0]} com {row[1]} casos")
                    else:
                        formatted.append(str(row))
                
                result_text = "\n".join(formatted)
                
                if len(sql_results) > 20:
                    result_text += f"\n... (mostrando 20 de {len(sql_results)} resultados)"
                
                return result_text
            else:
                # Lista simples
                return str(limited_results)
        
        return str(sql_results)

    def _call_llm(self, prompt: str) -> str:
        """Realiza chamada para o LLM com retry logic."""
        for attempt in range(self.config.max_retries):
            try:
                response = self._make_request(prompt)
                return response
                
            except LLMTimeoutError:
                if attempt == self.config.max_retries - 1:
                    raise
                self.logger.warning(
                    f"Timeout na tentativa {attempt + 1}, tentando novamente..."
                )
                time.sleep(2 ** attempt)  # Backoff exponencial
                
            except LLMCommunicationError as e:
                if attempt == self.config.max_retries - 1:
                    raise
                self.logger.warning(
                    f"Erro de comunicação na tentativa {attempt + 1}: {e}"
                )
                time.sleep(2 ** attempt)

        raise LLMCommunicationError("Falha após todas as tentativas de retry")

    def _make_request(self, prompt: str) -> str:
        """Faz a requisição HTTP para o LLM."""
        if not self.is_available():
            raise LLMUnavailableError(
                "Serviço LLM conversacional indisponível"
            )

        payload = {
            "model": self.config.model_name,
            "messages": [
                {
                    "role": "system",
                    "content": "Você é um assistente especialista em dados do SUS brasileiro."
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            "stream": self.config.stream,
            "options": {
                "temperature": self.config.temperature,
                "num_predict": self.config.max_tokens
            }
        }

        try:
            start_time = time.time()
            
            response = requests.post(
                self.chat_endpoint,
                json=payload,
                timeout=self.config.timeout
            )
            
            response_time = time.time() - start_time
            
            if response.status_code != 200:
                error_msg = f"Erro HTTP {response.status_code}: {response.text}"
                self.logger.error(error_msg)
                raise LLMCommunicationError(error_msg)

            response_data = response.json()
            
            if 'message' not in response_data:
                raise LLMCommunicationError(
                    "Resposta do LLM em formato inesperado"
                )

            llm_response = response_data['message']['content']
            
            self.logger.info(
                f"Resposta conversacional gerada em {response_time:.2f}s"
            )
            
            return llm_response.strip()

        except Timeout:
            raise LLMTimeoutError(
                f"Timeout na comunicação com LLM conversacional ({self.config.timeout}s)"
            )
        except RequestException as e:
            raise LLMCommunicationError(
                f"Erro na comunicação com LLM conversacional: {e}"
            )
        except json.JSONDecodeError as e:
            raise LLMCommunicationError(
                f"Erro ao decodificar resposta JSON do LLM: {e}"
            )

    def get_model_info(self) -> Dict[str, Any]:
        """Retorna informações sobre o modelo conversacional."""
        return {
            "model_name": self.config.model_name,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "timeout": self.config.timeout,
            "specialization": "Conversational responses for SUS healthcare data",
            "language": "Portuguese (Brazilian)",
            "available": self.is_available()
        }