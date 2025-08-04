"""
Serviço de comunicação com LLM especializado em conversação e domínio SUS.
"""

import json
import logging
import re
import sqlite3
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
    model_name: str
    temperature: float
    max_tokens: int
    timeout: int
    max_retries: int
    system_role: str = "assistant"
    stream: bool = False
    
    @classmethod
    def from_application_config(cls, app_config):
        """Create ConversationalConfig from ApplicationConfig"""
        return cls(
            model_name=app_config.conversational_llm_model,
            temperature=app_config.conversational_llm_temperature,
            max_tokens=app_config.conversational_llm_max_tokens,
            timeout=app_config.conversational_llm_timeout,
            max_retries=app_config.conversational_llm_max_retries
        )


class ConversationalLLMService:
    """
    Serviço especializado em comunicação com LLM para respostas conversacionais
    amigáveis no domínio SUS.
    """

    def __init__(
        self,
        config: ConversationalConfig,
        base_url: str = "http://localhost:11434"
    ):
        self.base_url = base_url.rstrip('/')
        self.config = config
        self.logger = logging.getLogger(__name__)
        # Enable propagation to see detailed logs in output
        self.logger.propagate = True
        
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
        self.logger.info(f"🗣️ Iniciando geração de resposta conversacional para: '{user_query}'")
        self.logger.info(f"📊 SQL executada: {sql_query}")
        self.logger.info(f"📋 Resultados SQL recebidos: {type(sql_results).__name__} com {len(sql_results) if hasattr(sql_results, '__len__') else 'N/A'} items")
        
        prompt = self._build_conversational_prompt(
            user_query, sql_query, sql_results, context
        )
        
        # Log do contexto completo enviado ao LLM
        self.logger.info("=" * 80)
        self.logger.info("🤖 CONTEXTO COMPLETO ENVIADO AO LLM CONVERSACIONAL:")
        self.logger.info("=" * 80)
        self.logger.info(prompt)
        self.logger.info("=" * 80)
        
        response = self._call_llm(prompt)
        
        self.logger.info(f"✅ Resposta conversacional gerada: '{response}'")
        return response

    def _build_conversational_prompt(
        self,
        user_query: str,
        sql_query: str,
        sql_results: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Constrói prompt otimizado usando técnicas avançadas de prompt engineering."""
        
        # Determina o tipo de consulta para template específico
        query_type = self._classify_query_type(user_query, sql_results)
        
        # Formatação dos resultados SQL
        results_text = self._format_sql_results(sql_results)
        
        # Few-shot examples baseados no tipo de consulta
        examples = self._get_few_shot_examples(query_type)
        
        # Buscar descrições CID dos resultados
        cid_descriptions = self._get_cid_descriptions(sql_results)
        cid_context = ""
        if cid_descriptions:
            cid_context = "\n### Descrições dos Códigos CID encontrados:\n"
            for code, description in cid_descriptions.items():
                cid_context += f"- {code}: {description}\n"

        # Chain-of-Thought prompting structure
        prompt = f"""# ESPECIALISTA EM ANÁLISE DE DADOS SUS

## IDENTIDADE E CONTEXTO
Você é um analista sênior especializado em dados do Sistema Único de Saúde (SUS) brasileiro, com expertise em:
- CNES (Cadastro Nacional de Estabelecimentos de Saúde)  
- CID-10 (Classificação Internacional de Doenças)
- Epidemiologia e saúde pública brasileira
- Comunicação técnica clara para gestores de saúde

## TAREFA
Transformar dados SQL em resposta conversacional precisa e útil.

## METODOLOGIA (Chain-of-Thought)
1. **ANÁLISE**: Entenda a pergunta do usuário
2. **INTERPRETAÇÃO**: Examine os dados fornecidos
3. **SÍNTESE**: Formule resposta direta e precisa
4. **VALIDAÇÃO**: Verifique consistência com dados

## EXEMPLOS DE REFERÊNCIA
{examples}

## DADOS DA CONSULTA ATUAL

### Pergunta do Usuário:
"{user_query}"

### Query SQL Executada:
```sql
{sql_query}
```

### Resultados Obtidos:
```
{results_text}
```
{cid_context}
### Contexto Adicional:
```json
{json.dumps(context or {}, ensure_ascii=False, indent=2)}
```

## INSTRUÇÕES DE EXECUÇÃO

### ✅ FAZER:
- Responder EXATAMENTE o que foi perguntado
- Usar APENAS dados dos resultados fornecidos
- Incluir números específicos e nomes exatos (cidades, valores)
- Manter linguagem clara e profissional
- Para rankings, destacar o primeiro colocado
- 🩺 CÓDIGOS CID: SEMPRE incluir a descrição completa quando mencionar códigos CID (ex: "I200 (Angina instável)", "I743 (Embolia e trombose de artérias dos membros inferiores)")
- 📋 MÚLTIPLOS ITENS: Quando a resposta contém vários códigos CID ou múltiplos itens, use listas numeradas ou bullet points para melhor visualização:
  * Para códigos CID múltiplos: "Os códigos CID para asfixia são: 1) R090 (Asfixia), 2) T71 (Asfixia), 3) P210 (Asfixia grave ao nascer)"
  * Para rankings: "As cidades com mais casos são: 1. São Paulo (1.524 casos), 2. Rio de Janeiro (987 casos), 3. Brasília (745 casos)"

### ❌ NÃO FAZER:
- Inventar dados não presentes nos resultados
- Adicionar contextualizações não solicitadas
- Usar cabeçalhos ou formatação complexa desnecessária
- Especular ou fazer suposições
- Responder "a cidade" ao invés do nome específico
- Listar múltiplos itens em formato de parágrafo corrido quando uma lista seria mais clara

## FORMATO DE RESPOSTA
Responda de forma direta e natural em português brasileiro, focando na utilidade prática.

IMPORTANTE: Sua resposta deve conter APENAS a informação solicitada, sem incluir instruções, templates ou metadados."""
        
        return prompt

    def _get_cid_descriptions(self, sql_results: Any) -> Dict[str, str]:
        """Busca descrições dos códigos CID que aparecem nos resultados."""
        try:
            # Extrair códigos CID dos resultados
            cid_codes = set()
            results_text = str(sql_results)
            
            # Padrões para códigos CID (letra + números)
            cid_pattern = r'\b[A-Z]\d{2,3}\b'
            matches = re.findall(cid_pattern, results_text)
            cid_codes.update(matches)
            
            if not cid_codes:
                return {}
            
            # Conectar ao banco e buscar descrições
            conn = sqlite3.connect('sus_database.db')
            cursor = conn.cursor()
            
            descriptions = {}
            for code in cid_codes:
                cursor.execute('SELECT descricao FROM cid_detalhado WHERE codigo = ?', (code,))
                result = cursor.fetchone()
                if result:
                    descriptions[code] = result[0]
            
            conn.close()
            return descriptions
            
        except Exception as e:
            self.logger.warning(f"Erro ao buscar descrições CID: {e}")
            return {}

    def _classify_query_type(self, user_query: str, sql_results: Any) -> str:
        """Classifica o tipo de consulta para usar template específico."""
        user_query_lower = user_query.lower()
        
        # Análise de padrões na pergunta
        if any(word in user_query_lower for word in ['quantos', 'quantas', 'total', 'número']):
            if any(word in user_query_lower for word in ['cidade', 'município', 'onde']):
                # Detecta se pede dados de "cada" entidade (lista completa)
                if any(word in user_query_lower for word in ['cada', 'por', 'em cada', 'todas']):
                    return "LISTAGEM_COMPLETA_GEOGRÁFICA"
                return "CONTAGEM_GEOGRÁFICA"
            return "CONTAGEM_SIMPLES"
        
        elif any(word in user_query_lower for word in ['qual', 'quais', 'que']):
            if any(word in user_query_lower for word in ['cidade', 'município']):
                return "IDENTIFICAÇÃO_GEOGRÁFICA"
            return "IDENTIFICAÇÃO_GERAL"
        
        elif any(word in user_query_lower for word in ['maior', 'menor', 'mais', 'menos', 'ranking']):
            return "COMPARAÇÃO_RANKING"
        
        elif any(word in user_query_lower for word in ['tempo médio', 'tempo medio', 'maior tempo', 'menor tempo', 'internação', 'internacao']):
            return "TEMPO_INTERNACAO"
        elif any(word in user_query_lower for word in ['média', 'mediana', 'percentual', '%']):
            return "ANÁLISE_ESTATÍSTICA"
        
        # Análise dos resultados
        if sql_results and isinstance(sql_results, (list, tuple)) and len(sql_results) > 1:
            return "MÚLTIPLOS_RESULTADOS"
        
        return "CONSULTA_GERAL"

    def _get_few_shot_examples(self, query_type: str) -> str:
        """Retorna exemplos few-shot baseados no tipo de consulta."""
        
        examples_map = {
            "CONTAGEM_SIMPLES": """
### Exemplo 1:
**Pergunta:** "Quantas pessoas morreram?"
**Dados:** {"total_mortes": 2202}
**Resposta:** "Foram registradas 2.202 mortes no SUS."

### Exemplo 2:
**Pergunta:** "Quantos casos de diabetes?"
**Dados:** {"total_casos": 15678}
**Resposta:** "Foram encontrados 15.678 casos de diabetes nos registros do SUS."

### Exemplo 3:
**Pergunta:** "Qual é o CID para Asfixia?"
**Dados:** [{"codigo": "R090", "descricao": "Asfixia"}, {"codigo": "T71", "descricao": "Asfixia"}, {"codigo": "P210", "descricao": "Asfixia grave ao nascer"}]
**Resposta:** "Os códigos CID para asfixia são: 1) R090 (Asfixia), 2) T71 (Asfixia), 3) P210 (Asfixia grave ao nascer)."

### Exemplo 4:
**Pergunta:** "Qual é o diagnóstico que possui o maior tempo médio de internação?"
**Dados:** [["F20", 11, 66.6]]
**Resposta:** "O diagnóstico F20 (Esquizofrenia) possui o maior tempo médio de internação com 66,6 dias."
""",
            
            "LISTAGEM_COMPLETA_GEOGRÁFICA": """
### Exemplo 1:
**Pergunta:** "Quantas mortes ao total em cada cidade?"
**Dados:** [{"cidade": "Uruguaiana", "mortes": 359}, {"cidade": "Ijuí", "mortes": 359}, {"cidade": "Passo Fundo", "mortes": 325}, {"cidade": "Porto Alegre", "mortes": 308}]
**Resposta:** "As mortes por cidade são: 1) Uruguaiana com 359 casos, 2) Ijuí com 359 casos, 3) Passo Fundo com 325 casos, 4) Porto Alegre com 308 casos."

### Exemplo 2:
**Pergunta:** "Casos por município"
**Dados:** [["São Paulo", 1524], ["Rio de Janeiro", 987], ["Brasília", 745]]
**Resposta:** "Os casos por município são: 1. São Paulo (1.524), 2. Rio de Janeiro (987), 3. Brasília (745)."
""",
            
            "IDENTIFICAÇÃO_GEOGRÁFICA": """
### Exemplo 1:
**Pergunta:** "Qual cidade tem mais casos?"
**Dados:** [{"rank": 1, "cidade": "São Paulo", "casos": 1524}, {"rank": 2, "cidade": "Rio de Janeiro", "casos": 987}]
**Resposta:** "São Paulo lidera com 1.524 casos registrados."

### Exemplo 2:
**Pergunta:** "Que município teve mais óbitos?"
**Dados:** ["Salvador", "842"]
**Resposta:** "Salvador registrou o maior número de óbitos com 842 casos."
""",
            
            "COMPARAÇÃO_RANKING": """
### Exemplo 1:
**Pergunta:** "Ranking das cidades com mais casos"
**Dados:** [["São Paulo", 1524], ["Rio de Janeiro", 987], ["Belo Horizonte", 756]]
**Resposta:** "São Paulo lidera o ranking com 1.524 casos, seguida por Rio de Janeiro (987) e Belo Horizonte (756)."

### Exemplo 2:
**Pergunta:** "Qual a cidade com menor número de casos?"
**Dados:** [["Pequena Cidade", 12], ["Outra Cidade", 45]]
**Resposta:** "Pequena Cidade apresentou o menor número com apenas 12 casos."
""",
            
            "TEMPO_INTERNACAO": """
### Exemplo 1:
**Pergunta:** "Qual é o diagnóstico que possui o maior tempo médio de internação?"
**Dados:** [["F20", 11, 66.6]]
**Resposta:** "O diagnóstico F20 (Esquizofrenia) possui o maior tempo médio de internação com 66,6 dias (baseado em 11 casos)."

### Exemplo 2:
**Pergunta:** "Qual o tempo médio de internação em 2017?"
**Dados:** [["2017", 15.3]]
**Resposta:** "O tempo médio de internação em 2017 foi de 15,3 dias."
""",

            "ANÁLISE_ESTATÍSTICA": """
### Exemplo 1:
**Pergunta:** "Qual a média de casos por município?"
**Dados:** {"media_casos": 234.5}
**Resposta:** "A média de casos por município é de 234,5."

### Exemplo 2:
**Pergunta:** "Percentual de mortalidade"
**Dados:** {"percentual": 3.2}
**Resposta:** "O percentual de mortalidade é de 3,2%."
"""
        }
        
        return examples_map.get(query_type, """
### Exemplo Geral:
**Pergunta:** "Informações sobre os dados"
**Dados:** [dados variados]
**Resposta:** "Baseado nos dados disponíveis, encontrei as seguintes informações relevantes."
""")

    def _format_sql_results(self, sql_results: Any) -> str:
        """Formata os resultados SQL para inclusão no prompt."""
        self.logger.info(f"🔄 Formatando resultados SQL: tipo={type(sql_results).__name__}")
        
        if sql_results is None:
            self.logger.info("❌ Resultados SQL são None")
            return "Nenhum resultado encontrado."
        
        if isinstance(sql_results, (list, tuple)):
            self.logger.info(f"📊 Resultados são lista/tupla com {len(sql_results)} items")
            
            if len(sql_results) == 0:
                self.logger.info("📭 Lista de resultados está vazia")
                return "Nenhum resultado encontrado."
            
            # Limita a quantidade de resultados para evitar prompts muito longos
            limited_results = sql_results[:20]
            self.logger.info(f"✂️ Limitando resultados para {len(limited_results)} items (de {len(sql_results)} totais)")
            
            # Log first few items to understand structure
            for i, item in enumerate(limited_results[:3]):
                self.logger.info(f"📋 Item {i}: tipo={type(item).__name__}, valor={str(item)[:100]}{'...' if len(str(item)) > 100 else ''}")
            
            # Handle structured results from query parser
            if isinstance(limited_results[0], dict):
                self.logger.info("🗂️ Processando resultados como dicionários")
                # Look for simple query results with a single value
                result_item = limited_results[0]
                self.logger.info(f"🔍 Primeiro item do dict: {result_item}")
                
                if "result" in result_item:
                    result_value = result_item["result"]
                    self.logger.info(f"🎯 Encontrou resultado simples: {result_value}")
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
                self.logger.info("📊 Processando resultados como tabelas (listas/tuplas)")
                # Resultados tabulares - analisa se é uma consulta de ranking
                formatted = []
                for i, row in enumerate(limited_results):
                    self.logger.info(f"📝 Linha {i}: {row}")
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
                
                self.logger.info(f"✅ Resultado tabular formatado: {result_text[:200]}{'...' if len(result_text) > 200 else ''}")
                return result_text
            else:
                # Lista simples
                self.logger.info("❓ Processando como lista simples")
                formatted_result = str(limited_results)
                self.logger.info(f"🔤 Resultado como string: {formatted_result[:200]}{'...' if len(formatted_result) > 200 else ''}")
                return formatted_result
        
        # Fallback para tipos não reconhecidos
        self.logger.info(f"🔤 Fallback: convertendo {type(sql_results)} para string")
        final_result = str(sql_results)
        self.logger.info(f"🔚 Resultado final: {final_result[:200]}{'...' if len(final_result) > 200 else ''}")
        return final_result

    def _call_llm(self, prompt: str) -> str:
        """Realiza chamada para o LLM com retry logic."""
        prompt_length = len(prompt)
        estimated_tokens = prompt_length // 4  # Rough token estimation
        
        self.logger.info(f"🔄 Iniciando chamada ao LLM conversacional {self.config.model_name}")
        self.logger.info(f"📏 Tamanho do prompt: {prompt_length} caracteres (~{estimated_tokens} tokens)")
        self.logger.info(f"⚙️ Configuração: temp={self.config.temperature}, max_tokens={self.config.max_tokens}")
        
        for attempt in range(self.config.max_retries):
            try:
                self.logger.info(f"🎯 Tentativa {attempt + 1}/{self.config.max_retries} - Enviando requisição para {self.config.model_name}")
                start_time = time.time()
                
                response = self._make_request(prompt)
                
                elapsed_time = time.time() - start_time
                response_length = len(response) if response else 0
                
                self.logger.info(f"✅ LLM conversacional respondeu em {elapsed_time:.2f}s")
                self.logger.info(f"📤 Resposta recebida: {response_length} caracteres")
                self.logger.info(f"💬 Primeira linha da resposta: '{response[:100]}{'...' if len(response) > 100 else ''}'")
                
                return response
                
            except LLMTimeoutError:
                if attempt == self.config.max_retries - 1:
                    self.logger.error(f"❌ Timeout final na tentativa {attempt + 1}")
                    raise
                self.logger.warning(
                    f"⏰ Timeout na tentativa {attempt + 1}, tentando novamente..."
                )
                time.sleep(2 ** attempt)  # Backoff exponencial
                
            except LLMCommunicationError as e:
                if attempt == self.config.max_retries - 1:
                    self.logger.error(f"❌ Erro de comunicação final na tentativa {attempt + 1}: {e}")
                    raise
                self.logger.warning(
                    f"⚠️ Erro de comunicação na tentativa {attempt + 1}: {e}"
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
            
            # Remove aspas desnecessárias para resposta mais amigável
            cleaned_response = self._clean_response(llm_response)
            
            self.logger.info(
                f"Resposta conversacional gerada em {response_time:.2f}s"
            )
            
            return cleaned_response

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

    def _clean_response(self, response: str) -> str:
        """Remove aspas desnecessárias e formatação extra da resposta."""
        if not response:
            return response
        
        cleaned = response.strip()
        
        # Remove aspas duplas no início e fim se presentes (múltiplas iterações para casos aninhados)
        while cleaned.startswith('"') and cleaned.endswith('"') and len(cleaned) > 2:
            cleaned = cleaned[1:-1].strip()
        
        # Remove aspas simples no início e fim se presentes  
        while cleaned.startswith("'") and cleaned.endswith("'") and len(cleaned) > 2:
            cleaned = cleaned[1:-1].strip()
        
        # Remove identificadores de template que podem vazar na resposta
        lines_to_remove = [
            "Tipo de consulta identificado:",
            "**RESPOSTA:**", 
            "---",
            "**Sua resposta deve seguir este padrão:**",
            "Resposta:",
            "- Seja direta e factual",
            "- Uma frase ou parágrafo curto", 
            "- Português brasileiro natural",
            "- Foque na utilidade prática"
        ]
        
        # Remove placeholders em colchetes e parênteses que podem vazar
        placeholder_patterns = [
            r'\[.*número.*exato.*\]',
            r'\[.*resposta.*direta.*\]', 
            r'\[.*factual.*\]',
            r'\[.*dados.*variados.*\]',
            r'\[.*informações.*relevantes.*\]',
            r'\(valor do campo.*\)',
            r'\[valor.*\]',
            r'\[.*número.*\]',
            r'- \(.*campo.*\)',
            r'`.*idade_media.*`'
        ]
        
        lines = cleaned.split('\n')
        filtered_lines = []
        
        for line in lines:
            line_stripped = line.strip()
            should_remove = False
            
            for remove_pattern in lines_to_remove:
                if remove_pattern in line_stripped:
                    should_remove = True
                    break
            
            # Remove linhas que são apenas números isolados (resquícios de listas de template)
            if line_stripped and line_stripped.replace('.', '').replace(')', '').isdigit():
                should_remove = True
            
            if not should_remove and line_stripped:
                filtered_lines.append(line.strip())
        
        # Reconstrói a resposta limpa
        final_response = '\n'.join(filtered_lines).strip()
        
        # Remove placeholders em colchetes usando regex
        import re
        for pattern in placeholder_patterns:
            final_response = re.sub(pattern, '', final_response, flags=re.IGNORECASE)
        
        # Remove espaços extras resultantes da remoção de placeholders
        final_response = ' '.join(final_response.split())
        
        # Última limpeza: remover aspas duplas que possam ter sobrado
        while final_response.startswith('"') and final_response.endswith('"') and len(final_response) > 2:
            final_response = final_response[1:-1].strip()
        
        # Log da limpeza para debug
        if final_response != response.strip():
            self.logger.info(f"🧹 Resposta limpa: '{response.strip()}' → '{final_response}'")
        
        return final_response

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