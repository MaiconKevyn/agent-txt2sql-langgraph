# Documentação dos Serviços - Projeto TXT2SQL Claude

## Visão Geral do Projeto

O **TXT2SQL Claude** é um sistema inteligente que converte perguntas em linguagem natural para consultas SQL, especialmente otimizado para dados do Sistema Único de Saúde (SUS) brasileiro. O projeto implementa uma arquitetura limpa baseada em microserviços, onde cada componente possui uma responsabilidade específica e bem definida.

---

## Arquitetura e Filosofia do Design

### Princípios Fundamentais

**1. Single Responsibility Principle (SRP)**
- Cada serviço tem uma única responsabilidade bem definida
- Facilita manutenção, testes e evolução independente
- Reduz acoplamento entre componentes

**2. Dependency Injection**
- Inversão de controle através de interfaces abstratas
- Facilita testes unitários e substituição de implementações
- Melhora a modularidade do sistema

**3. Domain-Driven Design**
- Foco no domínio específico (dados de saúde pública SUS)
- Entidades e objetos de valor bem definidos
- Linguagem ubíqua do domínio médico brasileiro

---

## Serviços Principais

### 1. LLM Communication Service 🤖
**Localização:** `src/application/services/llm_communication_service.py`

#### Responsabilidade
Gerenciar toda comunicação com modelos de linguagem (LLM), abstraindo as complexidades de conectividade e configuração.

#### Visão do Cientista de Dados
O LLM é o coração cognitivo do sistema. A decisão de usar **Ollama com Llama3** foi estratégica:
- **Controle local**: Evita dependências de APIs externas
- **Privacidade**: Dados de saúde permanecem no ambiente local
- **Customização**: Permite fine-tuning específico para terminologia médica SUS
- **Custo**: Elimina custos de APIs comerciais para queries frequentes

#### Tecnologias Utilizadas
- **LangChain Community**: Framework para abstração de LLMs
- **Requests**: Comunicação HTTP com servidor Ollama
- **Retry Logic**: Implementação robusta de tentativas com backoff exponencial

#### Benefícios da Implementação
- **Configuração flexível**: Temperatura, timeout, max tokens configuráveis
- **Health checks**: Monitoramento automático da disponibilidade do LLM
- **Tratamento de erros**: Diferentes tipos de exceções para diferentes cenários
- **Métricas**: Rastreamento de tempo de execução e uso de tokens

```python
@dataclass
class LLMConfig:
    model_name: str = "llama3"
    temperature: float = 0.0  # Determinístico para SQL
    timeout: int = 120
    max_retries: int = 3
```

---

### 2. Database Connection Service 🗄️
**Localização:** `src/application/services/database_connection_service.py`

#### Responsabilidade
Gerenciar conexões com o banco de dados, fornecendo abstrações para diferentes tipos de SGBD.

#### Visão do Cientista de Dados
A escolha do **SQLite** foi deliberada para este contexto:
- **Simplicidade**: Ideal para prototipagem e demonstrações
- **Performance**: Excelente para datasets de tamanho médio (< 1GB)
- **Portabilidade**: Arquivo único, fácil distribuição
- **Compatibilidade**: Suporte nativo no LangChain

#### Tecnologias Utilizadas
- **SQLite3**: SGBD leve e eficiente
- **LangChain SQLDatabase**: Abstração para uso com LLMs
- **Connection pooling**: Gerenciamento eficiente de conexões

#### Benefícios da Implementação
- **Thread-safe**: Suporte a múltiplas consultas simultâneas
- **Abstração**: Interface permite migração futura para PostgreSQL/MySQL
- **Health checks**: Validação automática de conectividade
- **Resource management**: Fechamento adequado de conexões

```python
class SQLiteDatabaseConnectionService(IDatabaseConnectionService):
    def get_connection(self) -> SQLDatabase:
        # LangChain-compatible connection
    
    def get_raw_connection(self) -> sqlite3.Connection:
        # Direct SQLite access for complex operations
```

---

### 3. Schema Introspection Service 🔍
**Localização:** `src/application/services/schema_introspection_service.py`

#### Responsabilidade
Analisar estrutura do banco de dados e fornecer contexto rico para geração de consultas SQL.

#### Visão do Cientista de Dados
Este serviço é crucial para a qualidade das consultas geradas. A estratégia inclui:
- **Contexto semântico**: Mapeamento de colunas técnicas para significados médicos
- **Dados de exemplo**: Amostras reais ajudam o LLM a entender formatos
- **Validações**: Conhecimento sobre códigos SUS, CID-10, etc.
- **Performance**: Cache inteligente evita introspecções repetidas

#### Tecnologias Utilizadas
- **SQLite PRAGMA**: Metadados nativos do SQLite
- **Dataclasses**: Estruturas tipadas para informações de schema
- **Caching**: Otimização de performance

#### Benefícios da Implementação
- **Contexto rico**: Descrições detalhadas de cada coluna
- **Exemplos práticos**: Queries de exemplo específicas do domínio SUS
- **Notas importantes**: Alertas sobre peculiaridades dos dados
- **Extensibilidade**: Fácil adaptação para outros domínios

```python
@dataclass
class ColumnInfo:
    name: str
    type: str
    nullable: bool
    primary_key: bool
    description: Optional[str] = None  # Descrição semântica
```

---

### 4. SUS Prompt Template Service 📋
**Localização:** `src/application/services/sus_prompt_template_service.py`

#### Responsabilidade
Fornecer templates especializados de prompts para diferentes tipos de análise no domínio SUS.

#### Visão do Cientista de Dados
Os prompts são a interface crítica entre humano e IA. A especialização por domínio SUS inclui:
- **Terminologia médica**: Conhecimento específico de códigos e procedimentos
- **Contexto brasileiro**: Regiões, estados, municípios
- **Tipos de análise**: Estatística, comparativa, temporal, geográfica
- **Formato de resposta**: Estruturação adequada para cada tipo de consulta

#### Tecnologias Utilizadas
- **Enum patterns**: Tipagem forte para diferentes tipos de prompts
- **Template engine**: Sistema flexível de interpolação
- **Knowledge base**: Base de conhecimento estruturada do SUS

#### Benefícios da Implementação
- **Especialização**: Prompts otimizados para cada tipo de análise
- **Consistência**: Formato padronizado de respostas
- **Contexto rico**: Incorporação automática de conhecimento do domínio
- **Flexibilidade**: Fácil adição de novos tipos de análise

```python
class PromptType(Enum):
    BASIC_RESPONSE = "basic_response"
    STATISTICAL_ANALYSIS = "statistical_analysis"
    COMPARATIVE_ANALYSIS = "comparative_analysis"
    TREND_ANALYSIS = "trend_analysis"
    GEOGRAPHIC_ANALYSIS = "geographic_analysis"
```

---

### 5. Query Processing Service ⚙️
**Localização:** `src/application/services/query_processing_service.py`

#### Responsabilidade
Coordenar o processamento completo de consultas: tradução para SQL, validação, execução e parsing de resultados.

#### Visão do Cientista de Dados
Este é o serviço mais complexo, implementando a lógica central do sistema:
- **LangChain Agents**: Uso de agentes inteligentes para geração de SQL
- **Validação robusta**: Prevenção de SQL injection e queries perigosas
- **Correção automática**: Fix de problemas comuns (case sensitivity)
- **Parsing inteligente**: Extração estruturada de resultados diversos

#### Tecnologias Utilizadas
- **LangChain Agents**: Framework de agentes para SQL
- **Regular Expressions**: Parsing e correção de queries
- **SQLDatabaseToolkit**: Ferramentas especializadas para SQL
- **Retry Logic**: Recuperação automática de falhas

#### Benefícios da Implementação
- **Robustez**: Múltiplas camadas de validação e correção
- **Segurança**: Proteção contra queries maliciosas
- **Flexibilidade**: Suporte a diferentes tipos de resultados
- **Observabilidade**: Logging detalhado para debugging

```python
def _fix_case_sensitivity_issues(self, sql_query: str) -> str:
    """Fix common case sensitivity issues in city names"""
    # Converts 'porto alegre' -> 'Porto Alegre'
    # Essential for Brazilian city name matching
```

---

### 6. Conversational LLM Service 💬
**Localização:** `src/application/services/conversational_llm_service.py`

#### Responsabilidade
Especializar comunicação com LLM para geração de respostas conversacionais amigáveis.

#### Visão do Cientista de Dados
Separação clara entre LLM para SQL (técnico) e LLM para conversação (amigável):
- **Temperatura diferenciada**: Mais criativo para linguagem natural
- **Prompts especializados**: Foco em explicação e contexto
- **Formato estruturado**: Respostas organizadas e didáticas
- **Fallback gracioso**: Degradação elegante em caso de falhas

#### Tecnologias Utilizadas
- **HTTP Requests**: Comunicação direta com API Ollama
- **JSON Processing**: Estruturação de payloads
- **Error Handling**: Diferentes tipos de exceções

#### Benefícios da Implementação
- **Especialização**: Configuração otimizada para conversação
- **Robustez**: Tratamento específico de timeouts e falhas
- **Contexto SUS**: Conhecimento embutido sobre saúde pública
- **Performance**: Otimização de prompts para respostas concisas

---

### 7. Conversational Response Service 🗣️
**Localização:** `src/application/services/conversational_response_service.py`

#### Responsabilidade
Orquestrar geração de respostas conversacionais completas com contexto e memória.

#### Visão do Cientista de Dados
Este serviço implementa inteligência conversacional avançada:
- **Memória de sessão**: Contexto de conversas anteriores
- **Determinação automática**: Escolha inteligente do tipo de resposta
- **Sugestões proativas**: Recomendações de análises adicionais
- **Métricas de qualidade**: Score de confiança das respostas

#### Tecnologias Utilizadas
- **Dataclasses**: Estruturas tipadas para contexto
- **Context Management**: Gerenciamento de sessões
- **Template Integration**: Uso dos templates especializados

#### Benefícios da Implementação
- **Experiência rica**: Conversas contextualizadas e inteligentes
- **Proatividade**: Sugestões automáticas de análises
- **Escalabilidade**: Suporte a múltiplas sessões simultâneas
- **Observabilidade**: Métricas detalhadas de performance

---

### 8. Error Handling Service ⚠️
**Localização:** `src/application/services/error_handling_service.py`

#### Responsabilidade
Centralizar tratamento de erros com categorização, logging e sugestões de recuperação.

#### Visão do Cientista de Dados
Um sistema robusto de tratamento de erros é essencial para produção:
- **Categorização inteligente**: Diferentes tipos de erro (DB, LLM, Sistema)
- **Severidade graduada**: Classificação de criticidade
- **Mensagens amigáveis**: Tradução de erros técnicos para usuários
- **Ações de recuperação**: Sugestões automáticas de resolução

#### Tecnologias Utilizadas
- **Logging Framework**: Sistema estruturado de logs
- **Enum Categories**: Tipagem forte para categorias de erro
- **Dataclasses**: Estruturas para informações de erro

#### Benefícios da Implementação
- **Centralização**: Ponto único para tratamento de erros
- **Observabilidade**: Logs estruturados para monitoramento
- **UX melhorada**: Mensagens compreensíveis para usuários
- **Debugging**: Rastreamento detalhado de problemas

---

### 9. User Interface Service 🖥️
**Localização:** `src/application/services/user_interface_service.py`

#### Responsabilidade
Abstrair interfaces de usuário permitindo múltiplas implementações (CLI, Web).

#### Visão do Cientista de Dados
Interface flexível preparada para evolução:
- **Abstração**: Mesma lógica para CLI e Web
- **Tipos diferenciados**: Basic, Interactive, Verbose
- **Validação de entrada**: Sanitização e proteção
- **Formatação contextual**: Apresentação adequada por tipo

#### Tecnologias Utilizadas
- **Abstract Base Classes**: Interfaces bem definidas
- **Enum Types**: Tipagem para diferentes modos
- **Input Validation**: Sanitização de entradas

#### Benefícios da Implementação
- **Flexibilidade**: Fácil adição de novas interfaces
- **Consistência**: Comportamento padronizado
- **Segurança**: Validação robusta de entradas
- **UX**: Diferentes níveis de verbosidade

---

### 10. Text2SQL Orchestrator 🎼
**Localização:** `src/application/orchestrator/text2sql_orchestrator.py`

#### Responsabilidade
Coordenar todos os serviços para fornecer fluxo completo de interação usuário-sistema.

#### Visão do Cientista de Dados
O orquestrador é o maestro que rege toda a sinfonia:
- **Dependency Injection**: Gerenciamento inteligente de dependências
- **Session Management**: Controle de sessões e contexto
- **Command Handling**: Processamento de comandos especiais
- **Graceful Degradation**: Funcionamento mesmo com falhas parciais

#### Tecnologias Utilizadas
- **Dependency Container**: Injeção de dependências
- **Session Management**: Controle de estado
- **Configuration**: Sistema flexível de configuração

#### Benefícios da Implementação
- **Coordenação**: Orquestração elegante de todos os serviços
- **Flexibilidade**: Configuração adaptável a diferentes cenários
- **Robustez**: Tolerância a falhas parciais
- **Extensibilidade**: Fácil adição de novos recursos

---

## Stack Tecnológico Detalhado

### Core Technologies

**LangChain Framework**
- **Justificativa**: Framework maduro para aplicações LLM
- **Benefícios**: Abstrações robustas, toolkit SQL, agentes inteligentes
- **Uso específico**: Geração de SQL, execução de queries, parsing de resultados

**Ollama + Llama3**
- **Justificativa**: Solução local, privada e customizável
- **Benefícios**: Controle total, sem custos de API, privacidade de dados
- **Uso específico**: Geração de SQL e respostas conversacionais

**SQLite**
- **Justificativa**: Simplicidade para demonstração e prototipagem
- **Benefícios**: Arquivo único, boa performance, fácil distribuição
- **Uso específico**: Armazenamento de dados SUS estruturados

### Supporting Libraries

**Pandas**
- Manipulação e análise de dados
- Usado para processamento de datasets SUS

**Requests**
- Comunicação HTTP com APIs
- Interface com servidor Ollama

**SQLAlchemy**
- ORM e abstração de banco de dados
- Compatibilidade com LangChain

**Streamlit**
- Interface web moderna e reativa
- Alternativa visual ao CLI

---

## Boas Práticas Implementadas

### 1. Clean Architecture
- **Separação de responsabilidades**: Cada camada tem função específica
- **Dependency Inversion**: Interfaces abstratas reduzem acoplamento
- **Testabilidade**: Código facilmente testável por unidade

### 2. Error Handling
- **Categorização**: Erros classificados por tipo e severidade
- **Graceful degradation**: Sistema continua funcionando com falhas parciais
- **User-friendly messages**: Erros técnicos traduzidos para usuários

### 3. Observability
- **Logging estruturado**: Logs categorizados e pesquisáveis
- **Métricas**: Rastreamento de performance e uso
- **Health checks**: Monitoramento automático de componentes

### 4. Security
- **Input validation**: Sanitização robusta de entradas
- **SQL injection protection**: Múltiplas camadas de proteção
- **Query whitelisting**: Apenas queries SELECT permitidas

### 5. Performance
- **Caching**: Schema e contexto em cache para performance
- **Connection pooling**: Gerenciamento eficiente de conexões
- **Lazy loading**: Carregamento sob demanda de recursos

---

## Conclusão

O projeto TXT2SQL Claude representa uma implementação moderna e robusta de um sistema inteligente para consultas em linguagem natural. A arquitetura baseada em microserviços garante:

- **Escalabilidade**: Cada serviço pode evoluir independentemente
- **Manutenibilidade**: Código organizado e bem documentado
- **Extensibilidade**: Fácil adição de novas funcionalidades
- **Robustez**: Tratamento abrangente de erros e falhas
- **Performance**: Otimizações em múltiplas camadas

A especialização para o domínio SUS demonstra como adaptar tecnologias genéricas para necessidades específicas, criando valor real para gestores de saúde pública no Brasil.

---

*Documentação criada para facilitar compreensão, manutenção e evolução do sistema TXT2SQL Claude.*