# 🤖 Proposta: Arquitetura Multi-Agent Verdadeira

## 🎯 Visão Atual vs Proposta

### **Estado Atual (Multi-Agent implícito):**
```
User Query → Orchestrator → Classification Service → Router Logic → Services
                              ↓
                         Pattern + LLM
                              ↓
                    [Conversational | SQL Processing]
```

### **Proposta: Multi-Agent Explícito e Extensível:**
```
User Query → Router Agent (LLM) → Agent Dispatcher → Specialized Agents
                ↓
        Intent Analysis
                ↓
    [SQL Agent | Conversational Agent | Graph Agent | Insight Agent | ...]
```

## 🏗️ Nova Arquitetura Multi-Agent

### **1. Router Agent (Central)**
**Responsabilidade**: Único ponto de entrada que sempre analisa intent e roteia
```python
class RouterAgent:
    """Central router that analyzes intent and dispatches to specialized agents"""
    
    def route_query(self, user_query: str) -> AgentRoutingDecision:
        # SEMPRE usa LLM conversacional para entender intent
        intent_analysis = self.analyze_intent_with_llm(user_query)
        
        return AgentRoutingDecision(
            target_agent=intent_analysis.recommended_agent,
            confidence=intent_analysis.confidence,
            reasoning=intent_analysis.reasoning,
            context=intent_analysis.extracted_context
        )
```

### **2. Specialized Agents**

#### **SQL Agent** (já existe)
```python
class SQLAgent:
    """Handles database queries and data extraction"""
    def process(self, query: str, context: Dict) -> AgentResponse:
        # Current SQL processing logic (Direct LLM + LangChain fallback)
        pass
```

#### **Conversational Agent** (já existe)
```python
class ConversationalAgent:
    """Handles explanations, definitions, general questions"""
    def process(self, query: str, context: Dict) -> AgentResponse:
        # Current conversational logic
        pass
```

#### **Graph Agent** (NOVO) 🆕
```python
class GraphAgent:
    """Generates charts and visualizations from data"""
    def process(self, query: str, context: Dict) -> AgentResponse:
        # 1. Extract data requirements
        # 2. Call SQL Agent if needed for data
        # 3. Generate visualization (matplotlib, plotly, etc.)
        # 4. Return chart + insights
        pass
```

#### **Insight Agent** (NOVO) 🆕
```python
class InsightAgent:
    """Generates analytical insights and summaries"""
    def process(self, query: str, context: Dict) -> AgentResponse:
        # 1. Analyze data patterns
        # 2. Generate statistical insights
        # 3. Provide recommendations
        # 4. Detect anomalies/trends
        pass
```

#### **Report Agent** (FUTURO) 🔮
```python
class ReportAgent:
    """Generates comprehensive reports and documentation"""
    def process(self, query: str, context: Dict) -> AgentResponse:
        # Generate formatted reports (PDF, HTML, etc.)
        pass
```

## 🔄 Fluxo de Execução

### **Router Agent Prompt (LLM)**
```
Você é um Router Agent especializado em analisar queries sobre dados do SUS brasileiro.

Analise a seguinte query do usuário e determine qual agente deve processar:

AGENTES DISPONÍVEIS:
1. SQL_AGENT: Para consultas que precisam extrair dados do banco
   - Exemplos: "Quantos pacientes?", "Casos por cidade", "Estatísticas"
   
2. CONVERSATIONAL_AGENT: Para explicações e definições
   - Exemplos: "O que significa CID J90?", "Como funciona o SUS?"
   
3. GRAPH_AGENT: Para visualizações e gráficos
   - Exemplos: "Gere um gráfico de mortes por sexo", "Mostre tendências"
   
4. INSIGHT_AGENT: Para análises e insights
   - Exemplos: "Analise padrões", "Que insights você pode dar?"

QUERY: "{user_query}"

Responda em JSON:
{
  "target_agent": "SQL_AGENT|CONVERSATIONAL_AGENT|GRAPH_AGENT|INSIGHT_AGENT",
  "confidence": 0.0-1.0,
  "reasoning": "explicação da decisão",
  "extracted_context": {
    "data_requirements": ["tabelas", "colunas"],
    "visualization_type": "chart_type se aplicável",
    "analysis_focus": "foco da análise"
  }
}
```

### **Agent Dispatcher**
```python
class AgentDispatcher:
    """Dispatches queries to appropriate agents based on router decisions"""
    
    def __init__(self):
        self.agents = {
            "SQL_AGENT": SQLAgent(),
            "CONVERSATIONAL_AGENT": ConversationalAgent(), 
            "GRAPH_AGENT": GraphAgent(),
            "INSIGHT_AGENT": InsightAgent()
        }
        
    def dispatch(self, routing_decision: AgentRoutingDecision, query: str) -> AgentResponse:
        target_agent = self.agents[routing_decision.target_agent]
        return target_agent.process(query, routing_decision.context)
```

## 🎨 Implementação dos Novos Agents

### **Graph Agent - Geração de Visualizações**
```python
class GraphAgent:
    def process(self, query: str, context: Dict) -> AgentResponse:
        # 1. Determinar tipo de visualização necessária
        viz_prompt = f"""
        Baseado na query "{query}", que tipo de gráfico seria mais apropriado?
        
        Opções: bar_chart, line_chart, pie_chart, scatter_plot, heatmap
        
        Responda também quais dados são necessários.
        """
        
        viz_analysis = self.llm.send_prompt(viz_prompt)
        
        # 2. Obter dados via SQL Agent se necessário
        if context.get('data_requirements'):
            sql_agent = SQLAgent()
            data_result = sql_agent.process(self._generate_data_query(context), {})
            
        # 3. Gerar visualização
        chart = self._create_visualization(viz_analysis, data_result)
        
        # 4. Gerar insights sobre o gráfico
        insights = self._generate_chart_insights(chart, data_result)
        
        return AgentResponse(
            content=f"Gráfico gerado: {chart.title}",
            chart=chart,
            insights=insights,
            metadata={"visualization_type": viz_analysis.chart_type}
        )
```

### **Insight Agent - Análise de Dados**
```python
class InsightAgent:
    def process(self, query: str, context: Dict) -> AgentResponse:
        # 1. Identificar que tipo de insight é solicitado
        insight_prompt = f"""
        Query: "{query}"
        
        Que tipo de análise seria mais valiosa?
        - Tendências temporais
        - Comparações demográficas  
        - Padrões geográficos
        - Correlações
        - Anomalias
        """
        
        # 2. Obter dados relevantes
        data = self._gather_analysis_data(context)
        
        # 3. Executar análise estatística
        stats = self._perform_statistical_analysis(data)
        
        # 4. Gerar insights em linguagem natural
        insights = self._generate_natural_language_insights(stats, data)
        
        return AgentResponse(
            content=insights,
            statistical_data=stats,
            recommendations=self._generate_recommendations(insights)
        )
```

## 🔧 Estrutura de Arquivos Proposta

```
src/application/agents/
├── __init__.py
├── base/
│   ├── agent_interface.py      # Interface base para todos os agents
│   ├── agent_response.py       # Estruturas de resposta padronizadas
│   └── agent_dispatcher.py     # Dispatcher central
├── router/
│   ├── router_agent.py         # Router Agent principal
│   └── intent_analyzer.py      # Análise de intent via LLM
├── specialized/
│   ├── sql_agent.py           # Agent para queries SQL (refatorado)
│   ├── conversational_agent.py # Agent conversacional (refatorado)  
│   ├── graph_agent.py         # NOVO: Geração de gráficos
│   ├── insight_agent.py       # NOVO: Análise e insights
│   └── report_agent.py        # FUTURO: Geração de relatórios
└── registry/
    └── agent_registry.py      # Registro dinâmico de agents
```

## 🎯 Benefícios da Nova Arquitetura

### **1. Extensibilidade** 🚀
- **Plug-and-play agents**: Novos agents podem ser adicionados facilmente
- **Registry pattern**: Agents se registram dinamicamente
- **Interface padronizada**: Todos seguem o mesmo contrato

### **2. Separation of Concerns** 🎯
- **Router Agent**: APENAS roteamento
- **Specialized Agents**: APENAS sua responsabilidade específica
- **Dispatcher**: APENAS orquestração

### **3. Escalabilidade** 📈
- **Agents independentes**: Podem ser escalados individualmente
- **Processamento paralelo**: Agents podem trabalhar em conjunto
- **Microserviços ready**: Cada agent pode virar um serviço

### **4. Testabilidade** 🧪
- **Testes isolados**: Cada agent pode ser testado independentemente
- **Mocking**: Router pode ser mockado para testar agents
- **Integration tests**: Fluxo completo testável

## 🔄 Migração Gradual

### **Fase 1: Refatorar Router**
1. Converter `QueryClassificationService` em `RouterAgent`
2. Sempre usar LLM para roteamento (não mais pattern matching)
3. Manter agents atuais funcionando

### **Fase 2: Adicionar Graph Agent**
1. Implementar `GraphAgent` para visualizações básicas
2. Integrar com matplotlib/plotly
3. Testar com queries de gráfico

### **Fase 3: Adicionar Insight Agent**
1. Implementar análises estatísticas
2. Geração de insights automáticos
3. Recommendations engine

### **Fase 4: Agent Registry**
1. Sistema de registro dinâmico
2. Discovery automático de agents
3. Load balancing entre agents

## 🎨 Exemplos de Uso

### **Router Agent sempre ativo:**
```
"Quantos pacientes existem?" → SQL_AGENT
"O que significa CID J90?" → CONVERSATIONAL_AGENT  
"Gere um gráfico de mortes por sexo" → GRAPH_AGENT
"Que insights você tem sobre os dados?" → INSIGHT_AGENT
"Analise padrões geográficos" → INSIGHT_AGENT + GRAPH_AGENT (chain)
```

### **Multi-agent chains:**
```
"Analise mortes por região e gere um mapa"
→ Router: INSIGHT_AGENT + GRAPH_AGENT
→ Insight Agent: analisa padrões regionais
→ Graph Agent: gera mapa de calor
→ Response: insights + visualização
```

Esta arquitetura transforma o sistema atual em uma plataforma multi-agent verdadeira, extensível e pronta para futuras funcionalidades como BI, relatórios automatizados, e análises preditivas! 🚀