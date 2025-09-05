# DataVisSUS Web Interface

Interface web independente para comunicação com o TXT2SQL Agent do DataVisSUS.

## 📋 Visão Geral

Esta é uma interface web moderna e responsiva que se conecta ao Agent TXT2SQL via API REST, permitindo consultas em linguagem natural sobre dados de saúde pública do SUS.

## 🚀 Funcionalidades

- **Interface Web Moderna**: Design responsivo com tema DataVisSUS
- **Comunicação via API**: Integração direta com o Agent via HTTP/JSON
- **Chat Inteligente**: Interface conversacional com histórico
- **Visualização de Schema**: Modal para visualizar estrutura do banco
- **Status em Tempo Real**: Monitoramento de conexão com o Agent
- **Exemplos Pré-definidos**: Consultas prontas para demonstração
- **Tratamento de Erros**: Feedback visual de erros e timeouts

## 🛠️ Instalação

### Pré-requisitos

- Node.js >= 16.0.0
- npm >= 8.0.0
- TXT2SQL Agent rodando na porta 8000

### Passos de Instalação

1. **Clone ou acesse o diretório do projeto**:
   ```bash
   cd /home/maiconkevyn/PycharmProjects/datavissus-web-interface
   ```

2. **Instale as dependências**:
   ```bash
   npm install
   ```

3. **Configure as variáveis de ambiente** (opcional):
   ```bash
   cp .env.example .env
   # Edite o arquivo .env conforme necessário
   ```

4. **Inicie o servidor**:
   ```bash
   npm start
   ```

O servidor estará disponível em `http://localhost:3000`

## 🔧 Configuração

### Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `PORT` | `3000` | Porta do servidor web |
| `HOST` | `0.0.0.0` | Host do servidor |
| `API_BASE_URL` | `http://localhost:8000` | URL do TXT2SQL Agent |
| `NODE_ENV` | `development` | Ambiente de execução |

### Configuração do Agent API

O Agent TXT2SQL deve estar rodando na porta 8000. Para iniciar o Agent:

```bash
cd /home/maiconkevyn/PycharmProjects/txt2sql_claude_s
python src/interfaces/api/main.py
```

## 📡 Arquitetura da Comunicação

```
Interface Web (Port 3000) → HTTP/JSON → Agent API (Port 8000)
```

### Endpoints Utilizados

- **POST** `/api/query` - Processa consultas em linguagem natural
- **GET** `/api/schema` - Obtém esquema do banco de dados
- **GET** `/api/health` - Verifica status da interface web
- **GET** `/api/agent-health` - Verifica status do Agent

## 🎯 Como Usar

1. **Acesse a interface**: Abra `http://localhost:3000` no navegador
2. **Verifique o status**: O indicador no topo direito deve mostrar "Online"
3. **Faça uma pergunta**: Digite sua consulta na caixa de texto
4. **Use exemplos**: Clique nos botões de exemplo na barra lateral
5. **Visualize o schema**: Use o botão "Schema" para ver a estrutura do banco

### Exemplos de Consultas

- "Qual é cidade com mais morte de homens?"
- "Qual é a idade média das mulheres que morreram?"
- "Quantos leitos de UTI existem em Minas Gerais?"
- "Quais foram as 5 cidades com mais mortes?"

## 🔒 Segurança

- **CORS configurado** para desenvolvimento local
- **Rate limiting** para prevenção de abuso
- **Headers de segurança** via Helmet.js
- **Sanitização de inputs** para prevenir XSS
- **Timeouts configurados** para prevenir travamentos

## 🚨 Solução de Problemas

### Agent Offline

Se o status mostrar "Agent Offline":

1. Verifique se o Agent está rodando: `http://localhost:8000/docs`
2. Confirme que não há firewall bloqueando a porta 8000
3. Restart o Agent: `python src/interfaces/api/main.py`

### Erro de CORS

Se houver erros de CORS:

1. Verifique a configuração `ALLOWED_ORIGINS` no Agent
2. Confirme que a URL da interface está incluída
3. Restart ambos os serviços

### Erro de Conexão

Para erros de "Failed to fetch":

1. Verifique se ambos os serviços estão rodando
2. Teste o Agent diretamente: `curl http://localhost:8000/health`
3. Verifique os logs do console do navegador

## 📝 Scripts Disponíveis

- `npm start` - Inicia o servidor em modo produção
- `npm run dev` - Inicia o servidor em modo desenvolvimento (com nodemon)
- `npm test` - Executa testes (não implementado ainda)

## 🛠️ Desenvolvimento

### Estrutura do Projeto

```
datavissus-web-interface/
├── package.json          # Dependências e scripts
├── server.js            # Servidor Express.js principal
├── config/
│   └── api.js          # Configuração da API
├── public/             # Arquivos estáticos
│   ├── index.html      # Interface principal
│   ├── app.js         # Lógica JavaScript do cliente
│   └── styles.css     # Estilos CSS
├── .env.example       # Exemplo de configuração
└── README.md         # Esta documentação
```

### Tecnologias Utilizadas

- **Backend**: Node.js, Express.js
- **Frontend**: HTML5, CSS3, JavaScript (Vanilla)
- **Segurança**: Helmet.js, CORS, Rate Limiting
- **Comunicação**: Fetch API, JSON
- **UI/UX**: FontAwesome, Google Fonts, CSS Grid/Flexbox

## 🔄 Integração com Agent

A comunicação com o Agent é feita através de proxying das requisições:

1. **Interface Web** recebe requisição do usuário
2. **Server.js** faz forward para o Agent API
3. **Agent** processa a consulta e retorna resultado
4. **Interface Web** exibe o resultado formatado

## 📊 Monitoramento

- **Status de Conexão**: Verificação automática a cada 30 segundos
- **Health Checks**: Endpoints dedicados para verificação
- **Error Tracking**: Logs detalhados de erros
- **Performance**: Medição de tempo de execução

## 🤝 Contribuição

Para contribuir com o projeto:

1. Faça fork do repositório
2. Crie uma branch para sua feature
3. Faça commit das mudanças
4. Abra um Pull Request

## 📄 Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para detalhes.

## 🆘 Suporte

Para suporte técnico ou dúvidas:

1. Verifique a seção de solução de problemas
2. Consulte os logs do console
3. Abra uma issue no repositório

---

**DataVisSUS Team** - Interface Web para Análise de Dados de Saúde Pública