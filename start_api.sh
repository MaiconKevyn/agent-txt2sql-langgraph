#!/bin/bash

# Script para inicializar a API TXT2SQL
echo "🚀 Inicializando API TXT2SQL..."

# Verificar se a porta 8000 está em uso
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "⚠️ Porta 8000 em uso, tentando porta 8001..."
    PORT=8001 python api_server.py
else
    echo "✅ Porta 8000 disponível"
    python api_server.py
fi