/**
 * Configuração da API do DataVisSUS Agent
 */

// Configuração base da API
const API_CONFIG = {
    // URL base do agent (ajuste conforme necessário)
    BASE_URL: process.env.API_BASE_URL || 'http://localhost:8000',
    
    // Endpoints disponíveis
    ENDPOINTS: {
        QUERY: '/query',
        HEALTH: '/health', 
        SCHEMA: '/schema',
        MODELS: '/models'
    },
    
    // Configurações de timeout
    TIMEOUTS: {
        QUERY: 120000,  // 2 minutes
        HEALTH: 5000,   // 5 seconds
        SCHEMA: 30000,  // 30 seconds
        MODELS: 10000   // 10 seconds
    },
    
    // Configurações de retry
    RETRY: {
        MAX_ATTEMPTS: 3,
        DELAY: 1000,
        BACKOFF_MULTIPLIER: 2
    }
};

module.exports = API_CONFIG;