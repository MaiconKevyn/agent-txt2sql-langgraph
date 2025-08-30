// App Configuration - Updated for separated interface
const API_BASE_URL = 'http://localhost:8000';

// State Management
let isLoading = false;
let messageHistory = [];

// DOM Elements
const elements = {
    messageInput: document.getElementById('messageInput'),
    sendBtn: document.getElementById('sendBtn'),
    chatMessages: document.getElementById('chatMessages'),
    loadingOverlay: document.getElementById('loadingOverlay'),
    schemaModal: document.getElementById('schemaModal'),
    schemaBtn: document.getElementById('schemaBtn'),
    closeSchemaModal: document.getElementById('closeSchemaModal'),
    clearBtn: document.getElementById('clearBtn'),
    statusIndicator: document.getElementById('statusIndicator'),
    statusText: document.getElementById('statusText'),
    errorToast: document.getElementById('errorToast'),
    schemaContent: document.getElementById('schemaContent'),
    exampleBtns: document.querySelectorAll('.example-btn'),
    themeToggle: document.getElementById('themeToggle')
};

// Initialize App
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
    setupEventListeners();
    checkServerStatus();
    setWelcomeTime();
    initializeTheme();
});

function initializeApp() {
    // Auto-resize textarea
    elements.messageInput.addEventListener('input', autoResizeTextarea);
    
    // Enable send button when there's text
    elements.messageInput.addEventListener('input', toggleSendButton);
    
    // Handle Enter key
    elements.messageInput.addEventListener('keydown', handleKeyDown);
    
    // Load message history from localStorage
    loadMessageHistory();
}

function setupEventListeners() {
    // Send message
    elements.sendBtn.addEventListener('click', sendMessage);
    
    // Schema modal
    elements.schemaBtn.addEventListener('click', showSchemaModal);
    elements.closeSchemaModal.addEventListener('click', hideSchemaModal);
    elements.schemaModal.addEventListener('click', function(e) {
        if (e.target === elements.schemaModal) {
            hideSchemaModal();
        }
    });
    
    // Schema controls
    const loadSchemaBtn = document.getElementById('loadSchemaBtn');
    if (loadSchemaBtn) {
        loadSchemaBtn.addEventListener('click', loadSelectedSchema);
    }
    
    // Clear chat
    elements.clearBtn.addEventListener('click', clearChat);
    
    // Theme toggle
    elements.themeToggle.addEventListener('click', toggleTheme);
    
    // Example buttons
    elements.exampleBtns.forEach(btn => {
        btn.addEventListener('click', function() {
            const question = this.getAttribute('data-question');
            elements.messageInput.value = question;
            autoResizeTextarea();
            toggleSendButton();
            sendMessage();
        });
    });
    
    // Error toast close
    const toastClose = elements.errorToast.querySelector('.toast-close');
    if (toastClose) {
        toastClose.addEventListener('click', hideErrorToast);
    }
    
    // Keyboard shortcuts
    document.addEventListener('keydown', function(e) {
        // Ctrl/Cmd + Enter to send message
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            if (!isLoading && elements.messageInput.value.trim()) {
                sendMessage();
            }
        }
        
        // Escape to close modal
        if (e.key === 'Escape') {
            if (elements.schemaModal.classList.contains('show')) {
                hideSchemaModal();
            }
            if (elements.errorToast.classList.contains('show')) {
                hideErrorToast();
            }
        }
    });
}

function autoResizeTextarea() {
    const textarea = elements.messageInput;
    textarea.style.height = 'auto';
    textarea.style.height = Math.min(textarea.scrollHeight, 120) + 'px';
}

function toggleSendButton() {
    const hasText = elements.messageInput.value.trim().length > 0;
    elements.sendBtn.disabled = !hasText || isLoading;
}

function handleKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        if (!isLoading && elements.messageInput.value.trim()) {
            sendMessage();
        }
    }
}

async function sendMessage() {
    const message = elements.messageInput.value.trim();
    if (!message || isLoading) return;
    
    // Add user message to chat
    addMessage(message, 'user');
    
    // Clear input
    elements.messageInput.value = '';
    autoResizeTextarea();
    toggleSendButton();
    
    // Add loading message to chat instead of overlay
    const loadingMessageId = addLoadingMessage();
    
    try {
        const response = await fetch(`${API_BASE_URL}/query`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            },
            credentials: 'include',
            mode: 'cors',
            cache: 'no-cache',
            body: JSON.stringify({ question: message })
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        // Remove loading message
        removeLoadingMessage(loadingMessageId);
        
        if (data.success) {
            // Use conversational response from the agent
            let responseContent = data.response || data.conversational_response || 'Consulta processada com sucesso.';
            
            // Add SQL query details if available (commented out)
            // if (data.sql_query) {
            //     responseContent += `\n\n**SQL Executado:**\n\`\`\`sql\n${data.sql_query}\n\`\`\``;
            // }
            
            // Results summary removed for cleaner output
            
            addMessage(responseContent, 'assistant', {
                executionTime: data.execution_time
            });
        } else {
            addMessage(data.error_message || 'Erro desconhecido', 'error');
        }
        
        // Update server status as online since we got a response
        updateServerStatus(true);
        
    } catch (error) {
        console.error('Error sending message:', error);
        // Remove loading message
        removeLoadingMessage(loadingMessageId);
        
        let errorMessage = 'Erro de conexão com o servidor.';
        
        if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
            errorMessage = 'Não foi possível conectar ao Agent. Verifique se o TXT2SQL Agent está rodando na porta 8001.';
            updateServerStatus(false);
        } else if (error.message.includes('HTTP 5')) {
            errorMessage = 'Erro interno do servidor. Tente novamente em alguns instantes.';
        } else {
            errorMessage = `Erro de conexão: ${error.message}`;
        }
        
        addMessage(errorMessage, 'error');
        showErrorToast('Erro ao conectar com o Agent. Verifique se o TXT2SQL Agent está rodando.');
    }
}

function addMessage(content, type = 'assistant', metadata = null) {
    const messageData = {
        content,
        type,
        timestamp: new Date().toISOString(),
        metadata
    };
    
    // Add to history
    messageHistory.push(messageData);
    saveMessageHistory();
    
    // Create message element
    const messageElement = createMessageElement(messageData);
    elements.chatMessages.appendChild(messageElement);
    
    // Scroll to bottom
    scrollToBottom();
}

function createMessageElement(messageData) {
    const { content, type, timestamp, metadata } = messageData;
    
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${type}-message`;
    
    const avatarDiv = document.createElement('div');
    avatarDiv.className = 'message-avatar';
    
    const contentDiv = document.createElement('div');
    contentDiv.className = 'message-content';
    
    // Set avatar icon
    const icon = type === 'user' ? 'fas fa-user' : 
                 type === 'error' ? 'fas fa-exclamation-triangle' : 
                 'fas fa-robot';
    avatarDiv.innerHTML = `<i class="${icon}"></i>`;
    
    // Create message text
    const textDiv = document.createElement('div');
    textDiv.className = 'message-text';
    
    if (type === 'error') {
        textDiv.innerHTML = `<strong>Erro:</strong> ${escapeHtml(content)}`;
    } else {
        textDiv.innerHTML = formatMessageContent(content);
    }
    
    // Create timestamp
    const timeDiv = document.createElement('div');
    timeDiv.className = 'message-time';
    timeDiv.textContent = formatTime(timestamp);
    
    // Add execution time if available
    if (metadata && metadata.executionTime) {
        timeDiv.innerHTML += ` • <i class="fas fa-clock"></i> ${metadata.executionTime.toFixed(2)}s`;
    }
    
    contentDiv.appendChild(textDiv);
    contentDiv.appendChild(timeDiv);
    
    messageDiv.appendChild(avatarDiv);
    messageDiv.appendChild(contentDiv);
    
    return messageDiv;
}

function formatMessageContent(content) {
    // Escape HTML first
    let formatted = escapeHtml(content);
    
    // Convert line breaks
    formatted = formatted.replace(/\n/g, '<br>');
    
    // Format code blocks (simple detection)
    formatted = formatted.replace(/```([\s\S]*?)```/g, '<pre style="background: #f1f5f9; padding: 0.75rem; border-radius: 6px; margin: 0.5rem 0; border-left: 4px solid var(--sus-primary); overflow-x: auto;"><code>$1</code></pre>');
    
    // Format inline code
    formatted = formatted.replace(/`([^`]+)`/g, '<code style="background: #f1f5f9; padding: 0.25rem 0.5rem; border-radius: 4px; font-family: monospace; font-size: 0.875em;">$1</code>');
    
    // Format bold text
    formatted = formatted.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
    
    // Format italic text
    formatted = formatted.replace(/\*(.*?)\*/g, '<em>$1</em>');
    
    return formatted;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatTime(timestamp) {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    
    if (diffMins < 1) return 'Agora';
    if (diffMins < 60) return `${diffMins}m atrás`;
    if (diffMins < 1440) return `${Math.floor(diffMins / 60)}h atrás`;
    
    return date.toLocaleDateString('pt-BR', {
        day: '2-digit',
        month: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function scrollToBottom() {
    elements.chatMessages.scrollTop = elements.chatMessages.scrollHeight;
}

function addLoadingMessage() {
    const loadingId = 'loading-' + Date.now();
    const loadingDiv = document.createElement('div');
    loadingDiv.className = 'message assistant-message loading-message';
    loadingDiv.id = loadingId;
    
    loadingDiv.innerHTML = `
        <div class="message-avatar">
            <i class="fas fa-robot"></i>
        </div>
        <div class="message-content">
            <div class="message-text">
                <div class="inline-loading">
                    <div class="inline-spinner">
                        <i class="fas fa-brain"></i>
                    </div>
                    <span>Consultando o Agent...</span>
                </div>
            </div>
        </div>
    `;
    
    elements.chatMessages.appendChild(loadingDiv);
    scrollToBottom();
    
    return loadingId;
}

function removeLoadingMessage(loadingId) {
    const loadingElement = document.getElementById(loadingId);
    if (loadingElement) {
        loadingElement.remove();
    }
}

function showLoading() {
    isLoading = true;
    elements.loadingOverlay.classList.add('show');
    toggleSendButton();
}

function hideLoading() {
    isLoading = false;
    elements.loadingOverlay.classList.remove('show');
    toggleSendButton();
}

async function showSchemaModal() {
    elements.schemaModal.classList.add('show');
    elements.schemaContent.textContent = 'Selecione uma tabela e clique em "Carregar" para visualizar o esquema.';
}

async function loadSelectedSchema() {
    const tableSelect = document.getElementById('tableSelect');
    const loadSchemaBtn = document.getElementById('loadSchemaBtn');
    const selectedTable = tableSelect.value;
    
    try {
        // Disable button and show loading
        loadSchemaBtn.disabled = true;
        loadSchemaBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Carregando esquema...';
        elements.schemaContent.innerHTML = `
            <div class="loading-message">
                <div class="inline-spinner">
                    <i class="fas fa-database"></i>
                </div>
                <span>Carregando esquema da tabela...</span>
            </div>
        `;
        
        // Build URL with table parameter if selected
        let url = `${API_BASE_URL}/schema`;
        if (selectedTable) {
            url += `?table=${encodeURIComponent(selectedTable)}`;
        }
        
        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            },
            credentials: 'include',
            mode: 'cors',
            cache: 'no-cache'
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        // Check if schema contains HTML table and render accordingly
        if (data.schema.includes('<div class="sample-data-table">') || data.schema.includes('class="schema-table"')) {
            elements.schemaContent.classList.remove('text-content');
            elements.schemaContent.innerHTML = data.schema;
            
            // Initialize table search functionality
            setTimeout(() => {
                initializeTableSearch();
            }, 100);
        } else {
            elements.schemaContent.classList.add('text-content');
            elements.schemaContent.textContent = data.schema;
        }
        
    } catch (error) {
        console.error('Error loading schema:', error);
        elements.schemaContent.textContent = `Erro ao carregar esquema: ${error.message}`;
        showErrorToast('Erro ao carregar o esquema do banco de dados');
    } finally {
        // Re-enable button
        loadSchemaBtn.disabled = false;
        loadSchemaBtn.innerHTML = '<i class="fas fa-sync-alt"></i> Carregar';
    }
}

function hideSchemaModal() {
    elements.schemaModal.classList.remove('show');
}

function clearChat() {
    if (confirm('Tem certeza que deseja limpar toda a conversa? Esta ação não pode ser desfeita.')) {
        elements.chatMessages.innerHTML = '';
        messageHistory = [];
        saveMessageHistory();
        
        // Add welcome message back
        setTimeout(() => {
            addWelcomeMessage();
        }, 100);
    }
}

function addWelcomeMessage() {
    const welcomeMessage = {
        content: 'Olá! Sou o assistente inteligente do DataVisSUS. Estou aqui para ajudá-lo a consultar dados de saúde pública de forma simples e intuitiva.',
        type: 'assistant',
        timestamp: new Date().toISOString()
    };
    
    const messageElement = createMessageElement(welcomeMessage);
    elements.chatMessages.appendChild(messageElement);
    scrollToBottom();
}

function setWelcomeTime() {
    const welcomeTimeElement = document.getElementById('welcomeTime');
    if (welcomeTimeElement) {
        welcomeTimeElement.textContent = formatTime(new Date().toISOString());
    }
}

async function checkServerStatus() {
    try {
        // Check web interface health
        const webResponse = await fetch(`${API_BASE_URL}/health`, {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            },
            credentials: 'include',
            mode: 'cors',
            cache: 'no-cache'
        });
        
        if (webResponse.ok) {
            // Check if Agent API is reachable
            const agentResponse = await fetch(`${API_BASE_URL}/agent-health`, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'include',
                mode: 'cors',
                cache: 'no-cache'
            });
            
            if (agentResponse.ok) {
                const agentData = await agentResponse.json();
                updateServerStatus(agentData.agent_status === 'online');
            } else {
                updateServerStatus(false);
            }
        } else {
            updateServerStatus(false);
        }
    } catch (error) {
        console.error('Server health check failed:', error);
        updateServerStatus(false);
    }
}

function updateServerStatus(isOnline) {
    const indicator = elements.statusIndicator.querySelector('i');
    const text = elements.statusText;
    
    if (isOnline) {
        indicator.style.color = '#10b981';
        text.textContent = 'Online';
        text.style.color = 'white';
    } else {
        indicator.style.color = '#ef4444';
        text.textContent = 'Agent Offline';
        text.style.color = '#fecaca';
    }
}

function showErrorToast(message) {
    const errorText = document.getElementById('errorText');
    errorText.textContent = message;
    elements.errorToast.classList.add('show');
    
    // Auto hide after 5 seconds
    setTimeout(() => {
        hideErrorToast();
    }, 5000);
}

function hideErrorToast() {
    elements.errorToast.classList.remove('show');
}

function saveMessageHistory() {
    try {
        // Keep only last 50 messages to avoid localStorage limits
        const recentHistory = messageHistory.slice(-50);
        localStorage.setItem('chatHistory', JSON.stringify(recentHistory));
    } catch (error) {
        console.warn('Failed to save message history:', error);
    }
}

function loadMessageHistory() {
    try {
        const saved = localStorage.getItem('chatHistory');
        if (saved) {
            messageHistory = JSON.parse(saved);
            
            // Clear current messages and reload from history
            elements.chatMessages.innerHTML = '';
            
            messageHistory.forEach(messageData => {
                const messageElement = createMessageElement(messageData);
                elements.chatMessages.appendChild(messageElement);
            });
            
            scrollToBottom();
        } else {
            // Add welcome message if no history
            addWelcomeMessage();
        }
    } catch (error) {
        console.warn('Failed to load message history:', error);
        addWelcomeMessage();
    }
}

// Utility Functions
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Check server status periodically
setInterval(checkServerStatus, 30000); // Every 30 seconds

// Handle visibility change to reconnect when page becomes visible
document.addEventListener('visibilitychange', function() {
    if (!document.hidden) {
        checkServerStatus();
    }
});

// Theme Management
function initializeTheme() {
    // Get saved theme or default to light
    const savedTheme = localStorage.getItem('theme') || 'light';
    document.documentElement.setAttribute('data-theme', savedTheme);
    updateThemeIcon(savedTheme);
}

function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';
    
    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    updateThemeIcon(newTheme);
}

function updateThemeIcon(theme) {
    const icon = elements.themeToggle.querySelector('i');
    if (theme === 'dark') {
        icon.className = 'fas fa-moon';
        elements.themeToggle.title = 'Alternar para Tema Claro';
    } else {
        icon.className = 'fas fa-sun';
        elements.themeToggle.title = 'Alternar para Tema Escuro';
    }
}

// Table Search Functionality
function initializeTableSearch() {
    const searchInputs = document.querySelectorAll('.column-filter');
    const table = document.getElementById('schema-data-table');
    
    if (!table || !searchInputs.length) return;
    
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    
    // Add event listeners to all search inputs
    searchInputs.forEach((input, columnIndex) => {
        // Real-time filtering with debounce
        input.addEventListener('input', debounce(() => {
            filterTable();
            highlightSearchTerms();
        }, 200));
        
        // Keyboard shortcuts
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                input.value = '';
                filterTable();
                highlightSearchTerms();
            }
            // Navigate between filter inputs with Tab/Shift+Tab
            if (e.key === 'ArrowRight' && e.ctrlKey) {
                e.preventDefault();
                const nextInput = searchInputs[columnIndex + 1];
                if (nextInput) nextInput.focus();
            }
            if (e.key === 'ArrowLeft' && e.ctrlKey) {
                e.preventDefault();
                const prevInput = searchInputs[columnIndex - 1];
                if (prevInput) prevInput.focus();
            }
        });
        
        // Visual feedback on focus - no longer needed with integrated design
        input.addEventListener('focus', () => {
            input.style.transform = 'scale(1.02)';
        });
        
        input.addEventListener('blur', () => {
            input.style.transform = 'scale(1)';
        });
    });
    
    function filterTable() {
        let visibleCount = 0;
        
        rows.forEach((row) => {
            let showRow = true;
            
            // Check each column filter
            searchInputs.forEach((input, columnIndex) => {
                const searchTerm = input.value.toLowerCase().trim();
                
                if (searchTerm) {
                    const cell = row.cells[columnIndex];
                    if (cell) {
                        const cellText = cell.textContent.toLowerCase();
                        if (!cellText.includes(searchTerm)) {
                            showRow = false;
                        }
                    }
                }
            });
            
            // Show/hide row
            if (showRow) {
                row.classList.remove('hidden-row');
                row.classList.add('highlight-match');
                visibleCount++;
            } else {
                row.classList.add('hidden-row');
                row.classList.remove('highlight-match');
            }
        });
        
        // Update row striping after filtering
        updateRowStriping();
        
        // Show count of filtered results
        updateFilterCount(visibleCount, rows.length);
    }
    
    function updateRowStriping() {
        const visibleRows = rows.filter(row => !row.classList.contains('hidden-row'));
        visibleRows.forEach((row, index) => {
            if (index % 2 === 0) {
                row.style.backgroundColor = '';
            } else {
                row.style.backgroundColor = 'var(--bg-secondary)';
            }
        });
    }
    
    function updateFilterCount(visible, total) {
        const filteredRecords = document.querySelector('.filtered-records');
        const filteredCount = document.querySelector('.filtered-count');
        
        if (visible !== total) {
            // Show filtered counter
            if (filteredRecords) {
                filteredRecords.style.display = 'flex';
                if (filteredCount) {
                    filteredCount.textContent = visible.toLocaleString();
                }
            }
        } else {
            // Hide filtered counter when showing all records
            if (filteredRecords) {
                filteredRecords.style.display = 'none';
            }
        }
    }
    
    function highlightSearchTerms() {
        // Remove existing highlights
        rows.forEach(row => {
            Array.from(row.cells).forEach(cell => {
                if (cell.dataset.originalText) {
                    cell.innerHTML = cell.dataset.originalText;
                }
            });
        });
        
        // Apply new highlights
        searchInputs.forEach((input, columnIndex) => {
            const searchTerm = input.value.toLowerCase().trim();
            if (searchTerm && searchTerm.length > 1) {
                rows.forEach(row => {
                    if (!row.classList.contains('hidden-row')) {
                        const cell = row.cells[columnIndex];
                        if (cell && !cell.querySelector('.null-value')) {
                            if (!cell.dataset.originalText) {
                                cell.dataset.originalText = cell.innerHTML;
                            }
                            const originalText = cell.dataset.originalText;
                            const regex = new RegExp(`(${searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
                            const highlighted = originalText.replace(regex, '<mark style="background: rgba(0, 168, 107, 0.3); padding: 0.1rem 0.2rem; border-radius: 2px;">$1</mark>');
                            cell.innerHTML = highlighted;
                        }
                    }
                });
            }
        });
    }
    
    // Clear all filters function
    function clearAllFilters() {
        searchInputs.forEach(input => {
            input.value = '';
            input.style.transform = 'scale(1)';
        });
        
        // Remove highlights and show all rows
        rows.forEach(row => {
            row.classList.remove('hidden-row', 'highlight-match');
            Array.from(row.cells).forEach(cell => {
                if (cell.dataset.originalText) {
                    cell.innerHTML = cell.dataset.originalText;
                    delete cell.dataset.originalText;
                }
            });
        });
        
        updateRowStriping();
        updateFilterCount(rows.length, rows.length);
        
        // Focus first search input for better UX
        if (searchInputs.length > 0) {
            searchInputs[0].focus();
        }
    }
    
    // Add clear all button to the filter bar
    const filterBar = document.querySelector('.filter-bar');
    if (filterBar) {
        const clearBtn = document.createElement('button');
        clearBtn.className = 'clear-filters-btn';
        clearBtn.innerHTML = '<i class="fas fa-eraser"></i>';
        clearBtn.title = 'Limpar todos os filtros';
        clearBtn.addEventListener('click', clearAllFilters);
        
        const clearWrapper = document.createElement('div');
        clearWrapper.className = 'clear-filters-wrapper';
        clearWrapper.appendChild(clearBtn);
        filterBar.appendChild(clearWrapper);
    }
}

// Export for potential future use
window.ChatApp = {
    sendMessage,
    clearChat,
    showSchemaModal,
    checkServerStatus,
    toggleTheme,
    initializeTableSearch
};