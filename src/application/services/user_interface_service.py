"""
Simple User Interface Service - Simplified CLI interactions for TXT2SQL
"""
import re
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class FormattedResponse:
    """Formatted response for user"""
    content: str
    success: bool
    execution_time: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


class SimpleUserInterface:
    """
    Simple user interface for CLI interactions.
    Focused on essential functionality without unnecessary abstractions.
    """

    def __init__(self):
        """Initialize simple user interface"""
        self._display_welcome()

    def get_user_input(self, prompt: str) -> str:
        """Get input from user with given prompt"""
        try:
            return input(f"\n💬 {prompt} ").strip()
        except (EOFError, KeyboardInterrupt):
            return ""

    def display_response(self, response: FormattedResponse) -> None:
        """Display formatted response to user"""
        if response.success:
            print(f"\n✅ {response.content}")
            
            # Show execution time if available
            if response.execution_time:
                print(f"⏱️ Tempo de execução: {response.execution_time:.2f}s")
            
            # Show routing information if available
            if response.metadata:
                self._display_metadata(response.metadata)
        else:
            print(f"\n❌ {response.content}")

    def display_error(self, error_message: str) -> None:
        """Display error message to user"""
        print(f"\n❌ Erro: {error_message}")

    def display_help(self) -> None:
        """Display help information"""
        help_text = """
🆘 AJUDA - TXT2SQL Claude

📋 COMANDOS DISPONÍVEIS:
• Digite sua pergunta em linguagem natural
• 'schema' - Mostra informações do banco de dados
• 'exemplos' - Mostra exemplos de perguntas
• 'ajuda' ou 'help' - Mostra esta ajuda
• 'sair', 'quit' ou 'exit' - Sai do programa

💡 EXEMPLOS DE PERGUNTAS:
• Quantos pacientes existem?
• Qual a idade média dos pacientes?
• Quantas mortes ocorreram em Porto Alegre?
• Quais são os diagnósticos mais comuns?
• Qual o custo total por estado?

🎯 DICAS:
• Use nomes de cidades para consultas geográficas
• Seja específico nas suas perguntas
• Use termos médicos quando apropriado
"""
        print(help_text)

    def _display_welcome(self) -> None:
        """Display welcome message"""
        print("\n🚀 Bem-vindo ao TXT2SQL Claude!")
        print("📊 Sistema de consultas em linguagem natural para dados SUS")
        print("💡 Digite 'ajuda' para ver comandos disponíveis\n")

    def _display_metadata(self, metadata: Dict[str, Any]) -> None:
        """Display metadata information if available"""
        # Show routing information
        if metadata.get("routing_applied"):
            query_type = metadata.get("query_classification", "unknown")
            confidence = metadata.get("classification_confidence", 0.0)
            
            if query_type == "DATABASE_QUERY":
                print(f"🔍 Consulta de banco de dados identificada (confiança: {confidence:.2f})")
            elif query_type == "CONVERSATIONAL_QUERY":
                print(f"💬 Pergunta conversacional identificada (confiança: {confidence:.2f})")
        
        # Show decomposition information
        if metadata.get("decomposition_applied"):
            strategy = metadata.get("decomposition_strategy", "unknown")
            complexity = metadata.get("complexity_score", 0)
            print(f"🧩 Decomposição aplicada: {strategy} (complexidade: {complexity})")


# Backwards compatibility - keep these for existing imports
class InterfaceType:
    """Simple interface type constants for backwards compatibility"""
    CLI_BASIC = "cli_basic"
    CLI_INTERACTIVE = "cli_interactive"
    CLI_VERBOSE = "cli_verbose"


class UserInterfaceFactory:
    """Simple factory for backwards compatibility"""
    
    @staticmethod
    def create_service(ui_type: str = "cli", interface_type: str = "cli_interactive"):
        """Create user interface service (simplified)"""
        return SimpleUserInterface()


class InputValidator:
    """Input validation utilities"""
    
    @staticmethod
    def validate_query_length(query: str, max_length: int) -> bool:
        """Validate query length"""
        return len(query.strip()) <= max_length
    
    @staticmethod
    def sanitize_input(query: str) -> str:
        """Sanitize user input"""
        if not query:
            return ""
        
        # Remove excessive whitespace
        sanitized = re.sub(r'\s+', ' ', query.strip())
        
        # Remove potentially dangerous characters for SQL injection
        # Keep basic punctuation needed for natural language
        sanitized = re.sub(r'[^\w\s\?,.!():-áàâãéèêíìîóòôõúùûçÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ]', '', sanitized)
        
        return sanitized


# Legacy aliases for backwards compatibility
IUserInterfaceService = SimpleUserInterface
CLIUserInterfaceService = SimpleUserInterface