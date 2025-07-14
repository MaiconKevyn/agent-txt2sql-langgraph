"""
Custom Domain Exceptions
"""


class ConversationalLLMError(Exception):
    """Exception raised for errors in conversational LLM service"""
    pass


class TemplateProcessingError(Exception):
    """Exception raised for template processing errors"""
    pass


class InvalidResponseError(Exception):
    """Exception raised for invalid response format"""
    pass


class LLMCommunicationError(Exception):
    """Exception raised for LLM communication errors"""
    pass


class LLMTimeoutError(Exception):
    """Exception raised for LLM timeout errors"""
    pass


class LLMUnavailableError(Exception):
    """Exception raised for LLM unavailable errors"""
    pass