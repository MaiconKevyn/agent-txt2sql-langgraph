# Domain Exceptions Package

from .custom_exceptions import (
    ConversationalLLMError,
    TemplateProcessingError,
    InvalidResponseError,
    LLMCommunicationError,
    LLMTimeoutError,
    LLMUnavailableError
)

__all__ = [
    'ConversationalLLMError', 
    'TemplateProcessingError', 
    'InvalidResponseError',
    'LLMCommunicationError',
    'LLMTimeoutError',
    'LLMUnavailableError'
]