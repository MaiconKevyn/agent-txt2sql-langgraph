"""
LLM Communication Service - Single Responsibility: Handle all LLM interactions
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from langchain_community.llms import Ollama
from dataclasses import dataclass
import time
import logging


@dataclass
class LLMResponse:
    """Response from LLM service"""
    content: str
    success: bool
    error_message: Optional[str] = None
    execution_time: Optional[float] = None
    tokens_used: Optional[int] = None


@dataclass
class LLMConfig:
    """Configuration for LLM service"""
    model_name: str = "llama3"
    temperature: float = 0.0
    timeout: int = 120
    max_retries: int = 3
    provider: str = "ollama"  # ollama, huggingface
    device: str = "auto"  # auto, cpu, cuda
    load_in_8bit: bool = False
    load_in_4bit: bool = False


class ILLMCommunicationService(ABC):
    """Interface for LLM communication"""
    
    @abstractmethod
    def send_prompt(self, prompt: str) -> LLMResponse:
        """Send prompt to LLM and get response"""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if LLM service is available"""
        pass
    
    @abstractmethod
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current model"""
        pass


class OllamaLLMCommunicationService(ILLMCommunicationService):
    """Ollama implementation of LLM communication service"""
    
    def __init__(self, config: LLMConfig):
        """
        Initialize Ollama LLM communication service
        
        Args:
            config: LLM configuration
        """
        self._config = config
        self._llm: Optional[Ollama] = None
        self._initialize_llm()
    
    def _initialize_llm(self) -> None:
        """Initialize the Ollama LLM instance"""
        try:
            self._llm = Ollama(
                model=self._config.model_name,
                temperature=self._config.temperature
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Ollama LLM: {str(e)}")
    
    def send_prompt(self, prompt: str) -> LLMResponse:
        """Send prompt to Ollama LLM and get response"""
        import time
        
        if not self._llm:
            return LLMResponse(
                content="",
                success=False,
                error_message="LLM not initialized"
            )
        
        start_time = time.time()
        
        for attempt in range(self._config.max_retries):
            try:
                response = self._llm.invoke(prompt)
                execution_time = time.time() - start_time
                
                return LLMResponse(
                    content=response,
                    success=True,
                    execution_time=execution_time
                )
                
            except Exception as e:
                if attempt == self._config.max_retries - 1:
                    execution_time = time.time() - start_time
                    return LLMResponse(
                        content="",
                        success=False,
                        error_message=f"Failed after {self._config.max_retries} attempts: {str(e)}",
                        execution_time=execution_time
                    )
                # Wait before retry
                time.sleep(1)
        
        return LLMResponse(
            content="",
            success=False,
            error_message="Unexpected error in retry loop"
        )
    
    def is_available(self) -> bool:
        """Check if Ollama LLM service is available"""
        try:
            if not self._llm:
                return False
            
            # Test with a simple prompt
            test_response = self._llm.invoke("Test")
            return isinstance(test_response, str)
            
        except Exception:
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current Ollama model"""
        return {
            "provider": "Ollama",
            "model_name": self._config.model_name,
            "temperature": self._config.temperature,
            "timeout": self._config.timeout,
            "max_retries": self._config.max_retries,
            "available": self.is_available()
        }
    
    def get_llm_instance(self) -> Optional[Ollama]:
        """Get the underlying Ollama LLM instance (for LangChain compatibility)"""
        return self._llm


class HuggingFaceLLMCommunicationService(ILLMCommunicationService):
    """Hugging Face implementation of LLM communication service for SQLCoder-7b-2"""
    
    def __init__(self, config: LLMConfig):
        """
        Initialize Hugging Face LLM communication service
        
        Args:
            config: LLM configuration
        """
        self._config = config
        self._tokenizer = None
        self._model = None
        self._logger = logging.getLogger(__name__)
        self._initialize_model()
    
    def _initialize_model(self) -> None:
        """Initialize the Hugging Face model and tokenizer"""
        try:
            from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
            import torch
            
            # Configure quantization if requested
            quantization_config = None
            if self._config.load_in_4bit:
                quantization_config = BitsAndBytesConfig(
                    load_in_4bit=True,
                    bnb_4bit_compute_dtype=torch.float16,
                    bnb_4bit_use_double_quant=True,
                    bnb_4bit_quant_type="nf4",
                    llm_int8_enable_fp32_cpu_offload=True
                )
            elif self._config.load_in_8bit:
                quantization_config = BitsAndBytesConfig(
                    load_in_8bit=True,
                    llm_int8_enable_fp32_cpu_offload=True
                )
            
            # Determine device
            if self._config.device == "auto":
                device = "cuda" if torch.cuda.is_available() else "cpu"
            else:
                device = self._config.device
            
            self._logger.info(f"Loading SQLCoder model: {self._config.model_name} on {device}")
            
            # Load tokenizer
            self._tokenizer = AutoTokenizer.from_pretrained(
                self._config.model_name,
                trust_remote_code=True
            )
            
            # Set pad token if not exists
            if self._tokenizer.pad_token is None:
                self._tokenizer.pad_token = self._tokenizer.eos_token
            
            # Load model
            model_kwargs = {
                "trust_remote_code": True,
                "torch_dtype": torch.float16 if device == "cuda" else torch.float32,
            }
            
            if quantization_config:
                model_kwargs["quantization_config"] = quantization_config
                model_kwargs["device_map"] = "auto"
            else:
                model_kwargs["device_map"] = device
            
            self._model = AutoModelForCausalLM.from_pretrained(
                self._config.model_name,
                **model_kwargs
            )
            
            self._logger.info("SQLCoder model loaded successfully")
            
        except Exception as e:
            self._logger.error(f"Failed to initialize Hugging Face model: {str(e)}")
            raise RuntimeError(f"Failed to initialize Hugging Face LLM: {str(e)}")
    
    def send_prompt(self, prompt: str) -> LLMResponse:
        """Send prompt to Hugging Face model and get response"""
        if not self._model or not self._tokenizer:
            return LLMResponse(
                content="",
                success=False,
                error_message="Model not initialized"
            )
        
        start_time = time.time()
        
        for attempt in range(self._config.max_retries):
            try:
                import torch
                
                # Tokenize input
                inputs = self._tokenizer(
                    prompt,
                    return_tensors="pt",
                    truncation=True,
                    max_length=4096,
                    padding=True
                )
                
                # Move to device
                device = next(self._model.parameters()).device
                inputs = {k: v.to(device) for k, v in inputs.items()}
                
                # Generate response
                with torch.no_grad():
                    outputs = self._model.generate(
                        **inputs,
                        max_new_tokens=512,
                        temperature=self._config.temperature,
                        do_sample=self._config.temperature > 0,
                        pad_token_id=self._tokenizer.pad_token_id,
                        eos_token_id=self._tokenizer.eos_token_id,
                        num_return_sequences=1
                    )
                
                # Decode response
                response_text = self._tokenizer.decode(
                    outputs[0][inputs['input_ids'].shape[1]:],
                    skip_special_tokens=True
                ).strip()
                
                execution_time = time.time() - start_time
                tokens_used = outputs[0].shape[1]
                
                return LLMResponse(
                    content=response_text,
                    success=True,
                    execution_time=execution_time,
                    tokens_used=tokens_used
                )
                
            except Exception as e:
                self._logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == self._config.max_retries - 1:
                    execution_time = time.time() - start_time
                    return LLMResponse(
                        content="",
                        success=False,
                        error_message=f"Failed after {self._config.max_retries} attempts: {str(e)}",
                        execution_time=execution_time
                    )
                # Wait before retry
                time.sleep(1)
        
        return LLMResponse(
            content="",
            success=False,
            error_message="Unexpected error in retry loop"
        )
    
    def is_available(self) -> bool:
        """Check if Hugging Face model is available"""
        try:
            return self._model is not None and self._tokenizer is not None
        except Exception:
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current Hugging Face model"""
        import torch
        
        device_info = "Unknown"
        if self._model:
            device_info = str(next(self._model.parameters()).device)
        
        return {
            "provider": "HuggingFace",
            "model_name": self._config.model_name,
            "temperature": self._config.temperature,
            "timeout": self._config.timeout,
            "max_retries": self._config.max_retries,
            "device": device_info,
            "load_in_8bit": self._config.load_in_8bit,
            "load_in_4bit": self._config.load_in_4bit,
            "available": self.is_available(),
            "cuda_available": torch.cuda.is_available() if torch else False
        }
    
    def get_llm_instance(self):
        """Get the underlying model instance (for compatibility)"""
        return self._model


class LLMCommunicationFactory:
    """Factory for creating LLM communication services"""
    
    @staticmethod
    def create_ollama_service(
        model_name: str = "llama3",
        temperature: float = 0.0,
        timeout: int = 120,
        max_retries: int = 3
    ) -> ILLMCommunicationService:
        """Create Ollama LLM communication service"""
        config = LLMConfig(
            model_name=model_name,
            temperature=temperature,
            timeout=timeout,
            max_retries=max_retries,
            provider="ollama"
        )
        return OllamaLLMCommunicationService(config)
    
    @staticmethod
    def create_huggingface_service(
        model_name: str = "defog/sqlcoder-7b-2",
        temperature: float = 0.0,
        timeout: int = 120,
        max_retries: int = 3,
        device: str = "auto",
        load_in_8bit: bool = False,
        load_in_4bit: bool = True
    ) -> ILLMCommunicationService:
        """Create Hugging Face LLM communication service"""
        config = LLMConfig(
            model_name=model_name,
            temperature=temperature,
            timeout=timeout,
            max_retries=max_retries,
            provider="huggingface",
            device=device,
            load_in_8bit=load_in_8bit,
            load_in_4bit=load_in_4bit
        )
        return HuggingFaceLLMCommunicationService(config)
    
    @staticmethod
    def create_service(
        provider: str,
        **kwargs
    ) -> ILLMCommunicationService:
        """Create LLM communication service based on provider"""
        if provider.lower() == "ollama":
            # Filter kwargs to only include valid Ollama parameters
            ollama_kwargs = {k: v for k, v in kwargs.items() if k in ['model_name', 'temperature', 'timeout', 'max_retries']}
            return LLMCommunicationFactory.create_ollama_service(**ollama_kwargs)
        elif provider.lower() == "huggingface":
            return LLMCommunicationFactory.create_huggingface_service(**kwargs)
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")