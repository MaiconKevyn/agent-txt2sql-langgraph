"""
Mistral Fine-tuned Service - Integration with our fine-tuned SUS model
Provides specialized Text-to-SQL generation using our trained Mistral-7b model
"""

import os
import torch
import logging
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass
import time

from .llm_communication_service import ILLMCommunicationService, LLMResponse, LLMConfig


@dataclass
class MistralConfig:
    """Configuration for fine-tuned Mistral model"""
    model_path: str = "/home/maiconkevyn/PycharmProjects/llama3-fine-tuning/models/sus_mistral_final"
    max_new_tokens: int = 200
    temperature: float = 0.1
    top_p: float = 0.9
    do_sample: bool = True
    device: str = "auto"  # auto, cpu, cuda


class MistralFineTunedService(ILLMCommunicationService):
    """Fine-tuned Mistral-7b service specialized for SUS Text-to-SQL"""
    
    def __init__(self, config: MistralConfig):
        """
        Initialize fine-tuned Mistral service
        
        Args:
            config: Configuration for the Mistral model
        """
        self._config = config
        self._model = None
        self._tokenizer = None
        self._logger = logging.getLogger(__name__)
        self._sus_template = self._get_sus_template()
        self._initialize_model()
    
    def _get_sus_template(self) -> str:
        """Get the SUS prompt template used during fine-tuning"""
        return """Abaixo está uma pergunta sobre dados do SUS brasileiro. Use o contexto fornecido para gerar uma consulta SQL correta.

### Pergunta:
{}

### Contexto:
{}

### SQL:
"""
    
    def _initialize_model(self) -> None:
        """Initialize the fine-tuned Mistral model"""
        try:
            self._logger.info(f"Loading fine-tuned Mistral model from: {self._config.model_path}")
            
            # Check if model path exists
            if not os.path.exists(self._config.model_path):
                raise FileNotFoundError(f"Model path not found: {self._config.model_path}")
            
            # Force CPU-only to avoid CUDA issues
            device = "cpu"
            self._logger.info(f"Using device: {device} (forced to avoid CUDA issues)")
            
            # Disable CUDA completely
            os.environ['CUDA_VISIBLE_DEVICES'] = ''
            
            # Skip Unsloth entirely and go directly to transformers
            try:
                # Skip Unsloth - it requires CUDA
                raise Exception("Skipping Unsloth due to CUDA issues")
                
                if device == "cuda":
                    # Load the fine-tuned model with CUDA
                    self._model, self._tokenizer = FastLanguageModel.from_pretrained(
                        model_name=self._config.model_path,
                        max_seq_length=2048,
                        dtype=None,
                        load_in_4bit=True,
                        device_map="auto"
                    )
                    self._logger.info("✅ Model loaded on CUDA with Unsloth")
                else:
                    # Load on CPU
                    self._model, self._tokenizer = FastLanguageModel.from_pretrained(
                        model_name=self._config.model_path,
                        max_seq_length=2048,
                        dtype=None,
                        load_in_4bit=False,  # Disable 4bit for CPU
                        device_map=None
                    )
                    self._logger.info("✅ Model loaded on CPU with Unsloth")
                
                # Enable inference mode
                FastLanguageModel.for_inference(self._model)
                
            except Exception as unsloth_error:
                self._logger.warning(f"Unsloth loading failed: {unsloth_error}")
                self._logger.info("🔄 Falling back to transformers...")
                
                # Fallback to transformers
                from transformers import AutoModelForCausalLM, AutoTokenizer
                from peft import PeftModel, PeftConfig
                
                # Load the PEFT config to get base model
                peft_config = PeftConfig.from_pretrained(self._config.model_path)
                base_model_name = peft_config.base_model_name_or_path
                
                self._logger.info(f"Loading base model: {base_model_name}")
                
                # Force CPU-only loading without device_map (which causes CUDA calls)
                self._model = AutoModelForCausalLM.from_pretrained(
                    base_model_name,
                    torch_dtype=torch.float32,
                    device_map=None,  # No device mapping to avoid CUDA
                    low_cpu_mem_usage=True
                )
                
                self._tokenizer = AutoTokenizer.from_pretrained(base_model_name)
                
                # Add pad token if missing
                if self._tokenizer.pad_token is None:
                    self._tokenizer.pad_token = self._tokenizer.eos_token
                
                # Load PEFT adapter
                self._model = PeftModel.from_pretrained(self._model, self._config.model_path)
                
                self._logger.info(f"✅ Model loaded on {device} with transformers")
            
            self._logger.info("✅ Fine-tuned Mistral model loaded successfully")
            
        except Exception as e:
            self._logger.error(f"Failed to initialize fine-tuned Mistral model: {str(e)}")
            raise RuntimeError(f"Failed to initialize Mistral model: {str(e)}")
    
    def send_prompt(self, prompt: str) -> LLMResponse:
        """Send prompt to fine-tuned Mistral model and get SQL response"""
        if not self._model or not self._tokenizer:
            return LLMResponse(
                content="",
                success=False,
                error_message="Model not initialized"
            )
        
        start_time = time.time()
        
        try:
            # Tokenize input
            inputs = self._tokenizer(
                prompt,
                return_tensors="pt",
                truncation=True,
                max_length=2048,
                padding=True
            )
            
            # Move to device if needed
            model_device = next(self._model.parameters()).device
            if str(model_device) != "cpu":
                inputs = {k: v.to(model_device) for k, v in inputs.items()}
            
            # Generate response
            with torch.no_grad():
                outputs = self._model.generate(
                    inputs.input_ids,
                    max_new_tokens=self._config.max_new_tokens,
                    temperature=self._config.temperature,
                    top_p=self._config.top_p,
                    do_sample=self._config.do_sample,
                    pad_token_id=self._tokenizer.eos_token_id,
                    eos_token_id=self._tokenizer.eos_token_id,
                    repetition_penalty=1.1,
                    num_return_sequences=1
                )
            
            # Decode response (only the new tokens)
            response_text = self._tokenizer.decode(
                outputs[0][inputs.input_ids.shape[1]:],
                skip_special_tokens=True
            ).strip()
            
            execution_time = time.time() - start_time
            
            self._logger.info(f"🤖 Mistral response generated in {execution_time:.2f}s")
            
            return LLMResponse(
                content=response_text,
                success=True,
                execution_time=execution_time,
                tokens_used=outputs[0].shape[1]
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self._logger.error(f"Error generating response: {str(e)}")
            
            return LLMResponse(
                content="",
                success=False,
                error_message=f"Generation failed: {str(e)}",
                execution_time=execution_time
            )
    
    def generate_sql_from_question(
        self, 
        question: str, 
        context: str = None
    ) -> LLMResponse:
        """
        Generate SQL directly from question using SUS template
        
        Args:
            question: Natural language question in Portuguese
            context: Database schema context (optional)
        
        Returns:
            LLMResponse with generated SQL
        """
        # Use default SUS context if not provided
        if context is None:
            context = self._get_default_sus_context()
        
        # Format prompt using SUS template
        prompt = self._sus_template.format(question, context)
        
        self._logger.info(f"🔍 Generating SQL for question: {question[:50]}...")
        
        # Get response from model
        response = self.send_prompt(prompt)
        
        if response.success:
            # Extract SQL from response
            sql = self._extract_sql_from_response(response.content)
            response.content = sql
            self._logger.info(f"✅ Generated SQL: {sql[:100]}...")
        
        return response
    
    def _get_default_sus_context(self) -> str:
        """Get default SUS database context"""
        return """CREATE TABLE sus_data (
    DIAG_PRINC TEXT,
    SEXO INTEGER,
    IDADE INTEGER, 
    MORTE INTEGER,
    CIDADE_RESIDENCIA_PACIENTE TEXT,
    DT_INTER TEXT,
    DT_SAIDA TEXT
)

CREATE TABLE cid_detalhado (
    codigo TEXT,
    descricao TEXT
)

-- SEXO: 1=Homem, 3=Mulher
-- MORTE: 1=Morte, 0=Alta/Transferência  
-- DIAG_PRINC: Código CID-10 (I=Cardíaco, J=Respiratório, E=Endócrino)
-- DT_INTER/DT_SAIDA: Formato YYYYMMDD
-- ESTAÇÕES BRASIL: Verão(12-03), Outono(03-06), Inverno(06-09), Primavera(09-12)"""
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract clean SQL from model response"""
        if not response:
            return "SELECT COUNT(*) FROM sus_data;"
        
        # Split by lines and look for SQL
        lines = response.split('\n')
        sql_lines = []
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('--') or line.startswith('#'):
                continue
            
            # Look for SQL statements
            if (line.upper().startswith('SELECT') or 
                line.upper().startswith('WITH') or
                sql_lines):  # Continue collecting if we're already in SQL
                
                sql_lines.append(line)
                
                # Stop if we hit a semicolon
                if line.endswith(';'):
                    break
        
        if sql_lines:
            sql = ' '.join(sql_lines)
            # Clean up and ensure semicolon
            sql = ' '.join(sql.split())
            if not sql.endswith(';'):
                sql += ';'
            return sql
        
        # Fallback: return the whole response if it looks like SQL
        clean_response = response.strip()
        if any(keyword in clean_response.upper() for keyword in ['SELECT', 'FROM', 'WHERE']):
            if not clean_response.endswith(';'):
                clean_response += ';'
            return clean_response
        
        # Ultimate fallback
        return "SELECT COUNT(*) FROM sus_data;"
    
    def is_available(self) -> bool:
        """Check if the fine-tuned model is available"""
        try:
            return self._model is not None and self._tokenizer is not None
        except Exception:
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the fine-tuned model"""
        device_info = "Unknown"
        if self._model:
            try:
                device_info = str(next(self._model.parameters()).device)
            except:
                device_info = "CPU"
        
        return {
            "provider": "MistralFineTuned",
            "model_path": self._config.model_path,
            "specialization": "SUS Brazilian Healthcare Text-to-SQL",
            "temperature": self._config.temperature,
            "max_new_tokens": self._config.max_new_tokens,
            "device": device_info,
            "available": self.is_available(),
            "cuda_available": torch.cuda.is_available(),
            "model_type": "LoRA Fine-tuned Mistral-7b",
            "training_dataset": "767 SUS examples with data augmentation"
        }
    
    def test_model(self) -> Dict[str, Any]:
        """Test the model with a simple SUS question"""
        test_question = "Quantos pacientes estão registrados no sistema?"
        
        try:
            response = self.generate_sql_from_question(test_question)
            
            return {
                "test_question": test_question,
                "success": response.success,
                "generated_sql": response.content if response.success else None,
                "execution_time": response.execution_time,
                "error": response.error_message if not response.success else None
            }
        except Exception as e:
            return {
                "test_question": test_question,
                "success": False,
                "error": str(e)
            }


class MistralFineTunedFactory:
    """Factory for creating Mistral fine-tuned service"""
    
    @staticmethod
    def create_service(
        model_path: str = "/home/maiconkevyn/PycharmProjects/llama3-fine-tuning/models/sus_mistral_final",
        temperature: float = 0.1,
        max_new_tokens: int = 200,
        device: str = "auto"
    ) -> MistralFineTunedService:
        """Create Mistral fine-tuned service"""
        config = MistralConfig(
            model_path=model_path,
            temperature=temperature,
            max_new_tokens=max_new_tokens,
            device=device
        )
        return MistralFineTunedService(config)
    
    @staticmethod
    def is_model_available(
        model_path: str = "/home/maiconkevyn/PycharmProjects/llama3-fine-tuning/models/sus_mistral_final"
    ) -> bool:
        """Check if the fine-tuned model is available"""
        try:
            return (
                os.path.exists(model_path) and
                os.path.exists(os.path.join(model_path, "adapter_model.safetensors")) and
                os.path.exists(os.path.join(model_path, "tokenizer.json"))
            )
        except Exception:
            return False


# Integration with existing LLM Communication Factory
def extend_llm_factory():
    """Extend the existing LLMCommunicationFactory to support Mistral fine-tuned"""
    from .llm_communication_service import LLMCommunicationFactory
    
    # Add method to create Mistral service
    def create_mistral_finetuned_service(**kwargs):
        return MistralFineTunedFactory.create_service(**kwargs)
    
    # Add to factory
    LLMCommunicationFactory.create_mistral_finetuned_service = staticmethod(create_mistral_finetuned_service)
    
    # Update create_service method to support mistral_finetuned
    original_create_service = LLMCommunicationFactory.create_service
    
    @staticmethod
    def enhanced_create_service(provider: str, **kwargs):
        if provider.lower() == "mistral_finetuned":
            return LLMCommunicationFactory.create_mistral_finetuned_service(**kwargs)
        else:
            return original_create_service(provider, **kwargs)
    
    LLMCommunicationFactory.create_service = enhanced_create_service


# Auto-extend factory when module is imported
extend_llm_factory()


if __name__ == "__main__":
    # Test the service
    print("🧪 Testing Mistral Fine-tuned Service...")
    
    if MistralFineTunedFactory.is_model_available():
        print("✅ Model files found")
        
        try:
            service = MistralFineTunedFactory.create_service()
            print("✅ Service created")
            
            test_result = service.test_model()
            print(f"🧪 Test result: {test_result}")
            
            model_info = service.get_model_info()
            print(f"ℹ️ Model info: {model_info}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
    else:
        print("❌ Model files not found. Please ensure the fine-tuned model is available.")