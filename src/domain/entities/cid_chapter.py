"""
CID Chapter Entity - Represents ICD-10 chapter classifications
"""
from dataclasses import dataclass
from typing import Optional, List
import re


@dataclass(frozen=True)
class CIDChapter:
    """
    CID Chapter entity representing ICD-10 chapter classifications
    
    Encapsulates ICD-10 chapter information including code ranges,
    descriptions, and semantic categorization according to 
    international healthcare standards used in Brazil.
    """
    numero_capitulo: int
    codigo_inicio: str
    codigo_fim: str
    descricao: str
    descricao_abrev: Optional[str] = None
    categoria_geral: Optional[str] = None
    
    def __post_init__(self):
        """Validate CID chapter data after initialization"""
        self._validate()
    
    def _validate(self):
        """Validate CID chapter data"""
        if not self.numero_capitulo or self.numero_capitulo < 1:
            raise ValueError(f"Invalid chapter number: {self.numero_capitulo}")
        
        if not self._is_valid_cid_code(self.codigo_inicio):
            raise ValueError(f"Invalid start code format: {self.codigo_inicio}")
            
        if not self._is_valid_cid_code(self.codigo_fim):
            raise ValueError(f"Invalid end code format: {self.codigo_fim}")
            
        if not self.descricao or len(self.descricao.strip()) < 5:
            raise ValueError(f"Invalid description: {self.descricao}")
    
    def _is_valid_cid_code(self, code: str) -> bool:
        """Validate CID-10 code format"""
        if not code:
            return False
        
        # CID-10 pattern: Letter followed by 2-3 digits
        pattern = r'^[A-Z]\d{2,3}$'
        return bool(re.match(pattern, code.strip()))
    
    def contains_code(self, cid_code: str) -> bool:
        """Check if a specific CID code falls within this chapter's range"""
        if not cid_code or not self._is_valid_cid_code(cid_code):
            return False
        
        code = cid_code.strip().upper()
        start = self.codigo_inicio.strip().upper()
        end = self.codigo_fim.strip().upper()
        
        # Extract letter and number parts for proper comparison
        def parse_cid_code(cid_code):
            letter = cid_code[0]
            number = int(cid_code[1:])
            return letter, number
        
        try:
            code_letter, code_num = parse_cid_code(code)
            start_letter, start_num = parse_cid_code(start)
            end_letter, end_num = parse_cid_code(end)
            
            # Check if code is within the letter range
            if code_letter < start_letter or code_letter > end_letter:
                return False
            
            # If same letter as start, check number is >= start number
            if code_letter == start_letter and code_num < start_num:
                return False
                
            # If same letter as end, check number is <= end number  
            if code_letter == end_letter and code_num > end_num:
                return False
                
            return True
            
        except (ValueError, IndexError):
            return False
    
    @property
    def code_range(self) -> str:
        """Get formatted code range string"""
        return f"{self.codigo_inicio}-{self.codigo_fim}"
    
    @property
    def roman_number(self) -> str:
        """Convert chapter number to Roman numeral"""
        roman_map = {
            1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
            6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X",
            11: "XI", 12: "XII", 13: "XIII", 14: "XIV", 15: "XV",
            16: "XVI", 17: "XVII", 18: "XVIII", 19: "XIX", 20: "XX",
            21: "XXI", 22: "XXII"
        }
        return roman_map.get(self.numero_capitulo, str(self.numero_capitulo))
    
    @property
    def full_title(self) -> str:
        """Get full chapter title with Roman numeral"""
        return f"Capítulo {self.roman_number} - {self.descricao.replace('Capitulo ', '').replace(f'{self.roman_number} - ', '')}"
    
    @property
    def category_type(self) -> str:
        """Get semantic category type based on description"""
        description_lower = self.descricao.lower()
        
        if any(word in description_lower for word in ['doenças', 'doença']):
            return "diseases"
        elif any(word in description_lower for word in ['transtornos', 'transtorno']):
            return "disorders"
        elif any(word in description_lower for word in ['neoplasias', 'tumor']):
            return "neoplasms"
        elif any(word in description_lower for word in ['lesões', 'lesão', 'envenenamento']):
            return "injuries"
        elif any(word in description_lower for word in ['gravidez', 'parto', 'perinatal']):
            return "pregnancy_birth"
        elif any(word in description_lower for word in ['malformações', 'congênitas']):
            return "congenital"
        elif any(word in description_lower for word in ['sintomas', 'sinais']):
            return "symptoms"
        elif any(word in description_lower for word in ['causas externas', 'externa']):
            return "external_causes"
        elif any(word in description_lower for word in ['fatores', 'contato', 'serviços']):
            return "health_factors"
        else:
            return "other"
    
    @property
    def is_chronic_condition_category(self) -> bool:
        """Determine if this chapter typically contains chronic conditions"""
        chronic_categories = ["diseases", "disorders", "neoplasms"]
        return self.category_type in chronic_categories
    
    @property
    def severity_level(self) -> str:
        """Get general severity level for conditions in this chapter"""
        if self.category_type == "neoplasms":
            return "high"
        elif self.category_type in ["injuries", "external_causes"]:
            return "high"
        elif self.category_type == "diseases":
            if any(word in self.descricao.lower() for word in ['circulatório', 'nervoso', 'respiratório']):
                return "medium-high"
            else:
                return "medium"
        elif self.category_type == "disorders":
            return "medium"
        elif self.category_type == "symptoms":
            return "low-medium"
        else:
            return "medium"
    
    def get_codes_in_range(self) -> List[str]:
        """Get list of all possible codes in this chapter's range (simplified)"""
        codes = []
        start_letter = self.codigo_inicio[0]
        start_num = int(self.codigo_inicio[1:])
        end_num = int(self.codigo_fim[1:])
        
        # Generate main codes (without subcategories)
        for num in range(start_num, end_num + 1):
            if num < 100:
                codes.append(f"{start_letter}{num:02d}")
            else:
                codes.append(f"{start_letter}{num}")
        
        return codes
    
    def get_chapter_summary(self) -> dict:
        """Get comprehensive chapter summary"""
        return {
            "numero": self.numero_capitulo,
            "titulo_romano": self.roman_number,
            "titulo_completo": self.full_title,
            "range_codigos": self.code_range,
            "descricao": self.descricao,
            "descricao_abrev": self.descricao_abrev,
            "categoria_geral": self.categoria_geral,
            "tipo_categoria": self.category_type,
            "condicoes_cronicas": self.is_chronic_condition_category,
            "nivel_severidade": self.severity_level,
            "total_codigos_principais": len(self.get_codes_in_range())
        }
    
    def search_relevance_score(self, search_term: str) -> float:
        """Calculate relevance score for search term (0.0 to 1.0)"""
        if not search_term:
            return 0.0
        
        search_lower = search_term.lower().strip()
        score = 0.0
        
        # Exact word matches in description (highest priority)
        desc_words = self.descricao.lower().split()
        search_words = search_lower.split()
        
        exact_word_matches = sum(1 for word in search_words if word in desc_words)
        if exact_word_matches > 0:
            score += 0.7 * (exact_word_matches / len(search_words))
        
        # Partial matches in description
        if search_lower in self.descricao.lower():
            score += 0.5
        
        # Exact matches in abbreviated description  
        if self.descricao_abrev and search_lower in self.descricao_abrev.lower():
            score += 0.4
        
        # Substring matches (partial words)
        substring_matches = 0
        for search_word in search_words:
            for desc_word in desc_words:
                if search_word in desc_word and search_word != desc_word:
                    substring_matches += 1
        
        if substring_matches > 0:
            score += 0.2 * min(substring_matches / len(search_words), 0.5)
        
        # Code range match
        if any(code.lower().startswith(search_lower) for code in [self.codigo_inicio.lower(), self.codigo_fim.lower()]):
            score += 0.3
        
        return min(score, 1.0)