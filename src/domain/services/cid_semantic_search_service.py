"""
CID Semantic Search Service - Domain service for CID-10 semantic operations
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Tuple
from ..entities.cid_chapter import CIDChapter
from ..entities.diagnosis import Diagnosis


class ICIDSemanticSearchService(ABC):
    """Interface for CID semantic search operations"""
    
    @abstractmethod
    def find_chapter_by_code(self, cid_code: str) -> Optional[CIDChapter]:
        """Find CID chapter that contains the given code"""
        pass
    
    @abstractmethod
    def search_chapters_by_description(self, search_term: str, limit: int = 10) -> List[Tuple[CIDChapter, float]]:
        """Search chapters by description with relevance scores"""
        pass
    
    @abstractmethod
    def get_all_chapters(self) -> List[CIDChapter]:
        """Get all available CID chapters"""
        pass
    
    @abstractmethod
    def validate_cid_code(self, cid_code: str) -> bool:
        """Validate if CID code exists in any chapter"""
        pass
    
    @abstractmethod
    def get_chapter_statistics(self) -> Dict[str, any]:
        """Get statistics about available chapters"""
        pass


class CIDSemanticSearchService(ICIDSemanticSearchService):
    """
    CID Semantic Search Service - Provides semantic search and validation
    for CID-10 codes and chapters using domain logic.
    
    This service operates at the domain layer and coordinates with
    repository layer for data access.
    """
    
    def __init__(self, cid_repository):
        """
        Initialize CID semantic search service
        
        Args:
            cid_repository: Repository for CID data access
        """
        self._cid_repository = cid_repository
        self._chapters_cache: Optional[List[CIDChapter]] = None
    
    def find_chapter_by_code(self, cid_code: str) -> Optional[CIDChapter]:
        """
        Find CID chapter that contains the given code
        
        Args:
            cid_code: CID-10 code (e.g., "A46", "C168")
            
        Returns:
            CIDChapter if found, None otherwise
        """
        if not cid_code or not self._is_valid_cid_format(cid_code):
            return None
        
        # Clean and normalize code
        normalized_code = cid_code.strip().upper()
        
        # Use repository to find chapter
        return self._cid_repository.get_chapter_by_code(normalized_code)
    
    def search_chapters_by_description(self, search_term: str, limit: int = 10) -> List[Tuple[CIDChapter, float]]:
        """
        Search chapters by description with relevance scores
        
        Args:
            search_term: Search term (e.g., "respiratório", "neoplasia")
            limit: Maximum number of results
            
        Returns:
            List of (CIDChapter, relevance_score) tuples sorted by relevance
        """
        if not search_term or not search_term.strip():
            return []
        
        chapters = self._get_cached_chapters()
        results = []
        
        for chapter in chapters:
            score = chapter.search_relevance_score(search_term)
            if score > 0.0:
                results.append((chapter, score))
        
        # Sort by relevance score (descending) and limit results
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:limit]
    
    def get_all_chapters(self) -> List[CIDChapter]:
        """Get all available CID chapters"""
        return self._get_cached_chapters()
    
    def validate_cid_code(self, cid_code: str) -> bool:
        """
        Validate if CID code exists in any chapter
        
        Args:
            cid_code: CID-10 code to validate
            
        Returns:
            True if code is valid and exists in some chapter
        """
        if not self._is_valid_cid_format(cid_code):
            return False
        
        chapter = self.find_chapter_by_code(cid_code)
        return chapter is not None
    
    def get_chapter_statistics(self) -> Dict[str, any]:
        """Get statistics about available chapters"""
        chapters = self._get_cached_chapters()
        
        if not chapters:
            return {
                "total_chapters": 0,
                "total_code_ranges": 0,
                "categories": {},
                "severity_distribution": {}
            }
        
        # Count by category type
        categories = {}
        severity_dist = {}
        
        for chapter in chapters:
            # Category distribution
            cat_type = chapter.category_type
            categories[cat_type] = categories.get(cat_type, 0) + 1
            
            # Severity distribution
            severity = chapter.severity_level
            severity_dist[severity] = severity_dist.get(severity, 0) + 1
        
        return {
            "total_chapters": len(chapters),
            "total_code_ranges": len(chapters),
            "categories": categories,
            "severity_distribution": severity_dist,
            "chapter_numbers": [ch.numero_capitulo for ch in chapters],
            "chronic_condition_chapters": len([ch for ch in chapters if ch.is_chronic_condition_category])
        }
    
    def find_related_chapters(self, cid_code: str, include_same_category: bool = True) -> List[CIDChapter]:
        """
        Find chapters related to the given CID code
        
        Args:
            cid_code: Reference CID code
            include_same_category: Include chapters of same category type
            
        Returns:
            List of related CIDChapter objects
        """
        primary_chapter = self.find_chapter_by_code(cid_code)
        if not primary_chapter:
            return []
        
        related = []
        chapters = self._get_cached_chapters()
        
        for chapter in chapters:
            if chapter.numero_capitulo == primary_chapter.numero_capitulo:
                continue  # Skip the same chapter
            
            # Include same category type
            if include_same_category and chapter.category_type == primary_chapter.category_type:
                related.append(chapter)
            
            # Include similar severity level
            elif chapter.severity_level == primary_chapter.severity_level:
                related.append(chapter)
        
        return related
    
    def suggest_codes_for_description(self, description: str, limit: int = 5) -> List[Tuple[str, CIDChapter, float]]:
        """
        Suggest CID codes based on description
        
        Args:
            description: Medical condition description
            limit: Maximum number of suggestions
            
        Returns:
            List of (suggested_code, chapter, relevance_score) tuples
        """
        chapter_results = self.search_chapters_by_description(description, limit * 2)
        suggestions = []
        
        for chapter, score in chapter_results[:limit]:
            # Suggest the starting code of the range as primary suggestion
            primary_code = chapter.codigo_inicio
            suggestions.append((primary_code, chapter, score))
        
        return suggestions
    
    def enrich_diagnosis(self, diagnosis: Diagnosis) -> Dict[str, any]:
        """
        Enrich diagnosis with CID chapter information
        
        Args:
            diagnosis: Diagnosis entity
            
        Returns:
            Dictionary with enriched information
        """
        chapter = self.find_chapter_by_code(diagnosis.primary_diagnosis_code)
        
        enriched = {
            "original_diagnosis": diagnosis.get_medical_summary(),
            "cid_chapter": None,
            "semantic_category": "unknown",
            "severity_context": "unknown",
            "related_chapters": []
        }
        
        if chapter:
            enriched.update({
                "cid_chapter": chapter.get_chapter_summary(),
                "semantic_category": chapter.category_type,
                "severity_context": chapter.severity_level,
                "related_chapters": [ch.get_chapter_summary() for ch in self.find_related_chapters(diagnosis.primary_diagnosis_code)]
            })
        
        return enriched
    
    def _get_cached_chapters(self) -> List[CIDChapter]:
        """Get chapters with caching for performance"""
        if self._chapters_cache is None:
            self._chapters_cache = self._cid_repository.get_all_chapters()
        return self._chapters_cache
    
    def _is_valid_cid_format(self, cid_code: str) -> bool:
        """Validate CID-10 format using domain logic"""
        if not cid_code:
            return False
        
        # Use existing validation from Diagnosis entity
        try:
            # Create temporary diagnosis to use its validation
            temp_diagnosis = Diagnosis(primary_diagnosis_code=cid_code)
            return True
        except ValueError:
            return False
    
    def invalidate_cache(self) -> None:
        """Invalidate cached chapters (useful after data updates)"""
        self._chapters_cache = None
    
    def get_diagnosis_statistics_by_chapter(self, diagnosis_codes: List[str]) -> Dict[str, Dict[str, any]]:
        """
        Get statistics of diagnosis codes grouped by chapter
        
        Args:
            diagnosis_codes: List of CID codes from actual data
            
        Returns:
            Dictionary with chapter-based statistics
        """
        chapter_stats = {}
        
        for code in diagnosis_codes:
            chapter = self.find_chapter_by_code(code)
            if chapter:
                chapter_key = f"Chapter_{chapter.numero_capitulo}"
                
                if chapter_key not in chapter_stats:
                    chapter_stats[chapter_key] = {
                        "chapter_info": chapter.get_chapter_summary(),
                        "code_count": 0,
                        "unique_codes": set(),
                        "severity_level": chapter.severity_level
                    }
                
                chapter_stats[chapter_key]["code_count"] += 1
                chapter_stats[chapter_key]["unique_codes"].add(code)
        
        # Convert sets to lists for JSON serialization
        for stats in chapter_stats.values():
            stats["unique_codes"] = list(stats["unique_codes"])
            stats["unique_code_count"] = len(stats["unique_codes"])
        
        return chapter_stats