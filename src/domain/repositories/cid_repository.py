"""
CID Repository Interface - Abstract interface for CID data access
"""
from abc import ABC, abstractmethod
from typing import List, Optional
from ..entities.cid_chapter import CIDChapter


class ICIDRepository(ABC):
    """
    Interface for CID data repository operations
    
    Defines the contract for CID-10 data access operations
    following the Repository pattern for clean architecture.
    """
    
    @abstractmethod
    def get_chapter_by_code(self, cid_code: str) -> Optional[CIDChapter]:
        """
        Get CID chapter that contains the given code
        
        Args:
            cid_code: CID-10 code (e.g., "A46", "C168")
            
        Returns:
            CIDChapter if found, None otherwise
        """
        pass
    
    @abstractmethod
    def get_chapter_by_number(self, chapter_number: int) -> Optional[CIDChapter]:
        """
        Get CID chapter by chapter number
        
        Args:
            chapter_number: Chapter number (1-22)
            
        Returns:
            CIDChapter if found, None otherwise
        """
        pass
    
    @abstractmethod
    def get_all_chapters(self) -> List[CIDChapter]:
        """
        Get all available CID chapters
        
        Returns:
            List of all CIDChapter objects
        """
        pass
    
    @abstractmethod
    def search_chapters_by_description(self, search_term: str) -> List[CIDChapter]:
        """
        Search chapters by description text
        
        Args:
            search_term: Search term
            
        Returns:
            List of matching CIDChapter objects
        """
        pass
    
    @abstractmethod
    def get_chapters_by_code_range(self, start_code: str, end_code: str) -> List[CIDChapter]:
        """
        Get chapters that overlap with the given code range
        
        Args:
            start_code: Starting CID code
            end_code: Ending CID code
            
        Returns:
            List of overlapping CIDChapter objects
        """
        pass
    
    @abstractmethod
    def get_chapters_by_category_type(self, category_type: str) -> List[CIDChapter]:
        """
        Get chapters by semantic category type
        
        Args:
            category_type: Category type (e.g., "diseases", "disorders")
            
        Returns:
            List of matching CIDChapter objects
        """
        pass