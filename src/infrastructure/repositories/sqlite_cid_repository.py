"""
SQLite CID Repository - Concrete implementation for CID data access
"""
import sqlite3
from typing import List, Optional, Dict, Any
from pathlib import Path

from ...domain.repositories.cid_repository import ICIDRepository
from ...domain.entities.cid_chapter import CIDChapter


class SQLiteCIDRepository(ICIDRepository):
    """
    SQLite implementation of CID repository
    
    Provides concrete implementation for CID-10 data access
    using SQLite database backend.
    """
    
    def __init__(self, database_path: str = "sus_database.db"):
        """
        Initialize SQLite CID repository
        
        Args:
            database_path: Path to SQLite database file
        """
        self.database_path = database_path
        self._ensure_database_exists()
    
    def get_chapter_by_code(self, cid_code: str) -> Optional[CIDChapter]:
        """Get CID chapter that contains the given code"""
        if not cid_code:
            return None
        
        normalized_code = cid_code.strip().upper()
        
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.cursor()
            
            # Query to find chapter containing the code
            query = """
                SELECT id, numero_capitulo, codigo_inicio, codigo_fim, 
                       descricao, descricao_abrev, categoria_geral
                FROM cid_capitulos 
                WHERE ? >= codigo_inicio AND ? <= codigo_fim
                ORDER BY numero_capitulo
                LIMIT 1
            """
            
            cursor.execute(query, (normalized_code, normalized_code))
            row = cursor.fetchone()
            
            if row:
                return self._row_to_chapter(row)
            
            return None
    
    def get_chapter_by_number(self, chapter_number: int) -> Optional[CIDChapter]:
        """Get CID chapter by chapter number"""
        if not chapter_number or chapter_number < 1:
            return None
        
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT id, numero_capitulo, codigo_inicio, codigo_fim,
                       descricao, descricao_abrev, categoria_geral
                FROM cid_capitulos 
                WHERE numero_capitulo = ?
            """
            
            cursor.execute(query, (chapter_number,))
            row = cursor.fetchone()
            
            if row:
                return self._row_to_chapter(row)
            
            return None
    
    def get_all_chapters(self) -> List[CIDChapter]:
        """Get all available CID chapters"""
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT id, numero_capitulo, codigo_inicio, codigo_fim,
                       descricao, descricao_abrev, categoria_geral
                FROM cid_capitulos 
                ORDER BY numero_capitulo
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            return [self._row_to_chapter(row) for row in rows]
    
    def search_chapters_by_description(self, search_term: str) -> List[CIDChapter]:
        """Search chapters by description text"""
        if not search_term or not search_term.strip():
            return []
        
        search_pattern = f"%{search_term.strip()}%"
        
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT id, numero_capitulo, codigo_inicio, codigo_fim,
                       descricao, descricao_abrev, categoria_geral
                FROM cid_capitulos 
                WHERE descricao LIKE ? OR descricao_abrev LIKE ?
                ORDER BY numero_capitulo
            """
            
            cursor.execute(query, (search_pattern, search_pattern))
            rows = cursor.fetchall()
            
            return [self._row_to_chapter(row) for row in rows]
    
    def get_chapters_by_code_range(self, start_code: str, end_code: str) -> List[CIDChapter]:
        """Get chapters that overlap with the given code range"""
        if not start_code or not end_code:
            return []
        
        start_normalized = start_code.strip().upper()
        end_normalized = end_code.strip().upper()
        
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.cursor()
            
            # Find chapters that overlap with the given range
            query = """
                SELECT id, numero_capitulo, codigo_inicio, codigo_fim,
                       descricao, descricao_abrev, categoria_geral
                FROM cid_capitulos 
                WHERE NOT (codigo_fim < ? OR codigo_inicio > ?)
                ORDER BY numero_capitulo
            """
            
            cursor.execute(query, (start_normalized, end_normalized))
            rows = cursor.fetchall()
            
            return [self._row_to_chapter(row) for row in rows]
    
    def get_chapters_by_category_type(self, category_type: str) -> List[CIDChapter]:
        """Get chapters by semantic category type"""
        # Since category_type is computed by the entity, we need to get all chapters
        # and filter by the computed property
        all_chapters = self.get_all_chapters()
        
        return [chapter for chapter in all_chapters 
                if chapter.category_type == category_type]
    
    def get_chapter_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about CID chapters"""
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.cursor()
            
            # Basic statistics
            stats = {}
            
            # Total chapters
            cursor.execute("SELECT COUNT(*) FROM cid_capitulos")
            stats['total_chapters'] = cursor.fetchone()[0]
            
            # Chapter number range
            cursor.execute("SELECT MIN(numero_capitulo), MAX(numero_capitulo) FROM cid_capitulos")
            min_chapter, max_chapter = cursor.fetchone()
            stats['chapter_range'] = {'min': min_chapter, 'max': max_chapter}
            
            # Chapters by first letter of code range
            cursor.execute("""
                SELECT SUBSTR(codigo_inicio, 1, 1) as letter, COUNT(*) as count
                FROM cid_capitulos 
                GROUP BY SUBSTR(codigo_inicio, 1, 1)
                ORDER BY letter
            """)
            
            letter_distribution = {}
            for letter, count in cursor.fetchall():
                letter_distribution[letter] = count
            
            stats['code_letter_distribution'] = letter_distribution
            
            return stats
    
    def _row_to_chapter(self, row: tuple) -> CIDChapter:
        """Convert database row to CIDChapter entity"""
        if not row or len(row) < 7:
            raise ValueError("Invalid database row for CID chapter")
        
        return CIDChapter(
            numero_capitulo=row[1],
            codigo_inicio=row[2],
            codigo_fim=row[3],
            descricao=row[4],
            descricao_abrev=row[5] if row[5] else None,
            categoria_geral=row[6] if row[6] else None
        )
    
    def _ensure_database_exists(self) -> None:
        """Ensure the database file exists"""
        db_path = Path(self.database_path)
        if not db_path.exists():
            raise FileNotFoundError(f"Database file not found: {self.database_path}")
        
        # Verify that the cid_capitulos table exists
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='cid_capitulos'
            """)
            
            if not cursor.fetchone():
                raise ValueError(f"Table 'cid_capitulos' not found in database: {self.database_path}")
    
    def count_chapters(self) -> int:
        """Get total count of chapters"""
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM cid_capitulos")
            return cursor.fetchone()[0]
    
    def get_chapters_with_code_counts(self) -> List[Dict[str, Any]]:
        """Get chapters with their estimated code counts"""
        chapters = self.get_all_chapters()
        
        result = []
        for chapter in chapters:
            codes_in_range = chapter.get_codes_in_range()
            
            result.append({
                "chapter": chapter.get_chapter_summary(),
                "estimated_code_count": len(codes_in_range),
                "sample_codes": codes_in_range[:5]  # First 5 codes as sample
            })
        
        return result