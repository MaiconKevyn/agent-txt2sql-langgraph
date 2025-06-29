"""
Comprehensive tests for CID-10 integration functionality
"""
import unittest
import sqlite3
import tempfile
import os
import sys
from pathlib import Path
from typing import List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import the CID-related modules
from src.domain.entities.cid_chapter import CIDChapter
from src.domain.services.cid_semantic_search_service import CIDSemanticSearchService
from src.infrastructure.repositories.sqlite_cid_repository import SQLiteCIDRepository
from src.application.container.dependency_injection import DependencyContainer, ServiceConfig
from src.domain.repositories.cid_repository import ICIDRepository
from src.domain.services.cid_semantic_search_service import ICIDSemanticSearchService


class TestCIDChapter(unittest.TestCase):
    """Test CIDChapter entity functionality"""
    
    def setUp(self):
        """Set up test data"""
        self.chapter_data = {
            "numero_capitulo": 1,
            "codigo_inicio": "A00",
            "codigo_fim": "B99",
            "descricao": "Capítulo I - Algumas Doenças infecciosas e parasitárias",
            "descricao_abrev": "I. Algumas Doenças infecciosas e parasitárias",
            "categoria_geral": "A"
        }
    
    def test_chapter_creation(self):
        """Test basic chapter creation"""
        chapter = CIDChapter(**self.chapter_data)
        
        self.assertEqual(chapter.numero_capitulo, 1)
        self.assertEqual(chapter.codigo_inicio, "A00")
        self.assertEqual(chapter.codigo_fim, "B99")
        self.assertTrue(chapter.descricao.startswith("Capítulo I"))
    
    def test_chapter_validation(self):
        """Test chapter validation"""
        # Invalid chapter number
        with self.assertRaises(ValueError):
            CIDChapter(
                numero_capitulo=0,
                codigo_inicio="A00",
                codigo_fim="B99",
                descricao="Test"
            )
        
        # Invalid code format
        with self.assertRaises(ValueError):
            CIDChapter(
                numero_capitulo=1,
                codigo_inicio="XXX",
                codigo_fim="B99",
                descricao="Test description"
            )
    
    def test_contains_code(self):
        """Test if chapter contains specific CID code"""
        chapter = CIDChapter(**self.chapter_data)
        
        # Codes within range
        self.assertTrue(chapter.contains_code("A46"))
        self.assertTrue(chapter.contains_code("B99"))
        self.assertTrue(chapter.contains_code("A00"))
        
        # Codes outside range
        self.assertFalse(chapter.contains_code("C00"))
        self.assertFalse(chapter.contains_code("Z99"))
        self.assertFalse(chapter.contains_code(""))
    
    def test_roman_number_conversion(self):
        """Test Roman numeral conversion"""
        chapter = CIDChapter(**self.chapter_data)
        self.assertEqual(chapter.roman_number, "I")
        
        chapter_20 = CIDChapter(
            numero_capitulo=20,
            codigo_inicio="V01",
            codigo_fim="Y98",
            descricao="Capítulo XX - Causas externas"
        )
        self.assertEqual(chapter_20.roman_number, "XX")
    
    def test_category_type_classification(self):
        """Test semantic category classification"""
        chapter = CIDChapter(**self.chapter_data)
        self.assertEqual(chapter.category_type, "diseases")
        
        neoplasm_chapter = CIDChapter(
            numero_capitulo=2,
            codigo_inicio="C00",
            codigo_fim="D48",
            descricao="Capítulo II - Neoplasias [tumores]"
        )
        self.assertEqual(neoplasm_chapter.category_type, "neoplasms")
    
    def test_search_relevance_score(self):
        """Test search relevance scoring"""
        chapter = CIDChapter(**self.chapter_data)
        
        # Exact match should score high
        score_exact = chapter.search_relevance_score("infecciosas")
        self.assertGreater(score_exact, 0.5)
        
        # Partial match should score lower but positive
        score_partial = chapter.search_relevance_score("algumas")
        self.assertGreater(score_partial, 0.0)
        
        # Different term with lower relevance
        score_lower = chapter.search_relevance_score("capítulo")
        self.assertGreater(score_lower, 0.0)
        
        # Ensure different scores for different terms
        self.assertNotEqual(score_exact, score_partial)
        
        # No match should score 0
        score_none = chapter.search_relevance_score("respiratório")
        self.assertEqual(score_none, 0.0)


class TestSQLiteCIDRepository(unittest.TestCase):
    """Test SQLite CID repository implementation"""
    
    def setUp(self):
        """Set up test database"""
        self.test_db_fd, self.test_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.test_db_fd)
        
        # Create test database with CID data
        self._create_test_database()
        self.repository = SQLiteCIDRepository(self.test_db_path)
    
    def tearDown(self):
        """Clean up test database"""
        if os.path.exists(self.test_db_path):
            os.unlink(self.test_db_path)
    
    def _create_test_database(self):
        """Create test database with sample CID data"""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Create CID chapters table
        cursor.execute("""
            CREATE TABLE cid_capitulos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                numero_capitulo INTEGER NOT NULL,
                codigo_inicio TEXT NOT NULL,
                codigo_fim TEXT NOT NULL,
                descricao TEXT NOT NULL,
                descricao_abrev TEXT,
                categoria_geral TEXT
            )
        """)
        
        # Insert test data
        test_chapters = [
            (1, "A00", "B99", "Capítulo I - Algumas Doenças infecciosas e parasitárias", "I. Doenças infecciosas", "A"),
            (2, "C00", "D48", "Capítulo II - Neoplasias [tumores]", "II. Neoplasias", "C"),
            (9, "I00", "I99", "Capítulo IX - Doenças do aparelho circulatório", "IX. Doenças circulatório", "I"),
            (10, "J00", "J99", "Capítulo X - Doenças do aparelho respiratório", "X. Doenças respiratório", "J")
        ]
        
        cursor.executemany("""
            INSERT INTO cid_capitulos 
            (numero_capitulo, codigo_inicio, codigo_fim, descricao, descricao_abrev, categoria_geral)
            VALUES (?, ?, ?, ?, ?, ?)
        """, test_chapters)
        
        conn.commit()
        conn.close()
    
    def test_get_all_chapters(self):
        """Test getting all chapters"""
        chapters = self.repository.get_all_chapters()
        self.assertEqual(len(chapters), 4)
        self.assertIsInstance(chapters[0], CIDChapter)
    
    def test_get_chapter_by_code(self):
        """Test getting chapter by CID code"""
        # Test existing code
        chapter = self.repository.get_chapter_by_code("A46")
        self.assertIsNotNone(chapter)
        self.assertEqual(chapter.numero_capitulo, 1)
        
        # Test code in different chapter
        chapter = self.repository.get_chapter_by_code("I50")
        self.assertIsNotNone(chapter)
        self.assertEqual(chapter.numero_capitulo, 9)
        
        # Test non-existing code
        chapter = self.repository.get_chapter_by_code("Z99")
        self.assertIsNone(chapter)
    
    def test_get_chapter_by_number(self):
        """Test getting chapter by number"""
        chapter = self.repository.get_chapter_by_number(2)
        self.assertIsNotNone(chapter)
        self.assertEqual(chapter.codigo_inicio, "C00")
        self.assertEqual(chapter.codigo_fim, "D48")
        
        # Test non-existing chapter
        chapter = self.repository.get_chapter_by_number(99)
        self.assertIsNone(chapter)
    
    def test_search_chapters_by_description(self):
        """Test searching chapters by description"""
        # Search for "respiratório"
        chapters = self.repository.search_chapters_by_description("respiratório")
        self.assertEqual(len(chapters), 1)
        self.assertEqual(chapters[0].numero_capitulo, 10)
        
        # Search for "doenças"
        chapters = self.repository.search_chapters_by_description("doenças")
        self.assertGreaterEqual(len(chapters), 3)  # Should find multiple matches
        
        # Search for non-existing term
        chapters = self.repository.search_chapters_by_description("xyz123")
        self.assertEqual(len(chapters), 0)
    
    def test_get_chapters_by_code_range(self):
        """Test getting chapters by code range"""
        # Range that overlaps with infectious diseases
        chapters = self.repository.get_chapters_by_code_range("A50", "B50")
        self.assertEqual(len(chapters), 1)
        self.assertEqual(chapters[0].numero_capitulo, 1)
        
        # Range that spans multiple chapters
        chapters = self.repository.get_chapters_by_code_range("A00", "D99")
        self.assertGreaterEqual(len(chapters), 2)
    
    def test_chapter_statistics(self):
        """Test chapter statistics"""
        stats = self.repository.get_chapter_statistics()
        self.assertEqual(stats['total_chapters'], 4)
        self.assertIn('chapter_range', stats)
        self.assertIn('code_letter_distribution', stats)


class TestCIDSemanticSearchService(unittest.TestCase):
    """Test CID semantic search service"""
    
    def setUp(self):
        """Set up test service"""
        self.test_db_fd, self.test_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.test_db_fd)
        
        # Create test database
        self._create_test_database()
        
        # Create repository and service
        self.repository = SQLiteCIDRepository(self.test_db_path)
        self.service = CIDSemanticSearchService(self.repository)
    
    def tearDown(self):
        """Clean up"""
        if os.path.exists(self.test_db_path):
            os.unlink(self.test_db_path)
    
    def _create_test_database(self):
        """Create test database"""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE cid_capitulos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                numero_capitulo INTEGER NOT NULL,
                codigo_inicio TEXT NOT NULL,
                codigo_fim TEXT NOT NULL,
                descricao TEXT NOT NULL,
                descricao_abrev TEXT,
                categoria_geral TEXT
            )
        """)
        
        test_chapters = [
            (1, "A00", "B99", "Capítulo I - Algumas Doenças infecciosas e parasitárias", "I. Doenças infecciosas", "A"),
            (2, "C00", "D48", "Capítulo II - Neoplasias [tumores]", "II. Neoplasias", "C"),
            (10, "J00", "J99", "Capítulo X - Doenças do aparelho respiratório", "X. Doenças respiratório", "J")
        ]
        
        cursor.executemany("""
            INSERT INTO cid_capitulos 
            (numero_capitulo, codigo_inicio, codigo_fim, descricao, descricao_abrev, categoria_geral)
            VALUES (?, ?, ?, ?, ?, ?)
        """, test_chapters)
        
        conn.commit()
        conn.close()
    
    def test_find_chapter_by_code(self):
        """Test finding chapter by code"""
        chapter = self.service.find_chapter_by_code("J44")
        self.assertIsNotNone(chapter)
        self.assertEqual(chapter.numero_capitulo, 10)
        
        # Invalid code
        chapter = self.service.find_chapter_by_code("XXX")
        self.assertIsNone(chapter)
    
    def test_search_chapters_by_description(self):
        """Test searching by description with relevance"""
        results = self.service.search_chapters_by_description("respiratório")
        
        self.assertGreater(len(results), 0)
        self.assertIsInstance(results[0], tuple)
        
        chapter, score = results[0]
        self.assertIsInstance(chapter, CIDChapter)
        self.assertGreater(score, 0.0)
        self.assertLessEqual(score, 1.0)
    
    def test_validate_cid_code(self):
        """Test CID code validation"""
        # Valid codes in our test data
        self.assertTrue(self.service.validate_cid_code("A46"))
        self.assertTrue(self.service.validate_cid_code("C50"))
        self.assertTrue(self.service.validate_cid_code("J44"))
        
        # Invalid codes
        self.assertFalse(self.service.validate_cid_code("Z99"))  # Not in our test chapters
        self.assertFalse(self.service.validate_cid_code("XXX"))  # Invalid format
        self.assertFalse(self.service.validate_cid_code(""))     # Empty
    
    def test_get_chapter_statistics(self):
        """Test chapter statistics"""
        stats = self.service.get_chapter_statistics()
        
        self.assertEqual(stats['total_chapters'], 3)
        self.assertIn('categories', stats)
        self.assertIn('severity_distribution', stats)
    
    def test_suggest_codes_for_description(self):
        """Test code suggestions"""
        suggestions = self.service.suggest_codes_for_description("tumor")
        
        self.assertGreater(len(suggestions), 0)
        
        code, chapter, score = suggestions[0]
        self.assertEqual(code, "C00")  # Should suggest start of neoplasm chapter
        self.assertGreater(score, 0.0)


class TestCIDIntegrationWithDI(unittest.TestCase):
    """Test CID integration with dependency injection container"""
    
    def setUp(self):
        """Set up DI container with CID services"""
        self.test_db_fd, self.test_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.test_db_fd)
        
        # Create test database
        self._create_test_database()
        
        # Create DI container with CID enabled
        config = ServiceConfig(
            database_path=self.test_db_path,
            enable_cid_semantic_search=True,
            cid_repository_type="sqlite"
        )
        self.container = DependencyContainer(config)
    
    def tearDown(self):
        """Clean up"""
        if hasattr(self, 'container'):
            self.container.shutdown()
        if os.path.exists(self.test_db_path):
            os.unlink(self.test_db_path)
    
    def _create_test_database(self):
        """Create test database with both SUS and CID data"""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Create CID chapters table
        cursor.execute("""
            CREATE TABLE cid_capitulos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                numero_capitulo INTEGER NOT NULL,
                codigo_inicio TEXT NOT NULL,
                codigo_fim TEXT NOT NULL,
                descricao TEXT NOT NULL,
                descricao_abrev TEXT,
                categoria_geral TEXT
            )
        """)
        
        # Create SUS data table (minimal for testing)
        cursor.execute("""
            CREATE TABLE sus_data (
                DIAG_PRINC TEXT,
                IDADE INTEGER,
                SEXO INTEGER,
                MORTE INTEGER
            )
        """)
        
        # Insert test CID data
        cursor.executemany("""
            INSERT INTO cid_capitulos 
            (numero_capitulo, codigo_inicio, codigo_fim, descricao, descricao_abrev, categoria_geral)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            (1, "A00", "B99", "Capítulo I - Doenças infecciosas", "I. Infecciosas", "A"),
            (10, "J00", "J99", "Capítulo X - Doenças respiratórias", "X. Respiratórias", "J")
        ])
        
        # Insert test SUS data
        cursor.executemany("""
            INSERT INTO sus_data (DIAG_PRINC, IDADE, SEXO, MORTE)
            VALUES (?, ?, ?, ?)
        """, [
            ("A46", 45, 1, 0),
            ("J44", 65, 3, 0),
            ("J18", 70, 1, 1)
        ])
        
        conn.commit()
        conn.close()
    
    def test_container_initialization_with_cid(self):
        """Test that container initializes CID services correctly"""
        self.container.initialize()
        
        # Check that CID services are registered
        cid_repo = self.container.get_service(ICIDRepository)
        self.assertIsNotNone(cid_repo)
        self.assertIsInstance(cid_repo, SQLiteCIDRepository)
        
        cid_service = self.container.get_service(ICIDSemanticSearchService)
        self.assertIsNotNone(cid_service)
        self.assertIsInstance(cid_service, CIDSemanticSearchService)
    
    def test_health_check_with_cid(self):
        """Test health check includes CID services"""
        self.container.initialize()
        health = self.container.health_check()
        
        self.assertEqual(health['status'], 'healthy')
        self.assertIn('cid_repository', health['services'])
        self.assertIn('cid_semantic_search', health['services'])
        self.assertTrue(health['services']['cid_repository']['healthy'])
        self.assertTrue(health['services']['cid_semantic_search']['healthy'])
    
    def test_cid_service_functionality(self):
        """Test CID service functionality through DI container"""
        self.container.initialize()
        
        cid_service = self.container.get_service(ICIDSemanticSearchService)
        
        # Test finding chapter by code
        chapter = cid_service.find_chapter_by_code("J44")
        self.assertIsNotNone(chapter)
        self.assertEqual(chapter.numero_capitulo, 10)
        
        # Test searching by description
        results = cid_service.search_chapters_by_description("respiratório")
        self.assertGreater(len(results), 0)


if __name__ == '__main__':
    # Create test suite
    test_classes = [
        TestCIDChapter,
        TestSQLiteCIDRepository,
        TestCIDSemanticSearchService,
        TestCIDIntegrationWithDI
    ]
    
    suite = unittest.TestSuite()
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"TESTS RUN: {result.testsRun}")
    print(f"FAILURES: {len(result.failures)}")
    print(f"ERRORS: {len(result.errors)}")
    print(f"SUCCESS RATE: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.failures:
        print(f"\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}")
    
    if result.errors:
        print(f"\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}")