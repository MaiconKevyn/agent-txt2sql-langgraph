"""
Test script for CID Semantic Search integration with database
"""
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.application.services.database_connection_service import DatabaseConnectionFactory
from src.infrastructure.repositories.repository_factory import CIDRepositoryFactory
from src.domain.services.cid_semantic_search_service import CIDSemanticSearchService


def test_cid_integration():
    """Test the complete CID semantic search integration"""
    print("🧪 Testing CID Semantic Search Integration with Database")
    print("=" * 60)
    
    try:
        # Initialize database connection
        print("1. Initializing database connection...")
        db_service = DatabaseConnectionFactory.create_sqlite_service("sus_database.db")
        
        # Test database connection
        if not db_service.test_connection():
            raise Exception("Database connection failed")
        print("   ✅ Database connection successful")
        
        # Initialize CID repository
        print("2. Initializing CID repository...")
        cid_repository = CIDRepositoryFactory.create_sqlite_repository(db_service)
        print("   ✅ CID repository created")
        
        # Test repository operations
        print("3. Testing repository operations...")
        
        # Test get all capitulos
        capitulos = cid_repository.get_all_capitulos()
        print(f"   📊 Found {len(capitulos)} CID chapters in database")
        
        # Test find by CID letter
        respiratory_chapter = cid_repository.find_by_cid_letter("J")
        if respiratory_chapter:
            print(f"   🫁 Respiratory chapter: {respiratory_chapter.descricao}")
        else:
            print("   ❌ Respiratory chapter not found")
        
        # Test CID mapping
        cid_mapping = cid_repository.get_cid_mapping()
        print(f"   📋 CID mapping contains {len(cid_mapping)} categories")
        
        # Initialize semantic search service
        print("4. Initializing semantic search service...")
        semantic_service = CIDSemanticSearchService(cid_repository)
        print("   ✅ Semantic search service initialized")
        
        # Test semantic search
        print("5. Testing semantic search...")
        
        test_queries = [
            "doenças respiratórias",
            "problemas do coração", 
            "câncer",
            "diabetes",
            "depressão"
        ]
        
        for query in test_queries:
            print(f"\n   🔍 Testing query: '{query}'")
            result = semantic_service.search_by_description(query)
            
            if result.best_match:
                print(f"      ✅ Best match: {result.best_match.cid_letter} - {result.best_match.category_name}")
                print(f"      📊 Confidence: {result.best_match.confidence_score:.2f}")
                print(f"      🔗 Matched terms: {', '.join(result.best_match.matched_terms[:3])}")
            else:
                print("      ❌ No match found")
        
        # Test explain match
        print("\n6. Testing match explanation...")
        explanation = semantic_service.explain_match("doenças respiratórias")
        print(f"   📖 Query explanation for 'doenças respiratórias':")
        print(f"      Total matches: {explanation['total_matches']}")
        print(f"      Has confident match: {explanation['has_confident_match']}")
        print(f"      Suggested CID: {explanation['suggested_cid']}")
        
        print("\n🎉 All tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up
        try:
            db_service.close_connection()
            print("\n🧹 Database connection closed")
        except:
            pass


if __name__ == "__main__":
    success = test_cid_integration()
    sys.exit(0 if success else 1)