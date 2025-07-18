#!/usr/bin/env python3
"""
Test script for improved evaluation logic
Tests the enhanced comparison against specific problematic cases
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from utils import EnhancedDataComparator

def test_gt012_case():
    """Test GT012 - Gender mapping case"""
    print("🧪 Testing GT012 - Gender mapping case")
    
    # Expected: [1, 31041], [3, 27614] 
    # Generated: ["Masculino", 31041], ["Feminino", 27614]
    
    expected_result = {
        'success': True,
        'data': [[1, 31041], [3, 27614]],
        'columns': ['SEXO', 'total'],
        'row_count': 2
    }
    
    generated_result = {
        'success': True,
        'data': [["Masculino", 31041], ["Feminino", 27614]],
        'columns': ['sexo', 'total_atendimentos'],
        'row_count': 2
    }
    
    comparison = EnhancedDataComparator.enhanced_compare_query_results(
        expected_result, generated_result
    )
    
    print(f"  Exact match: {comparison['exact_match']}")
    print(f"  Functional equivalence: {comparison['functional_equivalence']}")
    print(f"  Data equivalence: {comparison['data_equivalence']}")
    print(f"  Semantic equivalence: {comparison['semantic_equivalence']}")
    print(f"  Columns match: {comparison['columns_match']}")
    print(f"  Confidence: {comparison['confidence']:.3f}")
    print(f"  Reason: {comparison['reason']}")
    
    # Test checkpoints
    checkpoints = comparison.get('evaluation_checkpoints', {})
    print(f"  Overall assessment: {checkpoints.get('overall', {}).get('summary', 'N/A')}")
    
    # Should be functionally equivalent despite format differences
    assert comparison['functional_equivalence'], "GT012 should be functionally equivalent"
    print("  ✅ GT012 test passed!\n")


def test_gt013_case():
    """Test GT013 - Age group labels case"""
    print("🧪 Testing GT013 - Age group labels case")
    
    # Expected: ["Acima de 65", 1278], ["18-64 anos", 794], ["Menor de 18", 130]
    # Generated: ["Idoso", 1278], ["Adulto", 794], ["Menor", 130]
    
    expected_result = {
        'success': True,
        'data': [["Acima de 65", 1278], ["18-64 anos", 794], ["Menor de 18", 130]],
        'columns': ['faixa_etaria', 'total_mortes'],
        'row_count': 3
    }
    
    generated_result = {
        'success': True,
        'data': [["Idoso", 1278], ["Adulto", 794], ["Menor", 130]],
        'columns': ['faixa_etaria', 'total_mortos'],
        'row_count': 3
    }
    
    comparison = EnhancedDataComparator.enhanced_compare_query_results(
        expected_result, generated_result
    )
    
    print(f"  Exact match: {comparison['exact_match']}")
    print(f"  Functional equivalence: {comparison['functional_equivalence']}")
    print(f"  Data equivalence: {comparison['data_equivalence']}")
    print(f"  Semantic equivalence: {comparison['semantic_equivalence']}")
    print(f"  Columns match: {comparison['columns_match']}")
    print(f"  Confidence: {comparison['confidence']:.3f}")
    print(f"  Reason: {comparison['reason']}")
    
    # Should be functionally equivalent despite label differences
    assert comparison['functional_equivalence'], "GT013 should be functionally equivalent"
    print("  ✅ GT013 test passed!\n")


def test_gt014_case():
    """Test GT014 - Column name difference case"""
    print("🧪 Testing GT014 - Column name difference case")
    
    # Expected: [["R570", 43], ["I469", 41], ...] with 'total_casos' column
    # Generated: [["R570", 43], ["I469", 41], ...] with 'total_mortes' column
    
    expected_result = {
        'success': True,
        'data': [["R570", 43], ["I469", 41], ["J960", 39], ["B342", 37], ["A419", 36]],
        'columns': ['DIAG_PRINC', 'total_casos'],
        'row_count': 5
    }
    
    generated_result = {
        'success': True,
        'data': [["R570", 43], ["I469", 41], ["J960", 39], ["B342", 37], ["A419", 36]],
        'columns': ['DIAG_PRINC', 'total_mortes'],
        'row_count': 5
    }
    
    comparison = EnhancedDataComparator.enhanced_compare_query_results(
        expected_result, generated_result
    )
    
    print(f"  Exact match: {comparison['exact_match']}")
    print(f"  Functional equivalence: {comparison['functional_equivalence']}")
    print(f"  Data equivalence: {comparison['data_equivalence']}")
    print(f"  Semantic equivalence: {comparison['semantic_equivalence']}")
    print(f"  Columns match: {comparison['columns_match']}")
    print(f"  Confidence: {comparison['confidence']:.3f}")
    print(f"  Reason: {comparison['reason']}")
    
    # Should be functionally equivalent (same data, semantically equivalent columns)
    assert comparison['functional_equivalence'], "GT014 should be functionally equivalent"
    print("  ✅ GT014 test passed!\n")


def test_normalization_functions():
    """Test the individual normalization functions"""
    print("🧪 Testing normalization functions")
    
    # Test gender normalization
    assert EnhancedDataComparator.normalize_value("Masculino", "sexo") == 1
    assert EnhancedDataComparator.normalize_value("Feminino", "sexo") == 3
    assert EnhancedDataComparator.normalize_value(1, "sexo") == 1
    
    # Test age group normalization
    assert EnhancedDataComparator.normalize_value("Idoso", "faixa_etaria") == "Acima de 65"
    assert EnhancedDataComparator.normalize_value("Adulto", "faixa_etaria") == "18-64 anos"
    assert EnhancedDataComparator.normalize_value("Menor", "faixa_etaria") == "Menor de 18"
    
    # Test column normalization
    assert EnhancedDataComparator.normalize_column_name("total_atendimentos") == "total"
    assert EnhancedDataComparator.normalize_column_name("total_mortes") == "total"
    assert EnhancedDataComparator.normalize_column_name("total_casos") == "total"
    
    print("  ✅ Normalization functions test passed!\n")


def main():
    """Run all tests"""
    print("🚀 Testing Enhanced Query Evaluation Logic")
    print("=" * 60)
    
    try:
        test_normalization_functions()
        test_gt012_case()
        test_gt013_case()
        test_gt014_case()
        
        print("🎉 All tests passed! Enhanced evaluation logic is working correctly.")
        print("\nThe improved evaluation should now correctly identify:")
        print("- Gender code to label mappings (1→Masculino, 3→Feminino)")
        print("- Age group label equivalencies (Idoso→Acima de 65, etc.)")
        print("- Column name aliases (total_casos, total_mortes, etc.)")
        
    except AssertionError as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()