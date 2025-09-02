#!/usr/bin/env python3
"""
Ground Truth Query Validation

Use to validate the execution of SQL queries from ground_truth.json

Input: ground_truth.json
Output: Console log with validation results
"""

import json
import sys
from pathlib import Path

# Add current dir to path for imports
sys.path.append(str(Path(__file__).parent))

# Import database manager from utils
from utils import DatabaseManager


def validate_ground_truth_queries(ground_truth_path: str):
    """Validate all queries in ground truth file"""
    
    # Load ground truth
    with open(ground_truth_path, 'r', encoding='utf-8') as f:
        test_cases = json.load(f)
    
    # Connect to database (using PostgreSQL)
    database_path = "postgresql://postgres:1234@localhost:5432/sih_rs"
    db = DatabaseManager(database_path)
    
    print(f"🔍 Validating {len(test_cases)} queries from ground truth")
    print("="*60)
    
    valid_queries = 0
    invalid_queries = []
    
    for i, test_case in enumerate(test_cases):
        test_id = test_case["id"]
        question = test_case["question"]
        query = test_case["query"]
        tables = test_case["tables"]
        difficulty = test_case.get("difficulty", "unknown")
        
        print(f"\n[{i+1}/{len(test_cases)}] {test_id} ({difficulty})")
        print(f"📝 Question: {question}")
        print(f"🎯 Tables: {tables}")
        print(f"🔧 Query: {query}")
        
        try:
            # Execute query
            result = db.execute_query(query)
            
            # Check if result is valid
            if result['success']:
                data = result['data']
                row_count = result['row_count']
                
                if row_count > 0:
                    print(f"✅ Valid - {row_count} rows returned")
                    print(f"📊 Sample: {data[0] if data else 'No data'}")
                    valid_queries += 1
                else:
                    print(f"⚠️  Valid but empty result")
                    valid_queries += 1
            else:
                print(f"❌ Invalid - Error: {result['error']}")
                invalid_queries.append({
                    "id": test_id,
                    "error": result['error'],
                    "query": query
                })
                
        except Exception as e:
            print(f"❌ Invalid - Error: {str(e)}")
            invalid_queries.append({
                "id": test_id,
                "error": str(e),
                "query": query
            })
    
    print(f"\n{'='*60}")
    print(f"📊 VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total queries: {len(test_cases)}")
    print(f"Valid queries: {valid_queries}")
    print(f"Invalid queries: {len(invalid_queries)}")
    print(f"Success rate: {valid_queries/len(test_cases)*100:.1f}%")
    
    if invalid_queries:
        print(f"\n❌ INVALID QUERIES ({len(invalid_queries)}):")
        for invalid in invalid_queries:
            print(f"  {invalid['id']}: {invalid['error']}")
            print(f"    Query: {invalid['query']}")
    
    return valid_queries == len(test_cases), invalid_queries


def main():
    # Use absolute path to avoid path issues
    current_dir = Path(__file__).parent
    ground_truth_path = current_dir / "ground_truth.json"
    
    if not Path(ground_truth_path).exists():
        print(f"❌ Ground truth file not found: {ground_truth_path}")
        return 1
    
    success, invalid_queries = validate_ground_truth_queries(ground_truth_path)
    
    if success:
        print(f"\n🎉 All queries are valid!")
        return 0
    else:
        print(f"\n⚠️  {len(invalid_queries)} queries need to be fixed")
        return 1


if __name__ == "__main__":
    sys.exit(main())