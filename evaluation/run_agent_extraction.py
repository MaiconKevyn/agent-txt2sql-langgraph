#!/usr/bin/env python3
"""
Agent Data Extraction System for Text2SQL Agent

Executes each question from ground_truth.json through the LangGraph agent,
captures selected tables, generated queries, and agent responses.
This script does NOT perform evaluation - it only extracts data for later analysis.

Usage:
    python evaluation/run_agent_extraction.py
    
Output:
    results/agent_extraction_results.json
"""

import json
import time
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add root to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.agent.orchestrator import LangGraphOrchestrator


class AgentDataExtractor:
    """Data extractor for Text2SQL agent - captures agent responses without evaluation"""
    
    def __init__(self, ground_truth_path: str, output_dir: str = "results"):
        """
        Initialize data extractor
        
        Args:
            ground_truth_path: Path to ground truth JSON file
            output_dir: Directory to save extracted data
        """
        self.ground_truth_path = Path(ground_truth_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize the LangGraph orchestrator
        self.orchestrator = LangGraphOrchestrator(environment="development")
        
        # Results tracking
        self.results = []
        self.successful_runs = 0
        self.failed_runs = 0
        
    def load_ground_truth(self) -> List[Dict[str, Any]]:
        """Load ground truth test cases"""
        if not self.ground_truth_path.exists():
            raise FileNotFoundError(f"Ground truth file not found: {self.ground_truth_path}")
            
        with open(self.ground_truth_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def execute_single_case(self, test_case: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single test case through the agent
        
        Args:
            test_case: Ground truth test case
            
        Returns:
            Enhanced test case with agent results
        """
        question = test_case["question"]
        test_id = test_case["id"]
        
        print(f" Processing {test_id}: {question}")
        
        start_time = time.time()
        
        try:
            # Execute the query through the agent
            # Using process_query method from LangGraphOrchestrator
            result = self.orchestrator.process_query(
                user_query=question,
                session_id=f"eval_{test_id}",
                streaming=False
            )
            
            execution_time = time.time() - start_time
            
            # Extract metadata from the result
            # The orchestrator returns structured data in legacy format
            if isinstance(result, dict):
                # Get basic fields
                success = result.get("success", False)
                error = result.get("error_message", None)
                agent_generated_query = result.get("sql_query", "")
                final_answer = result.get("response", "")
                
                # Extract selected tables from metadata
                metadata = result.get("metadata", {})
                agent_selected_tables = metadata.get("tables_used", [])
                
                # Fallback to check different possible locations for tables
                if not agent_selected_tables:
                    # Check if it's in the root of result
                    agent_selected_tables = result.get("selected_tables", [])
                
            else:
                # Fallback if result format is different
                agent_selected_tables = []
                agent_generated_query = ""
                success = False
                error = "Unexpected result format"
                final_answer = str(result) if result else ""
            
            # Create enhanced result
            enhanced_case = test_case.copy()
            enhanced_case.update({
                "agent_selected_tables": agent_selected_tables,
                "agent_generated_query": agent_generated_query,
                "agent_final_answer": final_answer,
                "execution_time": round(execution_time, 3),
                "success": success,
                "error": error
            })
            
            if success:
                self.successful_runs += 1
                print(f" Success - Tables: {agent_selected_tables}")
            else:
                self.failed_runs += 1
                print(f" Failed - Error: {error}")
                
            return enhanced_case
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # Create failed result
            enhanced_case = test_case.copy()
            enhanced_case.update({
                "agent_selected_tables": [],
                "agent_generated_query": "",
                "agent_final_answer": "",
                "execution_time": round(execution_time, 3),
                "success": False,
                "error": str(e)
            })
            
            self.failed_runs += 1
            print(f" Exception - {str(e)}")
            
            return enhanced_case
    
    def run_extraction(self) -> Dict[str, Any]:
        """
        Run agent data extraction on all ground truth cases
        
        Returns:
            Complete agent response data for analysis
        """
        print(" Starting Agent Data Extraction")
        print("=" * 50)
        
        # Load ground truth
        try:
            ground_truth = self.load_ground_truth()
            print(f" Loaded {len(ground_truth)} test cases")
        except Exception as e:
            print(f" Failed to load ground truth: {e}")
            return {}
        
        start_time = time.time()
        
        # Process all test cases
        test_cases = ground_truth
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\n[{i}/{len(test_cases)}]", end=" ")
            
            enhanced_case = self.execute_single_case(test_case)
            self.results.append(enhanced_case)
        
        total_time = time.time() - start_time
        
        # Create results summary
        results_summary = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "total_cases": len(test_cases),
                "successful_runs": self.successful_runs,
                "failed_runs": self.failed_runs,
                "success_rate": self.successful_runs / len(test_cases) if test_cases else 0,
                "total_execution_time": round(total_time, 2),
                "avg_execution_time": round(total_time / len(test_cases), 3) if test_cases else 0
            },
            "results": self.results
        }
        
        print(f"\n{'=' * 50}")
        print(" EXTRACTION SUMMARY")
        print(f"{'=' * 50}")
        print(f"Total Cases: {len(test_cases)}")
        print(f"Successful: {self.successful_runs}")
        print(f"Failed: {self.failed_runs}")
        print(f"Success Rate: {self.successful_runs / len(test_cases) * 100:.1f}%")
        print(f"Total Time: {total_time:.2f}s")
        print(f"Average Time: {total_time / len(test_cases):.3f}s per case")
        
        return results_summary
    
    def save_results(self, results: Dict[str, Any], filename: str = "agent_extraction_results.json"):
        """
        Save extracted data to JSON file
        
        Args:
            results: Agent extraction results to save
            filename: Output filename
        """
        output_path = self.output_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Results saved to: {output_path}")
        return str(output_path)


def main():
    """Main execution function"""
    
    # Configuration
    current_dir = Path(__file__).parent
    ground_truth_path = current_dir / "ground_truth.json"
    output_dir = current_dir / "results"
    
    # Check if ground truth exists
    if not ground_truth_path.exists():
        print(f" Ground truth file not found: {ground_truth_path}")
        print("Available files:")
        for file in current_dir.glob("*.json"):
            print(f"  - {file.name}")
        return 1
    
    try:
        # Create and run data extractor
        extractor = AgentDataExtractor(
            ground_truth_path=ground_truth_path,
            output_dir=output_dir
        )
        
        # Run the extraction
        results = extractor.run_extraction()
        
        if results:
            # Save results
            output_file = extractor.save_results(results)
            
            print(f"\n Data extraction completed successfully!")
            print(f"📁 Results: {output_file}")
            
            return 0
        else:
            print(f"\n Data extraction failed")
            return 1
            
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())