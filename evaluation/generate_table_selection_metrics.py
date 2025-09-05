#!/usr/bin/env python3
"""
Table Selection Metrics Generator for Text2SQL Agent Evaluation

Analyzes the accuracy of table selection by comparing agent selected tables
with ground truth tables. Considers selection correct if all ground truth
tables are present in agent selected tables.

Usage:
    python evaluation/generate_table_selection_metrics.py
    
Output:
    results/table_selection_metrics.json
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Set
from collections import defaultdict


class TableSelectionMetricsGenerator:
    """Generator for table selection accuracy metrics"""
    
    def __init__(self, agent_results_path: str, output_dir: str = "results"):
        """
        Initialize metrics generator
        
        Args:
            agent_results_path: Path to agent extraction results JSON file
            output_dir: Directory to save metrics results
        """
        self.agent_results_path = Path(agent_results_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Results tracking
        self.detailed_results = []
        self.overall_metrics = {
            "correct_selections": 0,
            "incorrect_selections": 0,
            "total_cases": 0
        }
        self.difficulty_metrics = defaultdict(lambda: {
            "correct": 0,
            "total": 0,
            "accuracy": 0.0
        })
        
    def load_agent_results(self) -> List[Dict[str, Any]]:
        """Load agent extraction results"""
        if not self.agent_results_path.exists():
            raise FileNotFoundError(f"Agent results file not found: {self.agent_results_path}")
            
        with open(self.agent_results_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        return data.get("results", [])
    
    def compare_table_selections(
        self, 
        ground_truth_tables: List[str], 
        agent_selected_tables: List[str]
    ) -> Dict[str, Any]:
        """
        Compare ground truth tables with agent selected tables
        
        Args:
            ground_truth_tables: List of correct tables from ground truth
            agent_selected_tables: List of tables selected by agent
            
        Returns:
            Dictionary with comparison results
        """
        # Convert to sets for easier comparison
        gt_set = set(ground_truth_tables)
        agent_set = set(agent_selected_tables)
        
        # Check if all ground truth tables are present in agent selection
        is_correct = gt_set.issubset(agent_set)
        
        # Find missing tables (in GT but not in agent selection)
        missing_tables = list(gt_set - agent_set)
        
        # Find extra tables (in agent selection but not in GT)
        extra_tables = list(agent_set - gt_set)
        
        # Check if agent selected extra tables
        has_extra_tables = len(extra_tables) > 0
        
        return {
            "is_correct": is_correct,
            "has_extra_tables": has_extra_tables,
            "missing_tables": missing_tables,
            "extra_tables": extra_tables,
            "ground_truth_count": len(ground_truth_tables),
            "agent_selected_count": len(agent_selected_tables)
        }
    
    def analyze_single_case(self, test_case: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze table selection for a single test case
        
        Args:
            test_case: Single test case from agent results
            
        Returns:
            Detailed analysis result
        """
        test_id = test_case["id"]
        difficulty = test_case["difficulty"]
        ground_truth_tables = test_case.get("tables", [])
        agent_selected_tables = test_case.get("agent_selected_tables", [])
        
        # Compare table selections
        comparison = self.compare_table_selections(
            ground_truth_tables, 
            agent_selected_tables
        )
        
        # Create detailed result
        detailed_result = {
            "id": test_id,
            "difficulty": difficulty,
            "question": test_case["question"],
            "ground_truth_tables": ground_truth_tables,
            "agent_selected_tables": agent_selected_tables,
            **comparison
        }
        
        return detailed_result
    
    def calculate_overall_metrics(self) -> Dict[str, Any]:
        """Calculate overall table selection metrics"""
        correct_count = sum(1 for result in self.detailed_results if result["is_correct"])
        total_count = len(self.detailed_results)
        incorrect_count = total_count - correct_count
        
        accuracy = (correct_count / total_count * 100) if total_count > 0 else 0.0
        
        # Count cases with extra tables
        extra_tables_count = sum(1 for result in self.detailed_results if result["has_extra_tables"])
        
        # Count cases with missing tables  
        missing_tables_count = sum(1 for result in self.detailed_results if result["missing_tables"])
        
        return {
            "correct_selections": correct_count,
            "incorrect_selections": incorrect_count,
            "total_cases": total_count,
            "accuracy_percentage": round(accuracy, 2),
            "cases_with_extra_tables": extra_tables_count,
            "cases_with_missing_tables": missing_tables_count,
            "perfect_matches": sum(1 for result in self.detailed_results 
                                 if result["is_correct"] and not result["has_extra_tables"])
        }
    
    def calculate_difficulty_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Calculate metrics by difficulty level"""
        difficulty_stats = defaultdict(lambda: {"correct": 0, "total": 0})
        
        # Group by difficulty
        for result in self.detailed_results:
            difficulty = result["difficulty"]
            difficulty_stats[difficulty]["total"] += 1
            if result["is_correct"]:
                difficulty_stats[difficulty]["correct"] += 1
        
        # Calculate accuracy for each difficulty
        metrics = {}
        for difficulty, stats in difficulty_stats.items():
            accuracy = (stats["correct"] / stats["total"] * 100) if stats["total"] > 0 else 0.0
            metrics[difficulty] = {
                "correct": stats["correct"],
                "total": stats["total"],
                "accuracy": round(accuracy, 2)
            }
        
        return dict(metrics)
    
    def generate_summary_statistics(self) -> Dict[str, Any]:
        """Generate additional summary statistics"""
        if not self.detailed_results:
            return {}
        
        # Table count statistics
        gt_table_counts = [len(result["ground_truth_tables"]) for result in self.detailed_results]
        agent_table_counts = [len(result["agent_selected_tables"]) for result in self.detailed_results]
        
        # Most commonly missed/extra tables
        all_missing = []
        all_extra = []
        for result in self.detailed_results:
            all_missing.extend(result["missing_tables"])
            all_extra.extend(result["extra_tables"])
        
        from collections import Counter
        missing_counter = Counter(all_missing)
        extra_counter = Counter(all_extra)
        
        return {
            "ground_truth_table_stats": {
                "min_tables": min(gt_table_counts),
                "max_tables": max(gt_table_counts),
                "avg_tables": round(sum(gt_table_counts) / len(gt_table_counts), 2)
            },
            "agent_selected_table_stats": {
                "min_tables": min(agent_table_counts),
                "max_tables": max(agent_table_counts), 
                "avg_tables": round(sum(agent_table_counts) / len(agent_table_counts), 2)
            },
            "most_commonly_missed_tables": dict(missing_counter.most_common(5)),
            "most_commonly_extra_tables": dict(extra_counter.most_common(5))
        }
    
    def generate_metrics(self) -> Dict[str, Any]:
        """
        Generate comprehensive table selection metrics
        
        Returns:
            Complete metrics report
        """
        print(" Generating Table Selection Metrics")
        print("=" * 50)
        
        # Load agent results
        try:
            agent_results = self.load_agent_results()
            print(f" Loaded {len(agent_results)} test cases")
        except Exception as e:
            print(f" Failed to load agent results: {e}")
            return {}
        
        start_time = time.time()
        
        # Analyze each test case
        for i, test_case in enumerate(agent_results, 1):
            print(f" Analyzing {test_case['id']} ({i}/{len(agent_results)})")
            
            detailed_result = self.analyze_single_case(test_case)
            self.detailed_results.append(detailed_result)
            
            # Show result
            if detailed_result["is_correct"]:
                extra_info = f" (+{len(detailed_result['extra_tables'])} extra)" if detailed_result["has_extra_tables"] else ""
                print(f"   Correct{extra_info}")
            else:
                missing_info = f" (-{len(detailed_result['missing_tables'])} missing)" if detailed_result["missing_tables"] else ""
                print(f"   Incorrect{missing_info}")
        
        generation_time = time.time() - start_time
        
        # Calculate all metrics
        overall_metrics = self.calculate_overall_metrics()
        difficulty_metrics = self.calculate_difficulty_metrics()
        summary_stats = self.generate_summary_statistics()
        
        # Create final metrics report
        metrics_report = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "evaluation_type": "table_selection_accuracy",
                "source_file": str(self.agent_results_path.name),
                "generation_time": round(generation_time, 2)
            },
            "overall_metrics": overall_metrics,
            "by_difficulty": difficulty_metrics,
            "summary_statistics": summary_stats,
            "detailed_results": self.detailed_results
        }
        
        # Print summary
        print(f"\n{'=' * 50}")
        print(" TABLE SELECTION METRICS SUMMARY")
        print(f"{'=' * 50}")
        print(f"Total Cases: {overall_metrics['total_cases']}")
        print(f"Correct Selections: {overall_metrics['correct_selections']}")
        print(f"Incorrect Selections: {overall_metrics['incorrect_selections']}")
        print(f"Overall Accuracy: {overall_metrics['accuracy_percentage']}%")
        print(f"Perfect Matches: {overall_metrics['perfect_matches']}")
        print(f"Cases with Extra Tables: {overall_metrics['cases_with_extra_tables']}")
        print(f"Cases with Missing Tables: {overall_metrics['cases_with_missing_tables']}")
        
        print(f"\nAccuracy by Difficulty:")
        for difficulty, metrics in difficulty_metrics.items():
            print(f"  {difficulty.upper()}: {metrics['correct']}/{metrics['total']} ({metrics['accuracy']}%)")
        
        return metrics_report
    
    def save_metrics(self, metrics: Dict[str, Any], filename: str = "table_selection_metrics.json") -> str:
        """
        Save metrics report to JSON file
        
        Args:
            metrics: Metrics report to save
            filename: Output filename
            
        Returns:
            Path to saved file
        """
        output_path = self.output_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Metrics saved to: {output_path}")
        return str(output_path)


def main():
    """Main execution function"""
    
    # Configuration
    current_dir = Path(__file__).parent
    agent_results_path = current_dir / "results" / "agent_extraction_results.json"
    output_dir = current_dir / "results"
    
    # Check if agent results exist
    if not agent_results_path.exists():
        print(f" Agent results file not found: {agent_results_path}")
        print("Available files in results directory:")
        results_dir = current_dir / "results"
        if results_dir.exists():
            for file in results_dir.glob("*.json"):
                print(f"  - {file.name}")
        return 1
    
    try:
        # Create and run metrics generator
        generator = TableSelectionMetricsGenerator(
            agent_results_path=agent_results_path,
            output_dir=output_dir
        )
        
        # Generate metrics
        metrics = generator.generate_metrics()
        
        if metrics:
            # Save metrics
            output_file = generator.save_metrics(metrics)
            
            print(f"\n Table selection metrics generated successfully!")
            print(f"📁 Results: {output_file}")
            
            return 0
        else:
            print(f"\n Metrics generation failed")
            return 1
            
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())