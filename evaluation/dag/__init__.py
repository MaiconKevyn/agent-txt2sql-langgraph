"""
DAG-based Evaluation Pipeline

This module provides a lightweight DAG (Directed Acyclic Graph) implementation
for organizing and visualizing the evaluation pipeline using NetworkX.
"""

from .base import EvaluationDAG, TaskResult
from .pipeline import create_evaluation_pipeline

__all__ = ['EvaluationDAG', 'TaskResult', 'create_evaluation_pipeline']
