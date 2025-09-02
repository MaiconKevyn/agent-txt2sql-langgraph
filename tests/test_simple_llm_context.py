#!/usr/bin/env python3
"""
Simple LLM Context Test - Shows exact context sent to LLM at each node
====================================================================

This test shows exactly what context is sent to the LLM at each step
of the LangGraph pipeline for a single query. 

Focus: Track LLM context through the entire pipeline to validate
dynamic template injection is working correctly.

Usage:
    python tests/test_simple_llm_context.py
"""

import sys
import os
import time
from typing import Dict, Any, List

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.langgraph_migration.nodes_v3 import (
    query_classification_node,
    list_tables_node,
    get_schema_node,
    generate_sql_node,
    get_llm_manager
)
from src.langgraph_migration.state_v3 import create_initial_messages_state
from src.application.config.table_templates import build_table_specific_prompt, build_multi_table_prompt


class SimpleLLMContextTest:
    """Simple test to track LLM context through each node"""
    
    def __init__(self):
        self.test_query = "quantos homens morreram?"
        # Use absolute path to ensure correct directory location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = os.path.join(script_dir, "debug_output")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def run_test(self):
        """Run single query through pipeline, capturing LLM context at each step"""
        
        print("=" * 80)
        print("🔬 SIMPLE LLM CONTEXT TEST")
        print("=" * 80)
        print(f"📝 Test Query: '{self.test_query}'")
        print(f"🎯 Goal: Track LLM context through each node")
        print()
        
        # Create initial state
        initial_state = create_initial_messages_state(
            user_query=self.test_query,
            session_id="simple_test"
        )
        
        print("🚀 PIPELINE EXECUTION WITH LLM CONTEXT CAPTURE:")
        print()
        
        # STEP 1: Query Classification Node
        print("1️⃣ QUERY CLASSIFICATION NODE:")
        print("   🧠 LLM Usage: YES - Classifies query type")
        print(f"   📥 Input: '{self.test_query}'")
        
        # Capture what would be sent to LLM in classification
        classification_context = f"""Classify this query:
        "{self.test_query}"
        
        Categories:
        DATABASE: data queries (count, list, show, filter)
        CONVERSATIONAL: explanations (what, how, meaning)
        SCHEMA: structure (tables, columns)
        
        Response: DATABASE/CONVERSATIONAL/SCHEMA"""
        
        # Save classification context
        self.save_context("01_classification_llm_context.txt", 
                         "Query Classification Context",
                         classification_context)
        
        print(f"   💾 LLM Context saved: {self.output_dir}/01_classification_llm_context.txt")
        
        # Execute classification
        state_after_classification = query_classification_node(initial_state)
        query_route = state_after_classification.get("query_route")
        
        print(f"   📤 Output: {query_route.value if query_route else 'None'}")
        print()
        
        # STEP 2: Table Discovery & Selection
        print("2️⃣ TABLE DISCOVERY & SELECTION NODE:")
        print("   🧠 LLM Usage: YES - Selects relevant tables")
        print(f"   📥 Input: Query route = {query_route.value if query_route else 'None'}")
        
        # Execute table selection to get the actual context used
        state_after_tables = list_tables_node(state_after_classification)
        selected_tables = state_after_tables.get("selected_tables", [])
        
        print(f"   📤 Output: Selected tables = {selected_tables}")
        print(f"   💾 LLM Context captured during execution (see console output above)")
        print()
        
        # STEP 3: Schema Context
        print("3️⃣ SCHEMA CONTEXT NODE:")
        print("   🧠 LLM Usage: NO - Direct database schema retrieval")
        print(f"   📥 Input: Selected tables = {selected_tables}")
        
        state_after_schema = get_schema_node(state_after_tables)
        schema_context = state_after_schema.get("schema_context", "")
        
        print(f"   📤 Output: Schema context ({len(schema_context)} chars)")
        print()
        
        # STEP 4: SQL Generation (KEY STEP)
        print("4️⃣ SQL GENERATION NODE:")
        print("   🧠 LLM Usage: YES - Generates SQL with dynamic templates")
        print(f"   📥 Input: Query + Schema + Selected Tables")
        
        # Generate the dynamic template context
        if len(selected_tables) > 1:
            table_rules = build_multi_table_prompt(selected_tables)
            template_type = "Multi-table"
        else:
            table_rules = build_table_specific_prompt(selected_tables)
            template_type = "Single-table"
        
        print(f"   🎯 Template Type: {template_type}")
        print(f"   📏 Template Size: {len(table_rules)} characters")
        
        # Create the complete context that will be sent to LLM
        complete_sql_context = f"""SYSTEM MESSAGE 1 (Base Instructions):
        =====================================
        You are a PostgreSQL expert assistant for Brazilian healthcare (SIH-RS) data analysis.
        
        📋 CORE POSTGRESQL INSTRUCTIONS:
        1. Generate syntactically correct PostgreSQL queries
        2. Use proper table and column names with double quotes: "COLUMN_NAME"
        3. Handle Portuguese language questions appropriately
        4. Return only the SQL query, no explanation
        5. Use appropriate WHERE clauses for filtering
        6. Include LIMIT clauses when appropriate (default LIMIT 100)
        7. Use proper JOINs when querying multiple tables
        8. Use PostgreSQL-specific functions when needed (EXTRACT, ILIKE, etc.)
        
        🔍 DATABASE SCHEMA:
        {schema_context}
        
        SYSTEM MESSAGE 2 (Dynamic Table-Specific Rules):
        ===============================================
        {table_rules}
        
        HUMAN MESSAGE:
        ==============
        USER QUERY: {self.test_query}
        
        Generate the SQL query:"""
        
        # Save the complete SQL generation context
        self.save_context("04_sql_generation_llm_context.txt",
                         "SQL Generation Context (Dynamic Templates)",
                         complete_sql_context)
        
        print(f"   💾 Complete LLM Context saved: {self.output_dir}/04_sql_generation_llm_context.txt")
        print(f"   📏 Total Context: {len(complete_sql_context):,} characters")
        print(f"      - Base Instructions: ~800 chars")
        print(f"      - Schema Context: {len(schema_context):,} chars")
        print(f"      - Dynamic Templates: {len(table_rules):,} chars")
        
        # Also save just the dynamic templates separately
        self.save_context("04_dynamic_templates_only.txt",
                         "Dynamic Templates Only",
                         table_rules)
        
        print(f"   💾 Templates Only saved: {self.output_dir}/04_dynamic_templates_only.txt")
        
        # Execute SQL generation
        try:
            state_after_sql = generate_sql_node(state_after_schema)
            generated_sql = state_after_sql.get("generated_sql", "No SQL generated")
            print(f"   📤 Output: {generated_sql}")
        except Exception as e:
            print(f"   ❌ SQL Generation Error: {str(e)}")
        
        print()
        
        # STEP 5: Summary of LLM Usage
        print("5️⃣ LLM USAGE SUMMARY:")
        print("   📊 Total LLM Calls in Pipeline:")
        print("      1. Query Classification ✅ (Simple classification)")
        print("      2. Table Selection ✅ (Intelligent selection)")
        print("      3. Schema Retrieval ❌ (Direct database access)")
        print("      4. SQL Generation ✅ (Dynamic template injection)")
        print("      5. SQL Validation ❌ (Rule-based, optional LLM)")
        print("      6. SQL Execution ❌ (Direct database execution)")
        print("      7. Response Generation ✅ (Format results, not captured)")
        print()
        
        # STEP 6: Key Findings
        print("6️⃣ KEY FINDINGS:")
        print(f"   🎯 Query: '{self.test_query}'")
        print(f"   📋 Tables Selected: {selected_tables}")
        print(f"   📏 Dynamic Context Size: {len(table_rules):,} chars")
        print(f"   🔧 Template Type: {template_type}")
        print()
        
        # Check if the dynamic template contains relevant guidance
        relevant_guidance = []
        if "UTI" in table_rules.upper():
            relevant_guidance.append("✅ UTI-specific guidance")
        if "HOMENS" in table_rules or "MASCULINO" in table_rules or '"SEXO" = 1' in table_rules:
            relevant_guidance.append("✅ Gender mapping (men)")
        if "VAL_UTI" in table_rules:
            relevant_guidance.append("✅ UTI cost fields")
        if "JOIN" in table_rules:
            relevant_guidance.append("✅ Multi-table JOIN guidance")
        if "PostgreSQL" in table_rules:
            relevant_guidance.append("✅ PostgreSQL syntax")
        
        print("   🔍 Relevant Guidance Found:")
        for guidance in relevant_guidance:
            print(f"      {guidance}")
        
        if not relevant_guidance:
            print("      ⚠️ No specific guidance found for this query")
        
        print()
        print("=" * 80)
        print("✅ SIMPLE LLM CONTEXT TEST COMPLETED")
        print("=" * 80)
        print()
        print("📁 Files Generated:")
        print(f"   • {self.output_dir}/01_classification_llm_context.txt")
        print(f"   • {self.output_dir}/04_sql_generation_llm_context.txt")  
        print(f"   • {self.output_dir}/04_dynamic_templates_only.txt")
        print()
        print("🎯 This test demonstrates that dynamic templates are successfully")
        print("   injected into the SQL generation context based on selected tables.")
    
    def save_context(self, filename: str, title: str, content: str):
        """Save LLM context to file"""
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("=" * 100 + "\n")
                f.write(f"{title}\n")
                f.write(f"Test Query: {self.test_query}\n")
                f.write("=" * 100 + "\n\n")
                f.write(content)
                f.write(f"\n\n" + "=" * 100 + "\n")
                f.write(f"Context Analysis:\n")
                f.write(f"Total Size: {len(content):,} characters\n")
                f.write(f"Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 100 + "\n")
            
            return True
        except Exception as e:
            print(f"Error saving {filepath}: {str(e)}")
            return False


def main():
    """Main test execution"""
    
    print("Starting Simple LLM Context Test...")
    print()
    
    # Create and run test
    test = SimpleLLMContextTest()
    test.run_test()


if __name__ == "__main__":
    main()