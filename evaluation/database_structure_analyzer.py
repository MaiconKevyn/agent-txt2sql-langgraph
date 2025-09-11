#!/usr/bin/env python3
"""
Database Structure Analyzer
Comprehensive analysis of PostgreSQL database structure for documentation purposes.
"""

import psycopg2
import json
from typing import Dict, List, Any, Tuple
import sys

class DatabaseStructureAnalyzer:
    def __init__(self, connection_string: str):
        """Initialize with database connection string."""
        self.connection_string = connection_string
        self.conn = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            print("✓ Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to database: {e}")
            return False
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    def get_all_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name;
            """)
            return [row[0] for row in cur.fetchall()]
    
    def get_table_structure(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive structure information for a table."""
        structure = {
            'table_name': table_name,
            'columns': [],
            'primary_keys': [],
            'foreign_keys': [],
            'indexes': [],
            'constraints': [],
            'row_count': 0,
            'sample_data': []
        }
        
        # Get column information
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position;
            """, (table_name,))
            
            for row in cur.fetchall():
                column_info = {
                    'name': row[0],
                    'data_type': row[1],
                    'nullable': row[2] == 'YES',
                    'default': row[3],
                    'max_length': row[4],
                    'precision': row[5],
                    'scale': row[6]
                }
                structure['columns'].append(column_info)
        
        # Get primary keys
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = %s 
                    AND tc.constraint_type = 'PRIMARY KEY';
            """, (table_name,))
            structure['primary_keys'] = [row[0] for row in cur.fetchall()]
        
        # Get foreign keys
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_name = %s;
            """, (table_name,))
            
            for row in cur.fetchall():
                fk_info = {
                    'column': row[0],
                    'references_table': row[1],
                    'references_column': row[2]
                }
                structure['foreign_keys'].append(fk_info)
        
        # Get indexes
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    i.relname AS index_name,
                    a.attname AS column_name,
                    ix.indisunique AS is_unique
                FROM pg_class t,
                     pg_class i,
                     pg_index ix,
                     pg_attribute a
                WHERE t.oid = ix.indrelid
                    AND i.oid = ix.indexrelid
                    AND a.attrelid = t.oid
                    AND a.attnum = ANY(ix.indkey)
                    AND t.relkind = 'r'
                    AND t.relname = %s
                ORDER BY i.relname, a.attname;
            """, (table_name,))
            
            indexes = {}
            for row in cur.fetchall():
                index_name = row[0]
                if index_name not in indexes:
                    indexes[index_name] = {
                        'name': index_name,
                        'columns': [],
                        'is_unique': row[2]
                    }
                indexes[index_name]['columns'].append(row[1])
            
            structure['indexes'] = list(indexes.values())
        
        # Get row count
        with self.conn.cursor() as cur:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                structure['row_count'] = cur.fetchone()[0]
            except Exception as e:
                print(f"Warning: Could not get row count for {table_name}: {e}")
                structure['row_count'] = 0
        
        # Get sample data (first 3 rows)
        if structure['row_count'] > 0:
            with self.conn.cursor() as cur:
                try:
                    cur.execute(f"SELECT * FROM {table_name} LIMIT 3;")
                    rows = cur.fetchall()
                    column_names = [col['name'] for col in structure['columns']]
                    
                    for row in rows:
                        row_data = {}
                        for i, value in enumerate(row):
                            if i < len(column_names):
                                # Convert non-serializable types to string
                                if value is not None:
                                    try:
                                        json.dumps(value)  # Test if serializable
                                        row_data[column_names[i]] = value
                                    except (TypeError, ValueError):
                                        row_data[column_names[i]] = str(value)
                                else:
                                    row_data[column_names[i]] = None
                        structure['sample_data'].append(row_data)
                except Exception as e:
                    print(f"Warning: Could not get sample data for {table_name}: {e}")
        
        return structure
    
    def analyze_all_tables(self, target_tables: List[str] = None) -> Dict[str, Any]:
        """Analyze all tables or specific target tables."""
        if not self.connect():
            return {}
        
        try:
            all_tables = self.get_all_tables()
            print(f"Found {len(all_tables)} tables in database")
            
            if target_tables:
                # Filter to only target tables that exist
                tables_to_analyze = [t for t in target_tables if t in all_tables]
                missing_tables = [t for t in target_tables if t not in all_tables]
                if missing_tables:
                    print(f"Warning: Tables not found: {missing_tables}")
            else:
                tables_to_analyze = all_tables
            
            analysis = {
                'database_summary': {
                    'total_tables': len(all_tables),
                    'analyzed_tables': len(tables_to_analyze),
                    'tables_list': all_tables
                },
                'tables': {}
            }
            
            for table_name in tables_to_analyze:
                print(f"Analyzing table: {table_name}")
                try:
                    table_structure = self.get_table_structure(table_name)
                    analysis['tables'][table_name] = table_structure
                    print(f"  ✓ {table_name}: {len(table_structure['columns'])} columns, {table_structure['row_count']} rows")
                except Exception as e:
                    print(f"  ✗ Error analyzing {table_name}: {e}")
                    analysis['tables'][table_name] = {'error': str(e)}
            
            return analysis
            
        finally:
            self.close()

def main():
    """Main function to run the database analysis."""
    import os
    connection_string = (
        os.getenv("DATABASE_URL")
        or os.getenv("DATABASE_PATH")
        or "postgresql://postgres@localhost:5432/sih_rs"
    )
    # Normalize SQLAlchemy style URL for psycopg2 if needed
    if connection_string.startswith('postgresql+psycopg2://'):
        connection_string = connection_string.replace('postgresql+psycopg2://', 'postgresql://', 1)
    
    # Target tables as specified
    target_tables = [
        'internacoes', 'mortes', 'procedimentos', 'cid10', 'hospital', 
        'municipios', 'dado_ibge', 'uti_detalhes', 'condicoes_especificas', 
        'obstetricos', 'instrucao', 'vincprev', 'cbor', 'infehosp', 
        'diagnosticos_secundarios'
    ]
    
    analyzer = DatabaseStructureAnalyzer(connection_string)
    
    print("Starting comprehensive database structure analysis...")
    print("=" * 60)
    
    analysis = analyzer.analyze_all_tables(target_tables)
    
    if analysis:
        # Save to JSON file
        output_file = '/home/maiconkevyn/PycharmProjects/txt2sql_claude_s/evaluation/database_structure_analysis.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False, default=str)
        
        print("=" * 60)
        print(f"Analysis complete! Results saved to: {output_file}")
        
        # Print summary
        print("\nDATABASE ANALYSIS SUMMARY:")
        print("-" * 30)
        print(f"Total tables in database: {analysis['database_summary']['total_tables']}")
        print(f"Tables analyzed: {analysis['database_summary']['analyzed_tables']}")
        
        for table_name, table_info in analysis['tables'].items():
            if 'error' not in table_info:
                print(f"\n{table_name.upper()}:")
                print(f"  Columns: {len(table_info['columns'])}")
                print(f"  Rows: {table_info['row_count']:,}")
                print(f"  Primary Keys: {', '.join(table_info['primary_keys']) if table_info['primary_keys'] else 'None'}")
                print(f"  Foreign Keys: {len(table_info['foreign_keys'])}")
                print(f"  Indexes: {len(table_info['indexes'])}")
        
        return True
    else:
        print("Analysis failed!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
