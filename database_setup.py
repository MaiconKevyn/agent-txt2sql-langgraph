import pandas as pd
import sqlite3
from pathlib import Path
from typing import Optional, List, Tuple

def create_cid_capitulos_table(conn: sqlite3.Connection) -> int:
    """Create CID chapters table and import data from CSV"""
    
    # Read CID chapters CSV
    cid_csv_path = Path("data/CID-10-CAPITULOS_clean.csv")
    
    if not cid_csv_path.exists():
        print(f"Warning: CID CSV file not found at {cid_csv_path}")
        return 0
    
    # Read CSV with proper encoding
    df_cid = pd.read_csv(cid_csv_path, encoding='utf-8', delimiter=';')
    
    # Clean column names (remove any BOM or extra spaces)
    df_cid.columns = df_cid.columns.str.strip().str.replace('\ufeff', '')
    
    # Create CID chapters table with proper schema
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cid_capitulos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_capitulo INTEGER NOT NULL,
            codigo_inicio TEXT NOT NULL,
            codigo_fim TEXT NOT NULL,
            descricao TEXT NOT NULL,
            descricao_abrev TEXT,
            categoria_geral TEXT,
            UNIQUE(numero_capitulo, codigo_inicio, codigo_fim)
        )
    """)
    
    # Insert data from DataFrame
    records_inserted = 0
    for _, row in df_cid.iterrows():
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO cid_capitulos 
                (numero_capitulo, codigo_inicio, codigo_fim, descricao, descricao_abrev, categoria_geral)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                int(row['NUMCAP']),
                row['CATINIC'].strip(),
                row['CATFIM'].strip(), 
                row['DESCRICAO'].strip(),
                row.get('DESCRABREV', '').strip() if pd.notna(row.get('DESCRABREV')) else '',
                row.get('CID_GERAL', '').strip() if pd.notna(row.get('CID_GERAL')) else ''
            ))
            records_inserted += 1
        except Exception as e:
            print(f"Error inserting CID record {row.get('NUMCAP', 'unknown')}: {e}")
    
    conn.commit()
    print(f"CID chapters table created with {records_inserted} records")
    return records_inserted

def convert_date_columns(df):
    """Convert INTEGER date columns (YYYYMMDD) to proper DATE format"""
    
    def convert_integer_to_date(date_int):
        """Convert YYYYMMDD integer to YYYY-MM-DD string"""
        if pd.isna(date_int):
            return None
        try:
            date_str = str(int(date_int)).zfill(8)
            if len(date_str) != 8:
                return None
            year = date_str[:4]
            month = date_str[4:6] 
            day = date_str[6:8]
            # Validate date
            pd.to_datetime(f"{year}-{month}-{day}")
            return f"{year}-{month}-{day}"
        except (ValueError, TypeError):
            return None
    
    # Convert date columns if they exist
    if 'DT_INTER' in df.columns:
        df['dt_inter_date'] = df['DT_INTER'].apply(convert_integer_to_date)
        print(f"Converted DT_INTER: {df['dt_inter_date'].notna().sum()} valid dates")
    
    if 'DT_SAIDA' in df.columns:
        df['dt_saida_date'] = df['DT_SAIDA'].apply(convert_integer_to_date)
        print(f"Converted DT_SAIDA: {df['dt_saida_date'].notna().sum()} valid dates")
    
    return df

def create_database_from_csv():
    """Create SQLite database with SUS data and CID chapters"""
    
    # Read SUS CSV file
    csv_path = Path("data/dados_sus3.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"SUS data file not found at {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Convert date columns to proper DATE format
    df = convert_date_columns(df)
    
    # Connect to SQLite database
    db_path = "sus_database.db"
    conn = sqlite3.connect(db_path)
    
    try:
        # Create SUS data table with explicit column types
        cursor = conn.cursor()
        
        # Create table with proper DATE columns
        columns_sql = []
        for col in df.columns:
            if col in ['dt_inter_date', 'dt_saida_date']:
                columns_sql.append(f"{col} DATE")
            elif df[col].dtype in ['int64', 'int32']:
                columns_sql.append(f"{col} INTEGER")
            elif df[col].dtype in ['float64', 'float32']:
                columns_sql.append(f"{col} REAL")
            else:
                columns_sql.append(f"{col} TEXT")
        
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS sus_data (
                {', '.join(columns_sql)}
            )
        """)
        
        # Insert data using pandas
        df.to_sql('sus_data', conn, if_exists='replace', index=False)
        print(f"SUS data table created with {len(df)} records")
        
        # Create CID chapters table
        cid_records = create_cid_capitulos_table(conn)
        
        # Get table info for both tables
        tables = ['sus_data', 'cid_capitulos']
        for table in tables:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            record_count = cursor.fetchone()[0]
            
            print(f"\nTable '{table}' ({record_count} records):")
            print("Columns:")
            for col in columns:
                print(f"  - {col[1]} ({col[2]})")
        
        # Create helpful view for CID mapping
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS sus_data_with_cid AS
            SELECT 
                s.*,
                c.descricao as capitulo_cid,
                c.descricao_abrev as capitulo_abrev,
                c.numero_capitulo
            FROM sus_data s
            LEFT JOIN cid_capitulos c ON 
                s.DIAG_PRINC >= c.codigo_inicio AND 
                s.DIAG_PRINC <= c.codigo_fim
        """)
        
        print(f"\nDatabase created successfully at {db_path}")
        print(f"Created view 'sus_data_with_cid' for easy CID mapping")
        
    except Exception as e:
        print(f"Error creating database: {e}")
        raise
    finally:
        conn.close()
    
    return db_path

def get_database_info(db_path: str = "sus_database.db") -> dict:
    """Get comprehensive database information"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    info = {
        'tables': {},
        'views': {},
        'total_records': 0
    }
    
    try:
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for (table_name,) in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            info['tables'][table_name] = count
            info['total_records'] += count
        
        # Get all views  
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
        views = cursor.fetchall()
        
        for (view_name,) in views:
            info['views'][view_name] = 'view'
            
    except Exception as e:
        print(f"Error getting database info: {e}")
    finally:
        conn.close()
    
    return info

if __name__ == "__main__":
    create_database_from_csv()