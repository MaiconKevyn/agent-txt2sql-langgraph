#!/usr/bin/env python3
"""
Manual SQL Validator - Ferramenta para executar queries SQL manualmente
de forma iterativa

Autor: Claude Code Assistant
Data: 2025-07-10
"""

import sqlite3
import pandas as pd
import time
from datetime import datetime
from typing import Tuple
import argparse

class ManualSQLValidator:
    def __init__(self, database_path: str = "sus_database.db"):
        """
        Inicializa o validador SQL manual
        
        Args:
            database_path: Caminho para o banco de dados SQLite
        """
        self.database_path = database_path
        self.connection = None
        
    def connect_database(self) -> bool:
        """Conecta ao banco de dados"""
        try:
            self.connection = sqlite3.connect(self.database_path)
            print(f"✅ Conectado ao banco: {self.database_path}")
            return True
        except Exception as e:
            print(f"❌ Erro ao conectar ao banco: {e}")
            return False
    
    def disconnect_database(self):
        """Desconecta do banco de dados"""
        if self.connection:
            self.connection.close()
            print("🔌 Desconectado do banco de dados")
    
    def execute_manual_query(self, sql_query: str) -> Tuple[bool, pd.DataFrame, str]:
        """
        Executa uma query SQL manualmente
        
        Args:
            sql_query: Query SQL para executar
            
        Returns:
            Tuple com (sucesso, resultado_df, mensagem_erro)
        """
        if not self.connection:
            return False, pd.DataFrame(), "❌ Não conectado ao banco de dados"
        
        try:
            start_time = time.time()
            
            # Executar query
            df = pd.read_sql_query(sql_query, self.connection)
            
            execution_time = time.time() - start_time
            
            print(f"✅ Query executada com sucesso!")
            print(f"⏱️ Tempo de execução: {execution_time:.3f}s")
            print(f"📊 Registros retornados: {len(df)}")
            
            return True, df, f"Executada em {execution_time:.3f}s"
            
        except Exception as e:
            error_msg = f"❌ Erro na execução: {str(e)}"
            print(error_msg)
            return False, pd.DataFrame(), error_msg
    
    
    
    
    
    def display_results(self, df: pd.DataFrame, title: str = "Resultados"):
        """Exibe resultados de forma formatada"""
        print(f"\n{'='*50}")
        print(f"📊 {title}")
        print(f"{'='*50}")
        
        if df.empty:
            print("🔍 Nenhum resultado encontrado")
            return
        
        print(f"📈 Total de registros: {len(df)}")
        print(f"📋 Colunas: {list(df.columns)}")
        print("\n📊 Dados:")
        
        # Mostrar até 20 primeiras linhas
        display_df = df.head(20)
        
        # Formatação melhorada
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        
        print(display_df.to_string(index=False))
        
        if len(df) > 20:
            print(f"\n... e mais {len(df) - 20} registros")
    
    
    def interactive_mode(self):
        """Modo interativo para execução de queries SQL"""
        print("🚀 Iniciando Validador SQL Manual - Modo Interativo")
        print("=" * 60)
        
        if not self.connect_database():
            return
        
        try:
            while True:
                print("\n" + "="*60)
                print("📋 OPÇÕES:")
                print("1. Executar query SQL manual")
                print("2. Mostrar estrutura do banco")
                print("3. Sair")
                print("="*60)
                
                choice = input("🎯 Escolha uma opção (1-3): ").strip()
                
                if choice == "1":
                    self.manual_query_mode()
                elif choice == "2":
                    self.show_database_structure()
                elif choice == "3":
                    print("👋 Saindo...")
                    break
                else:
                    print("❌ Opção inválida!")
                
        except KeyboardInterrupt:
            print("\n\n👋 Saindo...")
        finally:
            self.disconnect_database()
    
    def manual_query_mode(self):
        """Modo para executar queries SQL manuais de forma iterativa"""
        print("\n🔧 MODO: Query SQL Manual Iterativo")
        print("-" * 40)
        
        print("💡 Dica: Digite suas queries SQL. Digite 'sair' para voltar ao menu.")
        print("💡 Exemplos:")
        print("   SELECT COUNT(*) FROM sus_data;")
        print("   SELECT MUNIC_RES, COUNT(*) FROM sus_data GROUP BY MUNIC_RES LIMIT 5;")
        print("   SELECT * FROM sus_data WHERE IDADE > 65 LIMIT 10;")
        
        query_count = 0
        
        while True:
            query_count += 1
            print(f"\n{'='*20} Query #{query_count} {'='*20}")
            
            sql_query = input("🔍 Digite sua query SQL (ou 'sair'): ").strip()
            
            if not sql_query:
                print("❌ Query vazia! Tente novamente.")
                query_count -= 1
                continue
            
            if sql_query.lower() in ['sair', 'exit', 'quit']:
                print("👋 Voltando ao menu principal...")
                break
            
            success, df, message = self.execute_manual_query(sql_query)
            
            if success:
                self.display_results(df, f"Resultados da Query #{query_count}")
                
                # Perguntar se quer salvar
                save = input("\n💾 Salvar resultados em CSV? (s/n): ").strip().lower()
                if save == 's':
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"manual_query_results_q{query_count}_{timestamp}.csv"
                    df.to_csv(filename, index=False)
                    print(f"💾 Resultados salvos em: {filename}")
            else:
                print(f"❌ Falha na execução: {message}")
                query_count -= 1  # Não contar queries que falharam
            
            # Perguntar se quer continuar
            continue_queries = input("\n➡️ Executar outra query? (s/n): ").strip().lower()
            if continue_queries != 's':
                print("👋 Voltando ao menu principal...")
                break
    
    
    
    
    def show_database_structure(self):
        """Mostra estrutura do banco de dados"""
        print("\n🗄️ ESTRUTURA DO BANCO DE DADOS")
        print("-" * 40)
        
        if not self.connection:
            print("❌ Não conectado ao banco de dados!")
            return
        
        cursor = self.connection.cursor()
        
        # Listar tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("📋 Tabelas:")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Detalhes de cada tabela
        for table in tables:
            table_name = table[0]
            print(f"\n📊 Tabela: {table_name}")
            
            # Estrutura da tabela
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            
            print("   Colunas:")
            for col in columns:
                print(f"     {col[1]} ({col[2]})")
            
            # Contagem de registros
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            print(f"   Registros: {count}")


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description="Manual SQL Validator")
    parser.add_argument("--database", "-d", default="sus_database.db", 
                       help="Caminho para o banco de dados SQLite")
    parser.add_argument("--query", "-q", help="Query SQL para executar diretamente")
    
    args = parser.parse_args()
    
    validator = ManualSQLValidator(args.database)
    
    if args.query:
        # Executar apenas query manual
        if not validator.connect_database():
            return
        
        success, df, _ = validator.execute_manual_query(args.query)
        if success:
            validator.display_results(df)
        
        validator.disconnect_database()
        
    else:
        # Modo interativo
        validator.interactive_mode()


if __name__ == "__main__":
    main()