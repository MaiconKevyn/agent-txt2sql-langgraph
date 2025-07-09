#!/usr/bin/env python3
"""
Manual SQL Validator - Ferramenta para executar queries SQL manualmente
e comparar com respostas do agente TXT2SQL

Autor: Claude Code Assistant
Data: 2025-07-02
"""

import sqlite3
import pandas as pd
import json
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import os
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
        self.results_log = []
        
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
    
    def execute_agent_query(self, natural_language_query: str) -> Tuple[bool, str, Dict]:
        """
        Executa a mesma pergunta no agente TXT2SQL
        
        Args:
            natural_language_query: Pergunta em língua natural
            
        Returns:
            Tuple com (sucesso, resposta_texto, metadados)
        """
        try:
            print(f"🤖 Executando no agente: {natural_language_query}")
            
            start_time = time.time()
            
            # Executar agente
            result = subprocess.run(
                ["python", "txt2sql_agent_clean.py", "--query", natural_language_query],
                capture_output=True,
                text=True,
                timeout=120  # 2 minutos timeout
            )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                # Extrair informações da saída
                output = result.stdout
                
                # Buscar SQL gerado
                sql_generated = ""
                for line in output.split('\n'):
                    if "🔧 SQL:" in line:
                        sql_generated = line.split("🔧 SQL:")[1].strip()
                        break
                
                metadata = {
                    "execution_time": execution_time,
                    "sql_generated": sql_generated,
                    "full_output": output,
                    "stderr": result.stderr
                }
                
                print(f"✅ Agente executado com sucesso!")
                print(f"⏱️ Tempo do agente: {execution_time:.3f}s")
                
                return True, output, metadata
            else:
                error_msg = f"❌ Erro no agente: {result.stderr}"
                print(error_msg)
                return False, error_msg, {"error": result.stderr}
                
        except subprocess.TimeoutExpired:
            error_msg = "⏰ Timeout na execução do agente"
            print(error_msg)
            return False, error_msg, {"error": "timeout"}
        except Exception as e:
            error_msg = f"❌ Erro na execução do agente: {str(e)}"
            print(error_msg)
            return False, error_msg, {"error": str(e)}
    
    def compare_results(self, manual_df: pd.DataFrame, agent_output: str, 
                       manual_query: str, natural_query: str) -> Dict:
        """
        Compara resultados entre execução manual e do agente
        
        Args:
            manual_df: DataFrame com resultado manual
            agent_output: Saída do agente
            manual_query: Query SQL manual
            natural_query: Pergunta em língua natural
            
        Returns:
            Dict com análise comparativa
        """
        comparison = {
            "timestamp": datetime.now().isoformat(),
            "natural_query": natural_query,
            "manual_query": manual_query,
            "manual_results_count": len(manual_df),
            "agent_output": agent_output,
            "analysis": {}
        }
        
        # Extrair dados do agente
        agent_sql = ""
        agent_results_count = 0
        agent_actual_value = None
        
        for line in agent_output.split('\n'):
            if "🔧 SQL:" in line:
                agent_sql = line.split("🔧 SQL:")[1].strip()
            elif "✅ Resultado:" in line and "registros encontrados" in line:
                try:
                    agent_results_count = int(line.split("✅ Resultado:")[1].split("registros encontrados")[0].strip())
                except:
                    pass
        
        # Para queries de agregação (COUNT, SUM, AVG), comparar o valor retornado, não o número de linhas
        is_aggregation_query = any(keyword in agent_sql.upper() for keyword in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']) if agent_sql else False
        
        if is_aggregation_query and len(manual_df) == 1:
            # Para agregações, o "resultado" é o valor da primeira coluna da primeira linha
            agent_actual_value = agent_results_count
            manual_actual_value = manual_df.iloc[0, 0]  # Primeiro valor da primeira linha
            
            comparison["is_aggregation"] = True
            comparison["manual_value"] = manual_actual_value
            comparison["agent_value"] = agent_actual_value
        
        comparison["agent_sql"] = agent_sql
        comparison["agent_results_count"] = agent_results_count
        
        # Análise comparativa
        analysis = comparison["analysis"]
        
        # Comparar resultados (considerar agregações)
        if comparison.get("is_aggregation"):
            # Para agregações, comparar os valores reais
            if comparison["manual_value"] == comparison["agent_value"]:
                analysis["results_count_match"] = True
                analysis["results_count_status"] = f"✅ Valores idênticos: {comparison['manual_value']}"
            else:
                analysis["results_count_match"] = False
                analysis["results_count_status"] = f"❌ Valores diferentes: Manual={comparison['manual_value']}, Agente={comparison['agent_value']}"
        else:
            # Para queries normais, comparar número de linhas
            if len(manual_df) == agent_results_count:
                analysis["results_count_match"] = True
                analysis["results_count_status"] = "✅ Mesmo número de resultados"
            else:
                analysis["results_count_match"] = False
                analysis["results_count_status"] = f"❌ Diferença: Manual={len(manual_df)}, Agente={agent_results_count}"
        
        # Comparar SQL (normalizado)
        if agent_sql and manual_query:
            manual_normalized = self.normalize_sql(manual_query)
            agent_normalized = self.normalize_sql(agent_sql)
            
            if manual_normalized == agent_normalized:
                analysis["sql_match"] = True
                analysis["sql_status"] = "✅ SQL idêntico"
            else:
                analysis["sql_match"] = False
                analysis["sql_status"] = "❌ SQL diferente"
                analysis["sql_diff"] = {
                    "manual": manual_normalized,
                    "agent": agent_normalized
                }
        
        # Validação de dados se possível
        if "❌" in agent_output or "erro" in agent_output.lower():
            analysis["agent_error"] = True
            analysis["agent_status"] = "❌ Agente teve erro"
        else:
            analysis["agent_error"] = False
            analysis["agent_status"] = "✅ Agente executou sem erros"
        
        # Validação específica para problema de colunas de localização
        self.check_location_column_usage(natural_query, agent_sql, analysis)
        
        return comparison
    
    def check_location_column_usage(self, natural_query: str, agent_sql: str, analysis: Dict):
        """
        Verifica se o agente está usando a coluna correta para localização
        
        Args:
            natural_query: Pergunta em língua natural
            agent_sql: SQL gerado pelo agente
            analysis: Dict de análise para adicionar alertas
        """
        if not agent_sql:
            return
        
        natural_lower = natural_query.lower()
        sql_upper = agent_sql.upper()
        
        # Detectar se a pergunta é sobre localização
        location_keywords = ['cidade', 'cidades', 'município', 'municípios', 'local', 'localidade', 'região']
        is_location_query = any(keyword in natural_lower for keyword in location_keywords)
        
        if not is_location_query:
            return
        
        # Verificar qual coluna está sendo usada
        uses_munic_res = 'MUNIC_RES' in sql_upper
        uses_munic_mov = 'MUNIC_MOV' in sql_upper
        uses_cidade_residencia = 'CIDADE_RESIDENCIA_PACIENTE' in sql_upper
        uses_wrong_column = uses_munic_res or uses_munic_mov
        
        # Detectar problemas específicos
        problems = []
        
        if 'município' in natural_lower or 'municípios' in natural_lower:
            if uses_wrong_column and not uses_cidade_residencia:
                wrong_column = "MUNIC_RES" if uses_munic_res else "MUNIC_MOV" if uses_munic_mov else "código"
                problems.append({
                    "type": "column_choice_warning",
                    "message": f"⚠️ Usando {wrong_column} (códigos numéricos) para pergunta sobre municípios",
                    "suggestion": "Use CIDADE_RESIDENCIA_PACIENTE para nomes legíveis de cidades",
                    "severity": "medium"
                })
        
        if 'cidade' in natural_lower or 'cidades' in natural_lower:
            if uses_wrong_column and not uses_cidade_residencia:
                wrong_column = "MUNIC_RES" if uses_munic_res else "MUNIC_MOV" if uses_munic_mov else "código"
                problems.append({
                    "type": "column_choice_error", 
                    "message": f"❌ Usando {wrong_column} (códigos) quando deveria usar CIDADE_RESIDENCIA_PACIENTE (nomes)",
                    "suggestion": "Para perguntas sobre cidades, sempre use CIDADE_RESIDENCIA_PACIENTE",
                    "severity": "high"
                })
        
        # Adicionar problemas à análise
        if problems:
            analysis["location_issues"] = problems
            analysis["location_status"] = f"⚠️ {len(problems)} problema(s) de localização detectado(s)"
            
            # Marcar como problema se houver erros de alta severidade
            high_severity_issues = [p for p in problems if p["severity"] == "high"]
            if high_severity_issues:
                analysis["has_location_errors"] = True
        else:
            analysis["location_status"] = "✅ Uso correto de colunas de localização"
            analysis["has_location_errors"] = False
    
    def normalize_sql(self, sql: str) -> str:
        """Normaliza SQL para comparação"""
        return sql.upper().replace('\n', ' ').replace('\t', ' ').strip()
    
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
    
    def save_comparison_log(self, comparison: Dict, filename: str = None):
        """Salva log de comparação"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"validation_log_{timestamp}.json"
        
        try:
            # Converter tipos numpy/pandas para tipos Python nativos
            def convert_types(obj):
                if hasattr(obj, 'item'):  # numpy types
                    return obj.item()
                elif hasattr(obj, 'to_dict'):  # pandas types
                    return obj.to_dict()
                return obj
            
            # Aplicar conversão recursivamente
            def clean_dict(d):
                if isinstance(d, dict):
                    return {k: clean_dict(v) for k, v in d.items()}
                elif isinstance(d, list):
                    return [clean_dict(v) for v in d]
                else:
                    return convert_types(d)
            
            cleaned_comparison = clean_dict(comparison)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(cleaned_comparison, f, indent=2, ensure_ascii=False)
            print(f"💾 Log salvo em: {filename}")
        except Exception as e:
            print(f"❌ Erro ao salvar log: {e}")
    
    def interactive_mode(self):
        """Modo interativo para validação"""
        print("🚀 Iniciando Validador SQL Manual - Modo Interativo")
        print("=" * 60)
        
        if not self.connect_database():
            return
        
        try:
            while True:
                print("\n" + "="*60)
                print("📋 OPÇÕES:")
                print("1. Executar query SQL manual")
                print("2. Testar pergunta no agente")
                print("3. Comparar query manual vs agente")
                print("4. Mostrar estrutura do banco")
                print("5. Sair")
                print("="*60)
                
                choice = input("🎯 Escolha uma opção (1-5): ").strip()
                
                if choice == "1":
                    self.manual_query_mode()
                elif choice == "2":
                    self.agent_test_mode()
                elif choice == "3":
                    self.comparison_mode()
                elif choice == "4":
                    self.show_database_structure()
                elif choice == "5":
                    print("👋 Saindo...")
                    break
                else:
                    print("❌ Opção inválida!")
                
        except KeyboardInterrupt:
            print("\n\n👋 Saindo...")
        finally:
            self.disconnect_database()
    
    def manual_query_mode(self):
        """Modo para executar queries SQL manuais"""
        print("\n🔧 MODO: Query SQL Manual")
        print("-" * 40)
        
        print("💡 Dica: Digite sua query SQL (termine com ';')")
        print("💡 Exemplos:")
        print("   SELECT COUNT(*) FROM sus_data;")
        print("   SELECT MUNIC_RES, COUNT(*) FROM sus_data GROUP BY MUNIC_RES LIMIT 5;")
        print("   SELECT * FROM sus_data WHERE IDADE > 65 LIMIT 10;")
        
        sql_query = input("\n🔍 Digite sua query SQL: ").strip()
        
        if not sql_query:
            print("❌ Query vazia!")
            return
        
        success, df, message = self.execute_manual_query(sql_query)
        
        if success:
            self.display_results(df, "Resultados da Query Manual")
            
            # Perguntar se quer salvar
            save = input("\n💾 Salvar resultados em CSV? (s/n): ").strip().lower()
            if save == 's':
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"manual_query_results_{timestamp}.csv"
                df.to_csv(filename, index=False)
                print(f"💾 Resultados salvos em: {filename}")
        else:
            print(f"❌ Falha na execução: {message}")
    
    def agent_test_mode(self):
        """Modo para testar perguntas no agente"""
        print("\n🤖 MODO: Teste do Agente")
        print("-" * 40)
        
        print("💡 Dica: Digite sua pergunta em português")
        print("💡 Exemplos:")
        print("   Quantos pacientes existem?")
        print("   Quais são os 5 municípios com mais mortes?")
        print("   Qual a média de idade dos pacientes?")
        
        natural_query = input("\n❓ Digite sua pergunta: ").strip()
        
        if not natural_query:
            print("❌ Pergunta vazia!")
            return
        
        success, output, metadata = self.execute_agent_query(natural_query)
        
        if success:
            print("\n🤖 RESPOSTA DO AGENTE:")
            print("-" * 40)
            print(output)
            
            if metadata.get("sql_generated"):
                print(f"\n🔧 SQL Gerado: {metadata['sql_generated']}")
        else:
            print(f"❌ Falha no agente: {output}")
    
    def comparison_mode(self):
        """Modo de comparação entre manual e agente"""
        print("\n⚖️ MODO: Comparação Manual vs Agente")
        print("-" * 40)
        
        # Obter pergunta natural
        natural_query = input("❓ Digite a pergunta em português: ").strip()
        if not natural_query:
            print("❌ Pergunta vazia!")
            return
        
        # Obter query SQL manual
        print("\n🔧 Agora digite a query SQL equivalente:")
        manual_sql = input("🔍 Query SQL: ").strip()
        if not manual_sql:
            print("❌ Query SQL vazia!")
            return
        
        print("\n🏃‍♂️ Executando comparação...")
        
        # Executar manual
        print("\n1️⃣ Executando query manual...")
        manual_success, manual_df, manual_msg = self.execute_manual_query(manual_sql)
        
        if not manual_success:
            print(f"❌ Falha na query manual: {manual_msg}")
            return
        
        # Executar agente
        print("\n2️⃣ Executando no agente...")
        agent_success, agent_output, agent_metadata = self.execute_agent_query(natural_query)
        
        if not agent_success:
            print(f"❌ Falha no agente: {agent_output}")
            return
        
        # Comparar resultados
        print("\n3️⃣ Comparando resultados...")
        comparison = self.compare_results(manual_df, agent_output, manual_sql, natural_query)
        
        # Mostrar comparação
        self.display_comparison(comparison)
        
        # Salvar log
        save_log = input("\n💾 Salvar log de comparação? (s/n): ").strip().lower()
        if save_log == 's':
            self.save_comparison_log(comparison)
    
    def display_comparison(self, comparison: Dict):
        """Exibe resultado da comparação"""
        print("\n" + "="*60)
        print("⚖️ RELATÓRIO DE COMPARAÇÃO")
        print("="*60)
        
        print(f"❓ Pergunta: {comparison['natural_query']}")
        print(f"🔧 Query Manual: {comparison['manual_query']}")
        print(f"🤖 Query do Agente: {comparison.get('agent_sql', 'N/A')}")
        
        analysis = comparison['analysis']
        
        print(f"\n📊 RESULTADOS:")
        print(f"   Manual: {comparison['manual_results_count']} registros")
        print(f"   Agente: {comparison['agent_results_count']} registros")
        print(f"   Status: {analysis.get('results_count_status', 'N/A')}")
        
        print(f"\n🔧 SQL:")
        print(f"   Status: {analysis.get('sql_status', 'N/A')}")
        
        print(f"\n🤖 AGENTE:")
        print(f"   Status: {analysis.get('agent_status', 'N/A')}")
        
        # Mostrar alertas de localização se existirem
        if 'location_status' in analysis:
            print(f"\n🗺️ LOCALIZAÇÃO:")
            print(f"   Status: {analysis['location_status']}")
            
            if 'location_issues' in analysis:
                for issue in analysis['location_issues']:
                    print(f"   {issue['message']}")
                    print(f"   💡 Sugestão: {issue['suggestion']}")
        
        # Resumo final
        has_problems = (
            not analysis.get('results_count_match') or 
            not analysis.get('sql_match', True) or 
            analysis.get('agent_error') or
            analysis.get('has_location_errors', False)
        )
        
        if not has_problems:
            print(f"\n✅ RESULTADO: Agente funcionou corretamente!")
        else:
            print(f"\n❌ RESULTADO: Possível problema detectado!")
            
            if not analysis.get('results_count_match'):
                print("   - Número de resultados diferente")
            if not analysis.get('sql_match', True):
                print("   - SQL gerado diferente")
            if analysis.get('agent_error'):
                print("   - Agente teve erro")
            if analysis.get('has_location_errors'):
                print("   - Problema com colunas de localização")
    
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
    parser.add_argument("--natural", "-n", help="Pergunta natural para testar no agente")
    parser.add_argument("--compare", "-c", action="store_true", 
                       help="Modo de comparação (requer --query e --natural)")
    
    args = parser.parse_args()
    
    validator = ManualSQLValidator(args.database)
    
    if args.query and args.natural and args.compare:
        # Modo comparação via linha de comando
        if not validator.connect_database():
            return
        
        print(f"🔍 Executando query manual: {args.query}")
        manual_success, manual_df, _ = validator.execute_manual_query(args.query)
        
        if manual_success:
            print(f"🤖 Testando no agente: {args.natural}")
            agent_success, agent_output, _ = validator.execute_agent_query(args.natural)
            
            if agent_success:
                comparison = validator.compare_results(manual_df, agent_output, args.query, args.natural)
                validator.display_comparison(comparison)
                validator.save_comparison_log(comparison)
        
        validator.disconnect_database()
        
    elif args.query:
        # Executar apenas query manual
        if not validator.connect_database():
            return
        
        success, df, _ = validator.execute_manual_query(args.query)
        if success:
            validator.display_results(df)
        
        validator.disconnect_database()
        
    elif args.natural:
        # Executar apenas no agente
        success, output, metadata = validator.execute_agent_query(args.natural)
        if success:
            print("🤖 RESPOSTA DO AGENTE:")
            print(output)
        
    else:
        # Modo interativo
        validator.interactive_mode()


if __name__ == "__main__":
    main()