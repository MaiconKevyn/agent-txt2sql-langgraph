#!/usr/bin/env python3
"""
Analysis Reporter - Análise estatística e visualização unificada

Este script consolida todas as análises estatísticas, visualizações e relatórios
em um único módulo, eliminando redundâncias dos scripts anteriores.

Autor: Claude Code Assistant
Data: 2025-07-13
"""

import sys
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from typing import Dict, List, Any
from scipy import stats

# Adicionar diretório raiz ao path
sys.path.append(str(Path(__file__).parent.parent))

from utils import FileManager


class AnalysisReporter:
    """Gerador de análises e relatórios consolidado"""
    
    def __init__(self, output_dir: str = "results"):
        """
        Inicializa o reporter
        
        Args:
            output_dir: Diretório de saída
        """
        self.file_manager = FileManager(output_dir)
        self.data = None
        self.df = None
        
        # Configurar matplotlib
        plt.style.use('default')
        sns.set_palette("husl")
    
    def load_evaluation_data(self, file_path: str):
        """
        Carrega dados de avaliação
        
        Args:
            file_path: Caminho para arquivo de avaliação
        """
        self.data = self.file_manager.load_json(file_path)
        
        # Converter para DataFrame para análises
        detailed_evals = self.data["detailed_evaluations"]
        self.df = pd.DataFrame(detailed_evals)
        
        print(f"📊 Dados carregados: {len(detailed_evals)} avaliações de {len(self.data['evaluation_summaries'])} modelos")
    
    def generate_statistical_analysis(self) -> Dict[str, Any]:
        """
        Gera análise estatística avançada
        
        Returns:
            Resultados da análise estatística
        """
        if self.df is None:
            raise ValueError("Dados não carregados")
        
        print("🔬 Gerando análise estatística...")
        
        analysis = {}
        
        # 1. Estatísticas descritivas
        analysis['descriptive_stats'] = {
            'total_evaluations': len(self.df),
            'models_count': self.df['model_name'].nunique(),
            'difficulties': self.df['difficulty'].value_counts().to_dict(),
            'overall_exact_match_rate': self.df['exact_match'].mean(),
            'overall_semantic_match_rate': self.df['semantic_equivalence'].mean(),
            'overall_sql_similarity': self.df['sql_similarity'].mean()
        }
        
        # 2. Análise por modelo
        model_stats = self.df.groupby('model_name').agg({
            'exact_match': ['mean', 'std', 'count'],
            'semantic_equivalence': ['mean', 'std'],
            'sql_similarity': ['mean', 'std'],
            'structure_match': 'mean',
            'execution_time': ['mean', 'std']
        }).round(3)
        
        # Converter para dict simples
        model_stats_dict = {}
        for model in model_stats.index:
            model_stats_dict[model] = {
                'exact_match_mean': model_stats.loc[model, ('exact_match', 'mean')],
                'exact_match_std': model_stats.loc[model, ('exact_match', 'std')],
                'exact_match_count': model_stats.loc[model, ('exact_match', 'count')],
                'semantic_mean': model_stats.loc[model, ('semantic_equivalence', 'mean')],
                'sql_similarity_mean': model_stats.loc[model, ('sql_similarity', 'mean')],
                'execution_time_mean': model_stats.loc[model, ('execution_time', 'mean')]
            }
        
        analysis['model_statistics'] = model_stats_dict
        
        # 3. Análise por dificuldade (simplificada)
        difficulty_stats = {}
        for difficulty in ['easy', 'medium', 'hard']:
            diff_data = self.df[self.df['difficulty'] == difficulty]
            if len(diff_data) > 0:
                difficulty_stats[difficulty] = {
                    'exact_match_rate': diff_data['exact_match'].mean(),
                    'semantic_rate': diff_data['semantic_equivalence'].mean(),
                    'sql_similarity': diff_data['sql_similarity'].mean()
                }
        
        analysis['difficulty_analysis'] = difficulty_stats
        
        # 4. Correlações
        numeric_cols = ['sql_similarity', 'execution_time']
        if 'exact_match' in self.df.columns:
            numeric_cols.extend(['exact_match', 'semantic_equivalence', 'structure_match'])
        
        correlation_matrix = self.df[numeric_cols].corr()
        analysis['correlations'] = correlation_matrix.to_dict()
        
        # 5. Testes estatísticos
        # ANOVA para diferenças entre modelos
        model_groups = [group['semantic_equivalence'].values 
                       for name, group in self.df.groupby('model_name')]
        
        if len(model_groups) > 1:
            f_stat, p_value = stats.f_oneway(*model_groups)
            analysis['anova_test'] = {
                'f_statistic': f_stat,
                'p_value': p_value,
                'significant': p_value < 0.05
            }
        
        # 6. Identificação de padrões
        analysis['patterns'] = self._identify_patterns()
        
        return analysis
    
    def _identify_patterns(self) -> Dict[str, Any]:
        """Identifica padrões interessantes nos dados"""
        patterns = {}
        
        # Queries onde SQL similarity é alta mas match é baixo
        high_sim_low_match = self.df[
            (self.df['sql_similarity'] > 0.7) & 
            (self.df['exact_match'] == False)
        ]
        
        patterns['high_similarity_low_match'] = {
            'count': len(high_sim_low_match),
            'percentage': len(high_sim_low_match) / len(self.df) * 100,
            'examples': high_sim_low_match[['query_id', 'model_name', 'sql_similarity']].head(3).to_dict('records')
        }
        
        # Modelos com alta variabilidade
        model_variability = self.df.groupby('model_name')['sql_similarity'].std().sort_values(ascending=False)
        patterns['model_variability'] = {
            'most_variable': model_variability.head(3).to_dict(),
            'most_consistent': model_variability.tail(3).to_dict()
        }
        
        # Performance por dificuldade
        difficulty_performance = self.df.groupby('difficulty')['semantic_equivalence'].mean().sort_values(ascending=False)
        patterns['difficulty_performance'] = difficulty_performance.to_dict()
        
        return patterns
    
    def create_comprehensive_dashboard(self, output_filename: str = None) -> str:
        """
        Cria dashboard visual consolidado
        
        Args:
            output_filename: Nome do arquivo de saída
            
        Returns:
            Caminho do arquivo gerado
        """
        if self.df is None:
            raise ValueError("Dados não carregados")
        
        print("📈 Criando dashboard visual...")
        
        # Configurar layout
        fig = plt.figure(figsize=(20, 16))
        
        # 1. Ranking por Equivalência Semântica (top-left)
        ax1 = plt.subplot(2, 3, 1)
        semantic_avg = self.df.groupby('model_name')['semantic_equivalence'].mean().sort_values(ascending=True)
        
        colors = plt.cm.RdYlGn(np.linspace(0.3, 0.9, len(semantic_avg)))
        bars1 = ax1.barh(range(len(semantic_avg)), semantic_avg.values,
                         color=colors, alpha=0.9, edgecolor='black', linewidth=2)
        
        ax1.set_title('Equivalência Semântica por Modelo', fontweight='bold', fontsize=14, pad=15)
        ax1.set_ylabel('Modelo', fontweight='bold', fontsize=12)
        ax1.set_xlabel('Taxa de Equivalência Semântica', fontweight='bold', fontsize=12)
        ax1.set_yticks(range(len(semantic_avg)))
        ax1.set_yticklabels(semantic_avg.index, fontweight='bold')
        ax1.set_xlim(0, 1.1)
        ax1.grid(True, alpha=0.4, axis='x', linestyle='--')
        
        # Adicionar valores
        for i, (bar, value) in enumerate(zip(bars1, semantic_avg.values)):
            ax1.text(value + 0.05, bar.get_y() + bar.get_height()/2.,
                    f'{value:.1%}', ha='left', va='center', fontweight='bold')
        
        # 2. Tempo de Execução Médio por Modelo (top-center)
        ax2 = plt.subplot(2, 3, 2)
        execution_time_avg = self.df.groupby('model_name')['execution_time'].mean().sort_values(ascending=True)
        
        # Usar cores invertidas (verde = mais rápido, vermelho = mais lento)
        colors = plt.cm.RdYlGn_r(np.linspace(0.3, 0.9, len(execution_time_avg)))
        bars2 = ax2.barh(range(len(execution_time_avg)), execution_time_avg.values,
                         color=colors, alpha=0.9, edgecolor='black', linewidth=2)
        
        ax2.set_title('Tempo de Execução Médio por Modelo', fontweight='bold', fontsize=14, pad=15)
        ax2.set_ylabel('Modelo', fontweight='bold', fontsize=12)
        ax2.set_xlabel('Tempo Médio (segundos)', fontweight='bold', fontsize=12)
        ax2.set_yticks(range(len(execution_time_avg)))
        ax2.set_yticklabels(execution_time_avg.index, fontweight='bold')
        ax2.set_xlim(0, max(execution_time_avg.values) * 1.1)
        ax2.grid(True, alpha=0.4, axis='x', linestyle='--')
        
        # Adicionar valores
        for i, (bar, value) in enumerate(zip(bars2, execution_time_avg.values)):
            ax2.text(value + max(execution_time_avg.values) * 0.02, bar.get_y() + bar.get_height()/2.,
                    f'{value:.2f}s', ha='left', va='center', fontweight='bold')
        
        # 3. Similaridade SQL por Modelo (top-right)
        ax3 = plt.subplot(2, 3, 3)
        similarity_avg = self.df.groupby('model_name')['sql_similarity'].mean().sort_values(ascending=True)
        
        colors = plt.cm.RdYlGn(np.linspace(0.3, 0.9, len(similarity_avg)))
        bars3 = ax3.barh(range(len(similarity_avg)), similarity_avg.values,
                         color=colors, alpha=0.9, edgecolor='black', linewidth=2)
        
        ax3.set_title('Similaridade SQL por Modelo', fontweight='bold', fontsize=14, pad=15)
        ax3.set_ylabel('Modelo', fontweight='bold', fontsize=12)
        ax3.set_xlabel('Similaridade SQL Média', fontweight='bold', fontsize=12)
        ax3.set_yticks(range(len(similarity_avg)))
        ax3.set_yticklabels(similarity_avg.index, fontweight='bold')
        ax3.set_xlim(0, 1.1)
        ax3.grid(True, alpha=0.4, axis='x', linestyle='--')
        
        # Adicionar valores
        for i, (bar, value) in enumerate(zip(bars3, similarity_avg.values)):
            ax3.text(value + 0.05, bar.get_y() + bar.get_height()/2.,
                    f'{value:.3f}', ha='left', va='center', fontweight='bold')
        
        # 4. Performance por Dificuldade (bottom-left)
        ax4 = plt.subplot(2, 3, 4)
        difficulty_pivot = self.df.pivot_table(
            values='semantic_equivalence',
            index='model_name',
            columns='difficulty',
            aggfunc='mean'
        )
        sns.heatmap(difficulty_pivot, annot=True, cmap='RdYlGn',
                   fmt='.2f', ax=ax4, cbar_kws={'label': 'Taxa de Equivalência'})
        ax4.set_title('Performance por Dificuldade', fontweight='bold', fontsize=14, pad=15)
        ax4.set_xlabel('Dificuldade', fontweight='bold', fontsize=12)
        ax4.set_ylabel('Modelo', fontweight='bold', fontsize=12)
        
        # 5. Distribuição de Similaridade SQL (bottom-center)
        ax5 = plt.subplot(2, 3, 5)
        for model in self.df['model_name'].unique():
            model_data = self.df[self.df['model_name'] == model]
            ax5.hist(model_data['sql_similarity'], alpha=0.6, label=model, bins=20)
        
        ax5.set_title('Distribuição de Similaridade SQL', fontweight='bold', fontsize=14, pad=15)
        ax5.set_xlabel('Similaridade SQL', fontweight='bold', fontsize=12)
        ax5.set_ylabel('Frequência', fontweight='bold', fontsize=12)
        ax5.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax5.grid(True, alpha=0.3)
        
        # 6. Correlação entre Métricas (bottom-right)
        ax6 = plt.subplot(2, 3, 6)
        corr_data = self.df[['sql_similarity', 'exact_match', 'semantic_equivalence', 'structure_match']].corr()
        sns.heatmap(corr_data, annot=True, cmap='RdBu_r', center=0,
                   ax=ax6, cbar_kws={'label': 'Correlação'})
        ax6.set_title('Correlação entre Métricas', fontweight='bold', fontsize=14, pad=15)
        
        plt.tight_layout()
        
        # Salvar dashboard
        if output_filename is None:
            timestamp = self.data.get('metadata', {}).get('timestamp', 'unknown')
            output_filename = f"analysis_dashboard_{timestamp}.png"
        
        file_path = self.file_manager.get_output_path(output_filename)
        plt.savefig(file_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return file_path
    
    def generate_detailed_report(self, analysis: Dict[str, Any]) -> str:
        """
        Gera relatório textual detalhado
        
        Args:
            analysis: Dados da análise estatística
            
        Returns:
            Relatório em formato texto
        """
        report = []
        report.append("=" * 80)
        report.append("RELATÓRIO CONSOLIDADO DE AVALIAÇÃO DE MODELOS TEXT2SQL")
        report.append("=" * 80)
        
        # Metadados
        metadata = self.data.get('metadata', {})
        report.append(f"Timestamp: {metadata.get('timestamp', 'N/A')}")
        report.append(f"Banco de dados: {metadata.get('database_path', 'N/A')}")
        report.append(f"Total de avaliações: {metadata.get('total_evaluations', 'N/A')}")
        report.append("")
        
        # Estatísticas gerais
        desc_stats = analysis['descriptive_stats']
        report.append("📊 ESTATÍSTICAS GERAIS:")
        report.append("-" * 50)
        report.append(f"Total de modelos avaliados: {desc_stats['models_count']}")
        report.append(f"Taxa geral de matches exatos: {desc_stats['overall_exact_match_rate']:.1%}")
        report.append(f"Taxa geral de equivalência semântica: {desc_stats['overall_semantic_match_rate']:.1%}")
        report.append(f"Similaridade SQL média: {desc_stats['overall_sql_similarity']:.3f}")
        report.append("")
        
        # Distribuição por dificuldade
        report.append("📈 DISTRIBUIÇÃO POR DIFICULDADE:")
        report.append("-" * 50)
        for diff, count in desc_stats['difficulties'].items():
            report.append(f"{diff.upper()}: {count} queries")
        report.append("")
        
        # Rankings
        summaries = self.data["evaluation_summaries"]
        
        # Ranking por equivalência semântica
        report.append("🏆 RANKING POR EQUIVALÊNCIA SEMÂNTICA:")
        report.append("-" * 50)
        by_semantic = sorted(summaries.items(),
                           key=lambda x: x[1]["semantic_match_rate"],
                           reverse=True)
        
        for i, (model_key, summary) in enumerate(by_semantic, 1):
            rate = summary["semantic_match_rate"]
            count = summary["semantic_matches"]
            total = summary["total_queries"]
            report.append(f"{i}. {summary['model_name']}: {rate:.1%} ({count}/{total})")
        report.append("")
        
        # Ranking por matches exatos
        report.append("🎯 RANKING POR MATCHES EXATOS:")
        report.append("-" * 50)
        by_exact = sorted(summaries.items(),
                         key=lambda x: x[1]["exact_match_rate"],
                         reverse=True)
        
        for i, (model_key, summary) in enumerate(by_exact, 1):
            rate = summary["exact_match_rate"]
            count = summary["exact_matches"]
            total = summary["total_queries"]
            report.append(f"{i}. {summary['model_name']}: {rate:.1%} ({count}/{total})")
        report.append("")
        
        # Análise estatística
        if 'anova_test' in analysis:
            anova = analysis['anova_test']
            report.append("📊 ANÁLISE ESTATÍSTICA:")
            report.append("-" * 50)
            report.append(f"ANOVA F-statistic: {anova['f_statistic']:.3f}")
            report.append(f"P-value: {anova['p_value']:.6f}")
            if anova['significant']:
                report.append("   → Diferenças estatisticamente significativas entre modelos")
            else:
                report.append("   → Sem diferenças estatisticamente significativas")
            report.append("")
        
        # Padrões identificados
        patterns = analysis.get('patterns', {})
        if patterns:
            report.append("🔍 PADRÕES IDENTIFICADOS:")
            report.append("-" * 50)
            
            high_sim = patterns.get('high_similarity_low_match', {})
            if high_sim.get('count', 0) > 0:
                report.append(f"Queries com alta similaridade SQL mas baixo match: {high_sim['count']} ({high_sim['percentage']:.1f}%)")
            
            variability = patterns.get('model_variability', {})
            if 'most_variable' in variability:
                most_var = list(variability['most_variable'].keys())[0]
                report.append(f"Modelo mais inconsistente: {most_var}")
                most_cons = list(variability['most_consistent'].keys())[0]
                report.append(f"Modelo mais consistente: {most_cons}")
            report.append("")
        
        # Recomendações
        report.append("💡 RECOMENDAÇÕES:")
        report.append("-" * 50)
        
        best_semantic = by_semantic[0][1]['model_name']
        best_exact = by_exact[0][1]['model_name']
        
        report.append(f"🏭 Para produção (melhor equivalência semântica): {best_semantic}")
        report.append(f"🎯 Para precisão máxima (melhor matches exatos): {best_exact}")
        
        if best_semantic != best_exact:
            report.append("ℹ️  Diferentes modelos se destacam em diferentes aspectos")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def save_complete_analysis(self, analysis: Dict[str, Any], report: str) -> str:
        """
        Salva análise completa em JSON
        
        Args:
            analysis: Dados da análise
            report: Relatório textual
            
        Returns:
            Caminho do arquivo salvo
        """
        timestamp = self.data.get('metadata', {}).get('timestamp', 'unknown')
        
        complete_analysis = {
            'metadata': self.data.get('metadata', {}),
            'statistical_analysis': analysis,
            'text_report': report,
            'generation_timestamp': timestamp
        }
        
        filename = f"complete_analysis_{timestamp}.json"
        file_path = self.file_manager.save_json(complete_analysis, filename)
        
        return file_path
    
    def run_complete_analysis(self, input_file: str):
        """
        Executa análise completa e gera todos os outputs
        
        Args:
            input_file: Arquivo de avaliação de entrada
        """
        print("🚀 Iniciando análise completa...")
        
        # Carregar dados
        self.load_evaluation_data(input_file)
        
        # Análise estatística
        analysis = self.generate_statistical_analysis()
        
        # Dashboard visual
        dashboard_path = self.create_comprehensive_dashboard()
        
        # Relatório textual
        report = self.generate_detailed_report(analysis)
        
        # Salvar análise completa
        analysis_path = self.save_complete_analysis(analysis, report)
        
        # Mostrar resultados
        print(report)
        
        print(f"\n✅ Análise completa concluída!")
        print(f"📊 Dashboard: {dashboard_path}")
        print(f"📋 Análise completa: {analysis_path}")


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description="Gerador de análises e relatórios")
    parser.add_argument("--input", required=True,
                       help="Arquivo de avaliação (evaluation_results_*.json)")
    parser.add_argument("--output-dir", default="results",
                       help="Diretório de saída")
    
    args = parser.parse_args()
    
    try:
        reporter = AnalysisReporter(args.output_dir)
        reporter.run_complete_analysis(args.input)
        
    except FileNotFoundError as e:
        print(f"❌ Arquivo não encontrado: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Erro durante análise: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()