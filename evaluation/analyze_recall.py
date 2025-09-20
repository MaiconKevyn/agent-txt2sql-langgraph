#!/usr/bin/env python3
"""Analyze table selection results with recall-based logic."""

import json

def analyze_with_recall(results_file):
    """Apply recall-based logic to existing results."""
    
    with open(results_file, 'r') as f:
        results = json.load(f)

    print('=== Aplicando Nova Lógica de Recall ===')
    print()

    correct_count = 0
    total_count = len(results)
    changes = 0

    for r in results:
        gt_set = set(r['ground_truth_tables'])
        sel_set = set(r['selected_tables'])
        
        # NEW LOGIC: Check if ALL ground truth tables are in selected tables (RECALL)
        new_match = gt_set.issubset(sel_set)
        
        # Calculate metrics
        recall = len(gt_set.intersection(sel_set)) / len(gt_set) if gt_set else 1.0
        precision = len(gt_set.intersection(sel_set)) / len(sel_set) if sel_set else 0.0
        
        if new_match:
            correct_count += 1
        
        old_match = r['tables_match']
        status_old = '✅' if old_match else '❌'
        status_new = '✅' if new_match else '❌'
        
        print(f'{r["id"]}: {r["question"][:45]}...')
        print(f'  Expected: {r["ground_truth_tables"]}')
        print(f'  Selected: {r["selected_tables"]}')
        print(f'  Old Logic (Exact): {status_old} | New Logic (Recall): {status_new}')
        print(f'  Recall: {recall:.2f} | Precision: {precision:.2f}')
        
        if old_match != new_match:
            changes += 1
            print(f'  🔄 CHANGED: {"Exact" if old_match else "Miss"} → {"Recall" if new_match else "Miss"}')
        print()

    old_accuracy = sum(1 for r in results if r['tables_match']) / total_count * 100
    new_accuracy = correct_count / total_count * 100
    improvement = new_accuracy - old_accuracy
    
    print(f'=== RESULTADOS FINAIS ===')
    print(f'Total de casos: {total_count}')
    print(f'Lógica antiga (Exact Match): {sum(1 for r in results if r["tables_match"])} acertos ({old_accuracy:.1f}%)')
    print(f'Nova lógica (Recall): {correct_count} acertos ({new_accuracy:.1f}%)')
    print(f'Melhoria: +{improvement:.1f} pontos percentuais')
    print(f'Casos que mudaram: {changes}')
    print()
    print('🎯 A nova lógica de recall considera correto quando TODAS as tabelas')
    print('   do ground truth estão presentes nas tabelas selecionadas,')
    print('   independente de tabelas extras selecionadas.')

if __name__ == "__main__":
    analyze_with_recall('evaluation/results/tables_result_13.json')