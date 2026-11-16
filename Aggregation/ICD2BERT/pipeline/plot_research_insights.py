#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
plot_research_insights.py
סקריפט מתקדם להפקת חבילת גרפים קליניים ומחקריים מלאה
להערכת ההשפעה של אגרגציית SNOMED CT על ביצועי המודל:
1. Calibration Curves (Brier Score & Reliability Diagrams)
2. Precision-Recall Curves (AUPRC)
3. Predicted Risk Separation Density (Cohort Separation)
4. Bootstrap Distribution & Stability Analysis
"""

import os
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss, precision_recall_curve, average_precision_score

plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
plt.rcParams['font.size'] = 11
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['axes.titlesize'] = 13
plt.rcParams['xtick.labelsize'] = 10
plt.rcParams['ytick.labelsize'] = 10
plt.rcParams['legend.fontsize'] = 10
plt.rcParams['figure.titlesize'] = 14

COLORS = {
    'mimic': '#1f77b4',           # כחול קלאסי
    'omop_raw': '#d62728',        # אדום (גולמי)
    'omop_aggregated': '#2ca02c'  # ירוק (מאוגרג)
}

LABELS = {
    'mimic': 'MIMIC-IV (ICD Baseline)',
    'omop_raw': 'OMOP (Raw SNOMED)',
    'omop_aggregated': 'OMOP (Aggregated SNOMED)'
}


def load_bundles(results_dir, modes):
    data = {}
    for mode in modes:
        path = os.path.join(results_dir, f"eval_bundle_{mode}.npz")
        if os.path.exists(path):
            data[mode] = np.load(path)
    return data


def plot_calibration_curves(bundles, output_dir):
    """
    עקומות כיול (Reliability Curves / Calibration) עבור Mortality ו-Rehospitalization.
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    tasks = [
        ("y_mort_test", "mort_probs", "Mortality"),
        ("y_rehosp_test", "rehosp_probs", "Rehospitalization (30d)")
    ]

    for ax, (y_key, p_key, task_name) in zip(axes, tasks):
        ax.plot([0, 1], [0, 1], 'k--', label='Perfect Calibration', alpha=0.7)

        for mode, bundle in bundles.items():
            if y_key in bundle and p_key in bundle:
                y_true = bundle[y_key]
                y_prob = bundle[p_key]
                prob_true, prob_pred = calibration_curve(y_true, y_prob, n_bins=10, strategy='uniform')
                brier = brier_score_loss(y_true, y_prob)

                ax.plot(
                    prob_pred, prob_true,
                    marker='o', linewidth=2,
                    color=COLORS.get(mode, 'blue'),
                    label=f"{LABELS.get(mode, mode)} (Brier: {brier:.4f})"
                )

        ax.set_title(f"{task_name} - Clinical Calibration", pad=12)
        ax.set_xlabel("Mean Predicted Probability (Confidence)")
        ax.set_ylabel("Observed Fraction of Positives (Actual Risk)")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.legend(loc="lower right", frameon=True, fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "clinical_calibration_curves.png"), dpi=300)
    plt.close()
    print("Saved: clinical_calibration_curves.png")


def plot_pr_curves(bundles, output_dir):
    """
    עקומות Precision-Recall עבור Mortality ו-Rehospitalization (דאטה לא מאוזן).
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    tasks = [
        ("y_mort_test", "mort_probs", "Mortality"),
        ("y_rehosp_test", "rehosp_probs", "Rehospitalization (30d)")
    ]

    for ax, (y_key, p_key, task_name) in zip(axes, tasks):
        for mode, bundle in bundles.items():
            if y_key in bundle and p_key in bundle:
                y_true = bundle[y_key]
                y_prob = bundle[p_key]
                precision, recall, _ = precision_recall_curve(y_true, y_prob)
                auprc = average_precision_score(y_true, y_prob)
                ax.plot(
                    recall, precision,
                    linewidth=2.2,
                    color=COLORS.get(mode, 'blue'),
                    label=f"{LABELS.get(mode, mode)} (AUPRC = {auprc:.4f})"
                )

        ax.set_title(f"{task_name} - Precision-Recall Curve", pad=12)
        ax.set_xlabel("Recall (Sensitivity)")
        ax.set_ylabel("Precision (Positive Predictive Value)")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.legend(loc="upper right", frameon=True, fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "precision_recall_comparison.png"), dpi=300)
    plt.close()
    print("Saved: precision_recall_comparison.png")


def plot_risk_distribution(bundles, output_dir):
    """
    התפלגות הסיכון החזוי (Separation of Classes) עבור Mortality ו-Rehospitalization.
    """
    tasks = [
        ("y_mort_test", "mort_probs", "Mortality", "No Death (0)", "Mortality (1)"),
        ("y_rehosp_test", "rehosp_probs", "Rehospitalization (30d)", "No Readmission (0)", "Readmission (1)")
    ]

    fig, axes = plt.subplots(2, len(bundles), figsize=(5.5 * len(bundles), 9), sharey="row")
    if len(bundles) == 1:
        axes = axes.reshape(2, 1)

    for row, (y_key, p_key, task_name, neg_lbl, pos_lbl) in enumerate(tasks):
        for col, (mode, bundle) in enumerate(bundles.items()):
            ax = axes[row, col]
            y_true = bundle[y_key]
            y_prob = bundle[p_key]

            ax.hist(y_prob[y_true == 0], bins=25, alpha=0.5, color='gray', label=neg_lbl, density=True)
            ax.hist(y_prob[y_true == 1], bins=25, alpha=0.7, color=COLORS.get(mode, 'blue'), label=pos_lbl, density=True)
            ax.set_title(f"{task_name}\n{LABELS.get(mode, mode)}", fontsize=11)
            ax.set_xlabel("Predicted Probability")
            ax.set_xlim(0, 1)
            ax.legend(frameon=True, fontsize=8.5)
            if col == 0:
                ax.set_ylabel("Probability Density")

    fig.suptitle("Predicted Risk Separation between Cohort Classes", fontsize=15, y=1.01)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "risk_separation_distribution.png"), dpi=300, bbox_inches='tight')
    plt.close()
    print("Saved: risk_separation_distribution.png")


def plot_bootstrap_stability(bundles, output_dir):
    """
    השוואת יציבות התוצאות על פני 10 חזרות Bootstrap בכל שלוש המשימות הקליניות.
    """
    tasks = [
        ("mortality_boots", "Mortality"),
        ("rehosp_boots", "Rehospitalization (30d)"),
        ("future_disease_boots", "Future Disease (Macro)")
    ]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, (b_key, title) in zip(axes, tasks):
        box_data, box_labels, box_colors = [], [], []
        for mode, bundle in bundles.items():
            if b_key in bundle:
                box_data.append(bundle[b_key])
                box_labels.append(mode.replace("omop_", "OMOP\n").upper())
                box_colors.append(COLORS.get(mode, 'blue'))

        bp = ax.boxplot(box_data, tick_labels=box_labels, patch_artist=True, widths=0.45)
        for patch, color in zip(bp["boxes"], box_colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.75)

        ax.set_title(title)
        ax.set_ylabel("Test AUROC (10 Bootstraps)")
        ax.grid(axis="y", linestyle=":", alpha=0.6)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "bootstrap_stability_analysis.png"), dpi=300)
    plt.close()
    print("Saved: bootstrap_stability_analysis.png")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results_dir", type=str, default="./results")
    parser.add_argument("--output_dir", type=str, default="./results/research_plots")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    modes = ["mimic", "omop_raw", "omop_aggregated"]
    bundles = load_bundles(args.results_dir, modes)

    if not bundles:
        print(f"No eval bundles found in {args.results_dir}!")
        return

    plot_calibration_curves(bundles, args.output_dir)
    plot_pr_curves(bundles, args.output_dir)
    plot_risk_distribution(bundles, args.output_dir)
    plot_bootstrap_stability(bundles, args.output_dir)
    print("Finished generating all advanced research plots successfully!")


if __name__ == "__main__":
    main()