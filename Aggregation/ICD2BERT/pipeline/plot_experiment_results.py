#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
plot_experiment_results.py
סקריפט עזר קל להרצה מקומית (ללא צורך ב-GPU) המייצר את כל הגרפים מתוך הקבצים שבתיקיית results/:
1. עקומות Loss של אימון מול ולידציה לאורך ה-Epochs (בדיקת Overfitting במודל השפה).
2. השוואת עקומות ROC (עבור Mortality ו-Rehospitalization על ה-Test Set).
3. השוואת ביצועי ה-AUROC של שלושת המצבים בכל 3 המשימות (כולל 95% Bootstrap CI).
"""

import os
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, roc_auc_score


def plot_loss_curves(results_dir, modes, output_dir):
    plt.figure(figsize=(10, 5.5))
    colors = {"mimic": "#1f77b4", "omop_raw": "#d62728", "omop_aggregated": "#2ca02c"}

    for mode in modes:
        csv_path = os.path.join(results_dir, f"loss_history_{mode}.csv")
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            c = colors.get(mode, None)
            if "train_mlm_loss" in df.columns:
                plt.plot(df["epoch"], df["train_mlm_loss"], label=f"{mode} - Train Loss", alpha=0.65, linestyle=":", color=c)
            if "val_mlm_loss" in df.columns:
                plt.plot(df["epoch"], df["val_mlm_loss"], label=f"{mode} - Val Loss", linewidth=2.3, color=c)

    plt.title("MLM Pre-training Loss: Train vs Validation (Overfitting Check)", fontsize=13)
    plt.xlabel("Epoch", fontsize=11)
    plt.ylabel("Loss (Cross Entropy)", fontsize=11)
    plt.grid(True, linestyle=":", alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "loss_comparison_curve.png"), dpi=300)
    plt.close()
    print("Saved loss_comparison_curve.png")


def plot_roc_comparison(results_dir, modes, output_dir):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    colors = {"mimic": "#1f77b4", "omop_raw": "#d62728", "omop_aggregated": "#2ca02c"}
    tasks = [
        ("y_mort_test", "mort_probs", "Mortality"),
        ("y_rehosp_test", "rehosp_probs", "Rehospitalization (30d)")
    ]

    for ax, (y_key, p_key, task_title) in zip(axes, tasks):
        ax.plot([0, 1], [0, 1], "k--", label="Random Chance (AUC = 0.50)")
        for mode in modes:
            bundle_path = os.path.join(results_dir, f"eval_bundle_{mode}.npz")
            if os.path.exists(bundle_path):
                data = np.load(bundle_path)
                if y_key in data and p_key in data:
                    y_true, y_prob = data[y_key], data[p_key]
                    fpr, tpr, _ = roc_curve(y_true, y_prob)
                    test_auc = roc_auc_score(y_true, y_prob)
                    ax.plot(fpr, tpr, label=f"{mode} (AUROC = {test_auc:.4f})", linewidth=2, color=colors.get(mode))

        ax.set_title(f"{task_title} - Test ROC Curves", fontsize=13)
        ax.set_xlabel("False Positive Rate", fontsize=11)
        ax.set_ylabel("True Positive Rate", fontsize=11)
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.legend(loc="lower right", fontsize=9.5)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "roc_comparison.png"), dpi=300)
    plt.close()
    print("Saved roc_comparison.png")


def plot_downstream_bars(results_dir, modes, output_dir):
    plt.figure(figsize=(10, 5.5))
    bar_width = 0.25
    tasks = [
        ("mortality_mean", "mortality_boots", "Mortality"),
        ("rehosp_mean", "rehosp_boots", "Rehospitalization (30d)"),
        ("future_disease_mean", "future_disease_boots", "Future Disease (Macro)")
    ]
    colors = {"mimic": "#4C72B0", "omop_raw": "#C44E52", "omop_aggregated": "#55A868"}
    x = np.arange(len(tasks))

    for i, mode in enumerate(modes):
        bundle_path = os.path.join(results_dir, f"eval_bundle_{mode}.npz")
        if os.path.exists(bundle_path):
            data = np.load(bundle_path)
            means, err_low, err_high = [], [], []
            for m_key, b_key, _ in tasks:
                m_val = float(data[m_key])
                boots = data[b_key]
                means.append(m_val)
                err_low.append(m_val - np.percentile(boots, 2.5))
                err_high.append(np.percentile(boots, 97.5) - m_val)

            offset = (i - 1) * bar_width
            bars = plt.bar(
                x + offset, means, width=bar_width, yerr=[err_low, err_high],
                capsize=4, label=mode.upper(), color=colors.get(mode)
            )
            for bar, val in zip(bars, means):
                plt.text(bar.get_x() + bar.get_width() / 2, val + 0.008, f"{val:.3f}",
                         ha="center", va="bottom", fontsize=9, fontweight="bold")

    plt.xticks(x, [t[2] for t in tasks], fontsize=11)
    plt.ylabel("Test AUROC (95% Bootstrap CI)", fontsize=12)
    plt.ylim(0.5, 0.90)
    plt.title("Downstream Clinical Tasks Comparison (10 Bootstrap Iterations)", fontsize=13)
    plt.legend(loc="upper right")
    plt.grid(axis="y", linestyle=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "overfitting_check_bars.png"), dpi=300)
    plt.close()
    print("Saved overfitting_check_bars.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--results_dir", type=str, default="./results")
    parser.add_argument("--output_dir", type=str, default="./results/plots")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    modes = ["mimic", "omop_raw", "omop_aggregated"]

    plot_loss_curves(args.results_dir, modes, args.output_dir)
    plot_roc_comparison(args.results_dir, modes, args.output_dir)
    plot_downstream_bars(args.results_dir, modes, args.output_dir)
    print("All plots generated successfully!")