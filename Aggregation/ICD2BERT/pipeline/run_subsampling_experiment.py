#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_subsampling_experiment.py
ניסוי Subsampling (10%, 25%, 50%, 100%) מיושר להיפר-פרמטרים של train_pipeline.py
ולפרוטוקול 10 חזרות Bootstrap של מאמר ICD2BERT עבור כל 3 המשימות הקליניות.
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier
from sklearn.metrics import roc_auc_score
from sklearn.utils import resample
from src.dataset_builder import extract_patient_records, build_token_sequences

MODES = ["mimic", "omop_raw", "omop_aggregated"]
LABELS = {
    "mimic": "MIMIC-IV (ICD Raw)",
    "omop_raw": "OMOP (SNOMED Raw)",
    "omop_aggregated": "OMOP (SNOMED Depth-3 Agg)"
}
COLORS = {
    "mimic": "#1f77b4",
    "omop_raw": "#d62728",
    "omop_aggregated": "#2ca02c"
}
FRACTIONS = [0.10, 0.25, 0.50, 1.00]
N_BOOTSTRAPS = 10


def get_aligned_train_labels(mode, X_train, data_dir="data"):
    """שחזור מדויק של תוויות האימון עבור כל 3 המשימות לפי הפיצול המקורי (seed=42, 70%)."""
    mapping = "mapping_snomed_depth3_production_clean.csv" if mode == "omop_aggregated" else None
    records, vocab_to_id = extract_patient_records(data_dir=data_dir, mapping_csv_path=mapping, mode=mode)
    _, y_mort, y_rehosp, y_fut = build_token_sequences(records, vocab_to_id)

    y_mort = np.array(y_mort)
    y_rehosp = np.array(y_rehosp)
    y_fut = np.array(y_fut)

    num_samples = len(records)
    np.random.seed(42)
    indices = np.random.permutation(num_samples)
    train_end = int(0.7 * num_samples)
    train_idx = indices[:train_end]

    assert len(train_idx) == len(X_train), f"Size mismatch: {len(train_idx)} vs {len(X_train)}"
    return y_mort[train_idx], y_rehosp[train_idx], y_fut[train_idx]


def eval_multilabel_sub(X_sub, y_sub, X_test, y_test, seed):
    """הערכת Multi-label עם הפרמטרים המלאים (n_estimators=250, max_depth=None)."""
    base_rf = RandomForestClassifier(
        n_estimators=250,
        max_depth=None,
        class_weight="balanced",
        random_state=seed,
        n_jobs=8
    )
    clf = MultiOutputClassifier(base_rf)
    clf.fit(X_sub, y_sub)
    preds = np.array([
        est.predict_proba(X_test)[:, 1] if len(est.classes_) > 1 else np.zeros(len(X_test))
        for est in clf.estimators_
    ]).T
    valid_aucs = [
        roc_auc_score(y_test[:, i], preds[:, i])
        for i in range(y_test.shape[1])
        if len(np.unique(y_test[:, i])) > 1
    ]
    return float(np.mean(valid_aucs))


def main():
    res_dir = "results"
    out_dir = "results/plots"
    os.makedirs(out_dir, exist_ok=True)

    summary_rows = []
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))

    for mode in MODES:
        bundle_path = os.path.join(res_dir, f"eval_bundle_{mode}.npz")
        if not os.path.exists(bundle_path):
            print(f"Warning: {bundle_path} not found, skipping...")
            continue

        bundle = np.load(bundle_path)
        X_train, X_test = bundle["X_train"], bundle["X_test"]
        y_mort_test, y_rehosp_test, y_fut_test = (
            bundle["y_mort_test"],
            bundle["y_rehosp_test"],
            bundle["y_fut_test"]
        )

        y_mort_train, y_rehosp_train, y_fut_train = get_aligned_train_labels(mode, X_train)

        # 1 & 2: Binary Tasks (Mortality & Rehospitalization)
        binary_tasks = [
            ("Mortality", y_mort_train, y_mort_test, axes[0], 10),
            ("Rehospitalization (30d)", y_rehosp_train, y_rehosp_test, axes[1], None)
        ]

        for task_name, y_tr, y_te, ax, depth in binary_tasks:
            means, stds = [], []
            for frac in FRACTIONS:
                n_samples = int(len(X_train) * frac)
                use_replace = (frac == 1.00)
                boot_aucs = []
                for seed in range(N_BOOTSTRAPS):
                    X_sub, y_sub = resample(
                        X_train, y_tr,
                        n_samples=n_samples,
                        replace=use_replace,
                        stratify=y_tr,
                        random_state=seed + 100
                    )
                    clf = RandomForestClassifier(
                        n_estimators=250,
                        max_depth=depth,
                        class_weight="balanced",
                        random_state=seed,
                        n_jobs=8
                    )
                    clf.fit(X_sub, y_sub)
                    boot_aucs.append(roc_auc_score(y_te, clf.predict_proba(X_test)[:, 1]))

                m_auc, s_auc = float(np.mean(boot_aucs)), float(np.std(boot_aucs))
                means.append(m_auc)
                stds.append(s_auc)
                summary_rows.append({
                    "Mode": mode,
                    "Task": task_name,
                    "Train_Pct": f"{int(frac * 100)}%",
                    "Samples": n_samples,
                    "AUROC_Mean": round(m_auc, 4),
                    "AUROC_Std": round(s_auc, 4)
                })
                print(f"[{mode:15s}] {task_name:23s} | {int(frac * 100):3d}% ({n_samples:5d}) -> AUROC: {m_auc:.4f} +/- {s_auc:.4f}")

            ax.errorbar(
                [int(f * 100) for f in FRACTIONS], means, yerr=stds,
                marker="o", linewidth=2.2, capsize=4, color=COLORS[mode], label=LABELS[mode]
            )

        # 3: Future Disease (Multi-label)
        means_f, stds_f = [], []
        for frac in FRACTIONS:
            n_samples = int(len(X_train) * frac)
            use_replace = (frac == 1.00)
            boot_aucs = []
            for seed in range(N_BOOTSTRAPS):
                X_sub, y_sub = resample(
                    X_train, y_fut_train,
                    n_samples=n_samples,
                    replace=use_replace,
                    random_state=seed + 100
                )
                boot_aucs.append(eval_multilabel_sub(X_sub, y_sub, X_test, y_fut_test, seed))

            m_auc, s_auc = float(np.mean(boot_aucs)), float(np.std(boot_aucs))
            means_f.append(m_auc)
            stds_f.append(s_auc)
            summary_rows.append({
                "Mode": mode,
                "Task": "Future Disease (Macro)",
                "Train_Pct": f"{int(frac * 100)}%",
                "Samples": n_samples,
                "AUROC_Mean": round(m_auc, 4),
                "AUROC_Std": round(s_auc, 4)
            })
            print(f"[{mode:15s}] {'Future Disease (Macro)':23s} | {int(frac * 100):3d}% ({n_samples:5d}) -> AUROC: {m_auc:.4f} +/- {s_auc:.4f}")

        axes[2].errorbar(
            [int(f * 100) for f in FRACTIONS], means_f, yerr=stds_f,
            marker="o", linewidth=2.2, capsize=4, color=COLORS[mode], label=LABELS[mode]
        )

    titles = [
        "Mortality - Low-Data Subsampling",
        "Rehospitalization - Low-Data Subsampling",
        "Future Disease - Low-Data Subsampling"
    ]
    for ax, title in zip(axes, titles):
        ax.set_title(title)
        ax.set_xlabel("Training Data Percentage (%)")
        ax.set_ylabel("Test AUROC (10 Bootstraps)")
        ax.set_xticks([10, 25, 50, 100])
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.legend(loc="lower right", fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "subsampling_learning_curve.png"), dpi=300)
    plt.close()

    pd.DataFrame(summary_rows).to_csv(os.path.join(res_dir, "subsampling_benchmark.csv"), index=False)
    print("\nSaved results/subsampling_benchmark.csv and results/plots/subsampling_learning_curve.png")


if __name__ == "__main__":
    main()
