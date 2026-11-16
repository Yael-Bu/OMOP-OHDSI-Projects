#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
train_pipeline.py
אימון BEHRT ב-PyTorch טהור עם בקרת איכות מלאה של שלב ה-MLM:
- מדידת Train Loss, Val MLM Loss, Val Perplexity, Top-1 Precision, Top-5 Recall, Top-10 Recall בכל Epoch
- שמירה וטעינה אוטומטית של הצ'קפוינט המיטבי (Best Model based on Val MLM Loss)
- חילוץ וקטורי [CLS] נקיים והערכת 3 התרחישים עם Grid-Search ו-10 חזרות Bootstrap:
  1. Mortality (תמותה)
  2. Rehospitalization (אשפוז חוזר תוך 30 יום)
  3. Future Disease (חיזוי מחלות עתידיות ב-Multi-label)
"""

import os
import math
import copy
import pickle
import argparse
import logging
import json
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.model_selection import ParameterGrid
from sklearn.utils import resample

from dataset_builder import extract_patient_records, build_token_sequences, ClinicalEHRDataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ==========================================
# 1. ארכיטקטורת BEHRT ב-PyTorch טהור (ממד 240)
# ==========================================
class BEHRTEmbeddings(nn.Module):
    def __init__(self, vocab_size: int, hidden_size: int = 240, max_len: int = 512, max_age: int = 125):
        super().__init__()
        self.disease_embeddings = nn.Embedding(vocab_size, hidden_size, padding_idx=0)
        self.age_embeddings = nn.Embedding(max_age, hidden_size)
        self.pos_embeddings = nn.Embedding(max_len, hidden_size)
        self.seg_embeddings = nn.Embedding(4, hidden_size)

        self.layer_norm = nn.LayerNorm(hidden_size)
        self.dropout = nn.Dropout(0.1)

    def forward(self, input_ids):
        seq_len = input_ids.size(1)
        device = input_ids.device

        age_ids = torch.full_like(input_ids, 50)
        seg_ids = torch.zeros_like(input_ids)
        pos_ids = torch.arange(seq_len, dtype=torch.long, device=device).unsqueeze(0).expand_as(input_ids)

        embeddings = (
            self.disease_embeddings(input_ids) +
            self.age_embeddings(age_ids) +
            self.pos_embeddings(pos_ids.clamp(0, 511)) +
            self.seg_embeddings(seg_ids)
        )
        return self.dropout(self.layer_norm(embeddings))


class BEHRTModel(nn.Module):
    def __init__(self, vocab_size: int, hidden_size: int = 240, num_layers: int = 4, num_heads: int = 4):
        super().__init__()
        self.embeddings = BEHRTEmbeddings(vocab_size, hidden_size)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_size,
            nhead=num_heads,
            dim_feedforward=hidden_size * 4,
            dropout=0.1,
            activation='gelu',
            batch_first=True
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.cls_head = nn.Linear(hidden_size, vocab_size)

    def forward(self, input_ids):
        x = self.embeddings(input_ids)
        src_key_padding_mask = (input_ids == 0)
        encoded = self.encoder(x, src_key_padding_mask=src_key_padding_mask)
        prediction_scores = self.cls_head(encoded)
        return encoded, prediction_scores


# ==========================================
# 2. פונקציות Collate והערכת איכות ה-MLM
# ==========================================
def behrt_train_collate_fn(batch, pad_token_id=0, mask_token_id=3, mlm_prob=0.15):
    input_ids_list = [item['input_ids'] for item in batch]
    padded_inputs = torch.nn.utils.rnn.pad_sequence(input_ids_list, batch_first=True, padding_value=pad_token_id)
    mlm_labels = padded_inputs.clone()

    prob_matrix = torch.full(padded_inputs.shape, mlm_prob)
    special_mask = (padded_inputs <= 2) | (padded_inputs == pad_token_id)
    prob_matrix.masked_fill_(special_mask, 0.0)

    masked_indices = torch.bernoulli(prob_matrix).bool()
    mlm_labels[~masked_indices] = -100

    replace_mask = torch.bernoulli(torch.full(padded_inputs.shape, 0.8)).bool() & masked_indices
    padded_inputs[replace_mask] = mask_token_id

    return {'input_ids': padded_inputs, 'labels': mlm_labels}


def behrt_eval_collate_fn(batch, pad_token_id=0):
    input_ids_list = [item['input_ids'] for item in batch]
    padded_inputs = torch.nn.utils.rnn.pad_sequence(input_ids_list, batch_first=True, padding_value=pad_token_id)
    return {
        'input_ids': padded_inputs,
        'mortality': torch.tensor([item['mortality'] for item in batch], dtype=torch.long),
        'rehospitalization': torch.tensor([item['rehospitalization'] for item in batch], dtype=torch.long),
        'future_disease': torch.stack([item['future_disease'] for item in batch])
    }


def evaluate_mlm_quality(model, val_mlm_loader, criterion, vocab_size, device):
    """
    בודק את איכות הייצוג של שלב ה-MLM על סט הוולידציה:
    - Val MLM Loss & Perplexity
    - Top-1 Precision (דיוק בניחוש המדויק של האבחנה שהוסתרה)
    - Top-5 & Top-10 Recall (האם האבחנה שהוסתרה נמצאת בין 5/10 הניחושים המובילים)
    """
    model.eval()
    total_val_loss = 0.0
    total_masked = 0
    correct_top1 = 0
    correct_top5 = 0
    correct_top10 = 0

    with torch.no_grad():
        for batch in val_mlm_loader:
            inputs = batch['input_ids'].to(device)
            targets = batch['labels'].to(device)

            _, logits = model(inputs)
            loss = criterion(logits.view(-1, vocab_size), targets.view(-1))
            total_val_loss += loss.item()

            # חישוב דיוק רק עבור הטוקנים שהוסתרו (targets != -100)
            mask = (targets != -100)
            if mask.sum().item() > 0:
                masked_logits = logits[mask]        # [num_masked, vocab_size]
                masked_targets = targets[mask]      # [num_masked]

                top10_preds = torch.topk(masked_logits, k=min(10, vocab_size), dim=-1).indices
                correct_top1 += (top10_preds[:, 0] == masked_targets).sum().item()
                correct_top5 += (top10_preds[:, :5] == masked_targets.unsqueeze(1)).any(dim=1).sum().item()
                correct_top10 += (top10_preds == masked_targets.unsqueeze(1)).any(dim=1).sum().item()
                total_masked += masked_targets.size(0)

    avg_val_loss = total_val_loss / max(len(val_mlm_loader), 1)
    perplexity = math.exp(min(avg_val_loss, 20.0))
    top1_acc = correct_top1 / max(total_masked, 1)
    top5_acc = correct_top5 / max(total_masked, 1)
    top10_acc = correct_top10 / max(total_masked, 1)

    model.train()
    return {
        "val_mlm_loss": avg_val_loss,
        "val_perplexity": perplexity,
        "val_top1_acc": top1_acc,
        "val_top5_recall": top5_acc,
        "val_top10_recall": top10_acc
    }


# ==========================================
# 3. הערכה מדעית: Grid-Search ו-10 חזרות Bootstrap
# ==========================================
def evaluate_binary_task_bootstrap(X_train, y_train, X_val, y_val, X_test, y_test, task_name, n_bootstraps=10):
    """ביצוע Grid-Search ו-10 חזרות Bootstrap עבור סיווג בינארי (Mortality, Rehospitalization)"""
    if len(np.unique(y_test)) < 2 or len(np.unique(y_train)) < 2:
        logging.warning(f"[{task_name}] Skipping evaluation: only one class present in labels (unique: {np.unique(y_test)}).")
        return 0.0, 0.0, (0.0, 0.0), [0.0] * n_bootstraps, np.zeros(len(y_test)), None

    param_grid = {
        'n_estimators': [50, 100, 250],
        'max_depth': [5, 10, None],
        'class_weight': ['balanced']
    }

    best_auc_val = -1.0
    best_params = {'n_estimators': 100, 'max_depth': 10, 'class_weight': 'balanced'}

    if len(np.unique(y_val)) > 1:
        for params in ParameterGrid(param_grid):
            clf = RandomForestClassifier(**params, random_state=42, n_jobs=4)
            clf.fit(X_train, y_train)
            preds = clf.predict_proba(X_val)[:, 1]
            val_auc = roc_auc_score(y_val, preds)
            if not np.isnan(val_auc) and val_auc > best_auc_val:
                best_auc_val = val_auc
                best_params = params

    logging.info(f"[{task_name}] Best Params on Val: {best_params} (Val AUROC: {best_auc_val:.4f})")

    test_aucs = []
    all_test_preds = []
    last_clf = None
    for seed in range(n_bootstraps):
        X_res, y_res = resample(X_train, y_train, replace=True, random_state=seed)
        if len(np.unique(y_res)) < 2:
            continue
        clf = RandomForestClassifier(**best_params, random_state=seed, n_jobs=4)
        clf.fit(X_res, y_res)
        last_clf = clf
        preds = clf.predict_proba(X_test)[:, 1]
        all_test_preds.append(preds)
        auc = roc_auc_score(y_test, preds)
        test_aucs.append(auc)

    if not test_aucs:
        return 0.0, 0.0, (0.0, 0.0), [0.0] * n_bootstraps, np.zeros(len(y_test)), None

    mean_preds = np.mean(all_test_preds, axis=0)
    mean_auc = float(np.mean(test_aucs))
    std_auc = float(np.std(test_aucs))
    ci_lower = float(np.percentile(test_aucs, 2.5))
    ci_upper = float(np.percentile(test_aucs, 97.5))
    return mean_auc, std_auc, (ci_lower, ci_upper), test_aucs, mean_preds, last_clf


def evaluate_multilabel_task_bootstrap(X_train, y_train, X_test, y_test, n_bootstraps=10):
    """ביצוע 10 חזרות Bootstrap וחישוב Macro-AUROC עבור חיזוי 25 פרקי המחלות (Future Disease)"""
    base_rf = RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced', random_state=42, n_jobs=4)
    clf = MultiOutputClassifier(base_rf)

    test_macro_aucs = []
    for seed in range(n_bootstraps):
        X_res, y_res = resample(X_train, y_train, replace=True, random_state=seed)
        clf.fit(X_res, y_res)

        preds = np.array([
            est.predict_proba(X_test)[:, 1] if len(est.classes_) > 1 else np.zeros(len(X_test))
            for est in clf.estimators_
        ]).T

        valid_aucs = []
        for i in range(y_test.shape[1]):
            if len(np.unique(y_test[:, i])) > 1:
                valid_aucs.append(roc_auc_score(y_test[:, i], preds[:, i]))

        if valid_aucs:
            test_macro_aucs.append(np.mean(valid_aucs))

    if not test_macro_aucs:
        return 0.0, 0.0, (0.0, 0.0), [0.0] * n_bootstraps

    mean_auc = float(np.mean(test_macro_aucs))
    std_auc = float(np.std(test_macro_aucs))
    ci_lower = float(np.percentile(test_macro_aucs, 2.5))
    ci_upper = float(np.percentile(test_macro_aucs, 97.5))
    return mean_auc, std_auc, (ci_lower, ci_upper), test_macro_aucs


# ==========================================
# 4. תהליך ההרצה הראשי (Pipeline)
# ==========================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="mimic", choices=["mimic", "omop_raw", "omop_aggregated"])
    parser.add_argument("--data_dir", type=str, default="data")
    parser.add_argument("--mapping_csv", type=str, default="mapping_snomed_depth3_production_clean.csv")
    parser.add_argument("--epochs", type=int, default=60)
    parser.add_argument("--batch_size", type=int, default=64)
    parser.add_argument("--lr", type=float, default=5e-4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output_dir", type=str, default="results")
    args = parser.parse_args()

    np.random.seed(args.seed)
    torch.manual_seed(args.seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(args.seed)

    os.makedirs(args.output_dir, exist_ok=True)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logging.info(f"Running on: {device} | Mode: {args.mode}")

    # שלב 1: טעינת רשומות ו-3 תוויות
    records, vocab_to_id = extract_patient_records(
        data_dir=args.data_dir,
        mapping_csv_path=args.mapping_csv,
        mode=args.mode
    )
    tokenized_seqs, y_mort, y_rehosp, y_fut = build_token_sequences(records, vocab_to_id)

    num_samples = len(tokenized_seqs)
    indices = np.random.permutation(num_samples)
    train_end = int(0.7 * num_samples)
    val_end = int(0.85 * num_samples)

    train_idx = indices[:train_end]
    val_idx = indices[train_end:val_end]
    test_idx = indices[val_end:]

    def get_ds(idx):
        return ClinicalEHRDataset(
            [tokenized_seqs[i] for i in idx],
            [y_mort[i] for i in idx],
            [y_rehosp[i] for i in idx],
            [y_fut[i] for i in idx]
        )

    train_ds, val_ds, test_ds = get_ds(train_idx), get_ds(val_idx), get_ds(test_idx)

    mask_id = vocab_to_id.get('[MASK]', 3)
    train_loader = DataLoader(
        train_ds, batch_size=args.batch_size, shuffle=True,
        collate_fn=lambda b: behrt_train_collate_fn(b, pad_token_id=0, mask_token_id=mask_id)
    )
    val_mlm_loader = DataLoader(
        val_ds, batch_size=args.batch_size, shuffle=False,
        collate_fn=lambda b: behrt_train_collate_fn(b, pad_token_id=0, mask_token_id=mask_id)
    )

    # שלב 2: אימון מקדים MLM + בדיקת איכות מלאה בכל Epoch + שמירת Best Model
    vocab_size = len(vocab_to_id)
    model = BEHRTModel(vocab_size=vocab_size, hidden_size=240, num_layers=4, num_heads=4).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=0.01)
    criterion = nn.CrossEntropyLoss(ignore_index=-100)

    logging.info(f"--- Phase 1: Pre-training MLM ({args.epochs} epochs) with Validation Quality Tracking ---")
    loss_history = []
    best_val_loss = float('inf')
    best_epoch = 1
    best_model_weights = copy.deepcopy(model.state_dict())

    model.train()
    for epoch in range(1, args.epochs + 1):
        total_loss = 0.0
        for step, batch in enumerate(train_loader):
            inputs = batch['input_ids'].to(device)
            targets = batch['labels'].to(device)

            optimizer.zero_grad()
            _, logits = model(inputs)
            loss = criterion(logits.view(-1, vocab_size), targets.view(-1))
            loss.backward()
            optimizer.step()

            total_loss += loss.item()
            if (step + 1) % 200 == 0:
                logging.info(f"Epoch {epoch}/{args.epochs} | Step {step+1}/{len(train_loader)} | Train Loss: {loss.item():.4f}")

        avg_train_loss = total_loss / len(train_loader)
        val_metrics = evaluate_mlm_quality(model, val_mlm_loader, criterion, vocab_size, device)

        # שמירת הצ'קפוינט הטוב ביותר לפי Val MLM Loss
        is_best = ""
        if val_metrics["val_mlm_loss"] < best_val_loss:
            best_val_loss = val_metrics["val_mlm_loss"]
            best_epoch = epoch
            best_model_weights = copy.deepcopy(model.state_dict())
            is_best = " [NEW BEST]"

        row = {
            "epoch": epoch,
            "train_mlm_loss": avg_train_loss,
            **val_metrics
        }
        loss_history.append(row)

        logging.info(
            f"--> Epoch {epoch}/{args.epochs} | Train Loss: {avg_train_loss:.4f} | "
            f"Val Loss: {val_metrics['val_mlm_loss']:.4f} | PPL: {val_metrics['val_perplexity']:.2f} | "
            f"Top-1: {val_metrics['val_top1_acc']*100:.2f}% | Top-5: {val_metrics['val_top5_recall']*100:.2f}% | "
            f"Top-10: {val_metrics['val_top10_recall']*100:.2f}%{is_best}"
        )

    # טעינת המודל הטוב ביותר (לפי Val MLM Loss) ושמירתו לדיסק
    model.load_state_dict(best_model_weights)
    best_model_path = os.path.join(args.output_dir, f"best_behrt_model_{args.mode}.pt")
    torch.save({
        "epoch": best_epoch,
        "val_mlm_loss": best_val_loss,
        "vocab_size": vocab_size,
        "model_state_dict": best_model_weights,
        "vocab_to_id": vocab_to_id
    }, best_model_path)
    logging.info(f"Loaded & saved Best BEHRT Model from Epoch {best_epoch} (Val MLM Loss: {best_val_loss:.4f}) -> {best_model_path}")

    pd.DataFrame(loss_history).to_csv(
        os.path.join(args.output_dir, f"loss_history_{args.mode}.csv"), index=False
    )

    # שלב 3: חילוץ וקטורי [CLS] נקיים מתוך המודל הטוב ביותר
    logging.info("--- Phase 2: Extracting clean [CLS] embeddings from Best Model ---")
    model.eval()

    def extract_cls(dataset):
        loader = DataLoader(dataset, batch_size=args.batch_size, shuffle=False, collate_fn=behrt_eval_collate_fn)
        feats, mort, rehosp, fut = [], [], [], []
        with torch.no_grad():
            for batch in loader:
                inputs = batch['input_ids'].to(device)
                encoded, _ = model(inputs)
                feats.append(encoded[:, 0, :].cpu().numpy())
                mort.append(batch['mortality'].numpy())
                rehosp.append(batch['rehospitalization'].numpy())
                fut.append(batch['future_disease'].numpy())
        return np.vstack(feats), np.concatenate(mort), np.concatenate(rehosp), np.vstack(fut)

    X_train, y_mort_train, y_rehosp_train, y_fut_train = extract_cls(train_ds)
    X_val, y_mort_val, y_rehosp_val, _ = extract_cls(val_ds)
    X_test, y_mort_test, y_rehosp_test, y_fut_test = extract_cls(test_ds)

    # שלב 4: הערכת כל 3 התרחישים עם Bootstrap
    logging.info("--- Phase 3: Evaluating all 3 Clinical Tasks (10 Bootstrap Iterations) ---")

    # תרחיש 1: Mortality
    mean_m, std_m, ci_m, boots_m, preds_m, clf_m = evaluate_binary_task_bootstrap(
        X_train, y_mort_train, X_val, y_mort_val, X_test, y_mort_test, task_name="Mortality"
    )
    logging.info(f"=== [Mortality] AUROC: {mean_m:.4f} +/- {std_m:.4f} | 95% CI: [{ci_m[0]:.4f}, {ci_m[1]:.4f}] ===")

    # תרחיש 2: Rehospitalization
    mean_r, std_r, ci_r, boots_r, preds_r, clf_r = evaluate_binary_task_bootstrap(
        X_train, y_rehosp_train, X_val, y_rehosp_val, X_test, y_rehosp_test, task_name="Rehospitalization"
    )
    logging.info(f"=== [Rehospitalization] AUROC: {mean_r:.4f} +/- {std_r:.4f} | 95% CI: [{ci_r[0]:.4f}, {ci_r[1]:.4f}] ===")

    # תרחיש 3: Future Disease
    mean_f, std_f, ci_f, boots_f = evaluate_multilabel_task_bootstrap(
        X_train, y_fut_train, X_test, y_fut_test, n_bootstraps=10
    )
    logging.info(f"=== [Future Disease] Macro AUROC: {mean_f:.4f} +/- {std_f:.4f} | 95% CI: [{ci_f[0]:.4f}, {ci_f[1]:.4f}] ===")

    # שמירת תוצאות מלאה ל-NPZ (כולל embeddings, תוויות והסתברויות עבור גרפים ו-UMAP)
    res_path = os.path.join(args.output_dir, f"eval_bundle_{args.mode}.npz")
    np.savez_compressed(
        res_path,
        X_train=X_train, X_val=X_val, X_test=X_test,
        y_mort_test=y_mort_test, mort_probs=preds_m,
        mortality_mean=mean_m, mortality_std=std_m, mortality_boots=boots_m,
        y_rehosp_test=y_rehosp_test, rehosp_probs=preds_r,
        rehosp_mean=mean_r, rehosp_std=std_r, rehosp_boots=boots_r,
        y_fut_test=y_fut_test,
        future_disease_mean=mean_f, future_disease_std=std_f, future_disease_boots=boots_f
    )

    # שמירת מודלי ה-Random Forest עבור ניתוחי SHAP עתידיים
    with open(os.path.join(args.output_dir, f"rf_models_{args.mode}.pkl"), "wb") as f:
        pickle.dump({"mortality_rf": clf_m, "rehosp_rf": clf_r}, f)

    best_row = loss_history[best_epoch - 1]
    summary_file = os.path.join(args.output_dir, "benchmark_summary.txt")
    with open(summary_file, "a") as f:
        f.write(f"\n================ Mode: {args.mode} (Vocab: {vocab_size:,} | Best MLM Epoch: {best_epoch}) ================\n")
        f.write(
            f"MLM Quality (Val):       Loss={best_row['val_mlm_loss']:.4f} | PPL={best_row['val_perplexity']:.2f} | "
            f"Top-1={best_row['val_top1_acc']*100:.2f}% | Top-5={best_row['val_top5_recall']*100:.2f}% | Top-10={best_row['val_top10_recall']*100:.2f}%\n"
        )
        f.write(f"Mortality AUROC:         {mean_m:.4f} +/- {std_m:.4f} (95% CI: {ci_m[0]:.4f}-{ci_m[1]:.4f})\n")
        f.write(f"Rehospitalization AUROC: {mean_r:.4f} +/- {std_r:.4f} (95% CI: {ci_r[0]:.4f}-{ci_r[1]:.4f})\n")
        f.write(f"Future Disease AUROC:    {mean_f:.4f} +/- {std_f:.4f} (95% CI: {ci_f[0]:.4f}-{ci_f[1]:.4f})\n")



if __name__ == "__main__":
    main()
