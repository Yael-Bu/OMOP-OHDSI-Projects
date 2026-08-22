import json
import logging
import os
import pickle
import random
import shutil
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sqlalchemy import create_engine
import torch
import torch.nn as nn
from torch.optim.lr_scheduler import CosineAnnealingLR, ReduceLROnPlateau
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# =====================================================================
# 1. DATA EXTRACTION
# =====================================================================
def extract_omop_data(db_uri: str, schema: str = "omop") -> pd.DataFrame:
    """Extracts diagnosis records and chronological visit sequences from OMOP CDM."""
    logging.info("Extracting diagnoses and visit sequences from OMOP database...")
    engine = create_engine(db_uri)

    sql_query = f"""
    WITH patient_visits AS (
        SELECT 
            vo.person_id AS subject_id,
            vo.visit_occurrence_id AS hadm_id,
            vo.visit_start_date,
            EXTRACT(YEAR FROM vo.visit_start_date) - p.year_of_birth AS age_at_visit,
            DENSE_RANK() OVER (
                PARTITION BY vo.person_id 
                ORDER BY vo.visit_start_date ASC, vo.visit_occurrence_id ASC
            ) AS visit_rank
        FROM 
            {schema}.visit_occurrence vo
            INNER JOIN {schema}.person p ON vo.person_id = p.person_id
    ),
    diagnoses AS (
        SELECT 
            co.person_id AS subject_id,
            co.visit_occurrence_id AS hadm_id,
            co.condition_concept_id,
            c.concept_code AS raw_code,
            c.concept_name AS condition_name,
            c.vocabulary_id
        FROM 
            {schema}.condition_occurrence co
            INNER JOIN {schema}.concept c ON co.condition_concept_id = c.concept_id
        WHERE 
            c.vocabulary_id IN ('SNOMED', 'ICD9CM', 'ICD10CM')
    )
    SELECT 
        v.subject_id,
        v.hadm_id,
        v.visit_start_date,
        v.age_at_visit,
        v.visit_rank,
        d.raw_code,
        d.condition_name,
        d.vocabulary_id
    FROM 
        patient_visits v
        INNER JOIN diagnoses d ON v.hadm_id = d.hadm_id
    ORDER BY 
        v.subject_id ASC, 
        v.visit_rank ASC;
    """

    df = pd.read_sql(sql_query, con=engine)
    logging.info(f"Extracted {len(df):,} total diagnosis records.")
    return df


# =====================================================================
# 2. 1:N POLY-HIERARCHY MAPPING & SINGLE-VISIT DEDUPLICATION
# =====================================================================
def apply_custom_mapping(
    df: pd.DataFrame,
    mapping_csv_path: Optional[str] = None,
    src_col: Optional[str] = None,
    tgt_col: Optional[str] = None,
) -> pd.DataFrame:
    """Preserves 1:N poly-hierarchy mappings via relational merge and deduplicates per visit[cite: 1, 2]."""
    df_clean = df.copy()
    df_clean["raw_code"] = df_clean["raw_code"].astype(str).str.strip()

    if mapping_csv_path is None or not os.path.exists(mapping_csv_path):
        logging.info("No mapping CSV provided. Using raw diagnosis codes (Baseline Control)[cite: 2].")
        df_clean["aggregated_code"] = df_clean["raw_code"]
    else:
        logging.info(f"Applying mapping from {mapping_csv_path} ({src_col} -> {tgt_col})...")
        map_df = pd.read_csv(mapping_csv_path, dtype=str)
        map_df = map_df[[src_col, tgt_col]].dropna().drop_duplicates()
        map_df[src_col] = map_df[src_col].str.strip()
        map_df[tgt_col] = map_df[tgt_col].str.strip()

        df_merged = df_clean.merge(
            map_df, left_on="raw_code", right_on=src_col, how="left"
        )
        df_merged["aggregated_code"] = df_merged[tgt_col].fillna(df_merged["raw_code"])
        df_clean = df_merged.drop(columns=[src_col, tgt_col], errors="ignore")

    initial_count = len(df_clean)
    df_dedup = df_clean.drop_duplicates(
        subset=["subject_id", "visit_rank", "aggregated_code"]
    ).copy()
    logging.info(
        f"Mapping & deduplication: {initial_count:,} -> {len(df_dedup):,} unique visit-code rows[cite: 2]."
    )
    return df_dedup


def build_vocabulary(
    df: pd.DataFrame, special_tokens: Optional[List[str]] = None
) -> Tuple[Dict[str, int], Dict[int, str]]:
    """Builds token-to-index dictionary with standard BEHRT special tokens[cite: 3]."""
    if special_tokens is None:
        special_tokens = ["[CLS]", "[PAD]", "[UNK]", "[SEP]", "[MASK]"]

    unique_codes = sorted(df["aggregated_code"].dropna().unique().tolist())
    vocab = {token: idx for idx, token in enumerate(special_tokens + unique_codes)}
    inv_vocab = {v: k for k, v in vocab.items()}
    logging.info(f"Vocabulary created with {len(vocab):,} unique tokens[cite: 3].")
    return vocab, inv_vocab


def prepare_sequences(
    df: pd.DataFrame, vocab: Dict[str, int], min_visits: int = 2
) -> List[Dict]:
    """Generates sequential timelines formatted with [CLS] and [SEP] tokens[cite: 3]."""
    df["code_id"] = df["aggregated_code"].map(lambda x: vocab.get(x, vocab["[UNK]"]))
    patient_records = []
    grouped_patients = df.groupby("subject_id")

    for person_id, p_group in grouped_patients:
        visits_list = []
        ages_list = []

        for _, v_group in p_group.groupby("visit_rank"):
            v_codes = v_group["code_id"].tolist()
            v_age = int(v_group["age_at_visit"].iloc[0])
            visits_list.append(v_codes)
            ages_list.append(v_age)

        if len(visits_list) >= min_visits:
            # 1. Full sequence for Masked Language Modeling Pre-training
            all_codes = [vocab["[CLS]"]]
            all_ages = [ages_list[0]]
            all_visit_ids = [0]

            for idx, (v_codes, v_age) in enumerate(zip(visits_list, ages_list)):
                all_codes.extend(v_codes + [vocab["[SEP]"]])
                all_ages.extend([v_age] * (len(v_codes) + 1))
                all_visit_ids.extend([idx + 1] * (len(v_codes) + 1))

            # 2. History sequence (up to penultimate visit) for Fine-tuning[cite: 3]
            train_codes = [vocab["[CLS]"]]
            train_ages = [ages_list[0]]
            train_visit_ids = [0]

            for idx, (v_codes, v_age) in enumerate(
                zip(visits_list[:-1], ages_list[:-1])
            ):
                train_codes.extend(v_codes + [vocab["[SEP]"]])
                train_ages.extend([v_age] * (len(v_codes) + 1))
                train_visit_ids.extend([idx + 1] * (len(v_codes) + 1))

            patient_records.append(
                {
                    "person_id": person_id,
                    "all_codes": all_codes,
                    "all_ages": all_ages,
                    "all_visit_ids": all_visit_ids,
                    "train_codes": train_codes,
                    "train_ages": train_ages,
                    "train_visit_ids": train_visit_ids,
                    "target_next_visit": visits_list[-1],
                }
            )

    return patient_records


# =====================================================================
# 3. PYTORCH DATASETS (MLM & NEXT VISIT)
# =====================================================================
class BEHRTMLMDataset(Dataset):
    """PyTorch Dataset for Stage 1: Masked Language Modeling (Pre-training)."""

    def __init__(
        self,
        data: List[Dict],
        max_len: int,
        vocab: Dict[str, int],
        mask_prob: float = 0.15,
        max_age: int = 120,
        max_visits: int = 100,
    ):
        self.data = data
        self.max_len = max_len
        self.vocab = vocab
        self.mask_prob = mask_prob
        self.max_age = max_age
        self.max_visits = max_visits
        self.special_ids = {
            vocab["[CLS]"],
            vocab["[PAD]"],
            vocab["[UNK]"],
            vocab["[SEP]"],
            vocab["[MASK]"],
        }

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        item = self.data[idx]
        codes = item["all_codes"][: self.max_len]
        ages = item["all_ages"][: self.max_len]
        visits = item["all_visit_ids"][: self.max_len]

        input_ids = []
        labels = []

        for c in codes:
            if c not in self.special_ids and random.random() < self.mask_prob:
                prob = random.random()
                if prob < 0.8:
                    input_ids.append(self.vocab["[MASK]"])
                elif prob < 0.9:
                    input_ids.append(
                        random.randint(len(self.special_ids), len(self.vocab) - 1)
                    )
                else:
                    input_ids.append(c)
                labels.append(c)
            else:
                input_ids.append(c)
                labels.append(-100)

        pad_len = self.max_len - len(input_ids)
        input_ids += [self.vocab["[PAD]"]] * pad_len
        ages += [0] * pad_len
        visits += [0] * pad_len
        labels += [-100] * pad_len

        ages = [max(0, min(int(a), self.max_age)) for a in ages]
        visits = [max(0, min(int(v), self.max_visits)) for v in visits]

        return {
            "input_ids": torch.tensor(input_ids, dtype=torch.long),
            "ages": torch.tensor(ages, dtype=torch.long),
            "visit_ids": torch.tensor(visits, dtype=torch.long),
            "attention_mask": torch.tensor(
                [1 if c != self.vocab["[PAD]"] else 0 for c in input_ids],
                dtype=torch.long,
            ),
            "labels": torch.tensor(labels, dtype=torch.long),
        }


class BEHRTNextVisitDataset(Dataset):
    """PyTorch Dataset for Stage 2: Next Visit Multi-label Prediction (Fine-tuning)[cite: 3]."""

    def __init__(
        self,
        data: List[Dict],
        max_len: int,
        vocab: Dict[str, int],
        num_classes: int,
        max_age: int = 120,
        max_visits: int = 100,
    ):
        self.data = data
        self.max_len = max_len
        self.vocab = vocab
        self.num_classes = num_classes
        self.max_age = max_age
        self.max_visits = max_visits

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        item = self.data[idx]
        codes = item["train_codes"][: self.max_len]
        ages = item["train_ages"][: self.max_len]
        visits = item["train_visit_ids"][: self.max_len]

        pad_len = self.max_len - len(codes)
        codes = codes + [self.vocab["[PAD]"]] * pad_len
        ages = ages + [0] * pad_len
        visits = visits + [0] * pad_len

        ages = [max(0, min(int(a), self.max_age)) for a in ages]
        visits = [max(0, min(int(v), self.max_visits)) for v in visits]

        target_vector = np.zeros(self.num_classes, dtype=np.float32)
        for code_id in item["target_next_visit"]:
            if code_id < self.num_classes:
                target_vector[code_id] = 1.0

        return {
            "input_ids": torch.tensor(codes, dtype=torch.long),
            "ages": torch.tensor(ages, dtype=torch.long),
            "visit_ids": torch.tensor(visits, dtype=torch.long),
            "attention_mask": torch.tensor(
                [1 if c != self.vocab["[PAD]"] else 0 for c in codes],
                dtype=torch.long,
            ),
            "targets": torch.tensor(target_vector, dtype=torch.float32),
        }


# =====================================================================
# 4. BEHRT MODEL ARCHITECTURE (4-WAY EMBEDDINGS + CLS POOLING)
# =====================================================================
class BEHRTBaseEncoder(nn.Module):
    """BEHRT Transformer Encoder with Concept, Position, Segment, and Age Embeddings[cite: 3]."""

    def __init__(
        self,
        vocab_size: int,
        max_len: int = 256,
        max_age: int = 120,
        max_visits: int = 100,
        hidden_size: int = 256,
        num_layers: int = 4,
        num_heads: int = 4,
        dropout: float = 0.1,
    ):
        super().__init__()
        self.vocab_size = vocab_size
        self.max_len = max_len
        self.max_age = max_age
        self.max_visits = max_visits

        self.concept_embeddings = nn.Embedding(vocab_size, hidden_size)
        self.position_embeddings = nn.Embedding(max_len, hidden_size)
        self.segment_embeddings = nn.Embedding(max_visits + 1, hidden_size)
        self.age_embeddings = nn.Embedding(max_age + 1, hidden_size)

        self.layer_norm = nn.LayerNorm(hidden_size)
        self.dropout = nn.Dropout(dropout)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_size,
            nhead=num_heads,
            dim_feedforward=hidden_size * 4,
            dropout=dropout,
            batch_first=True,
        )
        self.transformer_encoder = nn.TransformerEncoder(
            encoder_layer, num_layers=num_layers
        )

    def forward(
        self,
        input_ids: torch.Tensor,
        ages: torch.Tensor,
        visit_ids: torch.Tensor,
        attention_mask: torch.Tensor,
    ) -> torch.Tensor:
        seq_length = input_ids.size(1)
        position_ids = torch.arange(
            seq_length, dtype=torch.long, device=input_ids.device
        )
        position_ids = position_ids.unsqueeze(0).expand_as(input_ids)

        input_ids = torch.clamp(input_ids, min=0, max=self.vocab_size - 1)
        position_ids = torch.clamp(position_ids, min=0, max=self.max_len - 1)
        ages = torch.clamp(ages, min=0, max=self.max_age)
        visit_ids = torch.clamp(visit_ids, min=0, max=self.max_visits)

        token_emb = self.concept_embeddings(input_ids)
        pos_emb = self.position_embeddings(position_ids)
        seg_emb = self.segment_embeddings(visit_ids)
        age_emb = self.age_embeddings(ages)

        embeddings = self.dropout(
            self.layer_norm(token_emb + pos_emb + seg_emb + age_emb)
        )
        return self.transformer_encoder(
            embeddings, src_key_padding_mask=(attention_mask == 0)
        )


class BEHRTForMaskedLM(nn.Module):
    """BEHRT Pre-training Model with Token Prediction Head."""

    def __init__(self, encoder: BEHRTBaseEncoder):
        super().__init__()
        self.encoder = encoder
        hidden_size = encoder.concept_embeddings.embedding_dim
        self.mlm_head = nn.Linear(hidden_size, encoder.vocab_size)

    def forward(
        self,
        input_ids: torch.Tensor,
        ages: torch.Tensor,
        visit_ids: torch.Tensor,
        attention_mask: torch.Tensor,
    ) -> torch.Tensor:
        encoded_seq = self.encoder(input_ids, ages, visit_ids, attention_mask)
        return self.mlm_head(encoded_seq)


class BEHRTForNextVisit(nn.Module):
    """BEHRT Fine-tuning Model with [CLS] Token Representation[cite: 3]."""

    def __init__(self, encoder: BEHRTBaseEncoder):
        super().__init__()
        self.encoder = encoder
        hidden_size = encoder.concept_embeddings.embedding_dim
        self.classifier = nn.Linear(hidden_size, encoder.vocab_size)

    def forward(
        self,
        input_ids: torch.Tensor,
        ages: torch.Tensor,
        visit_ids: torch.Tensor,
        attention_mask: torch.Tensor,
    ) -> torch.Tensor:
        encoded_seq = self.encoder(input_ids, ages, visit_ids, attention_mask)
        cls_token_rep = encoded_seq[:, 0, :]
        return self.classifier(cls_token_rep)


# =====================================================================
# 5. METRICS EVALUATION & PLOTTING FUNCTIONS
# =====================================================================
def evaluate_next_visit(
    model: nn.Module,
    dataloader: DataLoader,
    criterion: nn.Module,
    device: torch.device,
    vocab_size: int,
    decision_threshold: float = 0.5,
) -> Dict[str, float]:
    """Computes full suite of evaluation metrics for the fine-tuning stage[cite: 3]."""
    model.eval()
    total_loss = 0.0

    all_logits, all_targets = [], []
    mrr_list, r10_list, r20_list, r_top1pct_list, r_top5pct_list = (
        [],
        [],
        [],
        [],
        [],
    )

    k_top1pct = max(1, int(vocab_size * 0.01))
    k_top5pct = max(1, int(vocab_size * 0.05))

    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch["input_ids"].to(device)
            ages = batch["ages"].to(device)
            visit_ids = batch["visit_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            targets = batch["targets"].to(device)

            logits = model(input_ids, ages, visit_ids, attention_mask)
            total_loss += criterion(logits, targets).item()

            logits_np = logits.cpu().numpy()
            targets_np = targets.cpu().numpy()
            all_logits.append(logits_np)
            all_targets.append(targets_np)

            for i in range(targets_np.shape[0]):
                actual = np.where(targets_np[i] == 1.0)[0]
                if len(actual) == 0:
                    continue
                actual_set = set(actual)
                sorted_idx = np.argsort(-logits_np[i])

                for rank, idx in enumerate(sorted_idx, start=1):
                    if idx in actual_set:
                        mrr_list.append(1.0 / rank)
                        break

                r10_list.append(
                    len(set(sorted_idx[:10]).intersection(actual_set))
                    / len(actual_set)
                )
                r20_list.append(
                    len(set(sorted_idx[:20]).intersection(actual_set))
                    / len(actual_set)
                )
                r_top1pct_list.append(
                    len(set(sorted_idx[:k_top1pct]).intersection(actual_set))
                    / len(actual_set)
                )
                r_top5pct_list.append(
                    len(set(sorted_idx[:k_top5pct]).intersection(actual_set))
                    / len(actual_set)
                )

    y_true = np.vstack(all_targets)
    y_prob = 1.0 / (1.0 + np.exp(-np.clip(np.vstack(all_logits), -15, 15)))
    y_pred = (y_prob >= decision_threshold).astype(np.float32)

    results = {
        "Loss": total_loss / len(dataloader),
        "MRR": float(np.mean(mrr_list)) if mrr_list else 0.0,
        "Recall@10": float(np.mean(r10_list)) if r10_list else 0.0,
        "Recall@20": float(np.mean(r20_list)) if r20_list else 0.0,
        f"Recall@Top_1% (K={k_top1pct})": (
            float(np.mean(r_top1pct_list)) if r_top1pct_list else 0.0
        ),
        f"Recall@Top_5% (K={k_top5pct})": (
            float(np.mean(r_top5pct_list)) if r_top5pct_list else 0.0
        ),
    }

    valid_cols = [
        c for c in range(y_true.shape[1]) if len(np.unique(y_true[:, c])) > 1
    ]
    if valid_cols:
        y_t, y_p, y_b = (
            y_true[:, valid_cols],
            y_prob[:, valid_cols],
            y_pred[:, valid_cols],
        )
        try:
            results["Macro_PR_AUC"] = float(
                average_precision_score(y_t, y_p, average="macro")
            )
            results["Micro_PR_AUC"] = float(
                average_precision_score(y_t, y_p, average="micro")
            )
        except Exception:
            results["Macro_PR_AUC"], results["Micro_PR_AUC"] = 0.0, 0.0

        try:
            results["Macro_AUROC"] = float(
                roc_auc_score(y_t, y_p, average="macro")
            )
            results["Micro_AUROC"] = float(
                roc_auc_score(y_t, y_p, average="micro")
            )
        except Exception:
            results["Macro_AUROC"], results["Micro_AUROC"] = 0.0, 0.0

        try:
            results["Macro_Precision"] = float(
                precision_score(y_t, y_b, average="macro", zero_division=0)
            )
            results["Macro_Recall"] = float(
                recall_score(y_t, y_b, average="macro", zero_division=0)
            )
            results["Macro_F1"] = float(
                f1_score(y_t, y_b, average="macro", zero_division=0)
            )
        except Exception:
            results["Macro_Precision"] = 0.0
            results["Macro_Recall"] = 0.0
            results["Macro_F1"] = 0.0
    else:
        results["Macro_PR_AUC"] = 0.0
        results["Micro_PR_AUC"] = 0.0
        results["Macro_AUROC"] = 0.0
        results["Micro_AUROC"] = 0.0
        results["Macro_Precision"] = 0.0
        results["Macro_Recall"] = 0.0
        results["Macro_F1"] = 0.0

    return results


def plot_single_experiment_history(
    history: dict, output_path: str, exp_name: str
):
    """Generates and saves detailed 4-panel training and validation metric curves for a single experiment."""
    epochs = range(1, len(history["ft_train_loss"]) + 1)
    mlm_epochs = range(1, len(history["mlm_train_loss"]) + 1)

    plt.style.use(
        "seaborn-v0_8-whitegrid"
        if "seaborn-v0_8-whitegrid" in plt.style.available
        else "default"
    )
    fig, axes = plt.subplots(2, 2, figsize=(16, 11))
    fig.suptitle(f"Training History & Performance: {exp_name}", fontsize=16, fontweight="bold")

    # Panel 1: Stage 1 MLM Pre-training Loss
    ax1 = axes[0, 0]
    ax1.plot(mlm_epochs, history["mlm_train_loss"], label="MLM Train Loss", marker="o", color="#2b5c8f")
    ax1.plot(mlm_epochs, history["mlm_val_loss"], label="MLM Val Loss", marker="s", color="#e7298a")
    ax1.set_title("Stage 1: MLM Pre-training Loss Curve", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Epoch")
    ax1.set_ylabel("Cross Entropy Loss")
    ax1.legend()
    ax1.grid(True, linestyle="--", alpha=0.6)

    # Panel 2: Stage 2 Fine-Tuning Loss
    ax2 = axes[0, 1]
    ax2.plot(epochs, history["ft_train_loss"], label="FT Train Loss", marker="o", color="#1b9e77")
    ax2.plot(epochs, history["ft_val_loss"], label="FT Val Loss", marker="s", color="#d95f02")
    ax2.set_title("Stage 2: Fine-Tuning Next-Visit Loss", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Epoch")
    ax2.set_ylabel("Weighted BCE Loss")
    ax2.legend()
    ax2.grid(True, linestyle="--", alpha=0.6)

    # Panel 3: Validation Ranking Dynamics (MRR & Recall@10)
    ax3 = axes[1, 0]
    ax3.plot(epochs, history["val_mrr"], label="MRR", marker="^", color="#2b5c8f")
    ax3.plot(epochs, history["val_recall10"], label="Recall@10", marker="d", color="#1f78b4")
    ax3.plot(epochs, history["val_recall20"], label="Recall@20", marker="v", color="#33a02c")
    ax3.set_title("Validation Clinical Ranking Trajectory", fontsize=12, fontweight="bold")
    ax3.set_xlabel("Epoch")
    ax3.set_ylabel("Score / Recall Rate")
    ax3.legend()
    ax3.grid(True, linestyle="--", alpha=0.6)

    # Panel 4: Discrimination & Precision-Recall Area
    ax4 = axes[1, 1]
    ax4.plot(epochs, history["val_macro_prauc"], label="Macro PR-AUC", marker="x", color="#e7298a")
    ax4.plot(epochs, history["val_macro_auroc"], label="Macro AUROC", marker="*", color="#7570b3")
    ax4.set_title("Validation Discrimination Trajectory (PR-AUC / AUROC)", fontsize=12, fontweight="bold")
    ax4.set_xlabel("Epoch")
    ax4.set_ylabel("Area Under Curve")
    ax4.legend()
    ax4.grid(True, linestyle="--", alpha=0.6)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(output_path, dpi=300)
    plt.close()
    logging.info(f"Saved experiment training curves to '{output_path}'.")


def plot_benchmark_summary(
    csv_path: str = "aggregation_two_stage_benchmark_summary.csv",
    output_png: str = "benchmark_comparison_charts.png",
):
    """Generates a 4-panel visual comparison plot across all evaluated ontology branches[cite: 3]."""
    if not os.path.exists(csv_path):
        logging.error(f"Cannot generate plots: '{csv_path}' not found[cite: 3].")
        return

    df = pd.read_csv(csv_path)
    if "Aggregation_Name" in df.columns:
        df.set_index("Aggregation_Name", inplace=True)

    plt.style.use(
        "seaborn-v0_8-whitegrid"
        if "seaborn-v0_8-whitegrid" in plt.style.available
        else "default"
    )
    fig, axes = plt.subplots(2, 2, figsize=(18, 13))
    fig.suptitle(
        "BEHRT Multi-Ontology Two-Stage Benchmark Summary",
        fontsize=18,
        fontweight="bold",
        y=0.98,
    )

    exp_names = [name.replace("_", "\n") for name in df.index]
    x = np.arange(len(df))
    width = 0.35

    # Panel 1: MRR & Macro PR-AUC
    ax1 = axes[0, 0]
    mrr_vals = df["MRR"] if "MRR" in df.columns else [0] * len(df)
    pr_auc_vals = (
        df["Macro_PR_AUC"] if "Macro_PR_AUC" in df.columns else [0] * len(df)
    )
    r1 = ax1.bar(x - width / 2, mrr_vals, width, label="MRR", color="#2b5c8f")
    r2 = ax1.bar(
        x + width / 2, pr_auc_vals, width, label="Macro PR-AUC", color="#e7298a"
    )
    ax1.set_title(
        "Ranking Quality (MRR) & Discrimination (Macro PR-AUC)",
        fontsize=13,
        fontweight="bold",
    )
    ax1.set_xticks(x)
    ax1.set_xticklabels(exp_names, fontsize=8)
    ax1.set_ylabel("Score", fontsize=11)
    ax1.legend(loc="upper right")

    # Panel 2: Absolute Recall@10 vs Recall@20
    ax2 = axes[0, 1]
    r10_vals = df["Recall@10"] if "Recall@10" in df.columns else [0] * len(df)
    r20_vals = df["Recall@20"] if "Recall@20" in df.columns else [0] * len(df)
    r3 = ax2.bar(
        x - width / 2, r10_vals, width, label="Recall@10", color="#1f78b4"
    )
    r4 = ax2.bar(
        x + width / 2, r20_vals, width, label="Recall@20", color="#33a02c"
    )
    ax2.set_title(
        "Absolute Top-K Coverage (Recall@10 vs Recall@20)",
        fontsize=13,
        fontweight="bold",
    )
    ax2.set_xticks(x)
    ax2.set_xticklabels(exp_names, fontsize=8)
    ax2.set_ylabel("Recall Rate", fontsize=11)
    ax2.legend(loc="upper left")

    # Panel 3: Relative Recalls (Top 1% vs Top 5%)
    ax3 = axes[1, 0]
    top1_col = [c for c in df.columns if "Top_1%" in c]
    top5_col = [c for c in df.columns if "Top_5%" in c]
    top1_vals = df[top1_col[0]] if top1_col else [0] * len(df)
    top5_vals = df[top5_col[0]] if top5_col else [0] * len(df)
    r5 = ax3.bar(
        x - width / 2, top1_vals, width, label="Recall@Top 1%", color="#ff7f00"
    )
    r6 = ax3.bar(
        x + width / 2, top5_vals, width, label="Recall@Top 5%", color="#6a3d9a"
    )
    ax3.set_title(
        "Normalized Relative Ranking (Top 1% vs Top 5% of Vocab)",
        fontsize=13,
        fontweight="bold",
    )
    ax3.set_xticks(x)
    ax3.set_xticklabels(exp_names, fontsize=8)
    ax3.set_ylabel("Normalized Recall Rate", fontsize=11)
    ax3.legend(loc="upper left")

    # Panel 4: Vocab Size Trade-off
    ax4 = axes[1, 1]
    vocab_sizes = (
        df["Vocab_Size"] if "Vocab_Size" in df.columns else [1000] * len(df)
    )
    ax4.scatter(vocab_sizes, r10_vals, color="#d95f02", s=140, zorder=5)
    for i, txt in enumerate(df.index):
        ax4.annotate(
            f" {txt}",
            (vocab_sizes.iloc[i], r10_vals.iloc[i]),
            fontsize=8,
            fontweight="semibold",
            xytext=(6, -4),
            textcoords="offset points",
        )
    ax4.set_title(
        "Trade-off: Vocabulary Dimension vs Recall@10",
        fontsize=13,
        fontweight="bold",
    )
    ax4.set_xlabel("Vocabulary Size (Log Scale)", fontsize=11)
    ax4.set_ylabel("Recall@10", fontsize=11)
    ax4.set_xscale("log")
    ax4.grid(True, which="both", ls="--", alpha=0.5)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(output_png, dpi=300, bbox_inches="tight")
    logging.info(f"Saved summary comparison charts to '{output_png}'[cite: 3].")
    plt.close()


# =====================================================================
# 6. TWO-STAGE EXPERIMENT RUNNER
# =====================================================================
def run_two_stage_experiment(
    df_raw: pd.DataFrame,
    exp_cfg: dict,
    fixed_train_ids: set,
    fixed_val_ids: set,
    device: torch.device,
    slurm_job_id: str,
    pretrain_epochs: int = 15,
    finetune_epochs: int = 20,
    batch_size: int = 64,
) -> dict:
    """Executes Stage 1 (Masked LM Pre-training) followed by Stage 2 (Fine-tuning)."""
    exp_name = exp_cfg["name"]
    exp_dir = os.path.join("experiments", exp_name)
    os.makedirs(exp_dir, exist_ok=True)

    # 1. Relational 1:N Mapping and Vocabulary
    df_mapped = apply_custom_mapping(
        df=df_raw,
        mapping_csv_path=exp_cfg.get("csv"),
        src_col=exp_cfg.get("src_col"),
        tgt_col=exp_cfg.get("tgt_col"),
    )
    vocab, _ = build_vocabulary(df=df_mapped)
    with open(os.path.join(exp_dir, "vocab.pkl"), "wb") as f:
        pickle.dump(vocab, f)

    all_sequences = prepare_sequences(df=df_mapped, vocab=vocab, min_visits=2)
    train_seqs = [s for s in all_sequences if s["person_id"] in fixed_train_ids]
    val_seqs = [s for s in all_sequences if s["person_id"] in fixed_val_ids]

    # Initialize Core Encoder
    base_encoder = BEHRTBaseEncoder(
        vocab_size=len(vocab),
        max_len=256,
        hidden_size=256,
        num_layers=4,
        num_heads=4,
    ).to(device)

    # Dictionary to track epoch dynamics for plotting
    history = {
        "mlm_train_loss": [],
        "mlm_val_loss": [],
        "ft_train_loss": [],
        "ft_val_loss": [],
        "val_mrr": [],
        "val_recall10": [],
        "val_recall20": [],
        "val_macro_prauc": [],
        "val_macro_auroc": [],
    }

    # -------------------------------------------------------------
    # STAGE 1: MASKED LANGUAGE MODELING PRE-TRAINING
    # -------------------------------------------------------------
    logging.info(
        f"\n[{exp_name}] --- Starting Stage 1: MLM Pre-training ({pretrain_epochs} Epochs) ---"
    )
    mlm_train_ds = BEHRTMLMDataset(train_seqs, max_len=256, vocab=vocab)
    mlm_val_ds = BEHRTMLMDataset(val_seqs, max_len=256, vocab=vocab)

    mlm_train_loader = DataLoader(
        mlm_train_ds, batch_size=batch_size, shuffle=True
    )
    mlm_val_loader = DataLoader(
        mlm_val_ds, batch_size=batch_size, shuffle=False
    )

    mlm_model = BEHRTForMaskedLM(base_encoder).to(device)
    mlm_criterion = nn.CrossEntropyLoss(ignore_index=-100)
    mlm_optimizer = torch.optim.AdamW(
        mlm_model.parameters(), lr=1e-4, weight_decay=0.01
    )
    mlm_scheduler = CosineAnnealingLR(mlm_optimizer, T_max=pretrain_epochs)

    best_mlm_loss = float("inf")
    for epoch in range(pretrain_epochs):
        mlm_model.train()
        train_loss = 0.0
        for batch in tqdm(
            mlm_train_loader,
            desc=f"MLM Epoch {epoch+1}/{pretrain_epochs}",
            leave=False,
        ):
            mlm_optimizer.zero_grad()
            input_ids = batch["input_ids"].to(device)
            ages = batch["ages"].to(device)
            visit_ids = batch["visit_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            logits = mlm_model(input_ids, ages, visit_ids, attention_mask)
            loss = mlm_criterion(logits.view(-1, len(vocab)), labels.view(-1))
            loss.backward()
            mlm_optimizer.step()
            train_loss += loss.item()

        mlm_scheduler.step()
        train_loss /= len(mlm_train_loader)

        mlm_model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for batch in mlm_val_loader:
                logits = mlm_model(
                    batch["input_ids"].to(device),
                    batch["ages"].to(device),
                    batch["visit_ids"].to(device),
                    batch["attention_mask"].to(device),
                )
                val_loss += mlm_criterion(
                    logits.view(-1, len(vocab)),
                    batch["labels"].to(device).view(-1),
                ).item()
        val_loss /= len(mlm_val_loader)

        history["mlm_train_loss"].append(train_loss)
        history["mlm_val_loss"].append(val_loss)

        logging.info(
            f"MLM Epoch {epoch+1:02d} | Train MLM Loss: {train_loss:.4f} | Val MLM Loss: {val_loss:.4f}"
        )
        if val_loss < best_mlm_loss:
            best_mlm_loss = val_loss
            torch.save(
                base_encoder.state_dict(),
                os.path.join(exp_dir, "pretrained_encoder.pt"),
            )

    # -------------------------------------------------------------
    # STAGE 2: SUPERVISED FINE-TUNING (NEXT VISIT PREDICTION)
    # -------------------------------------------------------------
    logging.info(
        f"\n[{exp_name}] --- Starting Stage 2: Fine-Tuning on Next-Visit Task ---"
    )
    base_encoder.load_state_dict(
        torch.load(os.path.join(exp_dir, "pretrained_encoder.pt"))
    )

    ft_train_ds = BEHRTNextVisitDataset(
        train_seqs, max_len=256, vocab=vocab, num_classes=len(vocab)
    )
    ft_val_ds = BEHRTNextVisitDataset(
        val_seqs, max_len=256, vocab=vocab, num_classes=len(vocab)
    )

    ft_train_loader = DataLoader(
        ft_train_ds, batch_size=batch_size, shuffle=True
    )
    ft_val_loader = DataLoader(ft_val_ds, batch_size=batch_size, shuffle=False)

    ft_model = BEHRTForNextVisit(base_encoder).to(device)
    pos_weight = torch.ones([len(vocab)], device=device) * exp_cfg["pos_weight"]
    ft_criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)

    ft_optimizer = torch.optim.AdamW(
        ft_model.parameters(), lr=3e-5, weight_decay=0.01
    )
    ft_scheduler = ReduceLROnPlateau(
        ft_optimizer, mode="min", factor=0.5, patience=2, min_lr=1e-6
    )

    best_val_loss = float("inf")
    best_metrics = {}
    best_epoch = 0
    patience, patience_counter = 5, 0

    for epoch in range(finetune_epochs):
        ft_model.train()
        train_loss = 0.0
        for batch in tqdm(
            ft_train_loader,
            desc=f"FT Epoch {epoch+1}/{finetune_epochs}",
            leave=False,
        ):
            ft_optimizer.zero_grad()
            logits = ft_model(
                batch["input_ids"].to(device),
                batch["ages"].to(device),
                batch["visit_ids"].to(device),
                batch["attention_mask"].to(device),
            )
            loss = ft_criterion(logits, batch["targets"].to(device))
            loss.backward()
            ft_optimizer.step()
            train_loss += loss.item()

        train_loss /= len(ft_train_loader)
        val_metrics = evaluate_next_visit(
            ft_model,
            ft_val_loader,
            ft_criterion,
            device,
            vocab_size=len(vocab),
        )
        val_loss = val_metrics["Loss"]
        ft_scheduler.step(val_loss)

        # Record metrics history
        history["ft_train_loss"].append(train_loss)
        history["ft_val_loss"].append(val_loss)
        history["val_mrr"].append(val_metrics.get("MRR", 0.0))
        history["val_recall10"].append(val_metrics.get("Recall@10", 0.0))
        history["val_recall20"].append(val_metrics.get("Recall@20", 0.0))
        history["val_macro_prauc"].append(val_metrics.get("Macro_PR_AUC", 0.0))
        history["val_macro_auroc"].append(val_metrics.get("Macro_AUROC", 0.0))

        logging.info(
            f"FT Epoch {epoch+1:02d} | Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f} | "
            f"MRR: {val_metrics.get('MRR', 0):.4f} | Recall@10: {val_metrics.get('Recall@10', 0):.4f} | "
            f"Macro PR-AUC: {val_metrics.get('Macro_PR_AUC', 0):.4f}"
        )

        if val_loss < best_val_loss - 1e-4:
            best_val_loss = val_loss
            best_metrics = val_metrics
            best_epoch = epoch + 1
            patience_counter = 0
            torch.save(
                ft_model.state_dict(),
                os.path.join(exp_dir, "best_finetuned_model.pt"),
            )
        else:
            patience_counter += 1
            if patience_counter >= patience:
                logging.info(
                    f"Early stopping fine-tuning triggered at Epoch {epoch+1}."
                )
                break

    best_metrics["Vocab_Size"] = len(vocab)
    best_metrics["Best_Epoch"] = best_epoch
    best_metrics["Aggregation_Name"] = exp_name

    # Save metrics JSON, config & history[cite: 3]
    with open(
        os.path.join(exp_dir, "metrics_summary.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(best_metrics, f, indent=4)

    with open(
        os.path.join(exp_dir, "config.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(
            {
                "experiment_name": exp_name,
                "slurm_job_id": slurm_job_id,
                "best_epoch": best_epoch,
                "parameters": exp_cfg,
            },
            f,
            indent=4,
        )

    with open(
        os.path.join(exp_dir, "training_history.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(history, f, indent=4)

    # Generate individual experiment curves
    plot_single_experiment_history(
        history=history,
        output_path=os.path.join(exp_dir, "training_curves.png"),
        exp_name=exp_name,
    )

    # Copy SLURM execution stdout into the experiment folder
    slurm_out_file = f"job-{slurm_job_id}.out"
    if os.path.exists(slurm_out_file):
        shutil.copy(slurm_out_file, os.path.join(exp_dir, "training.log"))

    return best_metrics


# =====================================================================
# 7. MAIN BENCHMARK EXECUTION
# =====================================================================
def main():
    """Main execution function running all 9 ontology aggregations + Baseline[cite: 1, 2]."""
    username = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASS")
    postgres_ip = os.environ.get("DB_HOST")
    db_name = os.environ.get("DB_NAME", "omop_med_db")
    port = os.environ.get("DB_PORT", "5432")
    slurm_job_id = os.environ.get("SLURM_JOBID", "local_run")

    if not all([username, password, postgres_ip]):
        raise ValueError(
            "Missing database configuration in environment variables."
        )

    DB_URI = f"postgresql://{username}:{password}@{postgres_ip}:{port}/{db_name}"
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logging.info(f"Execution Device: {device}")

    # 1. Single database extraction and fixed patient cohort split[cite: 2]
    df_raw = extract_omop_data(db_uri=DB_URI, schema="omop")
    unique_patients = list(df_raw["subject_id"].unique())
    random.seed(42)
    random.shuffle(unique_patients)
    split_point = int(len(unique_patients) * 0.85)
    train_ids = set(unique_patients[:split_point])
    val_ids = set(unique_patients[split_point:])

    # 2. Comprehensive 9 Aggregations + Baseline Configuration[cite: 1, 2]
    EXPERIMENT_CONFIGS = [
        # 1. ICD-9 CCS (Single-Level Baseline)[cite: 1, 2]
        {
            "name": "1_ICD9_CCS_SingleLevel",
            "csv": "mapping_icd9_to_ccs.csv",
            "src_col": "icd_code",
            "tgt_col": "target_ccs",
            "pos_weight": 4.0,
        },
        # 2. ICD-10 CCSR (Default IP Baseline)[cite: 1, 2]
        {
            "name": "2_ICD10_CCSR_DefaultIP",
            "csv": "mapping_icd10_to_ccsr.csv",
            "src_col": "icd_code",
            "tgt_col": "target_ccsr",
            "pos_weight": 5.0,
        },
        # 3. SNOMED Hybrid Tree (Max Lvl 2) - MICRO[cite: 1, 2]
        {
            "name": "3_SNOMED_HybridTree_Lvl2",
            "csv": "mapping_snomed_tree_lvl2_hybrid_ohdsi.csv",
            "src_col": "source_snomed_code",
            "tgt_col": "parent_snomed_code",
            "pos_weight": 25.0,
        },
        # 4. SNOMED Hybrid Tree (Max Lvl 3) - MESO Core[cite: 1, 2]
        {
            "name": "4_SNOMED_HybridTree_Lvl3",
            "csv": "mapping_snomed_tree_lvl3_hybrid_ohdsi.csv",
            "src_col": "source_snomed_code",
            "tgt_col": "parent_snomed_code",
            "pos_weight": 18.0,
        },
        # 5. SNOMED Hybrid Tree (Max Lvl 4) - MESO Core[cite: 1, 2]
        {
            "name": "5_SNOMED_HybridTree_Lvl4",
            "csv": "mapping_snomed_tree_lvl4_hybrid_ohdsi.csv",
            "src_col": "source_snomed_code",
            "tgt_col": "parent_snomed_code",
            "pos_weight": 15.0,
        },
        # 6. SNOMED to CCSR (Default IP + Fallback)[cite: 1, 2]
        {
            "name": "6_SNOMED_CCSR_DefaultIP_Fallback",
            "csv": (
                "mapping_snomed_to_ccsr_default_ip_with_ancestors_fallback.csv"
            ),
            "src_col": "source_snomed_code",
            "tgt_col": "target_ccsr_code",
            "pos_weight": 20.0,
        },
        # 7. SNOMED to CCSR (Multi-Category 1:N + Fallback)[cite: 1, 2]
        {
            "name": "7_SNOMED_CCSR_MultiCategory_Fallback",
            "csv": "mapping_snomed_to_ccsr_multi_with_ancestors_fallback.csv",
            "src_col": "source_snomed_code",
            "tgt_col": "target_ccsr_code",
            "pos_weight": 20.0,
        },
        # 8. SNOMED to OHDSI Standard Concept Groups[cite: 1, 2]
        {
            "name": "8_SNOMED_OHDSI_ConceptGroups",
            "csv": "mapping_snomed_ohdsi_concept_groups.csv",
            "src_col": "source_snomed_code",
            "tgt_col": "target_group_code",
            "pos_weight": 25.0,
        },
        # 9. SNOMED to ICD-10 3-Digit Categories[cite: 1, 2]
        {
            "name": "9_SNOMED_ICD10_3Digit",
            "csv": "mapping_snomed_to_icd10_3digit.csv",
            "src_col": "source_snomed_code",
            "tgt_col": "target_icd10_3digit",
            "pos_weight": 25.0,
        },
        # Control Baseline: Raw Unaggregated SNOMED Codes[cite: 1, 2]
        {
            "name": "10_BASELINE_Raw_SNOMED",
            "csv": None,
            "src_col": None,
            "tgt_col": None,
            "pos_weight": 25.0,
        },
    ]

    all_results = []
    for exp_cfg in EXPERIMENT_CONFIGS:
        csv_path = exp_cfg.get("csv")
        if csv_path is not None and not os.path.exists(csv_path):
            logging.warning(
                f"File '{csv_path}' not found. Skipping '{exp_cfg['name']}'..."
            )
            continue

        logging.info(
            f"\n{'='*70}\n STARTING TWO-STAGE BENCHMARK: {exp_cfg['name']}\n{'='*70}"
        )
        metrics = run_two_stage_experiment(
            df_raw=df_raw,
            exp_cfg=exp_cfg,
            fixed_train_ids=train_ids,
            fixed_val_ids=val_ids,
            device=device,
            slurm_job_id=slurm_job_id,
            pretrain_epochs=15,
            finetune_epochs=20,
            batch_size=64,
        )
        all_results.append(metrics)

    # 3. Export Comprehensive Comparative Results Table & Visualization Charts[cite: 3]
    if all_results:
        summary_csv = "aggregation_two_stage_benchmark_summary.csv"
        df_sum = pd.DataFrame(all_results)
        df_sum.set_index("Aggregation_Name", inplace=True)
        df_sum.to_csv(summary_csv)

        logging.info(
            "\n"
            + "=" * 80
            + "\n FINAL TWO-STAGE MULTI-ONTOLOGY BENCHMARK SUMMARY\n"
            + "=" * 80
        )
        logging.info("\n" + df_sum.to_string())

        plot_benchmark_summary(
            csv_path=summary_csv, output_png="benchmark_comparison_charts.png"
        )

    logging.info("Training pipeline finished[cite: 3].")


if __name__ == "__main__":
    main()