import ast
import json
import logging
import os
import pickle
import random
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.metrics import average_precision_score, roc_auc_score
from sqlalchemy import create_engine
from torch.optim.lr_scheduler import CosineAnnealingLR
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =====================================================================
# 1. DATA EXTRACTION
# =====================================================================
def extract_omop_data(db_uri: str, schema: str = "omop") -> pd.DataFrame:
    """Extracts diagnosis records and visit sequences from OMOP CDM PostgreSQL database."""
    logging.info("Extracting SNOMED diagnoses and visit sequences from database...")
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
    snomed_diagnoses AS (
        SELECT 
            co.person_id AS subject_id,
            co.visit_occurrence_id AS hadm_id,
            co.condition_concept_id,
            c.concept_code AS snomed_code,
            c.concept_name AS condition_name
        FROM 
            {schema}.condition_occurrence co
            INNER JOIN {schema}.concept c ON co.condition_concept_id = c.concept_id
        WHERE 
            c.vocabulary_id = 'SNOMED'
    )
    SELECT 
        v.subject_id,
        v.hadm_id,
        v.visit_start_date,
        v.age_at_visit,
        v.visit_rank,
        d.snomed_code,
        d.condition_name
    FROM 
        patient_visits v
        INNER JOIN snomed_diagnoses d ON v.hadm_id = d.hadm_id
    ORDER BY 
        v.subject_id ASC, 
        v.visit_rank ASC;
    """

    df = pd.read_sql(sql_query, con=engine)
    logging.info(f"Extracted {len(df)} total diagnosis rows.")
    return df


# =====================================================================
# 2. DATA PREPROCESSING AND AGGREGATION
# =====================================================================
def apply_snomed_aggregation(df: pd.DataFrame, mapping_csv_path: str) -> pd.DataFrame:
    """Maps raw SNOMED codes to CCSR categories using a provided mapping file."""
    logging.info(f"Loading aggregation mapping from {mapping_csv_path}...")
    agg_df = pd.read_csv(mapping_csv_path)

    snomed_to_ccsr = dict(
        zip(agg_df["source_snomed_code"].astype(str), agg_df["target_ccsr_code"].astype(str))
    )

    df["snomed_code"] = df["snomed_code"].astype(str)
    df["aggregated_code"] = (
        df["snomed_code"].map(snomed_to_ccsr).fillna(df["snomed_code"])
    )
    return df


def build_vocabulary(
    df: pd.DataFrame, special_tokens: List[str] = None
) -> Tuple[Dict[str, int], Dict[int, str]]:
    """Builds token-to-index and index-to-token dictionaries."""
    if special_tokens is None:
        special_tokens = ["[PAD]", "[UNK]", "[SEP]", "[MASK]"]

    unique_codes = df["aggregated_code"].unique().tolist()
    vocab = {token: idx for idx, token in enumerate(special_tokens + unique_codes)}
    inv_vocab = {v: k for k, v in vocab.items()}

    logging.info(f"Vocabulary created with {len(vocab)} unique tokens.")
    return vocab, inv_vocab


def audit_and_log_anomalies(
    df: pd.DataFrame,
    max_age: int = 120,
    max_visits: int = 100,
    output_log_path: str = "data_anomalies_report.csv",
) -> pd.DataFrame:
    """Audits patient dataset for out-of-bounds values and logs anomalies to a CSV file."""
    logging.info("Starting Data Quality Audit for out-of-bounds anomalies...")

    invalid_age = (df["age_at_visit"] < 0) | (df["age_at_visit"] > max_age)
    invalid_visit = (df["visit_rank"] < 1) | (df["visit_rank"] > max_visits)
    missing_codes = df["aggregated_code"].isna()

    anomalies_mask = invalid_age | invalid_visit | missing_codes
    anomalies_df = df[anomalies_mask].copy()

    if not anomalies_df.empty:
        reasons = []
        for _, row in anomalies_df.iterrows():
            row_reasons = []
            if row["age_at_visit"] < 0 or row["age_at_visit"] > max_age:
                row_reasons.append(f"Invalid Age ({row['age_at_visit']})")
            if row["visit_rank"] < 1 or row["visit_rank"] > max_visits:
                row_reasons.append(f"Invalid Visit Rank ({row['visit_rank']})")
            if pd.isna(row["aggregated_code"]):
                row_reasons.append("Missing Aggregated Code")
            reasons.append("; ".join(row_reasons))

        anomalies_df["anomaly_reason"] = reasons
        anomalies_df.to_csv(output_log_path, index=False)
        logging.warning(
            f"Audit completed: Identified {len(anomalies_df)} anomaly rows. Saved to '{output_log_path}'."
        )
    else:
        logging.info("Audit completed: No out-of-bounds data anomalies detected.")

    return anomalies_df


def prepare_next_visit_sequences(
    df: pd.DataFrame, vocab: Dict[str, int], min_visits: int = 2
) -> List[Dict]:
    """Structures patient timeline into input historical sequence and target next visit."""
    logging.info("Building patient sequences for Next Visit Prediction task...")
    df["code_id"] = df["aggregated_code"].map(
        lambda x: vocab.get(x, vocab["[UNK]"])
    )

    patient_records = []
    grouped_patients = df.groupby("subject_id")

    for person_id, p_group in tqdm(grouped_patients, desc="Processing patients"):
        visits_list = []
        ages_list = []

        for visit_rank, v_group in p_group.groupby("visit_rank"):
            v_codes = v_group["code_id"].tolist()
            v_age = int(v_group["age_at_visit"].iloc[0])
            visits_list.append(v_codes)
            ages_list.append(v_age)

        if len(visits_list) >= min_visits:
            train_visits = visits_list[:-1]
            train_ages = ages_list[:-1]
            target_next_visit = visits_list[-1]

            flat_codes = []
            flat_ages = []
            flat_visit_ids = []

            for idx, (v_codes, v_age) in enumerate(zip(train_visits, train_ages)):
                flat_codes.extend(v_codes + [vocab["[SEP]"]])
                flat_ages.extend([v_age] * (len(v_codes) + 1))
                flat_visit_ids.extend([idx + 1] * (len(v_codes) + 1))

            patient_records.append(
                {
                    "person_id": person_id,
                    "input_codes": flat_codes,
                    "input_ages": flat_ages,
                    "input_visit_ids": flat_visit_ids,
                    "target_next_visit": target_next_visit,
                }
            )

    logging.info(
        f"Sequence preparation completed. {len(patient_records)} valid patient timelines generated."
    )
    return patient_records


# =====================================================================
# 3. PYTORCH DATASET SETUP
# =====================================================================
class BEHRTNextVisitDataset(Dataset):
    """PyTorch Dataset for BEHRT Next Visit Prediction task with index bounds safety."""

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

        codes = item["input_codes"][: self.max_len]
        ages = item["input_ages"][: self.max_len]
        visits = item["input_visit_ids"][: self.max_len]

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
# 4. MODEL ARCHITECTURE
# =====================================================================
class BEHRTForNextVisit(nn.Module):
    """BEHRT Neural Network Model Architecture for Next Visit Prediction."""

    def __init__(
        self,
        vocab_size: int,
        max_age: int = 120,
        max_visits: int = 100,
        hidden_size: int = 256,
        num_layers: int = 4,
        num_heads: int = 4,
        dropout: float = 0.1,
    ):
        super().__init__()
        self.vocab_size = vocab_size
        self.max_age = max_age
        self.max_visits = max_visits

        self.concept_embeddings = nn.Embedding(vocab_size, hidden_size)
        self.age_embeddings = nn.Embedding(max_age + 1, hidden_size)
        self.visit_embeddings = nn.Embedding(max_visits + 1, hidden_size)

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
        self.classifier = nn.Linear(hidden_size, vocab_size)

    def forward(
        self,
        input_ids: torch.Tensor,
        ages: torch.Tensor,
        visit_ids: torch.Tensor,
        attention_mask: torch.Tensor,
    ) -> torch.Tensor:
        input_ids = torch.clamp(input_ids, min=0, max=self.vocab_size - 1)
        ages = torch.clamp(ages, min=0, max=self.max_age)
        visit_ids = torch.clamp(visit_ids, min=0, max=self.max_visits)

        c_emb = self.concept_embeddings(input_ids)
        a_emb = self.age_embeddings(ages)
        v_emb = self.visit_embeddings(visit_ids)

        embeddings = c_emb + a_emb + v_emb
        embeddings = self.layer_norm(embeddings)
        embeddings = self.dropout(embeddings)

        key_padding_mask = attention_mask == 0

        encoded_seq = self.transformer_encoder(
            embeddings, src_key_padding_mask=key_padding_mask
        )

        pooled_output = encoded_seq.mean(dim=1)
        logits = self.classifier(pooled_output)
        return logits


# =====================================================================
# 5. METRICS AND EVALUATION
# =====================================================================
def calculate_ranking_metrics_for_sample(
    logits: np.ndarray, targets: np.ndarray, k_values: List[int] = [5, 10, 20, 30]
) -> Dict[str, float]:
    """Calculates patient-level ranking metrics."""
    metrics = {}
    actual_positives = np.where(targets == 1.0)[0]
    num_positives = len(actual_positives)

    if num_positives == 0:
        return metrics

    sorted_indices = np.argsort(-logits)
    actual_set = set(actual_positives)

    # MRR (Mean Reciprocal Rank)
    mrr = 0.0
    for rank, idx in enumerate(sorted_indices, start=1):
        if idx in actual_set:
            mrr = 1.0 / rank
            break
    metrics["MRR"] = mrr

    # MAP (Mean Average Precision)
    running_hits = 0
    precisions_at_hits = []
    for rank, idx in enumerate(sorted_indices, start=1):
        if idx in actual_set:
            running_hits += 1
            precisions_at_hits.append(running_hits / rank)
    metrics["MAP"] = np.mean(precisions_at_hits) if precisions_at_hits else 0.0

    # Top-K Metrics
    for k in k_values:
        top_k = set(sorted_indices[:k])
        hits = len(top_k.intersection(actual_set))

        recall_k = hits / num_positives
        precision_k = hits / k
        f1_k = (
            (2 * precision_k * recall_k) / (precision_k + recall_k)
            if (precision_k + recall_k) > 0
            else 0.0
        )

        # NDCG@K
        dcg = sum(
            [
                1.0 / np.log2(rank + 1)
                for rank, idx in enumerate(sorted_indices[:k], start=1)
                if idx in actual_set
            ]
        )
        idcg = sum([1.0 / np.log2(i + 1) for i in range(1, min(num_positives, k) + 1)])
        ndcg_k = dcg / idcg if idcg > 0 else 0.0

        metrics[f"Recall@{k}"] = recall_k
        metrics[f"Precision@{k}"] = precision_k
        metrics[f"F1@{k}"] = f1_k
        metrics[f"NDCG@{k}"] = ndcg_k

    return metrics


def evaluate(
    model: nn.Module,
    dataloader: DataLoader,
    criterion: nn.Module,
    device: torch.device,
    k_values: List[int] = [5, 10, 20, 30],
) -> Dict[str, float]:
    """Runs evaluation and calculates classification & ranking metrics."""
    model.eval()
    total_loss = 0.0

    all_logits = []
    all_targets = []
    sample_metrics_list = []

    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch["input_ids"].to(device)
            ages = batch["ages"].to(device)
            visit_ids = batch["visit_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            targets = batch["targets"].to(device)

            logits = model(
                input_ids=input_ids,
                ages=ages,
                visit_ids=visit_ids,
                attention_mask=attention_mask,
            )

            loss = criterion(logits, targets)
            total_loss += loss.item()

            logits_np = logits.cpu().numpy()
            targets_np = targets.cpu().numpy()

            all_logits.append(logits_np)
            all_targets.append(targets_np)

            for i in range(targets_np.shape[0]):
                res = calculate_ranking_metrics_for_sample(
                    logits_np[i], targets_np[i], k_values=k_values
                )
                if res:
                    sample_metrics_list.append(res)

    results = {"Loss": total_loss / len(dataloader)}
    if sample_metrics_list:
        metric_keys = sample_metrics_list[0].keys()
        for key in metric_keys:
            results[key] = float(np.mean([m[key] for m in sample_metrics_list]))

    y_true = np.vstack(all_targets)
    y_logits = np.vstack(all_logits)
    y_pred_prob = 1.0 / (1.0 + np.exp(-np.clip(y_logits, -15, 15)))

    valid_cols = [
        col for col in range(y_true.shape[1]) if len(np.unique(y_true[:, col])) > 1
    ]

    if valid_cols:
        y_true_filtered = y_true[:, valid_cols]
        y_prob_filtered = y_pred_prob[:, valid_cols]

        try:
            results["Macro_ROC_AUC"] = float(
                roc_auc_score(y_true_filtered, y_prob_filtered, average="macro")
            )
            results["Macro_PR_AUC"] = float(
                average_precision_score(y_true_filtered, y_prob_filtered, average="macro")
            )
        except Exception:
            pass

    return results


def train_epoch(
    model: nn.Module,
    dataloader: DataLoader,
    optimizer: torch.optim.Optimizer,
    criterion: nn.Module,
    device: torch.device,
) -> float:
    """Trains the BEHRT model for one epoch."""
    model.train()
    total_loss = 0.0

    for batch in tqdm(dataloader, desc="Training", leave=False):
        optimizer.zero_grad()

        input_ids = batch["input_ids"].to(device)
        ages = batch["ages"].to(device)
        visit_ids = batch["visit_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        targets = batch["targets"].to(device)

        logits = model(
            input_ids=input_ids,
            ages=ages,
            visit_ids=visit_ids,
            attention_mask=attention_mask,
        )

        loss = criterion(logits, targets)
        loss.backward()
        optimizer.step()

        total_loss += loss.item()

    return total_loss / len(dataloader)


# =====================================================================
# 6. MAIN EXECUTION ROUTINE
# =====================================================================
def main():
    """Main execution pipeline with train/validation split, scheduler, and early stopping."""
    PROCESSED_DATA_PATH = "processed_patient_sequences.pkl"
    VOCAB_PATH = "vocab.pkl"
    MAPPING_PATH = "mapping_snomed_to_ccsr_with_ancestors.csv"
    BEST_MODEL_PATH = "behrt_best_model.pt"
    
    MAX_SEQ_LEN = 256
    BATCH_SIZE = 64
    EPOCHS = 20
    LEARNING_RATE = 5e-5
    PATIENCE = 4
    K_VALUES = [5, 10, 20, 30]

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logging.info(f"Using device: {device}")

    # 1. Load or Build Data
    if os.path.exists(PROCESSED_DATA_PATH) and os.path.exists(VOCAB_PATH):
        logging.info("Found cached dataset and vocabulary! Loading directly from disk...")
        with open(PROCESSED_DATA_PATH, "rb") as f:
            patient_sequences = pickle.load(f)
        with open(VOCAB_PATH, "rb") as f:
            vocab = pickle.load(f)
        logging.info(f"Loaded {len(patient_sequences)} sequences and vocabulary of size {len(vocab)}.")
    else:
        logging.info("Cached data not found. Running database extraction and aggregation pipeline...")
        username = os.environ.get("DB_USER")
        password = os.environ.get("DB_PASS")
        postgres_ip = os.environ.get("DB_HOST")
        db_name = os.environ.get("DB_NAME", "omop_med_db")
        port = os.environ.get("DB_PORT", "5432")

        if not all([username, password, postgres_ip]):
            raise ValueError("Missing database configuration in environment variables.")

        DB_URI = f"postgresql://{username}:{password}@{postgres_ip}:{port}/{db_name}"

        df_raw = extract_omop_data(db_uri=DB_URI, schema="omop")
        df_mapped = apply_snomed_aggregation(df=df_raw, mapping_csv_path=MAPPING_PATH)
        audit_and_log_anomalies(df=df_mapped, max_age=120, max_visits=100, output_log_path="data_anomalies_report.csv")
        vocab, inv_vocab = build_vocabulary(df=df_mapped)
        patient_sequences = prepare_next_visit_sequences(df=df_mapped, vocab=vocab, min_visits=2)

        logging.info("Saving processed sequences and vocabulary to disk...")
        with open(PROCESSED_DATA_PATH, "wb") as f:
            pickle.dump(patient_sequences, f)
        with open(VOCAB_PATH, "wb") as f:
            pickle.dump(vocab, f)

    # 2. Patient-level Train / Validation Split (85% / 15%)
    random.seed(42)
    random.shuffle(patient_sequences)
    split_idx = int(len(patient_sequences) * 0.85)
    train_sequences = patient_sequences[:split_idx]
    val_sequences = patient_sequences[split_idx:]

    logging.info(f"Dataset split: {len(train_sequences)} Train samples, {len(val_sequences)} Validation samples.")

    # 3. DataLoaders
    train_dataset = BEHRTNextVisitDataset(
        data=train_sequences,
        max_len=MAX_SEQ_LEN,
        vocab=vocab,
        num_classes=len(vocab),
        max_age=120,
        max_visits=100,
    )
    val_dataset = BEHRTNextVisitDataset(
        data=val_sequences,
        max_len=MAX_SEQ_LEN,
        vocab=vocab,
        num_classes=len(vocab),
        max_age=120,
        max_visits=100,
    )

    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE, shuffle=False)

    # 4. Model, Loss with Pos Weight & Scheduler
    model = BEHRTForNextVisit(
        vocab_size=len(vocab), hidden_size=256, num_layers=4, num_heads=4
    ).to(device)

    # Class imbalance handling
    pos_weight = torch.ones([len(vocab)], device=device) * 5.0
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    
    optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE, weight_decay=0.01)
    scheduler = CosineAnnealingLR(optimizer, T_max=EPOCHS)

    # 5. Training Loop with Early Stopping & Comprehensive Logging
    best_val_loss = float("inf")
    patience_counter = 0

    logging.info("Starting enhanced model training loop...")
    for epoch in range(EPOCHS):
        train_loss = train_epoch(
            model=model,
            dataloader=train_loader,
            optimizer=optimizer,
            criterion=criterion,
            device=device,
        )
        
        val_metrics = evaluate(
            model=model,
            dataloader=val_loader,
            criterion=criterion,
            device=device,
            k_values=K_VALUES,
        )
        
        scheduler.step()
        val_loss = val_metrics["Loss"]

        logging.info(
            f"Epoch {epoch + 1:02d}/{EPOCHS:02d} | "
            f"Train Loss: {train_loss:.4f} | "
            f"Val Loss: {val_loss:.4f} | "
            f"Macro PR-AUC: {val_metrics.get('Macro_PR_AUC', 0):.4f} | "
            f"MRR: {val_metrics.get('MRR', 0):.4f} | "
            f"Recall@10: {val_metrics.get('Recall@10', 0):.4f} | "
            f"Recall@20: {val_metrics.get('Recall@20', 0):.4f}"
        )

        # Checkpoint Best Model & Early Stopping
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), BEST_MODEL_PATH)
            logging.info(f"  --> Saved new best checkpoint to '{BEST_MODEL_PATH}'")
        else:
            patience_counter += 1
            logging.info(f"  --> No improvement for {patience_counter}/{PATIENCE} epochs.")
            if patience_counter >= PATIENCE:
                logging.info(f"Early stopping triggered at epoch {epoch + 1}.")
                break

    logging.info("Training pipeline finished.")


if __name__ == "__main__":
    main()