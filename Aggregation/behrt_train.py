import ast
import json
import logging
import os
import pickle
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sqlalchemy import create_engine
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =====================================================================
# 1. DATA EXTRACTION
# =====================================================================
def extract_omop_data(
    db_uri: str, schema: str = "omop"
) -> pd.DataFrame:
    """Extracts diagnosis records and visit sequences from OMOP CDM PostgreSQL database.

    Args:
        db_uri (str): SQLAlchemy connection string for the PostgreSQL database.
        schema (str): The database schema name. Defaults to 'omop'.

    Returns:
        pd.DataFrame: Retrieved patient diagnoses with computed visit ranks.
    """
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
def apply_snomed_aggregation(
    df: pd.DataFrame, mapping_csv_path: str
) -> pd.DataFrame:
    """Maps raw SNOMED codes to CCSR categories using a provided mapping file.

    Args:
        df (pd.DataFrame): Raw diagnosis DataFrame from database.
        mapping_csv_path (str): File path to CCSR aggregation CSV.

    Returns:
        pd.DataFrame: DataFrame with updated aggregated codes.
    """
    logging.info(f"Loading aggregation mapping from {mapping_csv_path}...")
    agg_df = pd.read_csv(mapping_csv_path)

    # Convert mapping columns to string dictionary using exact column headers
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
    """Builds token-to-index and index-to-token dictionaries.

    Args:
        df (pd.DataFrame): Processed DataFrame containing aggregated codes.
        special_tokens (List[str], optional): List of special BERT tokens.

    Returns:
        Tuple[Dict[str, int], Dict[int, str]]: Vocabulary mappings.
    """
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
    """Audits patient dataset for out-of-bounds values and logs anomalies to a CSV file.

    Args:
        df (pd.DataFrame): Processed patient DataFrame containing mapped codes and visits.
        max_age (int, optional): Maximum valid patient age. Defaults to 120.
        max_visits (int, optional): Maximum valid visit rank. Defaults to 100.
        output_log_path (str, optional): Destination path for CSV report. Defaults to "data_anomalies_report.csv".

    Returns:
        pd.DataFrame: DataFrame containing all identified anomaly records with descriptions.
    """
    logging.info("Starting Data Quality Audit for out-of-bounds anomalies...")

    # Define logical conditions for out-of-bounds anomalies
    invalid_age = (df["age_at_visit"] < 0) | (df["age_at_visit"] > max_age)
    invalid_visit = (df["visit_rank"] < 1) | (df["visit_rank"] > max_visits)
    missing_codes = df["aggregated_code"].isna()

    # Filter rows matching any anomaly condition
    anomalies_mask = invalid_age | invalid_visit | missing_codes
    anomalies_df = df[anomalies_mask].copy()

    if not anomalies_df.empty:
        # Generate specific description for each flagged anomaly row
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

        # Export findings to CSV audit report
        anomalies_df.to_csv(output_log_path, index=False)
        logging.warning(
            f"Audit completed: Identified {len(anomalies_df)} anomaly rows. "
            f"Saved detailed audit log to '{output_log_path}'."
        )
    else:
        logging.info("Audit completed: No out-of-bounds data anomalies detected.")

    return anomalies_df


def prepare_next_visit_sequences(
    df: pd.DataFrame, vocab: Dict[str, int], min_visits: int = 2
) -> List[Dict]:
    """Structures patient timeline into input historical sequence and target next visit.

    Args:
        df (pd.DataFrame): Processed DataFrame with code mappings.
        vocab (Dict[str, int]): Token vocabulary.
        min_visits (int): Minimum visit threshold for Next Visit Task.

    Returns:
        List[Dict]: List of structured patient sequence records.
    """
    logging.info("Building patient sequences for Next Visit Prediction task...")
    df["code_id"] = df["aggregated_code"].map(
        lambda x: vocab.get(x, vocab["[UNK]"])
    )

    patient_records = []
    grouped_patients = df.groupby("subject_id")

    for person_id, p_group in tqdm(grouped_patients, desc="Processing patients"):
        visits_list = []
        ages_list = []

        # Group by chronologically ranked visits
        for visit_rank, v_group in p_group.groupby("visit_rank"):
            v_codes = v_group["code_id"].tolist()
            v_age = int(v_group["age_at_visit"].iloc[0])
            visits_list.append(v_codes)
            ages_list.append(v_age)

        # Retain patients meeting the minimum visit constraint
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
        """Initializes the BEHRT Dataset.

        Args:
            data (List[Dict]): Patient sequence data records.
            max_len (int): Maximum sequence length cutoff.
            vocab (Dict[str, int]): Token vocabulary mapping.
            num_classes (int): Total vocabulary size for target multi-hot vector.
            max_age (int, optional): Upper bound for age embedding index. Defaults to 120.
            max_visits (int, optional): Upper bound for visit embedding index. Defaults to 100.
        """
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

        # Padding sequence to maximum length
        pad_len = self.max_len - len(codes)
        codes = codes + [self.vocab["[PAD]"]] * pad_len
        ages = ages + [0] * pad_len
        visits = visits + [0] * pad_len

        # Clamp values to prevent out-of-bounds CUDA indexing errors in Embedding layers
        ages = [max(0, min(int(a), self.max_age)) for a in ages]
        visits = [max(0, min(int(v), self.max_visits)) for v in visits]

        # Construct multi-hot vector for next visit targets
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
        """Initializes BEHRT Model with Concept, Age, and Visit Embeddings.

        Args:
            vocab_size (int): Size of diagnosis vocabulary.
            max_age (int): Maximum patient age bounds.
            max_visits (int): Maximum visit sequence bounds.
            hidden_size (int): Hidden embedding dimension size.
            num_layers (int): Number of Transformer Encoder layers.
            num_heads (int): Number of multi-head attention heads.
            dropout (float): Dropout probability.
        """
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
        """Forward pass for the BEHRT architecture with CUDA bounds protection.

        Args:
            input_ids (torch.Tensor): Concept token IDs [Batch, SeqLen].
            ages (torch.Tensor): Patient age values [Batch, SeqLen].
            visit_ids (torch.Tensor): Visit segment IDs [Batch, SeqLen].
            attention_mask (torch.Tensor): Attention mask [Batch, SeqLen].

        Returns:
            torch.Tensor: Logits for next visit diagnoses [Batch, VocabSize].
        """
        # Bounds Protection for CUDA Embeddings
        input_ids = torch.clamp(input_ids, min=0, max=self.vocab_size - 1)
        ages = torch.clamp(ages, min=0, max=self.max_age)
        visit_ids = torch.clamp(visit_ids, min=0, max=self.max_visits)

        # Sum multi-modal embeddings
        c_emb = self.concept_embeddings(input_ids)
        a_emb = self.age_embeddings(ages)
        v_emb = self.visit_embeddings(visit_ids)

        embeddings = c_emb + a_emb + v_emb
        embeddings = self.layer_norm(embeddings)
        embeddings = self.dropout(embeddings)

        # PyTorch Transformer Encoder expects key_padding_mask where True indicates padded values
        key_padding_mask = attention_mask == 0

        encoded_seq = self.transformer_encoder(
            embeddings, src_key_padding_mask=key_padding_mask
        )

        # Pooled representation using mean of sequence embeddings
        pooled_output = encoded_seq.mean(dim=1)
        logits = self.classifier(pooled_output)
        return logits


# =====================================================================
# 5. TRAINING ROUTINE
# =====================================================================
def train_epoch(
    model: nn.Module,
    dataloader: DataLoader,
    optimizer: torch.optim.Optimizer,
    criterion: nn.Module,
    device: torch.device,
) -> float:
    """Trains the BEHRT model for one epoch.

    Args:
        model (nn.Module): The BEHRT PyTorch model.
        dataloader (DataLoader): Training DataLoader.
        optimizer (torch.optim.Optimizer): Optimizer instance.
        criterion (nn.Module): Loss function.
        device (torch.device): Execution device (CPU or CUDA).

    Returns:
        float: Average training loss over the epoch.
    """
    model.train()
    total_loss = 0.0

    for batch in dataloader:
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
    """Main execution function running the complete BEHRT training pipeline with caching."""
    PROCESSED_DATA_PATH = "processed_patient_sequences.pkl"
    VOCAB_PATH = "vocab.pkl"
    MAPPING_PATH = "mapping_snomed_to_ccsr_with_ancestors.csv"
    
    MAX_SEQ_LEN = 256
    BATCH_SIZE = 32
    EPOCHS = 5
    LEARNING_RATE = 5e-5

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logging.info(f"Using device: {device}")

    # Check if preprocessed sequences and vocabulary exist on disk
    if os.path.exists(PROCESSED_DATA_PATH) and os.path.exists(VOCAB_PATH):
        logging.info("Found cached dataset and vocabulary! Loading directly from disk...")
        
        with open(PROCESSED_DATA_PATH, "rb") as f:
            patient_sequences = pickle.load(f)
            
        with open(VOCAB_PATH, "rb") as f:
            vocab = pickle.load(f)
            
        logging.info(f"Loaded {len(patient_sequences)} sequences and vocabulary of size {len(vocab)}.")

    else:
        logging.info("Cached data not found. Running database extraction and aggregation pipeline...")
        
        # Load database credentials from environment variables
        username = os.environ.get("DB_USER")
        password = os.environ.get("DB_PASS")
        postgres_ip = os.environ.get("DB_HOST")
        db_name = os.environ.get("DB_NAME", "omop_med_db")
        port = os.environ.get("DB_PORT", "5432")

        if not all([username, password, postgres_ip]):
            raise ValueError("Missing database configuration in environment variables.")

        DB_URI = f"postgresql://{username}:{password}@{postgres_ip}:{port}/{db_name}"

        # 1. Extract raw diagnoses from OMOP database
        df_raw = extract_omop_data(db_uri=DB_URI, schema="omop")

        # 2. Map SNOMED codes to CCSR categories
        df_mapped = apply_snomed_aggregation(
            df=df_raw, mapping_csv_path=MAPPING_PATH
        )

        # 2a. Audit and log any out-of-bounds anomalies
        audit_and_log_anomalies(
            df=df_mapped, 
            max_age=120, 
            max_visits=100, 
            output_log_path="data_anomalies_report.csv"
        )

        # 3. Build token vocabulary
        vocab, inv_vocab = build_vocabulary(df=df_mapped)

        # 4. Create patient sequence timelines
        patient_sequences = prepare_next_visit_sequences(
            df=df_mapped, vocab=vocab, min_visits=2
        )

        # 5. Save processed data and vocabulary to disk for future runs
        logging.info("Saving processed sequences and vocabulary to disk...")
        with open(PROCESSED_DATA_PATH, "wb") as f:
            pickle.dump(patient_sequences, f)
            
        with open(VOCAB_PATH, "wb") as f:
            pickle.dump(vocab, f)

    # Initialize PyTorch Dataset and DataLoader
    dataset = BEHRTNextVisitDataset(
        data=patient_sequences,
        max_len=MAX_SEQ_LEN,
        vocab=vocab,
        num_classes=len(vocab),
        max_age=120,
        max_visits=100,
    )
    dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

    # Instantiate BEHRT Model architecture, loss function, and optimizer
    model = BEHRTForNextVisit(
        vocab_size=len(vocab), hidden_size=256, num_layers=4, num_heads=4
    ).to(device)

    criterion = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE)

    # Model training loop
    logging.info("Starting model training loop...")
    for epoch in range(EPOCHS):
        avg_loss = train_epoch(
            model=model,
            dataloader=dataloader,
            optimizer=optimizer,
            criterion=criterion,
            device=device,
        )
        logging.info(f"Epoch {epoch + 1}/{EPOCHS} - Training Loss: {avg_loss:.4f}")

    logging.info("Training pipeline executed successfully.")


if __name__ == "__main__":
    main()