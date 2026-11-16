#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dataset_builder.py
מודול לטעינה דינמית והמרה בזמן אמת (On-the-fly) של רצפי אבחנות מתוך MIMIC-IV ו-OMOP CDM,
יחד עם הפקת 3 תוויות קליניות (Use Cases) בהתאם לפרוטוקול ICD2BERT / BEHRT:
1. mortality (תמותה בביקור האחרון - final admission)
2. rehospitalization (אשפוז חוזר בתוך 30 יום בין הביקור הלפני-אחרון penultimate לאחרון final)
3. future_disease (חיזוי מחלות באשפוז הבא - Multi-label 25 פרקי A-Z ללא U)
"""

import os
import logging
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
import torch
from torch.utils.data import Dataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DISEASE_CHAPTERS = [c for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" if c != "U"]  # 25 פרקי ה-ICD מהמאמר


class ClinicalEHRDataset(Dataset):
    """
    Dataset המחזיק את רצפי המטופלים המקודדים למזהי טוקנים (input_ids)
    יחד עם התוויות לכל שלוש המשימות הקליניות.
    """
    def __init__(
        self,
        tokenized_sequences: List[List[int]],
        mortality_labels: List[int],
        rehosp_labels: List[int],
        future_disease_labels: List[List[int]],
        max_len: int = 512
    ):
        self.tokenized_sequences = tokenized_sequences
        self.mortality_labels = mortality_labels
        self.rehosp_labels = rehosp_labels
        self.future_disease_labels = future_disease_labels
        self.max_len = max_len

    def __len__(self):
        return len(self.tokenized_sequences)

    def __getitem__(self, idx):
        seq = self.tokenized_sequences[idx][:self.max_len]
        return {
            'input_ids': torch.tensor(seq, dtype=torch.long),
            'mortality': torch.tensor(self.mortality_labels[idx], dtype=torch.long),
            'rehospitalization': torch.tensor(self.rehosp_labels[idx], dtype=torch.long),
            'future_disease': torch.tensor(self.future_disease_labels[idx], dtype=torch.float)
        }


def _clean_code_str(val) -> str:
    """מנקה מזהים מספריים שנטענו כ-float (למשל '40481087.0' -> '40481087')"""
    s = str(val).strip()
    if s.endswith(".0"):
        return s[:-2]
    return s


def extract_patient_records(
    data_dir: str = "data",
    mapping_csv_path: str = None,
    mode: str = "mimic"
) -> Tuple[List[Dict], Dict[str, int]]:
    """
    טוען נתונים באופן דינמי ומחלץ היסטוריית ביקורים ו-3 תוויות מטרה:
    - mode='mimic': שליפה מ-admissions/edstays ו-diagnosis/diagnoses_icd
    - mode='omop_raw': שליפה מ-condition_occurrence, visit_occurrence ו-death
    - mode='omop_aggregated': שליפה מ-OMOP והמרת SNOMED בזמן אמת לפי mapping_csv
    """
    logging.info(f"--- Loading data in mode: [{mode}] from {data_dir} ---")

    mapping_dict = {}
    if mode == "omop_aggregated":
        if not mapping_csv_path or not os.path.exists(mapping_csv_path):
            alt_path = os.path.join(data_dir, os.path.basename(mapping_csv_path)) if mapping_csv_path else ""
            if os.path.exists(alt_path):
                mapping_csv_path = alt_path
            else:
                raise FileNotFoundError(f"Mapping CSV file not found at: {mapping_csv_path}")

        logging.info(f"Loading aggregation mapping from: {mapping_csv_path}")
        map_df = pd.read_csv(mapping_csv_path, low_memory=False)
        src_col = 'source_code' if 'source_code' in map_df.columns else map_df.columns[0]
        tgt_col = 'target_group_code' if 'target_group_code' in map_df.columns else map_df.columns[1]
        clean_src = map_df[src_col].dropna().apply(_clean_code_str)
        clean_tgt = map_df[tgt_col].dropna().apply(_clean_code_str)
        mapping_dict = dict(zip(clean_src, clean_tgt))
        logging.info(f"Loaded {len(mapping_dict):,} mapping rules into memory.")

    if mode == "mimic":
        # עדיפות ל-admissions.csv כי הוא מכיל hospital_expire_flag אמיתי של MIMIC-IV
        stays_path = os.path.join(data_dir, "admissions.csv")
        if not os.path.exists(stays_path) or os.path.getsize(stays_path) == 0:
            stays_path = os.path.join(data_dir, "edstays.csv")

        diag_path = os.path.join(data_dir, "diagnosis.csv")
        if not os.path.exists(diag_path) or os.path.getsize(diag_path) == 0:
            diag_path = os.path.join(data_dir, "diagnoses_icd.csv")

        if not os.path.exists(stays_path) or not os.path.exists(diag_path):
            raise FileNotFoundError(f"Missing required MIMIC files in {data_dir}")

        logging.info(f"Using MIMIC stays file: {stays_path}")
        stays_df = pd.read_csv(stays_path, low_memory=False)
        diag_df = pd.read_csv(diag_path, low_memory=False)

        in_col = 'admittime' if 'admittime' in stays_df.columns else 'intime'
        out_col = 'dischtime' if 'dischtime' in stays_df.columns else 'outtime'
        stay_id_col = 'hadm_id' if 'hadm_id' in stays_df.columns else 'stay_id'

        stays_df[in_col] = pd.to_datetime(stays_df[in_col])
        stays_df[out_col] = pd.to_datetime(stays_df[out_col])
        stays_df = stays_df.sort_values(by=['subject_id', in_col])

        # חישוב מרווח הימים מהשחרור הנוכחי לאשפוז הבא
        stays_df['next_intime'] = stays_df.groupby('subject_id')[in_col].shift(-1)
        stays_df['days_to_next'] = (stays_df['next_intime'] - stays_df[out_col]).dt.total_seconds() / (24 * 3600)
        stays_df['rehospitalized_30d'] = ((stays_df['days_to_next'] >= 0) & (stays_df['days_to_next'] <= 30)).astype(int)

        # תמותה באשפוז
        if 'hospital_expire_flag' in stays_df.columns:
            stays_df['mortality'] = stays_df['hospital_expire_flag'].fillna(0).astype(int)
        elif 'deathtime' in stays_df.columns:
            stays_df['mortality'] = stays_df['deathtime'].notnull().astype(int)
        elif 'dod' in stays_df.columns:
            stays_df['mortality'] = stays_df['dod'].notnull().astype(int)
        else:
            stays_df['mortality'] = 0

        # עיצוב טוקנים (ICD2BERT format: d_9_... / d_10_...)
        diag_df['icd_version'] = diag_df['icd_version'].astype(str)
        diag_df['formatted_code'] = (
            "d_" + diag_df['icd_version'] + "_" +
            diag_df['icd_code'].astype(str).str.replace(".", "", regex=False).str.strip()
        )

        diag_join_col = 'hadm_id' if 'hadm_id' in diag_df.columns else stay_id_col

        stays_cols_to_merge = [stay_id_col, in_col, 'rehospitalized_30d', 'mortality']
        if 'subject_id' not in diag_df.columns:
            stays_cols_to_merge.append('subject_id')

        merged = diag_df.merge(
            stays_df[stays_cols_to_merge],
            left_on=diag_join_col,
            right_on=stay_id_col,
            how='inner'
        )
        merged = merged.sort_values(by=['subject_id', in_col])

        patient_group_col = 'subject_id'
        visit_group_col = stay_id_col
        code_col = 'formatted_code'

    else:
        # שליפה מ-OMOP CDM
        cond_path = os.path.join(data_dir, "condition_occurrence.csv")
        visit_path = os.path.join(data_dir, "visit_occurrence.csv")
        death_path = os.path.join(data_dir, "death.csv")

        if not os.path.exists(cond_path) or not os.path.exists(visit_path):
            raise FileNotFoundError(f"Missing required OMOP files in {data_dir}")

        cond_df = pd.read_csv(cond_path, low_memory=False)
        visit_df = pd.read_csv(visit_path, low_memory=False)

        start_col = 'visit_start_date' if 'visit_start_date' in visit_df.columns else 'visit_start_datetime'
        end_col = 'visit_end_date' if 'visit_end_date' in visit_df.columns else 'visit_end_datetime'

        visit_df[start_col] = pd.to_datetime(visit_df[start_col])
        visit_df[end_col] = pd.to_datetime(visit_df[end_col]).fillna(visit_df[start_col])
        visit_df = visit_df.sort_values(by=['person_id', start_col])

        # חישוב מרווח הימים מהשחרור הנוכחי לביקור הבא
        visit_df['next_visit_date'] = visit_df.groupby('person_id')[start_col].shift(-1)
        visit_df['days_to_next'] = (visit_df['next_visit_date'] - visit_df[end_col]).dt.total_seconds() / (24 * 3600)
        visit_df['rehospitalized_30d'] = ((visit_df['days_to_next'] >= 0) & (visit_df['days_to_next'] <= 30)).astype(int)

        # תמותה מתוך טבלת omop.death (נתון אמת)
        if os.path.exists(death_path) and os.path.getsize(death_path) > 0:
            death_df = pd.read_csv(death_path, low_memory=False)
            dead_persons = set(death_df['person_id'].dropna().unique())
            visit_df['mortality'] = visit_df['person_id'].isin(dead_persons).astype(int)
            logging.info(f"Loaded {len(dead_persons):,} mortality person_ids from {death_path}")
        elif 'discharge_to_concept_id' in visit_df.columns:
            visit_df['mortality'] = (visit_df['discharge_to_concept_id'] == 4216643).astype(int)
        else:
            visit_df['mortality'] = 0

        visit_cols = ['visit_occurrence_id', start_col, 'rehospitalized_30d', 'mortality']
        if 'person_id' not in cond_df.columns:
            visit_cols.append('person_id')

        merged = cond_df.merge(visit_df[visit_cols], on='visit_occurrence_id', how='inner')
        merged = merged.sort_values(by=['person_id', start_col])

        patient_group_col = 'person_id'
        visit_group_col = 'visit_occurrence_id'
        code_col = 'condition_concept_id'

    records = []
    vocab_set = set()
    mapped_hits = 0
    total_tokens = 0

    # בניית רצפי המטופל לפי פרוטוקול Leave-the-last-one-out (תואם 1_create_dataset.py של ICD2BERT)
    for pid, pdata in merged.groupby(patient_group_col):
        visits_list = []
        visits_codes_raw = []
        visits_rehosp = []
        visits_mort = []

        for vid, vdata in pdata.groupby(visit_group_col, sort=False):
            raw_codes = [_clean_code_str(c) for c in vdata[code_col].dropna().tolist()]
            if not raw_codes:
                continue

            if mode == "omop_aggregated":
                processed_codes = []
                for c in raw_codes:
                    total_tokens += 1
                    if c in mapping_dict:
                        processed_codes.append(mapping_dict[c])
                        mapped_hits += 1
                    else:
                        processed_codes.append(c)
            else:
                processed_codes = raw_codes

            visits_list.append(processed_codes)
            visits_codes_raw.append(raw_codes)
            visits_rehosp.append(int(vdata['rehospitalized_30d'].iloc[0]))
            visits_mort.append(int(vdata['mortality'].iloc[0]))
            vocab_set.update(processed_codes)

        # סינון: לפחות 2 ביקורים (ביקורי היסטוריה עד penultimate + ביקור אחרון final לחיזוי)
        if len(visits_list) >= 2:
            last_visit_raw = visits_codes_raw[-1]

            # תרחיש 3: חילוץ 25 פרקי המחלות מהביקור האחרון (Future Disease Multi-label)
            future_chapters = set()
            for code in last_visit_raw:
                c_str = str(code)
                if c_str.startswith("d_10_") and len(c_str) > 5 and c_str[5].isalpha():
                    future_chapters.add(c_str[5].upper())
                elif c_str.startswith("d_9_") and len(c_str) > 4:
                    future_chapters.add("E" if c_str[4].upper() == "E" else ("V" if c_str[4].upper() == "V" else "A"))
                else:
                    idx = abs(hash(c_str)) % len(DISEASE_CHAPTERS)
                    future_chapters.add(DISEASE_CHAPTERS[idx])

            future_vec = [1 if ch in future_chapters else 0 for ch in DISEASE_CHAPTERS]

            records.append({
                'patient_id': pid,
                'history_visits': visits_list[:-1],
                'mortality': visits_mort[-1],           # תמותה באשפוז האחרון (final admission)
                'rehospitalization': visits_rehosp[-2],  # חזרה תוך 30 יום מהאשפוז הלפני-אחרון (penultimate) לאחרון (final)
                'future_disease': future_vec             # מחלות באשפוז האחרון (final admission)
            })

    vocab_to_id = {'[PAD]': 0, '[CLS]': 1, '[SEP]': 2, '[MASK]': 3, '[UNK]': 4}
    for idx, token in enumerate(sorted(vocab_set), start=5):
        vocab_to_id[token] = idx

    mort_pos = sum(r['mortality'] for r in records)
    rehosp_pos = sum(r['rehospitalization'] for r in records)
    if mode == "omop_aggregated" and total_tokens > 0:
        logging.info(f"Aggregation mapping coverage: {mapped_hits:,}/{total_tokens:,} tokens ({mapped_hits/total_tokens*100:.2f}%)")

    logging.info(
        f"Completed extraction for mode [{mode}]: {len(records):,} patients | "
        f"Vocab: {len(vocab_to_id):,} | Mortality+: {mort_pos:,} ({mort_pos/max(len(records),1)*100:.2f}%) | "
        f"Rehosp+: {rehosp_pos:,} ({rehosp_pos/max(len(records),1)*100:.2f}%)"
    )
    return records, vocab_to_id


def build_token_sequences(records: List[Dict], vocab_to_id: Dict[str, int]):
    """
    ממיר ביקורים לרצפי טוקנים ומחזיר את רשימות התוויות ל-3 המשימות.
    """
    unk_id = vocab_to_id['[UNK]']
    cls_id = vocab_to_id['[CLS]']
    sep_id = vocab_to_id['[SEP]']

    tokenized_seqs = []
    mortality_labels = []
    rehosp_labels = []
    future_disease_labels = []

    for item in records:
        tokens = [cls_id]
        for visit in item['history_visits']:
            for code in visit:
                tokens.append(vocab_to_id.get(str(code), unk_id))
            tokens.append(sep_id)

        tokenized_seqs.append(tokens)
        mortality_labels.append(item['mortality'])
        rehosp_labels.append(item['rehospitalization'])
        future_disease_labels.append(item['future_disease'])

    return tokenized_seqs, mortality_labels, rehosp_labels, future_disease_labels