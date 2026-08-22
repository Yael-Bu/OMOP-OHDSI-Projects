import json
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine


def extract_cohort_statistics(db_uri: str, schema: str = "omop") -> dict:
  """Extracts demographic, visit, and condition statistics from OMOP CDM."""
  logging_header = "Extracting cohort statistics from OMOP CDM..."
  print(f"\n--- {logging_header} ---")
  engine = create_engine(db_uri)

  # 1. Query patient visits and demographics
  sql_cohort = f"""
    WITH patient_summary AS (
        SELECT 
            vo.person_id,
            vo.visit_occurrence_id,
            EXTRACT(YEAR FROM vo.visit_start_date) - p.year_of_birth AS age_at_visit,
            p.gender_concept_id
        FROM 
            {schema}.visit_occurrence vo
            INNER JOIN {schema}.person p ON vo.person_id = p.person_id
    ),
    patient_visit_counts AS (
        SELECT 
            person_id,
            COUNT(DISTINCT visit_occurrence_id) AS total_visits,
            MIN(age_at_visit) AS min_age,
            MAX(age_at_visit) AS max_age,
            AVG(age_at_visit) AS avg_age,
            MAX(gender_concept_id) AS gender_id
        FROM 
            patient_summary
        GROUP BY 
            person_id
    )
    SELECT * FROM patient_visit_counts;
    """

  # 2. Query total condition occurrences and distinct concepts
  sql_diagnoses = f"""
    SELECT 
        COUNT(*) AS total_diagnosis_events,
        COUNT(DISTINCT condition_concept_id) AS unique_condition_concepts,
        COUNT(DISTINCT person_id) AS patients_with_diagnoses
    FROM 
        {schema}.condition_occurrence;
    """

  print("Executing database queries...")
  df_patients = pd.read_sql(sql_cohort, con=engine)
  df_diag = pd.read_sql(sql_diagnoses, con=engine)

  # 3. Apply cohort inclusion criteria filters
  eligible_2plus = df_patients[df_patients["total_visits"] >= 2]
  eligible_5plus = df_patients[df_patients["total_visits"] >= 5]

  stats = {
      "total_patients_in_db": int(len(df_patients)),
      "total_visit_occurrences": int(df_patients["total_visits"].sum()),
      "total_condition_records": int(df_diag["total_diagnosis_events"].iloc[0]),
      "unique_condition_concepts": int(
          df_diag["unique_condition_concepts"].iloc[0]
      ),
      "patients_with_diagnoses": int(
          df_diag["patients_with_diagnoses"].iloc[0]
      ),
      "patients_with_at_least_2_visits": int(len(eligible_2plus)),
      "patients_with_at_least_5_visits": int(len(eligible_5plus)),
      "mean_visits_per_patient": float(df_patients["total_visits"].mean()),
      "median_visits_per_patient": float(df_patients["total_visits"].median()),
      "mean_age_at_visit": float(df_patients["avg_age"].mean()),
      "std_age_at_visit": float(df_patients["avg_age"].std()),
      "median_age_and_iqr": (
          f"{df_patients['avg_age'].median():.1f} (IQR:"
          f" {df_patients['avg_age'].quantile(0.25):.1f} -"
          f" {df_patients['avg_age'].quantile(0.75):.1f})"
      ),
  }

  print("\n" + "=" * 60)
  print(" COHORT & DATASET SUMMARY STATISTICS:")
  print("=" * 60)
  for key, value in stats.items():
    print(f"{key}: {value}")
  print("=" * 60)

  output_path = "cohort_statistics_summary.json"
  with open(output_path, "w", encoding="utf-8") as f:
    json.dump(stats, f, indent=4)
  print(f"Summary successfully exported to: '{output_path}'")

  return stats


if __name__ == "__main__":
  username = os.environ.get("DB_USER")
  password = os.environ.get("DB_PASS")
  postgres_ip = os.environ.get("DB_HOST")
  db_name = os.environ.get("DB_NAME", "omop_med_db")
  port = os.environ.get("DB_PORT", "5432")

  db_uri = f"postgresql://{username}:{password}@{postgres_ip}:{port}/{db_name}"
  extract_cohort_statistics(db_uri=db_uri, schema="omop")