# Kineret IBD Research – Severity Analysis and Prediction

## Overview

This project investigates the identification and prediction of severe disease in patients with **Inflammatory Bowel Disease (IBD)** using longitudinal electronic health record (EHR) data represented in the **OMOP Common Data Model (OMOP CDM)**.

The research combines exploratory data analysis, longitudinal clinical feature engineering, severity assessment, statistical analysis, and predictive modeling.

The current work focuses primarily on Crohn's disease and integrates information from multiple clinical domains, including diagnoses, visits, observations, procedures, medications, and laboratory measurements.

> **Data privacy:** The clinical data are accessed within a secure research environment and are not included in this repository. The repository contains research code and methodology only; patient-level data, institution-specific configuration, and protected research outputs are excluded.

---

## Research Objectives

The project aims to:

- characterize the available longitudinal IBD data
- reconstruct clinically meaningful patient timelines and hospitalization episodes
- derive clinically relevant patient-level features from OMOP data
- develop and evaluate a framework for identifying severe Crohn's disease
- statistically characterize differences between patients with different severity profiles
- investigate whether clinical information can be used to predict severe disease

The exact prediction design, outcome definition, and temporal framework are being developed and validated as part of the ongoing research.

---

## Research Workflow

```text
OMOP CDM Data
      │
      ▼
Data Exploration & Validation
      │
      ▼
Longitudinal Patient Timelines
      │
      ▼
Hospitalization Reconstruction
      │
      ▼
Clinical Feature Engineering
      │
      ▼
Severity Assessment
      │
      ▼
Statistical Analysis
      │
      ▼
Predictive Modeling
      │
      ▼
Evaluation & Interpretation
```

---

## Data

The analysis integrates the following OMOP domains:

| OMOP Table | Information Used |
|---|---|
| `person` | Demographics |
| `visit_occurrence` | Encounters and hospitalizations |
| `visit_detail` | Additional visit information |
| `condition_occurrence` | Diagnoses, complications, and symptoms |
| `observation` | Clinical observations |
| `procedure_occurrence` | Procedures and surgeries |
| `drug_exposure` | Medication exposure |
| `measurement` | Laboratory and physiological measurements |
| `death` | Survival status |

The standardized OMOP structure enables a common analytical methodology to be applied across multiple clinical data sources.

---

# Exploratory Data Analysis

Before constructing the patient-level dataset, exploratory and statistical analyses were performed across the main OMOP domains.

The analysis included:

- distributions and frequencies of clinical concepts
- Crohn's and colitis patient characterization
- age and timing of recorded diagnoses
- healthcare utilization and visit patterns
- medication and procedure concept analysis
- laboratory value distributions and measurement units
- missingness and data-quality inspection
- identification of candidate clinical concepts for feature construction
- generation of concept lists for clinical review

This stage was used both to understand the structure and quality of the available data and to support clinically meaningful feature definitions.

---

# Hospitalization Reconstruction

Raw OMOP visit records do not necessarily correspond one-to-one with clinically meaningful hospitalization episodes. A continuous episode of care may contain multiple visits representing different departments or encounter types.

To reconstruct these episodes, consecutive visits are merged when:

- their time intervals overlap, or
- the gap between them is no greater than **48 hours**

Each reconstructed hospitalization retains information such as:

- start and end time
- associated visit IDs
- visit types and departments
- inpatient status
- hospitalization number
- length of stay
- patient age at hospitalization

This creates a longitudinal hospitalization history for each patient.

---

# Crohn's Disease Timeline

Crohn's-related diagnoses are identified from longitudinal condition records and linked to reconstructed hospitalization episodes.

The workflow derives information including:

- first Crohn's-related hospitalization
- age at first Crohn's diagnosis
- Crohn's and colitis diagnosis history
- previous hospitalization history
- time since the previous hospitalization
- hospitalization burden before Crohn's diagnosis

This allows clinical information to be interpreted within the patient's disease timeline rather than as isolated EHR events.

---

# Clinical Feature Engineering

The longitudinal OMOP records are transformed into a structured patient-level feature table.

## Demographics and Utilization

Features include:

- birth year and sex
- survival status
- smoking
- age at first Crohn's diagnosis
- total visits and hospitalizations
- relevant hospitalizations
- hospitalization counts within different age ranges
- previous hospitalization history

## Disease and Complications

Current features include:

- Crohn's disease and colitis history
- fistula
- abscess
- fissure
- intestinal obstruction
- diarrhea
- abdominal pain

## Treatments and Procedures

Clinical history includes:

- bowel resection
- abscess drainage
- corticosteroid exposure
- biological treatment

Medication and procedure definitions are progressively validated using clinician-reviewed concepts and across the different clinical data sources.

## Laboratory and Physiological Features

Current measurements include:

- body temperature
- heart rate
- BMI and weight
- leukocyte count
- platelet count
- hemoglobin
- albumin
- CRP

Measurements retain their relationship to the hospitalization in which they were recorded, allowing the clinical timeline to be preserved during later analysis.

---

# Severity Assessment

The project includes a clinically informed framework for characterizing disease severity.

The current severity framework combines information from several domains, including:

- hospitalization burden
- disease complications
- surgical procedures
- medication exposure
- symptoms
- laboratory abnormalities
- physiological measurements

The implementation produces both an overall severity score and the individual contribution of each component, allowing the score to be inspected and evaluated.

The current score is an **intermediate research definition** rather than a finalized clinical ground truth. Its components and thresholds are being evaluated using data-level checks and clinical review.

---

# Statistical Analysis

Following feature construction, the project evaluates the characteristics associated with different severity profiles.

The statistical stage includes:

- descriptive characterization of the study population
- comparison of clinical features between patient groups
- analysis of continuous and categorical variables
- evaluation of statistical differences between severity groups
- construction of a clinical **Table 1**

Appropriate statistical tests are selected according to variable type, distribution, and group size.

The goal is to understand which clinical characteristics are associated with severity before and alongside predictive modeling.

---

# Predictive Modeling

A subsequent stage of the research will investigate whether clinical features derived from longitudinal EHR data can be used to **predict severe or complicated Crohn's disease**.

The predictive modeling workflow is expected to include:

- definition of the prediction cohort and outcome
- selection of information available at the relevant prediction point
- construction of the modeling dataset
- model training and validation
- evaluation using appropriate classification metrics
- analysis of feature importance
- interpretation of clinically relevant predictors

The specific model architecture, prediction point, and temporal design have not yet been finalized and will be determined together with the research and clinical team.

This separation is important: the current work establishes the clinical data representation and severity framework, while the predictive stage will evaluate how well available patient information can anticipate the defined outcome.

---

# Data Quality and Clinical Validation

Clinical EHR data require additional validation before statistical or predictive analysis.

The project therefore includes checks for:

- measurement units and clinically plausible ranges
- missing and inconsistent values
- terminology differences across clinical data sources
- medication and procedure concept definitions
- age relative to clinical events
- temporal relationships between diagnoses, hospitalizations, treatments, and measurements

Clinical concept definitions are also compared with clinician-reviewed lists where available.

Because the study includes multiple clinical data sources, concepts observed in one source are not assumed to represent all possible terminology used elsewhere.

---

# Repository Structure

```text
IBD_Research_Kineret/
│
├── README.md
└── feature_engineering.ipynb
```

### `feature_engineering.ipynb`

The notebook currently implements the main longitudinal feature-engineering workflow:

- OMOP data preparation
- patient-level organization
- hospitalization reconstruction
- Crohn's timeline construction
- clinical feature extraction
- patient-level dataset generation
- initial severity assessment

Additional statistical and modeling components will be added as the research progresses.

---

# Current Status

### Implemented

- exploratory analysis of the major OMOP domains
- clinical concept and measurement analysis
- longitudinal patient organization
- hospitalization reconstruction
- Crohn's disease timeline construction
- demographic and healthcare-utilization features
- complication and symptom features
- medication and procedure features
- laboratory and physiological features
- patient-level feature dataset
- preliminary severity framework

### Ongoing

- severity-score refinement and clinical validation
- cross-database concept validation
- statistical characterization and Table 1
- definition of the prediction framework
- predictive modeling and evaluation

---

# Technologies

- **Python**
- **Pandas**
- **NumPy**
- **Jupyter**
- **Matplotlib**
- **OMOP Common Data Model**
- **Parquet**

---

# Privacy and Reproducibility

The original analysis is performed within a controlled environment containing protected clinical data.

This repository therefore does not include patient-level datasets, identifiers, institution-specific paths, credentials, protected outputs, or restricted research-environment configuration.

The public repository documents the **analytical methodology and code structure** while preserving the privacy and governance requirements of the underlying clinical data.

---

# Disclaimer

This repository contains code developed for retrospective clinical research.

The severity framework and future predictive models are research tools and are **not validated clinical decision-support systems**. They should not be used for diagnosis, treatment decisions, or direct patient care.

---

## Project

**Kineret IBD Research Project**  
Longitudinal clinical data analysis, severity characterization, and predictive research using the OMOP Common Data Model.
