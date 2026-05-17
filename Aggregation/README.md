# Aggregation Pipeline Tools (OMOP-OHDSI-Projects)

This repository contains a suite of advanced Python pipeline tools designed for navigating, processing, and aggregating clinical hierarchies within the **OMOP Common Data Model (CDM)** using the **SNOMED CT** ontology. 

The project supports two primary architectural paradigms for clinical data analysis:
1. **Top-Down (Hierarchical Trees & Visualizations):** Generates clean, strict ancestral and descendant trees for specific diseases to support clinical mapping and review.
2. **Bottom-Up (Data Aggregation & Dimensionality Reduction):** Implements international OHDSI FeatureExtraction standards to aggregate thousands of granular clinical codes into robust, high-level groups for epidemiological research and machine learning models.

---

## 📂 Project Directory Structure

To run these scripts successfully, arrange your local workspace directory as follows. The large vocabulary files downloaded from Athena must be placed inside the `vocabulary/` subdirectory.

```text
Aggregation/
├── vocabulary/
│   ├── CONCEPT.csv                  # Large OMOP vocabulary table (TSV format)
│   └── CONCEPT_ANCESTOR.csv         # Large OMOP hierarchy table (TSV format)
├── .gitignore                       # Safeguard file to prevent pushing large data to GitHub
├── generate_trees.py                # Top-Down visualization and JSON tree builder
├── bottom_up_pipeline.py            # Bottom-Up OHDSI aggregation mapping tool
└── README.md                        # Project documentation (this file)
```

## 📥 How to Download Vocabularies from Athena (OHDSI)

The pipeline relies on standard OMOP CDM vocabulary files. Follow these steps to obtain the latest version from the official OHDSI Athena portal:

1. Create an Account: Go to OHDSI Athena and log in or create a free account.

2. Navigate to Downloads: Click on the Download tab in the top navigation bar.

2. Select Vocabularies: * By default, core vocabularies (None, Type Concept, etc.) are pre-selected.

3. Search for and check SNOMED (essential for clinical conditions). 
   (Optional) If you plan to expand the pipeline to drugs, check ATC.

4. Request Download: Scroll to the bottom, click Download, give your request a name, and submit.

5. Download the Zip: OHDSI will process your request and send an email notification once it is ready (usually within a few minutes to an hour). Go to the Download History tab on Athena to download the generated .zip file.

6. Extract the Files: Extract the contents of the zip file directly into a folder named vocabulary located inside your Aggregation project directory. Ensure the filenames are exactly CONCEPT.csv and CONCEPT_ANCESTOR.csv.

## 🛠️ Prerequisites & Installation
Before running the scripts, ensure you have the required Python libraries and system dependencies installed.

1. Python Libraries

Install the necessary data processing and graph utilities via terminal:

```Bash
pip install pandas graphviz
```

2. System Dependency (Graphviz)

The Python graphviz package is a wrapper that requires the actual Graphviz rendering binary installed on your operating system:

Mac (via Homebrew):

```Bash
brew install graphviz
```

Windows: Download the installer from the Graphviz Official Site. During installation, make sure to check the box: "Add Graphviz to the system PATH for all users".

3. GitHub Size Limitation Warning (.gitignore)

The processed vocabulary files and output sheets easily exceed GitHub's 100MB hard size limit. To avoid pre-receive hook declined errors during code commits, ensure your .gitignore file includes the following exclusions:

```text
vocabulary/
*.json
*.png
*.csv
.DS_Store
```

## 🚀 Script Documentation & Execution
### 1. Top-Down Tool: generate_trees.py

Purpose: Builds a clean, hierarchical tree structure starting from the absolute clinical root (Clinical finding - SNOMED 404684003), tracing all valid ancestral paths down to a target disease, and diving exactly N levels into its sub-types.

#### Special Features: 
* Automatically handles polyhierarchy upwards (merges shared ancestor paths).

* Deduplicates descendants downwards (prevents "spiderweb" loops so each sub-condition appears exactly once).

* Renders beautiful horizontal PNG layouts using clean polyline alignments and desaturated, professional color palettes.

* Configuration: Edit the diseases_pool dictionary at the bottom of the script to specify target codes and depths:

```python
diseases_pool = {
    "73211009": 3,   # Diabetes mellitus (Dives 3 levels below)
    "34000006": 2    # Crohn's disease (Dives 2 levels below)
}
```

#### How to Run:

```bash
python generate_trees.py
```

#### Outputs Generated: 

For each disease, a matching pair of files will appear:

* <Disease_Name>.json – Hierarchical structural JSON data.

* <Disease_Name>.png – Highly polished vector-graph diagram.

### 2. Bottom-Up Tool: bottom_up_pipeline.py

#### Purpose: 
Translates official OHDSI SQL FeatureExtraction logic into highly optimized Python processes. It eliminates overly broad administrative concepts (e.g., categories starting with "Disorder of...", "Disease of...") and maps highly granular patient-level conditions directly to stable, high-level clinical aggregation groups.

#### Clinical Exception Whitelist: 
Preserves 20 crucial, high-level concepts that would normally be filtered out by distance rules but are structurally vital for analytical modeling (e.g., 433736: Obesity, 433595: Edema, 441408: Vomiting).

#### How to Run:

```bash
python bottom_up_pipeline.py
```

#### Outputs Generated: 

* ohdsi_bottom_up_map.json: 
A lightning-fast lookup dictionary mapping thousands of internal descendant_concept_id keys directly to lists of approved ancestor_concept_id groups for backend code execution.

* ohdsi_bottom_up_readable_map.csv: 
A fully translated, tabular reference sheet containing clear columns for internal IDs, official SNOMED codes, and readable English names for both patient conditions and their rolled-up targets. Perfect for inspection using the VS Code Edit CSV extension or Excel.

## 📊 Output Interpretation Guide
When examining the generated files, rows represent logical mapping linkages. For example:

| descendant_concept_id | patient_snomed_code | patient_disease_name | ancestor_concept_id | aggregated_group_snomed_code | aggregated_group_name |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 43531597 | 201000119106 | Disorder due to well controlled type 2 diabetes mellitus | 201820 | 73211009 | Diabetes mellitus |

> **Interpretation:** If a longitudinal clinical dataset contains a patient diagnosed with the highly specific condition *Disorder due to well controlled type 2 diabetes mellitus*, the pipeline automatically rolls this data up, grouping the patient under the clean, stable category *Diabetes mellitus* for statistical model inputs.

Developed as part of the OMOP-OHDSI-Projects initiative.