import pandas as pd
import numpy as np
import os

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

def get_path(folder_path, file_name):
    """Joins directory path with file name safely."""
    return os.path.join(folder_path, file_name)


def clean_code(series):
    """Cleans quotes, dots, and trailing spaces from medical codes."""
    return series.astype(str).str.replace("'", "", regex=False).str.replace(".", "", regex=False).str.strip()


# ==========================================
# STEP 1: DATA LOADING AND PREPARATION
# ==========================================

def load_source_files(input_folder):
    """Loads and pre-cleans all required source files from the given directory."""
    print("\n--- Step 1: Loading Source Files ---")
    
    concept = pd.read_csv(get_path(input_folder, "CONCEPT.csv"), sep='\t', low_memory=False)
    concept_ancestor = pd.read_csv(get_path(input_folder, "CONCEPT_ANCESTOR.csv"), sep='\t', low_memory=False)

    relationship_path = get_path(input_folder, "CONCEPT_RELATIONSHIP.csv")
    if os.path.exists(relationship_path):
        concept_relationship = pd.read_csv(relationship_path, sep='\t', low_memory=False)
        has_relationships = True
    else:
        print("! Warning: CONCEPT_RELATIONSHIP.csv not found. SNOMED-to-CCSR mapping might be limited.")
        concept_relationship = None
        has_relationships = False

    ccsr_df = pd.read_csv(get_path(input_folder, "DXCCSR_v2026-1.csv"), low_memory=False)
    ccs_df = pd.read_csv(get_path(input_folder, "CCS_v2008.csv"), low_memory=False, skiprows=1)

    # Pre-cleaning external files data
    ccsr_df.columns = ccsr_df.columns.str.replace("'", "", regex=False).str.strip()
    ccsr_clean = ccsr_df[['ICD-10-CM CODE', 'Default CCSR CATEGORY IP', 'Default CCSR CATEGORY DESCRIPTION IP']].dropna()
    ccsr_clean.columns = ['icd_code', 'target_ccsr', 'ccsr_description']
    ccsr_clean['clean_code'] = clean_code(ccsr_clean['icd_code'])

    ccs_df.columns = ccs_df.columns.str.replace("'", "", regex=False).str.strip()
    ccs_clean = ccs_df[['ICD-9-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']].dropna()
    ccs_clean.columns = ['icd_code', 'target_ccs', 'ccs_description']
    ccs_clean['clean_code'] = clean_code(ccs_clean['icd_code'])

    return concept, concept_ancestor, concept_relationship, has_relationships, ccsr_clean, ccs_clean


# ==========================================
# STEP 2: MAPPING GENERATION TASKS (WITH DEFAULT FALSE)
# ==========================================

def get_icd9_ccs_mapping(ccs_clean, force_recreate=False):
    """Generates or loads the ICD-9 to CCS mapping file. Defaults to force_recreate=False."""
    file_name = "mapping_icd9_to_ccs.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)
    
    print(f"Generating: {file_name}")
    mapping = ccs_clean[['icd_code', 'target_ccs', 'ccs_description']]
    mapping.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name}")
    return mapping


def get_icd10_ccsr_mapping(ccsr_clean, force_recreate=False):
    """Generates or loads the ICD-10 to CCSR mapping file. Defaults to force_recreate=False."""
    file_name = "mapping_icd10_to_ccsr.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)
        
    print(f"Generating: {file_name}")
    mapping = ccsr_clean[['icd_code', 'target_ccsr', 'ccsr_description']]
    mapping.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name}")
    return mapping


def get_snomed_tree_mapping(concept, concept_ancestor, force_recreate=False):
    """Generates or loads the SNOMED Built-in Tree Level 2 mapping file. Defaults to force_recreate=False."""
    file_name = "mapping_snomed_tree_lvl2.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)

    print(f"Generating: {file_name}")
    snomed_concepts = concept[concept['vocabulary_id'] == 'SNOMED']
    snomed_tree = concept_ancestor.merge(snomed_concepts, left_on='descendant_concept_id', right_on='concept_id') \
                                  .merge(snomed_concepts, left_on='ancestor_concept_id', right_on='concept_id', suffixes=('_src', '_target'))
    snomed_tree_lvl2 = snomed_tree[snomed_tree['min_levels_of_separation'] == 2]

    mapping_snomed_tree = snomed_tree_lvl2[['concept_code_src', 'concept_name_src', 'concept_code_target', 'concept_name_target']].copy()
    mapping_snomed_tree.columns = ['source_snomed_code', 'source_snomed_name', 'parent_snomed_code', 'parent_snomed_name']
    mapping_snomed_tree.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name}")
    return mapping_snomed_tree


def get_snomed_ccsr_mapping(concept, concept_ancestor, concept_relationship, has_relationships, ccsr_clean, force_recreate=False):
    """Generates or loads the cross-vocabulary SNOMED to CCSR mapping file. Defaults to force_recreate=False."""
    file_name = "mapping_snomed_to_ccsr.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)

    print(f"Generating: {file_name}")
    snomed_concepts = concept[concept['vocabulary_id'] == 'SNOMED']
    icd10_concepts = concept[concept['vocabulary_id'] == 'ICD10CM'].copy() # Added copy to prevent warnings
    
    # CRITICAL FIX: Clean the dots from OMOP ICD-10 codes before matching with CCSR
    icd10_concepts['clean_icd10'] = clean_code(icd10_concepts['concept_code'])
    
    if has_relationships and concept_relationship is not None:
        maps_to = concept_relationship[concept_relationship['relationship_id'] == 'Maps to']
        icd10_to_snomed = maps_to.merge(icd10_concepts, left_on='concept_id_1', right_on='concept_id') \
                                 .merge(snomed_concepts, left_on='concept_id_2', right_on='concept_id', suffixes=('_icd10', '_snomed'))
        
        # Merge using the cleaned columns on both sides
        mapping_snomed_ccsr = icd10_to_snomed.merge(ccsr_clean, left_on='clean_icd10', right_on='clean_code', how='inner')
        mapping_snomed_ccsr_clean = mapping_snomed_ccsr[['concept_code_snomed', 'concept_name_snomed', 'target_ccsr', 'ccsr_description']].drop_duplicates()
    else:
        snomed_to_icd10 = concept_ancestor.merge(snomed_concepts, left_on='ancestor_concept_id', right_on='concept_id') \
                                          .merge(icd10_concepts, left_on='descendant_concept_id', right_on='concept_id', suffixes=('_snomed', '_icd10'))
        
        # Merge using the cleaned columns on both sides
        mapping_snomed_ccsr = snomed_to_icd10.merge(ccsr_clean, left_on='clean_icd10', right_on='clean_code', how='inner')
        mapping_snomed_ccsr_clean = mapping_snomed_ccsr[['concept_code_snomed', 'concept_name_snomed', 'target_ccsr', 'ccsr_description']].drop_duplicates()

    # If mapping is empty, provide a descriptive print to debug vocabulary overlap
    if mapping_snomed_ccsr_clean.empty:
        print("! Warning: SNOMED to CCSR mapping produced 0 rows. Check if CONCEPT_RELATIONSHIP contains 'Maps to' links between ICD10CM and SNOMED.")

    mapping_snomed_ccsr_clean.columns = ['source_snomed_code', 'source_snomed_name', 'target_ccsr_code', 'ccsr_description']
    mapping_snomed_ccsr_clean.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name}")
    return mapping_snomed_ccsr_clean


# ==========================================
# STEP 3: STATISTICS GENERATION
# ==========================================

def compute_and_save_statistics(ccs_clean, ccsr_clean, mapping_snomed_tree, mapping_snomed_ccsr_clean):
    """Computes, prints, and saves the node compression stats."""
    print("\n--- Step 3: Computing Summary Statistics ---")
    results = []

    # 3.1 Method: ICD-9 to CCS
    nodes_before_ccs = ccs_clean['clean_code'].nunique()
    nodes_after_ccs = ccs_clean[ccs_clean['target_ccs'] != '0']['target_ccs'].nunique()
    pct_ccs = ((nodes_before_ccs - nodes_after_ccs) / nodes_before_ccs) * 100 if nodes_before_ccs > 0 else 0
    results.append({
        'Source Vocabulary': 'ICD-9',
        'Aggregation Method': 'CCS (Single-Level)',
        'Nodes Before Aggregation': nodes_before_ccs,
        'Nodes After Aggregation': nodes_after_ccs,
        'Absolute Node Reduction': nodes_before_ccs - nodes_after_ccs,
        'Reduction Percentage': f"{pct_ccs:.2f}%"
    })

    # 3.2 Method: ICD-10 to CCSR
    nodes_before_ccsr = ccsr_clean['clean_code'].nunique()
    nodes_after_ccsr = ccsr_clean[ccsr_clean['target_ccsr'] != 'XXX000']['target_ccsr'].nunique()
    pct_ccsr = ((nodes_before_ccsr - nodes_after_ccsr) / nodes_before_ccsr) * 100 if nodes_before_ccsr > 0 else 0
    results.append({
        'Source Vocabulary': 'ICD-10',
        'Aggregation Method': 'CCSR (Default IP)',
        'Nodes Before Aggregation': nodes_before_ccsr,
        'Nodes After Aggregation': nodes_after_ccsr,
        'Absolute Node Reduction': nodes_before_ccsr - nodes_after_ccsr,
        'Reduction Percentage': f"{pct_ccsr:.2f}%"
    })

    # 3.3 Method: SNOMED Built-in Tree (Level 2)
    src_sn_col = 'source_snomed_code' if 'source_snomed_code' in mapping_snomed_tree.columns else mapping_snomed_tree.columns[0]
    parent_sn_col = 'parent_snomed_code' if 'parent_snomed_code' in mapping_snomed_tree.columns else mapping_snomed_tree.columns[2]
    
    nodes_before_snomed = mapping_snomed_tree[src_sn_col].nunique()
    nodes_after_snomed = mapping_snomed_tree[parent_sn_col].nunique()
    pct_snomed = ((nodes_before_snomed - nodes_after_snomed) / nodes_before_snomed) * 100 if nodes_before_snomed > 0 else 0
    results.append({
        'Source Vocabulary': 'SNOMED',
        'Aggregation Method': 'Built-in Tree (Lvl 2)',
        'Nodes Before Aggregation': nodes_before_snomed,
        'Nodes After Aggregation': nodes_after_snomed,
        'Absolute Node Reduction': nodes_before_snomed - nodes_after_snomed,
        'Reduction Percentage': f"{pct_snomed:.2f}%"
    })

    # 3.4 Method: SNOMED to CCSR
    src_sn_ccsr_col = 'source_snomed_code' if 'source_snomed_code' in mapping_snomed_ccsr_clean.columns else mapping_snomed_ccsr_clean.columns[0]
    target_ccsr_col = 'target_ccsr_code' if 'target_ccsr_code' in mapping_snomed_ccsr_clean.columns else mapping_snomed_ccsr_clean.columns[2]
    
    nodes_before_sn_ccsr = mapping_snomed_ccsr_clean[src_sn_ccsr_col].nunique()
    nodes_after_sn_ccsr = mapping_snomed_ccsr_clean[mapping_snomed_ccsr_clean[target_ccsr_col] != 'XXX000'][target_ccsr_col].nunique()
    pct_sn_ccsr = ((nodes_before_sn_ccsr - nodes_after_sn_ccsr) / nodes_before_sn_ccsr) * 100 if nodes_before_sn_ccsr > 0 else 0
    results.append({
        'Source Vocabulary': 'SNOMED',
        'Aggregation Method': 'CCSR (via ICD-10)',
        'Nodes Before Aggregation': nodes_before_sn_ccsr,
        'Nodes After Aggregation': nodes_after_sn_ccsr,
        'Absolute Node Reduction': nodes_before_sn_ccsr - nodes_after_sn_ccsr,
        'Reduction Percentage': f"{pct_sn_ccsr:.2f}%"
    })

    summary_df = pd.DataFrame(results)
    summary_df.to_csv("nodes_compression_summary_final.csv", index=False)

    print("\n==========================================================================================")
    print(" Summary Node Compression Table:")
    print("==========================================================================================")
    print(summary_df.to_string(index=False))


# ==========================================
# MAIN EXECUTION CORE
# ==========================================

def main():
    print("--- Starting Full Mapping Process ---")

    # CONFIGURATION PARAMETERS
    input_folder = "vocabulary_all" 
    force_recreate = False  # Simply change this to True whenever you want a fresh execution
    
    print(f"Reading source files from folder: {input_folder}")
    print(f"Force Recreate Mappings Flag: {force_recreate}")

    # Step 1: Load data from folder
    concept, concept_ancestor, concept_relationship, has_relationships, ccsr_clean, ccs_clean = load_source_files(input_folder)

    print("\n--- Step 2: Generating / Fetching Mapping Files ---")
    
    # Run mapping tasks (the parameter defaults to False inside functions if not provided, but here we pass it explicitly)
    get_icd9_ccs_mapping(ccs_clean, force_recreate)
    get_icd10_ccsr_mapping(ccsr_clean, force_recreate)
    mapping_snomed_tree = get_snomed_tree_mapping(concept, concept_ancestor, force_recreate)
    mapping_snomed_ccsr_clean = get_snomed_ccsr_mapping(concept, concept_ancestor, concept_relationship, has_relationships, ccsr_clean, force_recreate)

    # Step 3: Compute final metrics matrix
    compute_and_save_statistics(ccs_clean, ccsr_clean, mapping_snomed_tree, mapping_snomed_ccsr_clean)


if __name__ == "__main__":
    main()