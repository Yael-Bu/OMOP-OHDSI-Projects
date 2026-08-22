import os
import numpy as np
import pandas as pd

# =====================================================================
# UTILITY FUNCTIONS
# =====================================================================

def get_path(folder_path: str, file_name: str) -> str:
    """Joins directory path with file name safely."""
    return os.path.join(folder_path, file_name)


def clean_code(series: pd.Series) -> pd.Series:
    """Cleans quotes, dots, and trailing spaces from medical codes."""
    return (
        series.astype(str)
        .str.replace("'", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.strip()
    )


# =====================================================================
# STEP 1: DATA LOADING AND PREPARATION
# =====================================================================

def load_source_files(input_folder: str):
    """Loads and pre-cleans all required source vocabulary files."""
    print("\n--- Step 1: Loading Source Files ---")
    
    concept = pd.read_csv(get_path(input_folder, "CONCEPT.csv"), sep='\t', low_memory=False)
    concept_ancestor = pd.read_csv(get_path(input_folder, "CONCEPT_ANCESTOR.csv"), sep='\t', low_memory=False)

    relationship_path = get_path(input_folder, "CONCEPT_RELATIONSHIP.csv")
    if os.path.exists(relationship_path):
        concept_relationship = pd.read_csv(relationship_path, sep='\t', low_memory=False)
        has_relationships = True
    else:
        print("! Warning: CONCEPT_RELATIONSHIP.csv not found.")
        concept_relationship = None
        has_relationships = False

    ccsr_df = pd.read_csv(get_path(input_folder, "DXCCSR_v2026-1.csv"), low_memory=False)
    ccs_df = pd.read_csv(get_path(input_folder, "CCS_v2008.csv"), low_memory=False, skiprows=1)

    # 1. Pre-cleaning CCS (ICD-9)
    ccs_df.columns = ccs_df.columns.str.replace("'", "", regex=False).str.strip()
    ccs_clean = ccs_df[['ICD-9-CM CODE', 'CCS CATEGORY', 'CCS CATEGORY DESCRIPTION']].dropna()
    ccs_clean.columns = ['icd_code', 'target_ccs', 'ccs_description']
    ccs_clean['clean_code'] = clean_code(ccs_clean['icd_code'])

    # 2. Pre-cleaning CCSR (ICD-10) - Default Inpatient (1:1)
    ccsr_df.columns = ccsr_df.columns.str.replace("'", "", regex=False).str.strip()
    ccsr_clean_ip = ccsr_df[['ICD-10-CM CODE', 'Default CCSR CATEGORY IP', 'Default CCSR CATEGORY DESCRIPTION IP']].dropna()
    ccsr_clean_ip.columns = ['icd_code', 'target_ccsr', 'ccsr_description']
    ccsr_clean_ip['clean_code'] = clean_code(ccsr_clean_ip['icd_code'])

    # 3. Pre-cleaning CCSR (ICD-10) - Multi-Category / All Categories (1:N)
    category_cols = [c for c in ccsr_df.columns if 'CCSR CATEGORY' in c.upper() and 'DEFAULT' not in c.upper() and 'DESCRIPTION' not in c.upper()]
    if not category_cols:
        category_cols = [c for c in ccsr_df.columns if 'CCSR CATEGORY' in c.upper() and 'DESCRIPTION' not in c.upper()]
    
    melted_ccsr = ccsr_df.melt(
        id_vars=['ICD-10-CM CODE'],
        value_vars=category_cols,
        value_name='target_ccsr'
    ).dropna()
    melted_ccsr = melted_ccsr[melted_ccsr['target_ccsr'].astype(str).str.strip() != '']
    melted_ccsr['clean_code'] = clean_code(melted_ccsr['ICD-10-CM CODE'])
    melted_ccsr['ccsr_description'] = melted_ccsr['target_ccsr']
    ccsr_clean_multi = melted_ccsr[['clean_code', 'target_ccsr', 'ccsr_description']].drop_duplicates()

    return concept, concept_ancestor, concept_relationship, has_relationships, ccsr_clean_ip, ccsr_clean_multi, ccs_clean


# =====================================================================
# STEP 2: MAPPING GENERATION FUNCTIONS
# =====================================================================

def get_icd9_ccs_mapping(ccs_clean: pd.DataFrame, force_recreate: bool = False) -> pd.DataFrame:
    """Generates or loads the ICD-9 to CCS mapping file (1:1 baseline)."""
    file_name = "mapping_icd9_to_ccs.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)
    
    print(f"Generating: {file_name}")
    mapping = ccs_clean[['icd_code', 'target_ccs', 'ccs_description']]
    mapping.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name}")
    return mapping


def get_icd10_ccsr_mapping(ccsr_clean: pd.DataFrame, force_recreate: bool = False) -> pd.DataFrame:
    """Generates or loads the ICD-10 to CCSR mapping file (1:1 baseline)."""
    file_name = "mapping_icd10_to_ccsr.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)
        
    print(f"Generating: {file_name}")
    mapping = ccsr_clean[['icd_code', 'target_ccsr', 'ccsr_description']]
    mapping.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name}")
    return mapping


def get_snomed_tree_mapping(concept: pd.DataFrame, concept_ancestor: pd.DataFrame, 
                            max_levels: int = 3, force_recreate: bool = False) -> pd.DataFrame:
    """
    Generates Parameterized SNOMED Tree Mapping (e.g. 2, 3, or 4 levels) with OHDSI Guardrails:
    - Filters source and targets strictly to SNOMED Condition domain.
    - Excludes structural patterns and root concepts.
    - Applies fallback (max_levels -> ... -> 1 -> 0/self) ensuring 100% coverage.
    - Preserves polyhierarchy (1:N) at the highest valid level.
    """
    file_name = f"mapping_snomed_tree_lvl{max_levels}_hybrid_ohdsi.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)

    print(f"Generating: {file_name} (Max Levels: {max_levels})")
    
    snomed_conditions = concept[
        (concept['vocabulary_id'] == 'SNOMED') & 
        (concept['domain_id'] == 'Condition')
    ][['concept_id', 'concept_code', 'concept_name', 'domain_id']].copy()
    
    root_codes = {'138875005', '404684003'}  # SNOMED CT Concept & Clinical Finding
    
    name = snomed_conditions['concept_name'].fillna('')
    navigational_pattern_mask = (
        name.str.endswith('finding') |
        name.str.startswith('Finding of') |
        name.str.endswith('by site') |
        name.str.endswith('by body site') |
        name.str.endswith('by mechanism') |
        name.str.endswith('of body region') |
        name.str.endswith('of anatomical site') |
        name.str.contains('of specific body structure', regex=False) |
        snomed_conditions['concept_code'].isin(root_codes)
    )

    ohdsi_whitelist_ids = {
        433736, 433595, 441408, 72404, 192671, 137977, 434621, 437312, 
        439847, 4171917, 438555, 4299449, 375258, 76784, 40483532, 
        4145627, 434157, 433778, 258449, 313878
    }

    valid_target_ids = set(
        snomed_conditions[
            (~navigational_pattern_mask) | 
            (snomed_conditions['concept_id'].isin(ohdsi_whitelist_ids))
        ]['concept_id'].unique()
    )

    allowed_hops = list(range(max_levels + 1))
    candidate_relations = concept_ancestor[
        (concept_ancestor['min_levels_of_separation'].isin(allowed_hops)) &
        (concept_ancestor['descendant_concept_id'].isin(snomed_conditions['concept_id']))
    ]
    
    tree = candidate_relations.merge(
        snomed_conditions[['concept_id', 'concept_code', 'concept_name']], 
        left_on='descendant_concept_id', right_on='concept_id'
    ).merge(
        snomed_conditions[['concept_id', 'concept_code', 'concept_name']], 
        left_on='ancestor_concept_id', right_on='concept_id', 
        suffixes=('_src', '_target')
    )

    valid_tree = tree[
        (tree['min_levels_of_separation'] == 0) |
        (tree['concept_id_target'].isin(valid_target_ids))
    ]

    max_valid_sep = valid_tree.groupby('concept_id_src')['min_levels_of_separation'].transform('max')
    tree_final = valid_tree[valid_tree['min_levels_of_separation'] == max_valid_sep]

    mapping_snomed_tree = tree_final[[
        'concept_code_src', 'concept_name_src', 'concept_code_target', 'concept_name_target'
    ]].drop_duplicates()
    
    mapping_snomed_tree.columns = [
        'source_snomed_code', 'source_snomed_name', 'parent_snomed_code', 'parent_snomed_name'
    ]
    
    mapping_snomed_tree.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name} with {len(mapping_snomed_tree):,} mappings.")
    return mapping_snomed_tree


def get_snomed_ccsr_mapping_with_fallback(concept: pd.DataFrame, concept_relationship: pd.DataFrame, 
                                          concept_ancestor: pd.DataFrame, ccsr_clean: pd.DataFrame, 
                                          is_multi: bool = False, force_recreate: bool = False) -> pd.DataFrame:
    """
    Generates cross-vocabulary SNOMED Condition to CCSR mapping with 100% Coverage Fallback:
    - Supports both Default IP (1:1 per ICD) and Multi-category (1:N per ICD).
    - Phase 1: Direct mapping via OMOP 'Maps to'.
    - Phase 2: Bounded ancestor recovery (Max hop <= 2, Closest ancestors only).
    - Phase 3: Fallback to source code for unmapped orphans (preserves 100% patient data).
    """
    mode_str = "multi" if is_multi else "default_ip"
    file_name = f"mapping_snomed_to_ccsr_{mode_str}_with_ancestors_fallback.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)

    print(f"Generating: {file_name} (Mode: {mode_str})")
    
    snomed_conditions = concept[
        (concept['vocabulary_id'] == 'SNOMED') & 
        (concept['domain_id'] == 'Condition')
    ][['concept_id', 'concept_code', 'concept_name']].copy()
    
    icd10_concepts = concept[concept['vocabulary_id'] == 'ICD10CM'][['concept_id', 'concept_code']].copy()
    icd10_concepts['clean_icd10'] = clean_code(icd10_concepts['concept_code'])
    
    maps_to = concept_relationship[concept_relationship['relationship_id'] == 'Maps to']
    
    # Phase 1: Direct mapping
    icd10_to_snomed = maps_to.merge(
        icd10_concepts, left_on='concept_id_1', right_on='concept_id'
    ).merge(
        snomed_conditions, left_on='concept_id_2', right_on='concept_id', suffixes=('_icd10', '_snomed')
    )
    
    mapping_snomed_ccsr = icd10_to_snomed.merge(
        ccsr_clean, left_on='clean_icd10', right_on='clean_code', how='inner'
    )
    mapping_snomed_ccsr = mapping_snomed_ccsr[mapping_snomed_ccsr['target_ccsr'] != 'XXX000']
    
    base_mapping = mapping_snomed_ccsr[[
        'concept_id_snomed', 'concept_code_snomed', 'concept_name', 'target_ccsr', 'ccsr_description'
    ]].drop_duplicates()

    # Phase 2: Bounded ancestor recovery (Hop <= 2)
    MAX_RECOVERY_HOPS = 2
    mapped_snomed_ids = set(base_mapping['concept_id_snomed'].unique())
    unmapped_snomed = snomed_conditions[~snomed_conditions['concept_id'].isin(mapped_snomed_ids)]
    
    unmapped_relations = concept_ancestor[
        (concept_ancestor['descendant_concept_id'].isin(unmapped_snomed['concept_id'])) &
        (concept_ancestor['min_levels_of_separation'] > 0) &
        (concept_ancestor['min_levels_of_separation'] <= MAX_RECOVERY_HOPS)
    ]
    
    candidate_ancestors = unmapped_relations.merge(
        base_mapping[['concept_id_snomed', 'target_ccsr', 'ccsr_description']],
        left_on='ancestor_concept_id',
        right_on='concept_id_snomed'
    )
    
    min_sep = candidate_ancestors.groupby('descendant_concept_id')['min_levels_of_separation'].transform('min')
    closest_ancestors = candidate_ancestors[candidate_ancestors['min_levels_of_separation'] == min_sep]
    
    recovered_full = closest_ancestors.merge(
        snomed_conditions, left_on='descendant_concept_id', right_on='concept_id'
    )
    
    recovered_clean = recovered_full[[
        'concept_code', 'concept_name', 'target_ccsr', 'ccsr_description'
    ]].drop_duplicates()
    recovered_clean.columns = ['source_snomed_code', 'source_snomed_name', 'target_ccsr_code', 'ccsr_description']

    # Phase 3: Fallback for completely unmapped orphans -> Map to self
    recovered_ids = set(closest_ancestors['descendant_concept_id'].unique())
    orphan_snomed = snomed_conditions[
        (~snomed_conditions['concept_id'].isin(mapped_snomed_ids)) &
        (~snomed_conditions['concept_id'].isin(recovered_ids))
    ].copy()

    orphan_fallback = orphan_snomed[['concept_code', 'concept_name']].copy()
    orphan_fallback['target_ccsr_code'] = orphan_fallback['concept_code']
    orphan_fallback['ccsr_description'] = orphan_fallback['concept_name']
    orphan_fallback.columns = ['source_snomed_code', 'source_snomed_name', 'target_ccsr_code', 'ccsr_description']

    # Combine all 3 phases
    base_clean = base_mapping[[
        'concept_code_snomed', 'concept_name', 'target_ccsr', 'ccsr_description'
    ]].drop_duplicates()
    base_clean.columns = ['source_snomed_code', 'source_snomed_name', 'target_ccsr_code', 'ccsr_description']
    
    final_mapping = pd.concat([base_clean, recovered_clean, orphan_fallback]).drop_duplicates()
    final_mapping.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name} with {len(final_mapping):,} mappings ({mode_str}).")
    return final_mapping


def get_snomed_ohdsi_groups_mapping(concept: pd.DataFrame, concept_ancestor: pd.DataFrame, 
                                    force_recreate: bool = False) -> pd.DataFrame:
    """Generates OHDSI Condition Concept Groups Mapping."""
    file_name = "mapping_snomed_ohdsi_concept_groups.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)

    print(f"Generating: {file_name}")

    snomed_conditions = concept[
        (concept['vocabulary_id'] == 'SNOMED') & 
        (concept['domain_id'] == 'Condition')
    ][['concept_id', 'concept_code', 'concept_name']].copy()

    cf_ids = concept[(concept['vocabulary_id'] == 'SNOMED') & (concept['concept_code'] == '404684003')]['concept_id'].tolist()
    cf_id = cf_ids[0] if cf_ids else 441840

    ohdsi_whitelist_ids = {
        433736, 433595, 441408, 72404, 192671, 137977, 434621, 437312, 
        439847, 4171917, 438555, 4299449, 375258, 76784, 40483532, 
        4145627, 434157, 433778, 258449, 313878
    }

    cf_ancestors = concept_ancestor[concept_ancestor['ancestor_concept_id'] == cf_id]
    valid_depth_ids = set(
        cf_ancestors[
            (cf_ancestors['min_levels_of_separation'] > 2) |
            (cf_ancestors['descendant_concept_id'].isin(ohdsi_whitelist_ids))
        ]['descendant_concept_id'].unique()
    )

    name = snomed_conditions['concept_name'].fillna('')
    pattern_mask = (
        name.str.endswith('finding') |
        name.str.startswith('Disorder of') |
        name.str.startswith('Finding of') |
        name.str.startswith('Disease of') |
        name.str.startswith('Injury of') |
        name.str.endswith('by site') |
        name.str.endswith('by body site') |
        name.str.endswith('by mechanism') |
        name.str.endswith('of body region') |
        name.str.endswith('of anatomical site') |
        name.str.contains('of specific body structure', regex=False)
    )

    valid_group_ids = set(
        snomed_conditions[
            (snomed_conditions['concept_id'].isin(valid_depth_ids)) &
            (~pattern_mask)
        ]['concept_id'].unique()
    )

    desc_relations = concept_ancestor[
        (concept_ancestor['ancestor_concept_id'].isin(valid_group_ids)) &
        (concept_ancestor['descendant_concept_id'].isin(snomed_conditions['concept_id']))
    ]

    mapped = desc_relations.merge(
        snomed_conditions, left_on='descendant_concept_id', right_on='concept_id'
    ).merge(
        snomed_conditions, left_on='ancestor_concept_id', right_on='concept_id', suffixes=('_src', '_target')
    )

    mapped_clean = mapped[[
        'concept_code_src', 'concept_name_src', 'concept_code_target', 'concept_name_target'
    ]].drop_duplicates()
    mapped_clean.columns = ['source_snomed_code', 'source_snomed_name', 'target_group_code', 'target_group_name']

    mapped_src_ids = set(mapped['descendant_concept_id'].unique())
    orphans = snomed_conditions[~snomed_conditions['concept_id'].isin(mapped_src_ids)].copy()
    orphan_fallback = orphans[['concept_code', 'concept_name']].copy()
    orphan_fallback['target_group_code'] = orphan_fallback['concept_code']
    orphan_fallback['target_group_name'] = orphan_fallback['concept_name']
    orphan_fallback.columns = ['source_snomed_code', 'source_snomed_name', 'target_group_code', 'target_group_name']

    final_ohdsi_mapping = pd.concat([mapped_clean, orphan_fallback]).drop_duplicates()
    final_ohdsi_mapping.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name} with {len(final_ohdsi_mapping):,} mappings.")
    return final_ohdsi_mapping


def get_snomed_to_icd10_3digit_mapping(concept: pd.DataFrame, concept_relationship: pd.DataFrame, 
                                      force_recreate: bool = False) -> pd.DataFrame:
    """Generates SNOMED to ICD-10 3-Digit Truncated Category Mapping."""
    file_name = "mapping_snomed_to_icd10_3digit.csv"
    if not force_recreate and os.path.exists(file_name):
        print(f"✓ Using existing file: {file_name}")
        return pd.read_csv(file_name, low_memory=False)

    print(f"Generating: {file_name}")

    snomed_conditions = concept[
        (concept['vocabulary_id'] == 'SNOMED') & 
        (concept['domain_id'] == 'Condition')
    ][['concept_id', 'concept_code', 'concept_name']].copy()

    icd10_concepts = concept[concept['vocabulary_id'] == 'ICD10CM'][['concept_id', 'concept_code', 'concept_name']].copy()
    maps_to = concept_relationship[concept_relationship['relationship_id'] == 'Maps to']

    icd10_to_snomed = maps_to.merge(
        icd10_concepts, left_on='concept_id_1', right_on='concept_id'
    ).merge(
        snomed_conditions, left_on='concept_id_2', right_on='concept_id', suffixes=('_icd10', '_snomed')
    )

    icd10_to_snomed['clean_icd10'] = clean_code(icd10_to_snomed['concept_code_icd10'])
    icd10_to_snomed['icd10_3digit'] = icd10_to_snomed['clean_icd10'].str[:3]

    mapped_df = icd10_to_snomed[[
        'concept_code_snomed', 'concept_name_snomed', 'icd10_3digit'
    ]].drop_duplicates()
    mapped_df.columns = ['source_snomed_code', 'source_snomed_name', 'target_icd10_3digit']

    mapped_codes = set(mapped_df['source_snomed_code'].unique())
    orphans = snomed_conditions[~snomed_conditions['concept_code'].isin(mapped_codes)].copy()
    orphan_df = orphans[['concept_code', 'concept_name']].copy()
    orphan_df['target_icd10_3digit'] = orphan_df['concept_code']
    orphan_df.columns = ['source_snomed_code', 'source_snomed_name', 'target_icd10_3digit']

    final_icd3_mapping = pd.concat([mapped_df, orphan_df]).drop_duplicates()
    final_icd3_mapping.to_csv(file_name, index=False)
    print(f"✓ Saved: {file_name} with {len(final_icd3_mapping):,} mappings.")
    return final_icd3_mapping


# =====================================================================
# STEP 3: STATISTICS GENERATION (FOR ALL AGGREGATION METHODS)
# =====================================================================

def compute_and_save_statistics(
    ccs_clean: pd.DataFrame,
    ccsr_clean_ip: pd.DataFrame,
    tree_lvl2: pd.DataFrame,
    tree_lvl3: pd.DataFrame,
    tree_lvl4: pd.DataFrame,
    snomed_ccsr_default: pd.DataFrame,
    snomed_ccsr_multi: pd.DataFrame,
    ohdsi_groups: pd.DataFrame,
    icd10_3digit: pd.DataFrame,
    output_csv: str = "nodes_compression_summary_final.csv"
):
    """Computes, displays, and exports comprehensive vocabulary compression statistics."""
    print("\n--- Step 3: Computing Summary Statistics ---")
    results = []

    def _calc_stats(source_vocab: str, method_name: str, n_before: int, n_after: int):
        reduction = n_before - n_after
        pct = (reduction / n_before * 100.0) if n_before > 0 else 0.0
        return {
            'Source Vocabulary': source_vocab,
            'Aggregation Method': method_name,
            'Nodes Before Aggregation': n_before,
            'Nodes After Aggregation': n_after,
            'Absolute Node Reduction': reduction,
            'Reduction Percentage': f"{pct:.2f}%"
        }

    # 1. Baseline: ICD-9 to CCS
    n_before = ccs_clean['clean_code'].nunique()
    n_after = ccs_clean[ccs_clean['target_ccs'] != '0']['target_ccs'].nunique()
    results.append(_calc_stats('ICD-9', 'CCS (Single-Level)', n_before, n_after))

    # 2. Baseline: ICD-10 to CCSR
    n_before = ccsr_clean_ip['clean_code'].nunique()
    n_after = ccsr_clean_ip[ccsr_clean_ip['target_ccsr'] != 'XXX000']['target_ccsr'].nunique()
    results.append(_calc_stats('ICD-10', 'CCSR (Default IP)', n_before, n_after))

    # 3. SNOMED Tree Level 2 (Guardrailed)
    n_before = tree_lvl2['source_snomed_code'].nunique()
    n_after = tree_lvl2['parent_snomed_code'].nunique()
    results.append(_calc_stats('SNOMED (Condition)', 'Hybrid Tree (Max Lvl 2)', n_before, n_after))

    # 4. SNOMED Tree Level 3 (Guardrailed)
    n_before = tree_lvl3['source_snomed_code'].nunique()
    n_after = tree_lvl3['parent_snomed_code'].nunique()
    results.append(_calc_stats('SNOMED (Condition)', 'Hybrid Tree (Max Lvl 3)', n_before, n_after))

    # 5. SNOMED Tree Level 4 (Guardrailed)
    n_before = tree_lvl4['source_snomed_code'].nunique()
    n_after = tree_lvl4['parent_snomed_code'].nunique()
    results.append(_calc_stats('SNOMED (Condition)', 'Hybrid Tree (Max Lvl 4)', n_before, n_after))

    # 6. SNOMED to CCSR (Default IP + Fallback)
    n_before = snomed_ccsr_default['source_snomed_code'].nunique()
    n_after = snomed_ccsr_default['target_ccsr_code'].nunique()
    results.append(_calc_stats('SNOMED (Condition)', 'CCSR (Default IP + Fallback)', n_before, n_after))

    # 7. SNOMED to CCSR (Multi-Category 1:N + Fallback)
    n_before = snomed_ccsr_multi['source_snomed_code'].nunique()
    n_after = snomed_ccsr_multi['target_ccsr_code'].nunique()
    results.append(_calc_stats('SNOMED (Condition)', 'CCSR (Multi-Category 1:N + Fallback)', n_before, n_after))

    # 8. SNOMED to OHDSI Concept Groups
    n_before = ohdsi_groups['source_snomed_code'].nunique()
    n_after = ohdsi_groups['target_group_code'].nunique()
    results.append(_calc_stats('SNOMED (Condition)', 'OHDSI Standard Concept Groups', n_before, n_after))

    # 9. SNOMED to ICD-10 3-Digit
    n_before = icd10_3digit['source_snomed_code'].nunique()
    n_after = icd10_3digit['target_icd10_3digit'].nunique()
    results.append(_calc_stats('SNOMED (Condition)', 'ICD-10 3-Digit Categories', n_before, n_after))

    summary_df = pd.DataFrame(results)
    summary_df.to_csv(output_csv, index=False)

    print("\n" + "=" * 96)
    print(" SUMMARY NODE COMPRESSION TABLE (ALL METHODS):")
    print("=" * 96)
    print(summary_df.to_string(index=False))
    print("=" * 96)
    print(f"✓ Summary table saved to: {output_csv}\n")


# =====================================================================
# MAIN EXECUTION CORE
# =====================================================================

def main():
    print("--- Starting Comprehensive Multi-Ontology Mapping Pipeline ---")
    input_folder = "vocabulary_all"
    force_recreate = True

    # 1. Load source tables
    concept, concept_ancestor, concept_relationship, has_rel, ccsr_clean_ip, ccsr_clean_multi, ccs_clean = load_source_files(input_folder)

    # 2. Base Mappings
    print("\n--- Generating Base Reference Mappings ---")
    get_icd9_ccs_mapping(ccs_clean, force_recreate)
    get_icd10_ccsr_mapping(ccsr_clean_ip, force_recreate)

    # 3. Multi-Level SNOMED Tree Mappings
    print("\n--- Generating Multi-Level SNOMED Tree Mappings ---")
    tree_lvl2 = get_snomed_tree_mapping(concept, concept_ancestor, max_levels=2, force_recreate=force_recreate)
    tree_lvl3 = get_snomed_tree_mapping(concept, concept_ancestor, max_levels=3, force_recreate=force_recreate)
    tree_lvl4 = get_snomed_tree_mapping(concept, concept_ancestor, max_levels=4, force_recreate=force_recreate)

    # 4. SNOMED to CCSR with 100% Fallback (Default IP vs Multi-Category)
    print("\n--- Generating SNOMED to CCSR with Fallback (Default IP vs Multi-Category) ---")
    snomed_ccsr_default = get_snomed_ccsr_mapping_with_fallback(
        concept, concept_relationship, concept_ancestor, ccsr_clean_ip, is_multi=False, force_recreate=force_recreate
    )
    snomed_ccsr_multi = get_snomed_ccsr_mapping_with_fallback(
        concept, concept_relationship, concept_ancestor, ccsr_clean_multi, is_multi=True, force_recreate=force_recreate
    )

    # 5. OHDSI Concept Groups & ICD-10 3-Digit
    print("\n--- Generating Standard Ontology Groups & ICD-10 3-Digit ---")
    ohdsi_groups = get_snomed_ohdsi_groups_mapping(concept, concept_ancestor, force_recreate)
    icd10_3digit = get_snomed_to_icd10_3digit_mapping(concept, concept_relationship, force_recreate)

    # 6. Compute & Display Comprehensive Summary Statistics
    compute_and_save_statistics(
        ccs_clean=ccs_clean,
        ccsr_clean_ip=ccsr_clean_ip,
        tree_lvl2=tree_lvl2,
        tree_lvl3=tree_lvl3,
        tree_lvl4=tree_lvl4,
        snomed_ccsr_default=snomed_ccsr_default,
        snomed_ccsr_multi=snomed_ccsr_multi,
        ohdsi_groups=ohdsi_groups,
        icd10_3digit=icd10_3digit,
        output_csv="nodes_compression_summary_final.csv"
    )

    print("✓ All mapping files and statistics created successfully.")


if __name__ == "__main__":
    main()