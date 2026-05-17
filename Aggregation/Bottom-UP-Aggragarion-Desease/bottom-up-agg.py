import pandas as pd
import json
import re

# =====================================================================
# STAGE 1: DATA LOADING FUNCTION
# =====================================================================
def load_athena_tables(base_path='vocabulary/'):
    """
    Loads CONCEPT and CONCEPT_ANCESTOR tables with only required columns
    to optimize memory usage.
    """
    print("Loading Athena tables from disk...")
    concept_df = pd.read_csv(f'{base_path}CONCEPT.csv', sep='\t', low_memory=False,
                             usecols=['concept_id', 'concept_name', 'concept_code', 'vocabulary_id', 'domain_id'])
    ancestor_df = pd.read_csv(f'{base_path}CONCEPT_ANCESTOR.csv', sep='\t', low_memory=False,
                              usecols=['ancestor_concept_id', 'descendant_concept_id', 'min_levels_of_separation'])
    return concept_df, ancestor_df


# =====================================================================
# STAGE 2: OHDSI FILTERING LOGIC (VALID ANCESTORS)
# =====================================================================
def filter_valid_groups(concept_df, ancestor_df):
    """
    Applies OHDSI FeatureExtraction rules to discover valid clinical 
    aggregation groups (Ancestors), including the clinical exception whitelist.
    """
    print("Step 1: Identifying valid OHDSI ancestor groups...")
    
    # OHDSI Clinical Whitelist (Concepts close to root but clinically crucial)
    explicit_allowed_ids = {
        433736, 433595, 441408, 72404, 192671, 137977, 434621, 437312, 439847, 
        4171917, 438555, 4299449, 375258, 76784, 40483532, 4145627, 434157, 
        433778, 258449, 313878
    }
    
    # Filter Clinical Finding (441840) descendants by distance rule (>2 levels) or whitelist
    clinical_finding_descendants = ancestor_df[
        (ancestor_df['ancestor_concept_id'] == 441840) & 
        ((ancestor_df['min_levels_of_separation'] > 2) | 
         (ancestor_df['descendant_concept_id'].isin(explicit_allowed_ids)))
    ]
    
    candidate_ancestor_ids = set(clinical_finding_descendants['descendant_concept_id'].unique())
    
    # Keep only Condition domains
    valid_concepts = concept_df[
        (concept_df['concept_id'].isin(candidate_ancestor_ids)) &
        (concept_df['domain_id'] == 'Condition')
    ]
    
    # Strict OHDSI text-matching rules (NOT LIKE clauses)
    name_str = valid_concepts['concept_name'].str
    filtered_ancestors = valid_concepts[
        (~name_str.endswith('finding')) &
        (~name_str.startswith('Disorder of')) &
        (~name_str.startswith('Finding of')) &
        (~name_str.startswith('Disease of')) &
        (~name_str.startswith('Injury of')) &
        (~name_str.endswith('by site')) &
        (~name_str.endswith('by body site')) &
        (~name_str.endswith('by mechanism')) &
        (~name_str.endswith('of body region')) &
        (~name_str.endswith('of anatomical site')) &
        (~name_str.contains('of specific body structure', regex=False))
    ]
    
    valid_groups = set(filtered_ancestors['concept_id'].unique())
    print(f"-> Found {len(valid_groups)} valid clinical aggregation groups.")
    return valid_groups


# =====================================================================
# STAGE 3: RELATIONSHIP MAPPING
# =====================================================================
def build_bottom_up_map(ancestor_df, valid_group_ids_set):
    """
    Extracts distinct parent-child links mapping specific granular concepts 
    to approved high-level clinical groups.
    """
    print("Step 2: Mapping granular concepts to valid ancestors...")
    bottom_up_map_df = ancestor_df[ancestor_df['ancestor_concept_id'].isin(valid_group_ids_set)]
    return bottom_up_map_df[['descendant_concept_id', 'ancestor_concept_id']].drop_duplicates()


# =====================================================================
# STAGE 4: EXPORT ARTIFACTS (JSON & CSV)
# =====================================================================
def export_to_json(bottom_up_map_df, output_filename='ohdsi_bottom_up_map.json'):
    """
    Transforms the map into a fast lookup dictionary and saves it as a JSON file.
    """
    print(f"Step 3A: Building lookup dictionary and saving to JSON: {output_filename}")
    bottom_up_lookup = bottom_up_map_df.groupby('descendant_concept_id')['ancestor_concept_id'].apply(list).to_dict()
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump({str(k): v for k, v in bottom_up_lookup.items()}, f, ensure_ascii=False, indent=4)


def export_to_readable_csv(bottom_up_map_df, concept_df, output_filename='ohdsi_bottom_up_readable_map.csv'):
    """
    Translates OHDSI internal IDs into official codes and names, 
    creating a tidy tabular CSV layout optimized for Excel.
    """
    print(f"Step 3B: Merging names and codes and saving to CSV: {output_filename}")
    names_lookup = concept_df[['concept_id', 'concept_name', 'concept_code', 'vocabulary_id']]
    
    # Merge Patient/Descendant Details
    final_table = bottom_up_map_df.merge(
        names_lookup, left_on='descendant_concept_id', right_on='concept_id', how='left'
    ).rename(columns={
        'concept_name': 'patient_disease_name',
        'concept_code': 'patient_snomed_code',
        'vocabulary_id': 'patient_vocabulary'
    }).drop(columns=['concept_id'])
    
    # Merge Ancestor/Group Details
    final_table = final_table.merge(
        names_lookup, left_on='ancestor_concept_id', right_on='concept_id', how='left'
    ).rename(columns={
        'concept_name': 'aggregated_group_name',
        'concept_code': 'aggregated_group_snomed_code',
        'vocabulary_id': 'aggregated_group_vocabulary'
    }).drop(columns=['concept_id'])
    
    # Reorder columns for optimal readability
    final_table = final_table[[
        'descendant_concept_id', 'patient_snomed_code', 'patient_disease_name', 'patient_vocabulary',
        'ancestor_concept_id', 'aggregated_group_snomed_code', 'aggregated_group_name', 'aggregated_group_vocabulary'
    ]]
    
    final_table.to_csv(output_filename, index=False, encoding='utf-8')


# =====================================================================
# MAIN PIPELINE EXECUTION
# =====================================================================
if __name__ == "__main__":
    print("--- Starting OHDSI Bottom-Up Aggregation Pipeline ---")
    
    # 1. Load Tables
    concepts, ancestors = load_athena_tables(base_path='vocabulary/')
    
    # 2. Run Filter Logic
    valid_group_ids = filter_valid_groups(concepts, ancestors)
    
    # 3. Build Raw Map
    raw_mapping_df = build_bottom_up_map(ancestors, valid_group_ids)
    
    # 4. Generate JSON Output
    export_to_json(raw_mapping_df, output_filename='ohdsi_bottom_up_map.json')
    
    # 5. Generate CSV Output
    export_to_readable_csv(raw_mapping_df, concepts, output_filename='ohdsi_bottom_up_readable_map.csv')
    
    print("\n--- Pipeline Completed Successfully! ---")
    print("Generated files:")
    print("1. 'ohdsi_bottom_up_map.json'          -> Fast dictionary for code aggregation logic")
    print("2. 'ohdsi_bottom_up_readable_map.csv' -> Human-readable reference sheet for Excel")