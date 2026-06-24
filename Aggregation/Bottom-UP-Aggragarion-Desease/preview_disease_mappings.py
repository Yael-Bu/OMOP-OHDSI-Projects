import pandas as pd

# 1. Load the readable mapping file generated from the pipeline execution
df = pd.read_csv('ohdsi_bottom_up_readable_map.csv')

# 2. Define the list of target diseases to analyze dynamically
target_diseases = ['Diabetes', 'Crohn']

print("--- Starting Dynamic Batch Analysis ---")

for disease in target_diseases:
    print(f"\nAnalyzing mappings for target term: '{disease}'")
    
    # 3. Filter rows dynamically based on the current disease term
    filtered_mappings = df[df['aggregated_group_name'].str.contains(disease, case=False, na=False)]
    
    print(f"-> Found {len(filtered_mappings)} related relationships in the file.")
    
    if not filtered_mappings.empty:
        # 4. Extract the first row as a representative sample
        sample_row = filtered_mappings.iloc[0]
        
        print("   =========================================================")
        print(f"   🚀 Sample Bottom-Up Mapping for {disease}:")
        print("   =========================================================")
        print(f"   📍 Source Concept (Patient / Leaf Descendant):")
        print(f"      - Disease Name:  {sample_row['patient_disease_name']}")
        print(f"      - SNOMED Code:   {sample_row['patient_snomed_code']}")
        print(f"      - OMOP ID:       {sample_row['descendant_concept_id']}")
        print("\n      ⬇️   Rolls up and maps upward to...   ⬇️\n")
        print(f"   🎯 Aggregated Group (Clinical Ancestor):")
        print(f"      - Group Name:    {sample_row['aggregated_group_name']}")
        print(f"      - SNOMED Code:   {sample_row['aggregated_group_snomed_code']}")
        print(f"      - OMOP ID:       {sample_row['ancestor_concept_id']}")
        print("   =========================================================")
        
        # 5. Save the filtered results to a dedicated file for this specific disease
        output_filename = f"{disease.lower()}_mappings_preview.csv"
        filtered_mappings.to_csv(output_filename, index=False)
        print(f"   💾 Saved all '{disease}' mappings to: '{output_filename}'")
    else:
        print(f"   ⚠️ No matching concepts found for '{disease}' in the 'aggregated_group_name' column.")

print("\n--- Batch Analysis Completed ---")