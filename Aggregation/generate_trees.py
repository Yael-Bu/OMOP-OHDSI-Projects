import pandas as pd
from collections import defaultdict
import json
import re
from graphviz import Digraph

# =====================================================================
# 1. DATA LOADING & GLOBAL CONFIGURATION
# =====================================================================
print("Loading Athena tables into memory... (This might take a moment)")

# Load only the required columns to optimize memory usage
concept_df = pd.read_csv('vocabulary/CONCEPT.csv', sep='\t', low_memory=False,
                         usecols=['concept_id', 'concept_name', 'concept_code', 'vocabulary_id'])
ancestor_df = pd.read_csv('vocabulary/CONCEPT_ANCESTOR.csv', sep='\t', low_memory=False)

# Create a fast-lookup dictionary for concept names and codes keyed by OMOP concept_id
concept_dict = concept_df.set_index('concept_id')[['concept_name', 'concept_code']].to_dict('index')

def get_omop_id(snomed_code):
    """
    Retrieves the internal OMOP concept_id for a given official SNOMED CT code.
    """
    res = concept_df[(concept_df['concept_code'] == str(snomed_code)) & (concept_df['vocabulary_id'] == 'SNOMED')]
    return int(res.iloc[0]['concept_id']) if not res.empty else None

def clean_filename(name):
    """
    Sanitizes the disease name by replacing invalid characters to create a safe filename.
    """
    return re.sub(r'[\\/*?:"<>| ]', '_', name)


# =====================================================================
# 2. CORE TREE GENERATION LOGIC
# =====================================================================
def generate_disease_tree(target_snomed_code, levels_below):
    """
    Builds a strict hierarchical tree starting from the absolute root "Clinical finding" (404684003),
    drilling down to the target disease, and continuing exactly 'levels_below' underneath it.
    """
    root_snomed = "404684003"  # Strict absolute root: Clinical finding
    
    root_id = get_omop_id(root_snomed)
    target_id = get_omop_id(target_snomed_code)
    
    if not root_id or not target_id:
        return None, "Unknown_Disease"
        
    disease_name = concept_dict[target_id]['concept_name']
    
    # Phase A: Filter the "allowed universe" to keep the graph clean.
    # Includes all ancestors of the target disease (the path up to the root) 
    # and all descendants of the target disease within the requested depth.
    ancestors = set(ancestor_df[ancestor_df['descendant_concept_id'] == target_id]['ancestor_concept_id'])
    descendants = set(ancestor_df[(ancestor_df['ancestor_concept_id'] == target_id) & 
                                  (ancestor_df['max_levels_of_separation'] <= levels_below)]['descendant_concept_id'])
    allowed_universe = ancestors | descendants | {root_id, target_id}
    
    # Phase B: Extract direct parent-child relationships (separation = 1) within the universe
    direct_relations = ancestor_df[
        (ancestor_df['min_levels_of_separation'] == 1) & 
        (ancestor_df['ancestor_concept_id'].isin(allowed_universe)) & 
        (ancestor_df['descendant_concept_id'].isin(allowed_universe))
    ]
    
    parent_to_children = defaultdict(list)
    for _, row in direct_relations.iterrows():
        parent_to_children[int(row['ancestor_concept_id'])].append(int(row['descendant_concept_id']))
        
    # Phase C: Recursive function to construct the JSON tree top-down
    def build_branch(current_id, current_level_below_target=None):
        info = concept_dict.get(current_id, {"concept_name": "Unknown", "concept_code": "Unknown"})
        
        node = {
            "snomed_id": str(info['concept_code']),
            "name": info['concept_name'],
            "children": []
        }
        
        # If we reached the target disease, start counting levels downwards
        if current_id == target_id:
            current_level_below_target = 0
            
        # Boundary check: stop going deeper if max depth below target is reached
        go_deeper = True
        if current_level_below_target is not None and current_level_below_target >= levels_below:
            go_deeper = False
            
        if go_deeper:
            next_level_below = (current_level_below_target + 1) if current_level_below_target is not None else None
            for child_id in parent_to_children.get(current_id, []):
                child_tree = build_branch(child_id, next_level_below)
                if child_tree:
                    node["children"].append(child_tree)
                    
        return node

    return build_branch(root_id), disease_name


# =====================================================================
# 3. GRAPHVIZ VISUALIZATION GENERATOR
# =====================================================================
def save_tree_image(tree_data, output_filename):
    """
    Generates a visual diagram of the tree using Graphviz and saves it as a PNG.
    Nodes are rendered with text boxes containing the Name and SNOMED ID.
    """
    # rankdir='LR' structures the tree horizontally (Left to Right) for better readability
    dot = Digraph(comment='SNOMED Strict Tree', graph_attr={'rankdir': 'LR', 'splines': 'ortho'})
    
    # Use a mutable counter list to ensure completely unique Graphviz node IDs,
    # preventing accidental merging of nodes with shared SNOMED IDs in the visual layout.
    counter = [0]
    
    def add_node_to_graph(node):
        current_node_id = f"node_{counter[0]}"
        counter[0] += 1
        
        # Label layout: Disease Name followed by the SNOMED ID on a new line
        label = f"{node['name']}\n({node['snomed_id']})"
        dot.node(current_node_id, label, shape='box', style='rounded')
        
        for child in node.get('children', []):
            child_node_id = add_node_to_graph(child)
            dot.edge(current_node_id, child_node_id)
            
        return current_node_id

    add_node_to_graph(tree_data)
    
    # Renders the PNG image and cleans up temporary .gv source files
    dot.render(output_filename, format='png', cleanup=True)


# =====================================================================
# 4. BATCH PROCESSING EXECUTION (DYNAMIC DEPTHS)
# =====================================================================
if __name__ == "__main__":
    # Define your target diseases pool: { "SNOMED_CODE": LEVELS_BELOW }
    diseases_pool = {
        "73211009": 3,   # Diabetes mellitus (Digs 3 levels below)
        "34000006": 2,   # Crohn's disease (Digs 2 levels below)
        "59621000": 1,   # Essential hypertension (Digs 1 level below)
        "422504002": 4   # Ischemic stroke (Digs 4 levels below)
    }

    print("\n--- Starting Dynamic Batch Processing ---")
    
    for snomed_code, levels_below in diseases_pool.items():
        print(f"\nProcessing SNOMED Code: {snomed_code} (Requested levels below: {levels_below})...")
        
        # 1. Generate the tree data structure
        tree, disease_raw_name = generate_disease_tree(snomed_code, levels_below=levels_below)
        
        if not tree:
            print(f"Skipping {snomed_code}: Concept not found in database.")
            continue
            
        # 2. Sanitize the name for filenames
        base_file_name = clean_filename(disease_raw_name)
        json_path = f"{base_file_name}.json"
        
        print(f"-> Found disease name: '{disease_raw_name}'")
        print(f"-> Saving JSON hierarchy to: {json_path}")
        
        # 3. Export the tree into a structured JSON file
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(tree, f, ensure_ascii=False, indent=4)
            
        # 4. Export the tree into a visual PNG image
        print(f"-> Generating PNG Diagram: {base_file_name}.png")
        try:
            save_tree_image(tree, base_file_name)
        except Exception as e:
            print(f"Warning: Could not generate image for {disease_raw_name}. "
                  f"Ensure Graphviz system binary is installed and mapped to PATH. Error: {e}")

    print("\n--- Done! All dynamic trees and images have been generated successfully. ---")