import pandas as pd
from collections import defaultdict
import json
import re
from graphviz import Digraph

# =====================================================================
# 1. DATA LOADING & GLOBAL CONFIGURATION
# =====================================================================
print("Loading Athena tables into memory... (This might take a moment)")

# Load only the required columns to optimize memory usage (Using your vocabulary/ folder path)
concept_df = pd.read_csv('vocabulary/CONCEPT.csv', sep='\t', low_memory=False,
                         usecols=['concept_id', 'concept_name', 'concept_code', 'vocabulary_id'])
ancestor_df = pd.read_csv('vocabulary/CONCEPT_ANCESTOR.csv', sep='\t', low_memory=False)

# Create a fast-lookup dictionary for concept names and codes keyed by OMOP concept_id
concept_dict = concept_df.set_index('concept_id')[['concept_name', 'concept_code']].to_dict('index')

def get_omop_id(snomed_code):
    """Retrieves the internal OMOP concept_id for a given official SNOMED CT code."""
    res = concept_df[(concept_df['concept_code'] == str(snomed_code)) & (concept_df['vocabulary_id'] == 'SNOMED')]
    return int(res.iloc[0]['concept_id']) if not res.empty else None

def clean_filename(name):
    """Sanitizes the disease name by replacing invalid characters to create a safe filename."""
    return re.sub(r'[\\/*?:"<>| ]', '_', name)


# =====================================================================
# 2. OPTIMIZED TREE GENERATION LOGIC (CLEAN DOWNWARDS PATHS)
# =====================================================================
def generate_disease_tree(target_snomed_code, levels_below):
    """
    Builds a strict hierarchical tree starting from the absolute root "Clinical finding" (404684003).
    - Upwards: Keeps all ancestral branches.
    - Downwards: Deduplicates nodes so each descendant appears exactly ONCE.
    """
    root_snomed = "404684003"  # Absolute root: Clinical finding
    
    root_id = get_omop_id(root_snomed)
    target_id = get_omop_id(target_snomed_code)
    
    if not root_id or not target_id:
        return None, "Unknown_Disease"
        
    disease_name = concept_dict[target_id]['concept_name']
    
    # Phase A: Filter the allowed universe (All ancestors + descendants within depth)
    ancestors = set(ancestor_df[ancestor_df['descendant_concept_id'] == target_id]['ancestor_concept_id'])
    descendants = set(ancestor_df[(ancestor_df['ancestor_concept_id'] == target_id) & 
                                  (ancestor_df['max_levels_of_separation'] <= levels_below)]['descendant_concept_id'])
    allowed_universe = ancestors | descendants | {root_id, target_id}
    
    # Phase B: Extract direct parent-child relationships (separation = 1)
    direct_relations = ancestor_df[
        (ancestor_df['min_levels_of_separation'] == 1) & 
        (ancestor_df['ancestor_concept_id'].isin(allowed_universe)) & 
        (ancestor_df['descendant_concept_id'].isin(allowed_universe))
    ]
    
    parent_to_children = defaultdict(list)
    for _, row in direct_relations.iterrows():
        parent_to_children[int(row['ancestor_concept_id'])].append(int(row['descendant_concept_id']))
        
    # Set to track and prevent duplicate nodes underneath the target disease
    visited_descendants = set()

    # Phase C: Recursive function to construct the JSON tree top-down
    def build_branch(current_id, current_level_below_target=None):
        info = concept_dict.get(current_id, {"concept_name": "Unknown", "concept_code": "Unknown"})
        
        node = {
            "snomed_id": str(info['concept_code']),
            "name": info['concept_name'],
            "children": []
        }
        
        # Trigger downward level counting when reaching the target disease
        if current_id == target_id:
            current_level_below_target = 0
            
        if current_level_below_target is not None:
            visited_descendants.add(current_id)
            
        go_deeper = True
        if current_level_below_target is not None and current_level_below_target >= levels_below:
            go_deeper = False
            
        if go_deeper:
            next_level_below = (current_level_below_target + 1) if current_level_below_target is not None else None
            for child_id in parent_to_children.get(current_id, []):
                
                # CRITICAL: If we are going downwards below the target, skip already visited nodes
                if next_level_below is not None and child_id in visited_descendants:
                    continue  # Prevents duplication and polyhierarchical clutter downwards
                    
                child_tree = build_branch(child_id, next_level_below)
                if child_tree:
                    node["children"].append(child_tree)
                    
        return node

    return build_branch(root_id), disease_name


# =====================================================================
# 3. OPTIMIZED GRAPHVIZ VISUALIZATION GENERATOR (MERGES ANCESTORS)
# =====================================================================
# =====================================================================
# 3. OPTIMIZED GRAPHVIZ VISUALIZATION GENERATOR (CLEAN & TIDY LAYOUT)
# =====================================================================
def save_tree_image(tree_data, output_filename):
    """
    Generates an ultra-clean, well-spaced visual diagram.
    Uses line concentration and custom spacing to prevent spiderweb layouts.
    """
    # סביבת העיצוב המתקדמת של הגרף
    dot = Digraph(comment='SNOMED Unified Tree', graph_attr={
        'rankdir': 'LR',          # זרימה משמאל לימין
        'splines': 'polyline',    # קווים ישרים ומסודרים (אפשר להחליף ל-'ortho' לזוויות 90 מעלות מדויקות)
        'nodesep': '0.35',        # המרווח האנכי בין הריבועים (מונע צפיפות)
        'ranksep': '2.2',         # המרווח האופקי בין רמה לרמה (נותן מקום לקווים להתפרס)
        'concentrate': 'true',    # מאחד קווים מקבילים לקו אחד מרכזי - קריטי לסדר בעיניים!
        'ordering': 'out'         # שומר על סדר הבנים קבוע כדי למנוע הצטלבות קווים
    })
    
    drawn_edges = set()
    
    def add_node_to_graph(node):
        node_id = str(node['snomed_id'])
        label = f"{node['name']}\n({node['snomed_id']})"
        
        # עיצוב הציורים עצמם (צבע עדין, פונט קריא ופינות מעוגלות)
        dot.node(node_id, label, shape='box', style='rounded,filled', 
                 fillcolor='#fafafa', color='#888888', fontname='Helvetica', fontsize='10')
        
        for child in node.get('children', []):
            child_id = add_node_to_graph(child)
            edge_key = (node_id, child_id)
            
            if edge_key not in drawn_edges:
                # עיצוב החצים (צבע אפור כהה וחץ קטן יותר שלא יכביד)
                dot.edge(node_id, child_id, color='#555555', arrowsize='0.6')
                drawn_edges.add(edge_key)
                
        return node_id

    add_node_to_graph(tree_data)
    dot.render(output_filename, format='png', cleanup=True)


# =====================================================================
# 4. BATCH PROCESSING EXECUTION (DYNAMIC DEPTHS)
# =====================================================================
if __name__ == "__main__":
    # Your full list of target diseases with custom depths
    diseases_pool = {
        "73211009": 3,   # Diabetes mellitus (Digs 3 levels below)
        "34000006": 3,   # Crohn's disease (Digs 3 levels below)
        "59621000": 3,   # Essential hypertension (Digs 3 levels below)
        "422504002": 3   # Ischemic stroke (Digs 3 levels below)
    }

    print("\n--- Starting Unified Batch Processing ---")
    
    for snomed_code, levels_below in diseases_pool.items():
        print(f"\nProcessing SNOMED Code: {snomed_code} (Requested levels below: {levels_below})...")
        
        # 1. Generate the optimized tree data structure
        tree, disease_raw_name = generate_disease_tree(snomed_code, levels_below=levels_below)
        
        if not tree:
            print(f"Skipping {snomed_code}: Concept not found in database.")
            continue
            
        base_file_name = clean_filename(disease_raw_name)
        json_path = f"{base_file_name}.json"
        
        # 2. Export JSON
        print(f"-> Saving JSON hierarchy to: {json_path}")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(tree, f, ensure_ascii=False, indent=4)
            
        # 3. Export Unified Clean PNG
        print(f"-> Generating Clean PNG Diagram: {base_file_name}.png")
        try:
            save_tree_image(tree, base_file_name)
        except Exception as e:
            print(f"Warning: Could not generate image for {disease_raw_name}. Error: {e}")

    print("\n--- Done! All unified trees and images have been generated successfully. ---")