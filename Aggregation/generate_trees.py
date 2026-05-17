# =====================================================================
# 🚀 START OF SCRIPT: generate_trees.py
# =====================================================================
import pandas as pd
from collections import defaultdict
import json
import re
from typing import Dict, Any, Tuple, Set, List, Optional
from graphviz import Digraph

# Global lookup dictionary and dataframes to be populated during initialization
concept_dict: Dict[int, Dict[str, str]] = {}
concept_df: pd.DataFrame = pd.DataFrame()
ancestor_df: pd.DataFrame = pd.DataFrame()


def load_vocabulary_data(base_path: str = 'vocabulary/') -> Tuple[pd.DataFrame, pd.DataFrame, Dict[int, Dict[str, str]]]:
    """
    Loads OMOP Athena vocabulary tables into memory and extracts a fast-lookup dictionary.
    
    Args:
        base_path (str): The relative or absolute path to the directory containing vocabulary CSVs.
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, Dict[int, Dict[str, str]]]: A tuple containing:
            - concept_df (DataFrame): Filtered concepts table.
            - ancestor_df (DataFrame): Hierarchical relationship ancestor table.
            - concept_dict (dict): Dictionary mapping internal OMOP concept_ids to names and codes.
    """
    print("Loading Athena tables into memory... (This might take a moment)")
    
    # Load only the required columns to optimize memory overhead
    c_df = pd.read_csv(f'{base_path}CONCEPT.csv', sep='\t', low_memory=False,
                       usecols=['concept_id', 'concept_name', 'concept_code', 'vocabulary_id'])
    a_df = pd.read_csv(f'{base_path}CONCEPT_ANCESTOR.csv', sep='\t', low_memory=False)
    
    # Create a highly optimized dictionary for fast name and code retrieval
    c_dict = c_df.set_index('concept_id')[['concept_name', 'concept_code']].to_dict('index')
    
    return c_df, a_df, c_dict


def get_omop_id(snomed_code: str) -> Optional[int]:
    """
    Retrieves the internal OMOP concept_id for a given standard SNOMED CT code.
    
    Args:
        snomed_code (str): The official clinical SNOMED identifier string.
        
    Returns:
        Optional[int]: The corresponding internal integer OMOP concept_id if found, else None.
    """
    res = concept_df[(concept_df['concept_code'] == str(snomed_code)) & (concept_df['vocabulary_id'] == 'SNOMED')]
    return int(res.iloc[0]['concept_id']) if not res.empty else None


def clean_filename(name: str) -> str:
    """
    Sanitizes raw clinical disease names by replacing illegal path characters with underscores.
    
    Args:
        name (str): The raw disease concept name text.
        
    Returns:
        str: A sanitized string safe for cross-platform file generation.
    """
    return re.sub(r'[\\/*?:"<>| ]', '_', name)


def generate_disease_tree(target_snomed_code: str, levels_below: int) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Constructs a strict hierarchical top-down data structure starting from the 
    absolute SNOMED root "Clinical finding", tracing down to the target disease, 
    and expanding exactly 'levels_below' underneath it.
    
    Features:
        - Upwards Path: Retains all structural polyhierarchical ancestral paths.
        - Downwards Path: Strictly deduplicates downstream nodes so each 
          sub-condition appears exactly ONCE.
          
    Args:
        target_snomed_code (str): The starting anchor SNOMED disease code.
        levels_below (int): Maximum depth separation allowed below the target disease.
        
    Returns:
        Tuple[Optional[Dict[str, Any]], str]: A tuple containing:
            - JSON Tree Structure (dict or None if concepts are missing).
            - Raw Disease Name (str) extracted from the database.
    """
    root_snomed: str = "404684003"  # Strict absolute root: Clinical finding
    
    root_id = get_omop_id(root_snomed)
    target_id = get_omop_id(target_snomed_code)
    
    if not root_id or not target_id:
        return None, "Unknown_Disease"
        
    disease_name: str = concept_dict[target_id]['concept_name']
    
    # Phase A: Filter the allowed universe to reduce processing complexity
    ancestors: Set[int] = set(ancestor_df[ancestor_df['descendant_concept_id'] == target_id]['ancestor_concept_id'])
    descendants: Set[int] = set(ancestor_df[(ancestor_df['ancestor_concept_id'] == target_id) & 
                                            (ancestor_df['max_levels_of_separation'] <= levels_below)]['descendant_concept_id'])
    allowed_universe: Set[int] = ancestors | descendants | {root_id, target_id}
    
    # Phase B: Extract direct parent-child relationships (separation distance = 1)
    direct_relations = ancestor_df[
        (ancestor_df['min_levels_of_separation'] == 1) & 
        (ancestor_df['ancestor_concept_id'].isin(allowed_universe)) & 
        (ancestor_df['descendant_concept_id'].isin(allowed_universe))
    ]
    
    parent_to_children: Dict[int, List[int]] = defaultdict(list)
    for _, row in direct_relations.iterrows():
        parent_to_children[int(row['ancestor_concept_id'])].append(int(row['descendant_concept_id']))
        
    # State tracker to block duplicate nodes underneath the target disease branch
    visited_descendants: Set[int] = set()

    # Phase C: Recursive nested function to map out the tree top-down
    def build_branch(current_id: int, current_level_below_target: Optional[int] = None) -> Dict[str, Any]:
        info = concept_dict.get(current_id, {"concept_name": "Unknown", "concept_code": "Unknown"})
        
        node: Dict[str, Any] = {
            "snomed_id": str(info['concept_code']),
            "name": info['concept_name'],
            "children": []
        }
        
        # Trigger downward level restrictions when hitting the target disease node
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
                
                # CRITICAL DEDUPLICATION: Skip already rendered branches if navigating below target
                if next_level_below is not None and child_id in visited_descendants:
                    continue  
                    
                child_tree = build_branch(child_id, next_level_below)
                if child_tree:
                    node["children"].append(child_tree)
                    
        return node

    return build_branch(root_id), disease_name


def save_tree_image(tree_data: Dict[str, Any], output_filename: str) -> None:
    """
    Renders the internal dictionary tree into a polished, desaturated horizontal graph.
    
    Args:
        tree_data (Dict[str, Any]): The structured JSON/dictionary map of the clinical paths.
        output_filename (str): Target base file name string for output presentation assets.
    """
    # Graph layout configuration properties
    dot = Digraph(comment='SNOMED Unified Tree', graph_attr={
        'rankdir': 'LR',          # Left-to-Right layout orientation
        'splines': 'polyline',    # Clean angular segmented connector routes
        'nodesep': '0.35',        # Vertical padding distance between sibling boxes
        'ranksep': '2.2',         # Horizontal spacing room separating generational tiers
        'concentrate': 'true',    # Merges multiple parallel lines into single cleaner paths
        'ordering': 'out'         # Sustains rigid hierarchical order to optimize link intersections
    })
    
    drawn_edges: Set[Tuple[str, str]] = set()
    
    def add_node_to_graph(node: Dict[str, Any]) -> str:
        node_id = str(node['snomed_id'])
        label = f"{node['name']}\n({node['snomed_id']})"
        
        # Professional, soft palette styling to mitigate visual overload
        dot.node(node_id, label, shape='box', style='rounded,filled', 
                 fillcolor='#fafafa', color='#888888', fontname='Helvetica', fontsize='10')
        
        for child in node.get('children', []):
            child_id = add_node_to_graph(child)
            edge_key = (node_id, child_id)
            
            if edge_key not in drawn_edges:
                # Lightweight dark grey arrow connectors
                dot.edge(node_id, child_id, color='#555555', arrowsize='0.6')
                drawn_edges.add(edge_key)
                
        return node_id

    add_node_to_graph(tree_data)
    dot.render(output_filename, format='png', cleanup=True)


# =====================================================================
# MAIN PIPELINE EXECUTION
# =====================================================================
def main() -> None:
    """
    Main orchestrator function. Executes load functions, iterates across targeted 
    disease parameters pools, and saves analytical data structures and visual images.
    """
    global concept_df, ancestor_df, concept_dict
    
    # Initialize vocabulary references from data files
    concept_df, ancestor_df, concept_dict = load_vocabulary_data(base_path='vocabulary/')

    # Define execution configurations: { "SNOMED_CODE": LEVELS_BELOW_DEPTH }
    diseases_pool: Dict[str, int] = {
        "73211009": 3,   # Diabetes mellitus
        "34000006": 3,   # Crohn's disease
        "59621000": 3,   # Essential hypertension
        "422504002": 3   # Ischemic stroke
    }

    print("\n--- Starting Unified Batch Processing ---")
    
    for snomed_code, levels_below in diseases_pool.items():
        print(f"\nProcessing SNOMED Code: {snomed_code} (Requested levels below: {levels_below})...")
        
        # 1. Map paths out inside core function
        tree, disease_raw_name = generate_disease_tree(snomed_code, levels_below=levels_below)
        
        if not tree:
            print(f"Skipping {snomed_code}: Concept not found in database.")
            continue
            
        base_file_name = clean_filename(disease_raw_name)
        json_path = f"{base_file_name}.json"
        
        # 2. Export structural backend JSON file
        print(f"-> Saving JSON hierarchy to: {json_path}")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(tree, f, ensure_ascii=False, indent=4)
            
        # 3. Export presentation vector PNG graph
        print(f"-> Generating Clean PNG Diagram: {base_file_name}.png")
        try:
            save_tree_image(tree, base_file_name)
        except Exception as e:
            print(f"Warning: Could not generate image for {disease_raw_name}. Error: {e}")

    print("\n--- Done! All unified trees and images have been generated successfully. ---")


if __name__ == "__main__":
    main()
# =====================================================================
# 🛑 END OF SCRIPT
# =====================================================================