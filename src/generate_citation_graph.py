#!/usr/bin/env python3
"""
Script to create separate NetworkX graphs for each specified sub-area from an edge list CSV file,
and save each graph in GEXF format.

The CSV is expected to have the following columns:
    origin_doi, target_doi, origin_sub_area, target_sub_area

If either origin_sub_area or target_sub_area is empty, it is replaced with "unknown".

For each sub-area in the provided list, if either the origin_sub_area or the target_sub_area
matches the sub-area, the corresponding nodes and edge will be added to that sub-area's graph.

Graphs are saved with filenames like:
    "ai_open_citations.gexf", "se_open_citations.gexf", "arch_open_citations.gexf", etc.

Usage:
    python script_name.py input.csv
"""

import csv
import networkx as nx
import argparse

INPUT_FILE = "../data/open_citations_edge_list.csv"

# List of sub-areas for which separate graphs will be generated.
AREAS = [
    "ai",
    "arch",
    "bio",
    "chi",
    "cse",
    "data",
    "dbis",
    "ds",
    "formal",
    "graphics",
    "hardware",
    "ir",
    "net",
    "or",
    "pl",
    "robotics",
    "se",
    "security",
    "theory",
    "vision",
]

def create_subarea_graphs(file_path, areas):
    """
    Read the CSV file and create a dictionary of directed graphs for each sub-area.
    
    Each row in the CSV is processed. If the row's origin_sub_area or target_sub_area
    (after replacing empty values with "unknown") matches one of the provided sub-areas,
    the nodes and edge from that row are added to the corresponding sub-area graph.
    
    Args:
        file_path (str): Path to the CSV file.
        areas (list): List of sub-areas for which graphs will be generated.
    
    Returns:
        dict: A dictionary mapping each sub-area (str) to its corresponding NetworkX DiGraph.
    """
    # Initialize a directed graph for each sub-area.
    graphs = { area: nx.DiGraph() for area in areas }
    
    # Open and read the CSV file.
    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            origin_doi = row['origin_doi'].strip()
            target_doi = row['target_doi'].strip()
            origin_sub_area = row['origin_sub_area'].strip() if row['origin_sub_area'].strip() else "unknown"
            target_sub_area = row['target_sub_area'].strip() if row['target_sub_area'].strip() else "unknown"
            
            # Determine which sub-area graphs this row should be added to.
            qualified_areas = set()
            if origin_sub_area in areas:
                qualified_areas.add(origin_sub_area)
            if target_sub_area in areas:
                qualified_areas.add(target_sub_area)
            
            # Add the nodes and edge to each qualified sub-area graph.
            for area in qualified_areas:
                # Add origin node with its sub_area attribute if not already present.
                if origin_doi not in graphs[area]:
                    graphs[area].add_node(origin_doi, sub_area=origin_sub_area)
                # Add target node with its sub_area attribute if not already present.
                if target_doi not in graphs[area]:
                    graphs[area].add_node(target_doi, sub_area=target_sub_area)
                # Add an edge from the origin DOI to the target DOI.
                graphs[area].add_edge(origin_doi, target_doi)
                
    return graphs

def save_graphs(graphs):
    """
    Save each sub-area graph in the dictionary to a GEXF file.
    
    The output filename is generated using the pattern: "<sub_area>_open_citations.gexf".
    
    Args:
        graphs (dict): A dictionary mapping sub-area (str) to its NetworkX graph.
    """
    for area, graph in graphs.items():
        filename = f"../data/sub_areas/{area}_open_citations.gexf"
        nx.write_gexf(graph, filename)
        print(f"Graph for sub-area '{area}' saved as {filename}")

def main():
    """
    Main function to parse command-line arguments, generate the sub-area graphs,
    and save them in GEXF format.
    """
    
    
    # Create a dictionary of graphs, one per sub-area.
    subarea_graphs = create_subarea_graphs(INPUT_FILE, AREAS)
    
    # Save each graph to its corresponding GEXF file.
    save_graphs(subarea_graphs)

if __name__ == "__main__":
    main()
