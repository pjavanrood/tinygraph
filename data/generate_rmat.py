#!/usr/bin/env python3
"""Generate R-MAT graphs for tinygraph workloads."""

import argparse
import os
import random


def generate_rmat_edge(scale, a=0.57, b=0.19, c=0.19):
    """Generate a single R-MAT edge using recursive descent."""
    u, v = 0, 0
    for _ in range(scale):
        u, v = u << 1, v << 1
        r = random.random()
        if r < a:  # Quadrant 1
            pass
        elif r < a + b:  # Quadrant 2
            v += 1
        elif r < a + b + c:  # Quadrant 3
            u += 1
        else:  # Quadrant 4
            u += 1
            v += 1
    return u, v


def parse_graph_file(input_file):
    """Parse a tinygraph workload file and extract final graph state."""
    edges = set()
    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split()
            if parts[0] == 'A' and len(parts) >= 3:
                # Add edge
                edges.add((int(parts[1]), int(parts[2])))
            elif parts[0] == 'D' and len(parts) >= 3:
                # Delete edge
                edge = (int(parts[1]), int(parts[2]))
                edges.discard(edge)
    return list(edges)


def visualize_graph(edges, title="R-MAT Graph"):
    """Visualize the graph using matplotlib and networkx."""
    try:
        import networkx as nx
        import matplotlib.pyplot as plt
    except ImportError:
        print("⚠️  Visualization requires: pip install networkx matplotlib")
        return
    
    G = nx.DiGraph()
    G.add_edges_from(edges)
    
    plt.figure(figsize=(12, 10))
    pos = nx.spring_layout(G, k=0.5, iterations=50)
    nx.draw(G, pos, node_size=300, node_color='lightblue', 
            with_labels=True, arrows=True, edge_color='gray',
            font_size=8, font_weight='bold', arrowsize=10)
    plt.title(f"{title} ({len(G.nodes())} nodes, {len(G.edges())} edges)")
    plt.tight_layout()
    plt.show()
    print(f"📊 Visualization displayed")


def generate_rmat_graph(scale=4, edge_factor=8, output_file="rmat_graph.txt", visualize=False):
    """
    Generate an R-MAT graph and write it in tinygraph format.
    
    Args:
        scale: Number of nodes = 2^scale
        edge_factor: Number of edges = nodes * edge_factor
        output_file: Output file path
        visualize: Whether to create a visualization
    """
    num_nodes = 2 ** scale
    num_edges = num_nodes * edge_factor
    
    # Generate edges and remove duplicates
    edges = set()
    while len(edges) < num_edges:
        u, v = generate_rmat_edge(scale)
        if u != v:  # Avoid self-loops
            edges.add((u, v))
    
    # Write edges in tinygraph format: "A from to"
    with open(output_file, 'w') as f:
        for u, v in sorted(edges):
            f.write(f"A {u} {v}\n")
        f.write("Q\n")
    
    print(f"Generated R-MAT graph: {num_nodes} nodes, {len(edges)} edges")
    print(f"Written to: {output_file}")
    
    if visualize:
        visualize_graph(list(edges), f"Generated R-MAT Graph")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and visualize R-MAT graphs")
    parser.add_argument("--scale", type=int, default=4, 
                        help="Scale parameter (nodes = 2^scale, default: 4)")
    parser.add_argument("--edge-factor", type=int, default=8,
                        help="Edge factor (edges = nodes * factor, default: 8)")
    parser.add_argument("--output", type=str, default="rmat_graph.txt",
                        help="Output file path (default: rmat_graph.txt)")
    parser.add_argument("--visualize", action="store_true",
                        help="Generate a PNG visualization (requires networkx, matplotlib)")
    parser.add_argument("--visualize-file", type=str,
                        help="Visualize an existing graph file (skips generation)")
    
    args = parser.parse_args()
    
    # If visualizing an existing file, just do that
    if args.visualize_file:
        print(f"Reading graph from: {args.visualize_file}")
        edges = parse_graph_file(args.visualize_file)
        print(f"Found {len(edges)} edges")
        filename = os.path.basename(args.visualize_file)
        visualize_graph(edges, filename)
    else:
        # Generate a new graph
        generate_rmat_graph(args.scale, args.edge_factor, args.output, args.visualize)

