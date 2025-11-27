#!/usr/bin/env python3
"""
Script to sample k% of lines from a file.

Usage:
    python sample_lines.py <input_file> <k> [output_file]

Arguments:
    input_file: Path to the input file
    k: Percentage of lines to sample (0-100)
    output_file: Optional output file path. If not provided, prints to stdout.

Examples:
    python sample_lines.py data.txt 10
    python sample_lines.py data.txt 25 output.txt
"""

import sys
import random
import argparse


def sample_lines(input_file, k_percent, output_file=None, seed=None):
    """
    Sample k% of lines from a file.
    
    Args:
        input_file: Path to input file
        k_percent: Percentage of lines to sample (0-100)
        output_file: Optional output file path. If None, prints to stdout.
        seed: Optional random seed for reproducibility
    """
    if seed is not None:
        random.seed(seed)
    
    # Validate percentage
    if k_percent < 0 or k_percent > 100:
        raise ValueError("k must be between 0 and 100")
    
    # Read all lines from the file
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not lines:
        print("Warning: Input file is empty.", file=sys.stderr)
        return
    
    # Calculate number of lines to sample
    total_lines = len(lines)
    num_to_sample = max(1, int(total_lines * k_percent / 100))
    
    # Sample lines randomly
    sampled_lines = random.sample(lines, num_to_sample)
    
    # Output results
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.writelines(sampled_lines)
            print(f"Sampled {num_to_sample} lines ({k_percent}%) from {total_lines} total lines.")
            print(f"Output written to '{output_file}'")
        except Exception as e:
            print(f"Error writing to output file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        # Print to stdout
        sys.stdout.writelines(sampled_lines)


def main():
    parser = argparse.ArgumentParser(
        description='Sample k% of lines from a file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('input_file', help='Path to the input file')
    parser.add_argument('k', type=float, help='Percentage of lines to sample (0-100)')
    parser.add_argument('output_file', nargs='?', default=None,
                       help='Optional output file path (default: stdout)')
    parser.add_argument('--seed', type=int, default=None,
                       help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    try:
        sample_lines(args.input_file, args.k, args.output_file, args.seed)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()





