#!/usr/bin/env python
import os
import sys

# If the script is run directly (instead of via -m), add the project root to sys.path.
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    __package__ = "src"

import logging
import yaml
from typing import Dict, Any, List
from src.process_data import process_data
from src.process_data_all import process_data_all

def parse_args(args: List[str]) -> Dict[str, str]:
    """
    Parse command-line arguments.
    
    Args:
        args (List[str]): List of command-line arguments.
        
    Returns:
        Dict[str, str]: Dictionary mapping argument names to their values.
    """
    if len(args) % 2 != 0:
        raise ValueError("Invalid number of arguments. Arguments must be provided in pairs.")
    return {args[i].strip('-'): args[i+1] for i in range(0, len(args), 2)}

def main() -> None:
    """
    Main function to parse arguments, load configuration, and invoke the appropriate data processing function.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if len(sys.argv) < 5:
        print("Usage: python src/run.py <command> -cfg <config_file> -dataset <dataset> -dirout <output_dir>")
        sys.exit(1)
    
    command: str = sys.argv[1]
    try:
        args: Dict[str, str] = parse_args(sys.argv[2:])
    except ValueError as e:
        logging.error(e)
        sys.exit(1)
    
    config_file: str = args.get("cfg")
    dataset: str = args.get("dataset")
    output_dir: str = args.get("dirout")
    
    with open(config_file, "r") as f:
        config: Dict[str, Any] = yaml.safe_load(f)
    
    if command == "process_data":
        process_data(config, dataset, output_dir)
    elif command == "process_data_all":
        process_data_all(config, dataset, output_dir)
    else:
        logging.error("Invalid command. Use 'process_data' or 'process_data_all'.")
        sys.exit(1)

if __name__ == "__main__":
    main()
