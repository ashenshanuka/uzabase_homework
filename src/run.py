import os
import sys

# If the script is run directly (not with the -m flag), add the project root folder to sys.path.
# This ensures that modules from the 'src' folder are importable even when running this script directly.
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
    Converts a list of command-line arguments into a dictionary. 
    Each argument pair is expected in the format: -key value. 
    For example, [-cfg, config.yaml, -dataset, my_dataset] becomes 
    {'cfg': 'config.yaml', 'dataset': 'my_dataset'}.

    Raises a ValueError if the number of arguments is not even.
    """
    if len(args) % 2 != 0:
        raise ValueError("Invalid number of arguments. Arguments must be provided pairs of '-key value'")
    return {args[i].strip('-'): args[i+1] for i in range(0, len(args), 2)}

def main() -> None:
    """
    
     Orchestrates the data processing workflow by:
      1. Configuring logging.
      2. Checking that sufficient command-line arguments are provided. - validates arguments
      3. Extracting a command (process_data or process_data_all).
      4. Parsing the remaining arguments for configuration, dataset, and output folder.
      5. Loading the specified YAML configuration.
      6. Invoking the chosen function with the parsed arguments.
      

    Example usage:
        python src/run.py process_data -cfg config.yaml -dataset my_dataset -dirout output/

    The command must be followed by these required options:
      -cfg: path to the YAML configuration file
      -dataset: name of the dataset to process
      -dirout: directory where output should be saved

    Raises:
        SystemExit: If there are not enough arguments or if the command is invalid.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    # A minimum of one command plus three required parameters (each with a corresponding value) is needed.
    # That is 1 (command) + 6 (three pairs) = 7 total arguments including the script name.
    # command, -cfg <file>, -dataset <name>, -dirout <folder>.
    if len(sys.argv) < 5:
        print("Usage: python src/run.py <command> -cfg <config_file> -dataset <dataset> -dirout <output_dir>")
        sys.exit(1)

    # The first argument is the command (either 'process_data' or 'process_data_all').
    command: str = sys.argv[1]

    # The remaining arguments are parsed into a dictionary by parse_args.
    try:
        args: Dict[str, str] = parse_args(sys.argv[2:])
    except ValueError as e:
        logging.error(e)
        sys.exit(1)

    config_file: str = args.get("cfg")
    dataset: str = args.get("dataset")
    output_dir: str = args.get("dirout")

    # Reads the YAML configuration file.
    with open(config_file, "r") as f:
        config: Dict[str, Any] = yaml.safe_load(f)

    # Chooses the appropriate function based on the command.
    if command == "process_data":
        process_data(config, dataset, output_dir)
    elif command == "process_data_all":
        process_data_all(config, dataset, output_dir)
    else:
        logging.error("Invalid command. Use 'process_data' or 'process_data_all'.")
        sys.exit(1)

if __name__ == "__main__":
    main()
