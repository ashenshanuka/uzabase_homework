#!/bin/bash

echo "Opening a new Git Bash terminal for process_data..."
mintty -e bash -c "python src/run.py process_data -cfg config/cfg.yaml -dataset news -dirout 'ztmp/data/'; exit" &

echo "Waiting for the first terminal to close..."
wait

echo "Opening a new Git Bash terminal for process_data_all..."
mintty -e bash -c "python src/run.py process_data_all -cfg config/cfg.yaml -dataset news -dirout 'ztmp/data/'; exit" &

echo "Pipeline execution completed."
