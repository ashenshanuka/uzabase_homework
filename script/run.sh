#!/bin/bash

# Run the first command
echo "Counting occurrences of the given word in the description column..."
python src/run.py process_data -cfg config/cfg.yaml -dataset news -dirout 'ztmp/data/'

# Optional brief pause (remove or adjust as needed)
sleep 2

echo "Word occurrence counting completed. Proceeding to count unique word occurrences..."

# Run the second command automatically
echo "Counting occurrences of all unique words in the description column..."
python src/run.py process_data_all -cfg config/cfg.yaml -dataset news -dirout 'ztmp/data/'

# Final message
echo "Pipeline execution completed."

# Pause before closing
echo "Press any key to close the terminal..."
read -n 1 -s
