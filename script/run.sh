# first command for specific words counting
echo "Counting occurrences of the given word in the description column..."
python src/run.py process_data -cfg config/cfg.yaml -dataset news -dirout 'ztmp/data/'


sleep 2

echo "Word occurrence counting completed. Proceeding to count unique word occurrences..."

# second command for all unique word counting
echo "Counting occurrences of all unique words in the description column..."
python src/run.py process_data_all -cfg config/cfg.yaml -dataset news -dirout 'ztmp/data/'


echo "Pipeline execution completed."


echo "Press any key to close the terminal..."
read -n 1 -s
