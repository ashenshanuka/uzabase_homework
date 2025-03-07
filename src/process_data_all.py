import logging
import os
import glob
import shutil
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from src.utils import get_spark_session

def setup_logger(log_filename: str) -> logging.Logger:
    """
    Set up a dedicated logger to output to a file in the logs directory.
    
    Args:
        log_filename (str): Name of the log file.
        
    Returns:
        logging.Logger: The configured logger.
    """
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, log_filename)
    # Use a dedicated logger name for process_data_all.
    logger = logging.getLogger("process_data_all")
    logger.setLevel(logging.INFO)
    # Remove any existing handlers for this logger.
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

# Set up logging for process_data_all.
logger = setup_logger("Data_processed_all.txt")

def process_data_all(config: Dict[str, Any], dataset: str, output_dir: str) -> None:
    """
    Process the dataset to count occurrences of all unique words in the description column.
    The description is tokenized, converted to lower-case, and punctuation is removed.
    The resulting table is saved as a single Parquet file.
    
    Args:
        config (Dict[str, Any]): Configuration loaded from YAML.
        dataset (str): Dataset identifier.
        output_dir (str): Relative output directory.
    """
    logger.info("Starting process_data_all")
    spark = get_spark_session("ProcessDataAll")
    
    input_path: str = config.get("input_path", "dataset/test.jsonl")
    logger.info(f"Reading dataset from {input_path}")
    df: DataFrame = spark.read.json(input_path)
    
    words_df: DataFrame = df.select(F.explode(F.split(F.col("description"), "\\s+")).alias("raw_word"))
    words_df = words_df.select(F.lower(F.col("raw_word")).alias("word"))
    words_df = words_df.select(F.regexp_replace(F.col("word"), r'[^\w]', '').alias("word"))
    words_df = words_df.filter(F.col("word") != "")
    
    count_df: DataFrame = words_df.groupBy("word").agg(F.count("*").alias("count"))
    count_df = count_df.coalesce(1)
    
    current_date: str = datetime.now().strftime("%Y%m%d")
    temp_output_path: str = os.path.join(output_dir, f"temp_word_count_all_{current_date}")
    final_file_path: str = os.path.join(output_dir, f"word_count_all_{current_date}.parquet")
    
    logger.info(f"Writing temporary Parquet output to {temp_output_path}")
    count_df.write.mode("overwrite").parquet(temp_output_path)
    
    part_files = glob.glob(os.path.join(temp_output_path, "part-*"))
    if not part_files:
        logger.error("No part file found in temporary output directory.")
        spark.stop()
        return
    part_file: str = part_files[0]
    logger.info(f"Found part file: {part_file}")
    
    logger.info(f"Moving file to final output: {final_file_path}")
    shutil.move(part_file, final_file_path)
    
    shutil.rmtree(temp_output_path)
    logger.info(f"Removed temporary directory: {temp_output_path}")
    
    logger.info(f"Reading back final Parquet file from {final_file_path}")
    read_back_df: DataFrame = spark.read.parquet(final_file_path)
    read_back_df.show(truncate=False)
    
    logger.info("process_data_all completed successfully")
    logging.shutdown()
    spark.stop()

if __name__ == "__main__":
    pass
