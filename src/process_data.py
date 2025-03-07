import sys
import os
import logging
import glob
import shutil
import argparse
import yaml
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.utils import get_spark_session

# Dynamically set the Spark worker/driver Python to use the same interpreter
# that's running this script (works in local & container).
venv_python = sys.executable
os.environ["PYSPARK_PYTHON"] = venv_python
os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python


def setup_logger(log_filename: str) -> logging.Logger:
    """
    Sets up a logger that writes to a specified log file in the ../logs directory.
    """
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, log_filename)
    logger = logging.getLogger("process_data")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(handler)
    return logger


logger = setup_logger("Data_processed.txt")


def process_data(config: Dict[str, Any], dataset: str, output_dir: str) -> None:
    """
    Reads a JSONL input, cleans and tokenizes the 'description' field, then counts
    the occurrences of specific target words, storing the result in a single Parquet file.
    """
    logger.info("Starting process_data")
    spark = get_spark_session("ProcessData")
    spark.sparkContext.setLogLevel("ERROR")
    
    input_path = config.get("input_path", "dataset/test.jsonl")
    logger.info(f"Reading dataset from {input_path}")
    df: DataFrame = spark.read.json(input_path)
    
    # Clean 'description': lowercase, remove punctuation, split into words
    cleaned_df = df.select(F.regexp_replace(F.lower(F.col("description")), r'[^\w\s]', '').alias("cleaned"))
    words_df = cleaned_df.select(F.explode(F.split(F.col("cleaned"), "\\s+")).alias("word")).filter(F.col("word") != "")
    
    # Define target words and count them
    target_words = ["president", "the", "asia"]
    grouped_df = words_df.groupBy("word").agg(F.count("*").alias("count"))
    
    # Filter out the row counts for just the target words
    target_counts_df = grouped_df.filter(F.col("word").isin(target_words)) \
                                 .select("word", F.col("count").alias("target_count"))
    
    # Create a DataFrame of target words with count=0, then left-join to fill missing counts
    schema = StructType([
        StructField("word", StringType(), False),
        StructField("count", IntegerType(), True)
    ])
    default_df = spark.createDataFrame([(w, 0) for w in target_words], schema=schema) \
                      .withColumnRenamed("count", "default_count")
    
    final_df: DataFrame = default_df.join(target_counts_df, on="word", how="left") \
        .select("word", F.coalesce(F.col("target_count"), F.col("default_count")).alias("count")) \
        .coalesce(1)
    
    current_date = datetime.now().strftime("%Y%m%d")
    temp_output = os.path.join(output_dir, f"temp_word_count_{current_date}")
    final_file = os.path.join(output_dir, f"word_count_{current_date}.parquet")
    
    logger.info(f"Writing temporary Parquet output to {temp_output}")
    final_df.write.mode("overwrite").parquet(temp_output)
    
    # Move the single part file to the final Parquet file name
    part_files = glob.glob(os.path.join(temp_output, "part-*"))
    if not part_files:
        logger.error("No part file found in temporary output directory.")
        spark.stop()
        return
    
    logger.info(f"Found part file: {part_files[0]}")
    logger.info(f"Moving file to final output: {final_file}")
    shutil.move(part_files[0], final_file)
    shutil.rmtree(temp_output)
    logger.info(f"Removed temporary directory: {temp_output}")
    
    # (Optional) Read back the final file to confirm
    logger.info(f"Reading back final Parquet file from {final_file}")
    spark.read.parquet(final_file).show(truncate=False)
    logger.info("process_data completed successfully")
    
    # Flush logs and stop Spark
    for h in logger.handlers:
        h.flush()
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process dataset to count target words and output as Parquet.")
    parser.add_argument("-cfg", "--config", required=True, help="Path to configuration YAML file.")
    parser.add_argument("-dataset", required=True, help="Dataset identifier (e.g., 'news').")
    parser.add_argument("-dirout", required=True, help="Output directory for Parquet file.")
    args = parser.parse_args()
    
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)
    
    process_data(config, args.dataset, args.dirout)
