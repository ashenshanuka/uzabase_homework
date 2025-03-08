import os
import glob
import shutil
import logging
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import functions as F, DataFrame
from src.utils import get_spark_session


def setup_logger(log_filename: str, logger_name: str) -> logging.Logger:
    """
    Setup a logger with specified log file name and logger name.
    """
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, log_filename)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

    return logger


#"Data_processed.txt" as per requirement for logging
logger = setup_logger("Data_processed.txt", "process_data_logger")


def process_data(config: Dict[str, Any], dataset: str, output_dir: str) -> None:
    """
    Count occurrences of given specified words from the dataset and save results as a Parquet file.
    """
    logger.info("Starting process_data: Counting specified target words")
    
    # Initialize Spark
    spark = get_spark_session("ProcessData")
    logger.info("Spark session initialized successfully for process_data.")

    # Determine the path to the input data
    input_path = config.get("input_path", "dataset/test.jsonl")
    logger.info(f"Reading data from input path: {input_path}")

    # Read data into DataFrame
    df: DataFrame = spark.read.json(input_path)
    logger.info("Data read into Spark DataFrame successfully for process_data.")

    # Log basic info about the DataFrame
    record_count = df.count()
    logger.info(f"DataFrame Schema: {df.schema.simpleString()}")
    logger.info(f"Number of records in input DataFrame: {record_count}")

    # Define target words
    target_words = ["president", "the", "asia"]
    logger.info(f"Target words: {target_words}")

    # Tokenization column into words and filter
    logger.info("Tokenization the 'description' column into individual words and filtering on target words.")
    words_df = (
        df.select(F.explode(F.split(F.col("description"), "\\s+")).alias("word"))
          .select(F.lower("word").alias("word"))
          .select(F.regexp_replace("word", r"[^\w]", "").alias("word"))  # remove special chars
          .filter(F.col("word").isin(target_words))
    )

    filtered_count = words_df.count()
    logger.info(f"Number of rows after applying target word filter: {filtered_count}")

    # Group by word, Count word occurrences.
    logger.info("Grouping by targeted word and counting occurrences")
    count_df = (
        words_df.groupBy("word")
                .agg(F.count("*").alias("count"))
                .coalesce(1)
    )

    # Prepare file paths
    current_date = datetime.now().strftime("%Y%m%d")
    temp_output_path = os.path.join(output_dir, f"temp_word_count_{current_date}")
    final_file_path = os.path.join(output_dir, f"word_count_{current_date}.parquet")
    logger.info(f"Temporary output path: {temp_output_path}")
    logger.info(f"Final output path: {final_file_path}")

    # Save data as Parquet
    logger.info("Saving word counts to temp Parquet directory.")
    count_df.write.mode("overwrite").parquet(temp_output_path)

    # Validate part files
    logger.info("Looking for part-* files in the temporary directory.")
    part_files = glob.glob(os.path.join(temp_output_path, "part-*"))
    if not part_files:
        logger.error("No part-* files found in temporary directory. Process ended.")
        spark.stop()
        return
    else:
        logger.info(f"Part files found: {part_files}")

    # Move the single output parquet to final_file_path
    logger.info("Moving and renaming the part file to final destination.")
    shutil.move(part_files[0], final_file_path)

    # Clean up temp folder
    logger.info(f"Removing temporary directory: {temp_output_path}")
    shutil.rmtree(temp_output_path)

    logger.info(f"Final Parquet file saved at: {final_file_path}")

    # Verification step
    logger.info("Reading the final Parquet file for verification.")
    result_df = spark.read.parquet(final_file_path)
    final_count = result_df.count()
    logger.info(f"Number of distinct words in final result: {final_count}")
    result_df.show(truncate=False)

    logger.info("process_data: Counting target words completed successfully.")
    spark.stop()
    logger.info("Spark session stopped for process_data.")


if __name__ == "__main__":
    pass
