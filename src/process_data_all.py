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


#"Data_processed_all.txt" as per requirement for logging
logger = setup_logger("Data_processed_all.txt", "process_data_all_logger")


def process_data_all(config: Dict[str, Any], dataset: str, output_dir: str) -> None:
    """
    Count occurrences of ALL unique words (including numeric values, ignoring punctuation/special chars)
    and save results as a Parquet file.
    """
    logger.info("Starting process_data_all: Counting ALL unique words")

    # Initialize Spark
    spark = get_spark_session("ProcessDataAll")
    logger.info("Spark session initialized successfully for process_data_all.")

    # Determine the path to the input data
    input_path = config.get("input_path", "dataset/test.jsonl")
    logger.info(f"(ALL) Reading data from input path: {input_path}")

    # Read data into DataFrame
    df: DataFrame = spark.read.json(input_path)
    logger.info("(ALL) Data read into Spark DataFrame successfully.")

    # Log basic info about the DataFrame
    record_count = df.count()
    logger.info(f"(ALL) DataFrame Schema: {df.schema.simpleString()}")
    logger.info(f"(ALL) Number of records in input DataFrame: {record_count}")

    # Tokenization the 'description' column into individual words (ignore punctuation/special characters)
    # cKeep numbers as words, remove extra underscores
    logger.info("(ALL) Tokenization the 'description' column into individual words and cleaning punctuation/special chars.")
    words_df = (
        df.select(F.explode(F.split(F.col("description"), "\\s+")).alias("word"))
          .select(F.lower("word").alias("word"))
          .select(F.regexp_replace("word", r"[^\w]", "").alias("word"))  # Remove punctuation/special chars
          .filter(F.col("word") != "")  # Remove empty strings
    )

    row_count_words = words_df.count()
    logger.info(f"(ALL) Number of total words (rows) after cleaning: {row_count_words}")

    # Group by each unique word and count
    logger.info("(ALL) Grouping by word and counting occurrences for all unique words.")
    count_df = (
        words_df.groupBy("word")
                .agg(F.count("*").alias("count"))
                .coalesce(1)
    )

    # Prepare file paths
    current_date = datetime.now().strftime("%Y%m%d")
    temp_output_path = os.path.join(output_dir, f"temp_word_count_all_{current_date}")
    final_file_path = os.path.join(output_dir, f"word_count_all_{current_date}.parquet")
    logger.info(f"(ALL) Temporary output path: {temp_output_path}")
    logger.info(f"(ALL) Final output path: {final_file_path}")

    # Save data as Parquet
    logger.info("(ALL) Saving word counts to temp Parquet directory.")
    count_df.write.mode("overwrite").parquet(temp_output_path)

    # Validate part files
    logger.info("(ALL) Validating part files in the temporary directory.")
    part_files = glob.glob(os.path.join(temp_output_path, "part-*"))
    if not part_files:
        logger.error("(ALL) No part-* files found in temporary directory. Process ended..")
        spark.stop()
        return
    else:
        logger.info(f"(ALL) Part files found: {part_files}")

    # Move the single output parquet to final_file_path
    logger.info("(ALL) Moving and renaming the part file to the final destination.")
    shutil.move(part_files[0], final_file_path)

    # Clean up the temporary folder
    logger.info(f"(ALL) Removing temporary directory: {temp_output_path}")
    shutil.rmtree(temp_output_path)

    logger.info(f"(ALL) Final Parquet file saved at: {final_file_path}")

    # Verification step
    logger.info("(ALL) Reading the final Parquet file for verification.")
    result_df = spark.read.parquet(final_file_path)
    final_count = result_df.count()
    logger.info(f"(ALL) Number of unique words in final result: {final_count}")
    result_df.show(truncate=False)

    logger.info("process_data_all: Counting ALL unique words completed successfully.")
    spark.stop()
    logger.info("(ALL) Spark session stopped for process_data_all.")


if __name__ == "__main__":
    pass
