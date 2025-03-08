"""
This module contains tests for the data processing functions in the 'src' package.
Tests check file creation, schema integrity, target word presence, and data consistency.
"""

import os
import sys
from datetime import datetime

# Adds the project root to sys.path so that the "src" package can be imported.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, LongType
from pyspark.sql import functions as F
from src.process_data import process_data
from src.process_data_all import process_data_all
from src.utils import get_spark_session


def get_output_file(output_dir: str, prefix: str) -> str:
    """
    Constructs the output Parquet file path.

    This function appends a date suffix and the .parquet extension
    to the given prefix, and places the file in the specified output directory.

    Args:
        output_dir (str): Path to the output directory.
        prefix (str): Prefix used in the file name.

    Returns:
        str: Full path to the expected output file.
    """
    current_date = datetime.now().strftime("%Y%m%d")
    return os.path.join(output_dir, f"{prefix}_{current_date}.parquet")


def get_new_spark_session(app_name: str = "TestRead") -> SparkSession:
    """
    Creates a new Spark session for testing purposes.

    The returned session uses the Python executable from the current environment.

    Args:
        app_name (str, optional): Name of the Spark application. Defaults to "TestRead".

    Returns:
        SparkSession: A configured Spark session.
    """
    return get_spark_session(app_name)


@pytest.fixture
def config() -> dict:
    """
    Provides a configuration dictionary for testing.

    Returns:
        dict: A dictionary containing paths or parameters needed by data processing functions.
    """
    return {"input_path": "dataset/test.jsonl"}


@pytest.fixture
def output_dir(tmp_path) -> str:
    """
    Creates a temporary directory for output files and returns its path.

    Args:
        tmp_path: Pytest fixture that provides a temporary directory unique to the test invocation.

    Returns:
        str: Path to the newly created output directory.
    """
    out_dir = tmp_path / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    return str(out_dir)


def test_process_data_file_exists(output_dir: str, config: dict) -> None:
    """
    Checks that process_data creates a Parquet file with the expected name.

    Args:
        output_dir (str): Path to the output directory.
        config (dict): Configuration dictionary for the data processing function.

    Raises:
        AssertionError: If the expected output file is not found.
    """
    process_data(config, "news", output_dir)
    output_file = get_output_file(output_dir, "word_count")
    assert os.path.exists(output_file), "Output Parquet file for specific words does not exist"


def test_process_data_all_file_exists(output_dir: str, config: dict) -> None:
    """
    Checks that process_data_all creates a Parquet file with the expected name.

    Args:
        output_dir (str): Path to the output directory.
        config (dict): Configuration dictionary for the data processing function.

    Raises:
        AssertionError: If the expected output file is not found.
    """
    process_data_all(config, "news", output_dir)
    output_file = get_output_file(output_dir, "word_count_all")
    assert os.path.exists(output_file), "Output Parquet file for all words does not exist"


def test_specific_schema(output_dir: str, config: dict) -> None:
    """
    Runs process_data and verifies that the output Parquet file has the expected schema.

    The test checks that:
      - The columns "word" and "count" exist.
      - The "word" column is of StringType.
      - The "count" column is either IntegerType or LongType.

    Args:
        output_dir (str): Path to the output directory.
        config (dict): Configuration dictionary for the data processing function.

    Raises:
        AssertionError: If the schema does not match expectations.
    """
    process_data(config, "news", output_dir)
    output_file = get_output_file(output_dir, "word_count")
    spark = get_new_spark_session("TestRead_Schema")
    df = spark.read.parquet(output_file)
    schema = df.schema
    field_names = schema.fieldNames()

    assert "word" in field_names, "Schema missing 'word' column"
    assert "count" in field_names, "Schema missing 'count' column"
    assert isinstance(schema["word"].dataType, StringType), "'word' column is not StringType"
    assert isinstance(schema["count"].dataType, (IntegerType, LongType)), \
        "'count' column is not numeric (expected IntegerType or LongType)"

    spark.stop()


def test_specific_target_values(output_dir: str, config: dict) -> None:
    """
    Runs process_data and checks that specific target words appear in the output.

    Args:
        output_dir (str): Path to the output directory.
        config (dict): Configuration dictionary for the data processing function.

    Raises:
        AssertionError: If any of the target words are missing from the output file.
    """
    process_data(config, "news", output_dir)
    output_file = get_output_file(output_dir, "word_count")
    spark = get_new_spark_session("TestRead_Targets")
    df = spark.read.parquet(output_file)
    words = [row["word"] for row in df.collect()]

    for target in ["president", "the", "asia"]:
        assert target in words, f"Target word {target} is missing in output"

    spark.stop()


def test_specific_and_all_consistency(output_dir: str, config: dict) -> None:
    """
    Runs both process_data and process_data_all, then compares counts for specific target words.
    The test confirms that each target word has the same count in both outputs.

    Args:
        output_dir (str): Path to the output directory.
        config (dict): Configuration dictionary for the data processing function.

    Raises:
        AssertionError: If any target word has a different count between the two outputs.
    """
    process_data(config, "news", output_dir)
    process_data_all(config, "news", output_dir)

    specific_file = get_output_file(output_dir, "word_count")
    all_file = get_output_file(output_dir, "word_count_all")

    spark = get_new_spark_session("TestRead_Consistency")
    df_specific = spark.read.parquet(specific_file)
    df_all = spark.read.parquet(all_file)

    for target in ["president", "the", "asia"]:
        specific_count_rows = df_specific.filter(F.col("word") == target).select("count").collect()
        all_count_rows = df_all.filter(F.col("word") == target).select("count").collect()

        specific_count = specific_count_rows[0]["count"] if specific_count_rows else 0
        all_count = all_count_rows[0]["count"] if all_count_rows else 0

        assert specific_count == all_count, \
            f"Count for {target} mismatch: specific({specific_count}) != all({all_count})"

    spark.stop()
