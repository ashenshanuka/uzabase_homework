import os
import sys
from datetime import datetime

# Ensure the project root is in sys.path so that "src" can be imported.
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
    Returns the expected output file path given the output directory and a prefix.
    """
    current_date = datetime.now().strftime("%Y%m%d")
    return os.path.join(output_dir, f"{prefix}_{current_date}.parquet")

def get_new_spark_session(app_name: str = "TestRead") -> SparkSession:
    """
    Creates a new Spark session for reading output.
    """
    return get_spark_session(app_name)

@pytest.fixture
def config() -> dict:
    """
    Fixture returning a configuration dictionary.
    """
    return {"input_path": "dataset/test.jsonl"}

@pytest.fixture
def output_dir(tmp_path) -> str:
    """
    Fixture creating a temporary output directory and returning its path as a string.
    """
    out_dir = tmp_path / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    return str(out_dir)

def test_process_data_file_exists(output_dir: str, config: dict) -> None:
    """
    Test that process_data produces a file with the expected name.
    """
    process_data(config, "news", output_dir)
    output_file = get_output_file(output_dir, "word_count")
    assert os.path.exists(output_file), "Output Parquet file for specific words does not exist"

def test_process_data_all_file_exists(output_dir: str, config: dict) -> None:
    """
    Test that process_data_all produces a file with the expected name.
    """
    process_data_all(config, "news", output_dir)
    output_file = get_output_file(output_dir, "word_count_all")
    assert os.path.exists(output_file), "Output Parquet file for all words does not exist"

def test_specific_schema(output_dir: str, config: dict) -> None:
    """
    Run process_data and verify that the output Parquet file has the expected schema.
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
    # Accept either IntegerType or LongType for count.
    assert isinstance(schema["count"].dataType, (IntegerType, LongType)), "'count' column is not numeric (expected IntegerType or LongType)"
    spark.stop()

def test_specific_target_values(output_dir: str, config: dict) -> None:
    """
    Run process_data and check that the target words appear in the output.
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
    Run both process_data and process_data_all, then verify that for each target word
    the count is the same between the two outputs.
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
        assert specific_count == all_count, f"Count for {target} mismatch: specific({specific_count}) != all({all_count})"
    spark.stop()

