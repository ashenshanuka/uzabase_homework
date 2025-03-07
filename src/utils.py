import logging
import os
from pyspark.sql import SparkSession
from typing import Any

def get_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession with the given application name.
    This session is configured so that both the driver and worker processes use
    the specified Python executable from the virtual environment.
    
    Args:
        app_name (str): The name of the Spark application.
    
    Returns:
        SparkSession: An active SparkSession.
    """
    python_path: str = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "venv", "Scripts", "python.exe"))
    logging.info(f"Initializing Spark session with app name: {app_name}")
    spark: SparkSession = SparkSession.builder.appName(app_name) \
        .config("spark.pyspark.python", python_path) \
        .config("spark.pyspark.driver.python", python_path) \
        .getOrCreate()
    return spark
