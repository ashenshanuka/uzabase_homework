"""
This module provides a function to create a Spark session that uses the current Python environment.

It ensures that PySpark uses the same Python executable as the rest of the application.
"""

import logging
import os
import sys
from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession configured to use the current Python environment.

    This function sets the 'spark.pyspark.python' and 'spark.pyspark.driver.python' Spark
    configurations to the path of the active Python executable. This makes sure that PySpark
    uses the same environment as the code that calls it.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: A Spark session with Python executable configuration.
    """
    # Retrieves the path of the active Python executable.
    python_path = sys.executable
    logging.info(f"Using Python executable for Spark: {python_path}")

    # Builds a new SparkSession using the active Python environment.
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.pyspark.python", python_path)
        .config("spark.pyspark.driver.python", python_path)
        .getOrCreate()
    )

    return spark
