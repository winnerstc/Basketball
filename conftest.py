# conftest.py
import pytest
import os
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Fixture for creating a Spark Session."""
    os.environ["HADOOP_USER_NAME"] = "testing_user"
    spark = (
        SparkSession.builder
        .appName("testing_app")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield spark
    spark.stop()