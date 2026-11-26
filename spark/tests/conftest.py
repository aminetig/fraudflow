import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Une session Spark locale, partagée par tous les tests : pas besoin de Docker."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("fraudflow-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
