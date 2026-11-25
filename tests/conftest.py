from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark() -> Iterator[SparkSession]:
    spark = (
        SparkSession.builder
        .master('local[1]')
        .config('spark.sql.shuffle.partitions', 1)
        .config('spark.ui.enabled', False)
        .getOrCreate()
    )

    yield spark

    spark.stop()
