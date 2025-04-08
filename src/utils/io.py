"""Utilities for reading and writing data."""""
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame


def get_project_root() -> Path:
    """Returns the root directory of the project."""
    return Path(os.path.dirname(__file__)).parent.parent


def read_parquet(spark: SparkSession, path: Path | str, header: bool = True) -> DataFrame:
    """Reads a parquet file and returns a dataframe."""
    df = spark.read.option("header", header).parquet(str(path))
    return df


def read_bq_table(spark: SparkSession, table_ref: str):
    """Reads a BigQuery table and returns a dataframe."""
    df = spark.read.format("bigquery").load(table_ref)
    return df


def write_bq_table(df: DataFrame, table_ref: str, mode: str = "overwrite", method: str = "direct"):
    """Writes a dataframe to a BigQuery table."""
    assert mode in ["overwrite", "append"], "mode must be either 'overwrite' or 'append'"
    assert method in ["direct", "indirect"], "method must be either 'direct' or 'indirect'"
    (df
     .write
     .format("bigquery")
     .mode(mode)
     .option("writeMethod", method)
     .save(table_ref)
     )

