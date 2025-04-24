"""Spark config utils."""
import os
import configparser

from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession

from .io import get_project_root

load_dotenv()
GCP_SERVICE_ACCOUNT_KEY = os.getenv("GCP_SERVICE_ACCOUNT_KEY")


def get_spark_app_config(env: str) -> SparkConf:
    """Returns the SparkConf object for the current environment."""
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    if env == "local":
        config.read(get_project_root() / "spark.conf")
        for k, v in config.items(env):
            spark_conf.set(k, v)

    return spark_conf


def get_spark_session(
        env: str | None = None,
        app_name: str | None = None,
        gcs_temp_bucket: str | None = None,
        bq_job_labels: dict | None = None,
) -> SparkSession:
    """
    Returns the Spark Session object for the current environment.

    Args:
        env: The environment to use.
        app_name: The name of the Spark application.
        gcs_temp_bucket: The GCS bucket to use for temporary files.
        bq_job_labels: Labels to apply to BigQuery jobs (load or query).
    """
    builder = SparkSession.builder if app_name is None else SparkSession.builder.appName(app_name)
    conf = get_spark_app_config(env)
    spark = builder.config(conf=conf).getOrCreate()

    if gcs_temp_bucket:
        spark.conf.set("temporaryGcsBucket", gcs_temp_bucket)

    for k, v in (bq_job_labels or {}).items():
        spark.conf.set(f"bigQueryJobLabel.{k}", v)

    # setup GCS credentials for Makefile-bigquery connector and hadoop-gcs connector
    # this is not needed when running on Dataproc
    if env == "local":
        spark.conf.set("credentialsFile", str(get_project_root() / GCP_SERVICE_ACCOUNT_KEY))
        spark._jsc.hadoopConfiguration().set(
            "google.cloud.auth.service.account.json.keyfile",
            str(get_project_root() / GCP_SERVICE_ACCOUNT_KEY)
        )

    return spark