"""Query 3 - read data from BigQuery and write to BigQuery using Spark."""
import argparse
import os
import json

from pyspark.sql.functions import desc

from bq_spark.utils.io import read_bq_table, write_bq_table
from bq_spark.utils.spark import get_spark_session


def main(args=None):
    """Main function to run the query."""
    spark = get_spark_session(
        env=args.env,
        app_name=args.app_name,
        gcs_temp_bucket=args.gcs_temp_bucket,
        bq_job_labels=args.bq_job_labels,
    )

    sales = read_bq_table(spark, f"{args.gcp_project_id}.{args.dataset_id}.store_sales")
    dt = read_bq_table(spark, f"{args.gcp_project_id}.{args.dataset_id}.date_dim")
    item = read_bq_table(spark, f"{args.gcp_project_id}.{args.dataset_id}.item")

    result = (
        sales
        .join(dt, sales.ss_sold_date_sk == dt.d_date_sk)
        .join(item, sales.ss_item_sk == item.i_item_sk)
        .filter(item.i_manufact_id == 128)
        .filter(dt.d_moy == 11)
        .groupby([dt.d_year, item.i_brand, item.i_brand_id])
        .agg({"ss_ext_sales_price": "sum"})
        .toDF("d_year", "brand", "brand_id", "sum_agg")
        .select("d_year", "brand_id", "brand", "sum_agg")
        .sort("d_year", desc("sum_agg"), "brand_id")
    )

    write_bq_table(
        df=result,
        table_ref=f"{args.gcp_project_id}.{args.dataset_id}.query3_result",
        mode=args.bq_write_mode,
        method=args.bq_write_method,
    )
    spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=str, help="Environment to run the query.", default="local")
    parser.add_argument("--gcp_project_id", type=str, help="GCP project ID.", default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument("--app_name", type=str, help="App name for Spark session.")
    parser.add_argument("--dataset_id", type=str,
                        help="BigQuery dataset containing the tpcds input tables.")
    parser.add_argument("--gcs_temp_bucket", type=str, required=False,
                        help="GCS bucket for temporary files.")
    parser.add_argument("--bq_job_labels", type=json.loads, required=False, help="BigQuery job labels.")
    parser.add_argument("--bq_write_mode", type=str, required=False,
                        help="Overwrite or append.", default="overwrite")
    parser.add_argument("--bq_write_method", type=str, required=False,
                        help="Direct or indirect.", default="direct")
    args = parser.parse_args()

    if args.bq_write_method == "indirect" and args.gcs_temp_bucket is None:
        raise ValueError("GCS temp bucket is required for indirect write method.")

    main(args)




