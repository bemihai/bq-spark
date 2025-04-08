"""Query 3 - read data from GCS bucket and write to GCS bucket."""
import argparse

from pyspark.sql.functions import desc

from src.utils.io import read_parquet
from src.utils.spark import get_spark_session


def main(args=None):
    """Main function to run the query."""
    spark = get_spark_session(app_name=args.app_name)

    sales = read_parquet(spark, f"{args.data_bucket}/store_sales/")
    dt = read_parquet(spark,f"{args.data_bucket}/date_dim/")
    item = read_parquet(spark,f"{args.data_bucket}/item/")

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

    result.write.mode("overwrite").parquet(f"{args.data_bucket}/query3_result")
    spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--app_name", type=str, help="App name for Spark session.")
    parser.add_argument("--data_bucket", type=str, help="GCS bucket containing the tpcds input files.")
    args = parser.parse_args()

    main(args)

