
from pyspark.sql.functions import to_date, to_timestamp

from src.utils import get_project_root
from src.utils.spark import get_spark_session

if __name__ == "__main__":

    spark = get_spark_session(app_name="hello_spark")

    # read dataframe from csv
    fire_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(str(get_project_root() / "data/sf_fire.csv"))
    )

    # spark dataframes are immutable: each transformation returns a new dataframe
    # rename columns (drop spaces from names)
    new_col_names = list(map(lambda x: x.replace(" ", ""), fire_df.columns))
    fire_df = fire_df.toDF(*new_col_names)

    # change date columns to have date type
    fire_df = (
        fire_df
        .withColumn("CallDate", to_date("CallDate", "MM/dd/yyyy"))
        .withColumn("WatchDate", to_date("WatchDate", "MM/dd/yyyy"))
        .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
    )

    fire_df.show()

    spark.stop()