# Spark vs. BigQuery 

The scope of this project is to compare Spark and BigQuery from a performance and cost perspective.
We are going to run the same queries using both technologies and compare the results. 

There are three different scenarios we are going to test:
1. Run queries as Spark jobs on Dataproc and read/write data from/to GCS (as parquet files).
2. Run queries as Spark jobs on Dataproc and read/write data from/to BigQuery.
3. Run queries as BigQuery jobs and read/write data from/to BigQuery.

## Data

The data we are going to use is from the [TPC-DS](https://beam.apache.org/documentation/sdks/java/testing/tpcds/) 
benchmark, and we consider three different size datasets: 10 GB, 100 GB and 1 TB.  
The data is publicly available on GCS in the following bucket `gs://beam-tpcds/datasets/parquet`.

The query we use is Query #3 from TPC-DS:
```sql
SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand, SUM(ss_ext_sales_price) sum_agg
FROM  date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    AND store_sales.ss_item_sk = item.i_item_sk
    AND item.i_manufact_id = 128
    AND dt.d_moy=11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg desc, brand_id
```

The comparison is done on execution time, cost, and developer experience.

## Prerequisites

- Set up a GCP project and enable all required APIs: BigQuery, Dataproc, Cloud Storage, Billing, etc.
  create a group and add the service account to the group.
- Create a service account with read/write permissions to GCS, BigQuery, Dataproc. If working in a test 
  environment, you can simply use the `Owner` role for the service account. We are going to need a 
  json key for the service account to authenticate to GCP from the local dev environment.
- Install the [gcloud](https://cloud.google.com/sdk/docs/install) SDK and make sure you can run Spark locally (
  install Java, download Spark binaries, set paths, etc.).
- Install the [uv](https://github.com/astral-sh/uv) package/project manager.

## Setting up the local environment

### Create a virtual environment

Create a virtual environment using `uv` and install all the required packages:
```bash
uv venv .venv --python 3.12
uv sync
```

### Set up the environment variables

Add the following environment variables to a `.env` file so that they are loaded automatically:
```bash
ENV=local
GCP_PROJECT_ID=<gcp-project-id>
GCP_REGION=<data-region>
GCP_SERVICE_ACCOUNT_KEY=<relative/path/to/service/account/key.json>
```

### Copy data to GCS and BigQuery

- Set up the Application Default Credentials (ADC) in your local shell:
```bash
gcloud auth application-default login
````

- Create a bucket in your project and copy the data from the public GCS bucket to your own bucket:
```bash
BUCKET_NAME=<your-bucket-name>
gsutil -m cp -r gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB gs://$BUCKET_NAME/tpcds/1GB
```

- Load the data from the public GCS bucket to BigQuery:
```bash
DATASET_ID=<your-dataset-id>
bq mk -d --data_location=$GCP_REGION $DATASET_ID
bq load --source_format PARQUET $DATASET_ID.store_sales "gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/store_sales/part*.snappy.parquet"
bq load --source_format PARQUET $DATASET_ID.date_dim "gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/date_dim/part*.snappy.parquet""
bq load --source_format PARQUET $DATASET_ID.item "gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/item/part*.snappy.parquet"
```

The smallest dataset is enough to test the code locally, but you can copy the other datasets as well to have 
them ready for running the queries in GCP.

- Install Spark connectors for BigQuery and GCS (they are only required for local development):
  - We make the Spark BigQuery connector available as a package in `spark.conf`:
    ```yaml
    spark.jars.packages = com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.1
    ```
    At runtime, the connector will be downloaded from Maven and added to the classpath. We provide 
    the service account key in the Spark configuration as well:
    ```python
    GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    spark.conf.set("credentialsFile", str(get_project_root() / GCP_SERVICE_ACCOUNT)) 
    ```
  - The Hadoop connector for GCS is also included in `spark.conf`:
    ```yaml
    spark.jars = https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
    spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
    spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS
    spark.hadoop.google.cloud.auth.service.account.enable=true
    ```
    The connector is downloaded from GCS and added to the classpath. To make it work, we have to do two things:
    1. Provide the service account key in the Spark configuration:
    ```python
    GCP_SERVICE_ACCOUNT = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile", 
        str(get_project_root() / GCP_SERVICE_ACCOUNT)
    )
    ```
    2. To ensure that pyspark's dependencies are consistent with the Hadoop connector, we need to replace
      the `guava` library from pyspark jars (`.venv/lib/python3.12/site-packages/pyspark/jars`) 
      with the one used by the connector (version `30.1-jre`, download it from 
      [here](https://repo1.maven.org/maven2/com/google/guava/guava/30.1-jre/guava-30.1-jre.jar)).

       
## Run the queries locally

You can test the queries locally before submitting them to Dataproc as Spark jobs or to BigQuery.  

### Case 1: local Spark + GCS
To run the query locally using Spark and read/write data from/to GCS, you can use the following command:
```bash
APP_NAME=<spark-app-name>
python ./src/spark_gcs.py --app_name $APP_NAME --data_bucket gs://$BUCKET_NAME/$DATSASET_ID 
```
The resulting parquet file is saved at `gs://$BUCKET_NAME/$DATASET_ID/query3_result/`.

We can read/write the parquet files from the local file system instead of GCS just by providing 
the local folder as the `data_bucket` argument. 

### Case 2: local Spark + BigQuery

To run the query locally using Spark and read/write data from/to BigQuery, you can use the following command:
```bash
APP_NAME=<spark-app-name>
python ./src/spark_bq.py --app_name $APP_NAME --dataset_id $DATASET_ID 
  --bq_write_method indirect  --gcs_temp_bucket $BUCKET_NAME 
```
Indirect write mode uses a temporary bucket to dump the data before loading it to BigQuery. 
We can also use the `direct` write mode, which uses the BigQuery Storage API to write the data
directly to BigQuery. In this case, we don't need to provide the `gcs_temp_bucket` argument, but
this method adds additional costs to the query.
```bash
APP_NAME=<spark-app-name>
python ./src/spark_bq.py --app_name $APP_NAME --dataset_id $DATASET_ID 
  --bq_write_method direct 
```

We can also run this script entirely locally by setting up first the [BigQuery emulator](https://github.com/goccy/bigquery-emulator).  

### Case 3: BigQuery 

To run the query using BigQuery (cloud or emulator), you can use the following command:
```bash
python ./src/bq_native.py --dataset_id $DATASET_ID  
```

## Run the queries in GCP

Pre-requisites:
- Cloud Dataproc API enabled
- Subnet has Private Google Access enabled: configuration https://cloud.google.com/vpc/docs/configure-private-google-access#config-pga

### Case 1: Spark on Dataproc + GCS

### Case 2: Spark on Dataproc + BigQuery

### Case 3: BigQuery


## References:
- [BigQuery vs Spark Comparison](https://medium.com/qodea/bigquery-spark-or-dataflow-a-story-of-speed-and-other-comparisons-fb1b8fea3619)
- [PySpark on Dataproc Serverless](https://medium.com/qodea/running-pyspark-jobs-on-google-cloud-using-serverless-dataproc-f16cef5ec6b9)
- [Hadoop GCS Connector in PySpark](https://kontext.tech/article/689/pyspark-read-file-in-google-cloud-storage)
- [Spark BigQuery Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)
- [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage)