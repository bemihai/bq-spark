bq mk -d --data_location=europe-west1 tpcds_1GB
bq load --source_format PARQUET tpcds_1GB.store_sales gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/store_sales/part*.snappy.parquet
bq load --source_format PARQUET tpcds_1GB.date_dim gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/date_dim/part*.snappy.parquet
bq load --source_format PARQUET tpcds_1GB.item gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/item/part*.snappy.parquet

bq mk -d --data_location=europe-west1 tpcds_100GB
bq load --source_format PARQUET tpcds_100GB.store_sales gs://beam-tpcds/datasets/parquet/nonpartitioned/100GB/store_sales/part*.snappy.parquet
bq load --source_format PARQUET tpcds_100GB.date_dim gs://beam-tpcds/datasets/parquet/nonpartitioned/100GB/date_dim/part*.snappy.parquet
bq load --source_format PARQUET tpcds_100GB.item gs://beam-tpcds/datasets/parquet/nonpartitioned/100GB/item/part*.snappy.parquet

bq mk -d --data_location=europe-west1 tpcds_1TB
bq load --source_format PARQUET tpcds_1TB.store_sales gs://beam-tpcds/datasets/parquet/nonpartitioned/1000GB/store_sales/part*.snappy.parquet
bq load --source_format PARQUET tpcds_1TB.date_dim gs://beam-tpcds/datasets/parquet/nonpartitioned/1000GB/date_dim/part*.snappy.parquet
bq load --source_format PARQUET tpcds_1TB.item gs://beam-tpcds/datasets/parquet/nonpartitioned/1000GB/item/part*.snappy.parquet