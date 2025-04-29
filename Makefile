#!make
include .env

APP_NAME ?= $$(cat pyproject.toml| grep name | cut -d" " -f3 | sed  's/"//g')
VERSION ?= 0.1.0
DATASET_ID ?= tpcds_1gb
DATASET_PATH ?= tpcds_1gb

# default shell
SHELL := /bin/bash

# default goal
.DEFAULT_GOAL := help

.PHONY: help gcp-setup gcp-clean build run-gcs run-bq run-bq-native run-bq-ind

help:
	@echo "Available commands: TODO"

gcp-setup:
	@echo "Setup GCP resources and data"
	@gcloud auth application-default login
	@gcloud storage buckets create gs://${BUCKET_NAME} --location=${GCP_REGION}
	@gsutil -m cp -r gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB gs://${BUCKET_NAME}/tpcds/1GB
	@bq mk -d --data_location=${GCP_REGION} ${DATASET_ID}
	@bq load --source_format PARQUET ${DATASET_ID}.store_sales gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/store_sales/part*.snappy.parquet
	@bq load --source_format PARQUET ${DATASET_ID}.date_dim gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/date_dim/part*.snappy.parquet
	@bq load --source_format PARQUET ${DATASET_ID}.item gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/item/part*.snappy.parquet

build:
	@echo "Package code and dependencies"
	@rm -Rf ./dist & rm -Rf ./build_dir & rm -f requirements.txt
	@mkdir ./build_dir
	@uv sync --no-dev
	@uv export -q --format requirements-txt --no-dev --no-hashes --no-header --output-file requirements.txt
	@uv build
	@uv pip install -r requirements.txt --target ./build_dir
	@unzip -u ./dist/*-py3-none-any.whl -d ./build_dir
	@cd ./build_dir && zip -r ../dist/build_dir.zip .
	@mv ./dist/build_dir.zip ./dist/${APP_NAME}-${VERSION}.zip
	@rm -Rf ./build_dir & rm -f requirements.txt
	@gsutil cp -r ./dist/${APP_NAME}-${VERSION}.zip gs://${BUCKET_NAME}/code/
	@gsutil cp ./src/bq_spark/*.py gs://${BUCKET_NAME}/code/
	@rm -Rf ./dist

run-bq:
	@gcloud dataproc batches submit --project ${GCP_PROJECT_ID} --region ${GCP_REGION} pyspark gs://${BUCKET_NAME}/code/spark_bq.py \
	--py-files gs://${BUCKET_NAME}/code/${APP_NAME}-${VERSION}.zip --version 2.2 \
	--properties spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.app.name=${APP_NAME} \
	--labels usecase=p2804_q3_spark_bq_${DATASET_ID} --async \
	--network ${GCP_NETWORK} --service-account=${GCP_SERVICE_ACCOUNT} -- \
	--env cloud --gcp_project_id ${GCP_PROJECT_ID} --app_name ${APP_NAME} --dataset_id ${DATASET_ID} --bq_write_method direct

run-bq-ind:
	@gcloud dataproc batches submit --project ${GCP_PROJECT_ID} --region ${GCP_REGION} pyspark gs://${BUCKET_NAME}/code/spark_bq.py \
	--py-files gs://${BUCKET_NAME}/code/${APP_NAME}-${VERSION}.zip --version 2.2 \
	--properties spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.app.name=${APP_NAME} \
	--labels usecase=p2804_q3_spark_bq_gcs_${DATASET_ID} --async \
	--network ${GCP_NETWORK} --service-account=${GCP_SERVICE_ACCOUNT} -- \
	--env cloud --gcp_project_id ${GCP_PROJECT_ID} --app_name ${APP_NAME} --dataset_id ${DATASET_ID} --bq_write_method indirect --gcs_temp_bucket ${BUCKET_NAME}

run-gcs:
	@gcloud dataproc batches submit --project ${GCP_PROJECT_ID} --region ${GCP_REGION} pyspark gs://${BUCKET_NAME}/code/spark_gcs.py \
	--py-files gs://${BUCKET_NAME}/code/${APP_NAME}-${VERSION}.zip --version 2.2 \
	--properties spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.app.name=${APP_NAME} \
	--labels usecase=p2804_q3_spark_gcs_${DATASET_PATH} --async \
	--network ${GCP_NETWORK} --service-account=${GCP_SERVICE_ACCOUNT} -- \
	--env cloud --app_name ${APP_NAME} --data_bucket gs://${BUCKET_NAME}/${DATASET_PATH}

run-bq-native:
	@gcloud dataproc batches submit --project ${GCP_PROJECT_ID} --region ${GCP_REGION} pyspark gs://${BUCKET_NAME}/code/bq_native.py \
	--py-files gs://${BUCKET_NAME}/code/${APP_NAME}-${VERSION}.zip --version 2.2 \
	--properties spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.app.name=${APP_NAME} \
	--labels usecase=p2804_q3_spark_bq_native_${DATASET_ID} --async \
	--network ${GCP_NETWORK} --service-account=${GCP_SERVICE_ACCOUNT} -- \
	--gcp_project_id ${GCP_PROJECT_ID} --dataset_id ${DATASET_ID}

gcp-clean:
	@echo "Clean up GCP data"













