"""Query 3 - run query using the BigQuery client."""
import argparse
import os
import json

from google.cloud import bigquery as bq

from dotenv import load_dotenv


load_dotenv()
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")


def main(args=None):
    """Main function to run the query."""
    query = f"""
        SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
        FROM  {GCP_PROJECT_ID}.{args.dataset_id}.date_dim dt, 
              {GCP_PROJECT_ID}.{args.dataset_id}.store_sales, 
              {GCP_PROJECT_ID}.{args.dataset_id}.item
        WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
            AND store_sales.ss_item_sk = item.i_item_sk
            AND item.i_manufact_id = 128
            AND dt.d_moy=11
        GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        ORDER BY dt.d_year, sum_agg desc, brand_id
        """

    client = bq.Client(project=GCP_PROJECT_ID)
    write_disposition = "WRITE_TRUNCATE" if args.bq_write_mode == "overwrite" else "WRITE_APPEND"
    job_config = bq.QueryJobConfig(
        labels=args.bq_job_labels,
        write_disposition=write_disposition,
        destination=f"{GCP_PROJECT_ID}.{args.dataset_id}.query3_result",
    )
    client.query(query, job_config=job_config)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset_id", type=str,
                        help="BigQuery dataset containing the tpcds input tables.")
    parser.add_argument("--bq_job_labels", type=json.loads, required=False, help="BigQuery job labels.")
    parser.add_argument("--bq_write_mode", type=str, required=False,
                        help="Overwrite or append.", default="overwrite")
    args = parser.parse_args()


    main(args)




