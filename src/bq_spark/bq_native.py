"""Query 3 - run query using the BigQuery client."""
import argparse
import os

from google.cloud import bigquery as bq


def main(args=None):
    """Main function to run the query."""
    query_label = f"p2804_q3_bq_native_{args.dataset_id}"
    query = f"""
        SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
        FROM  {args.gcp_project_id}.{args.dataset_id}.date_dim dt, 
              {args.gcp_project_id}.{args.dataset_id}.store_sales, 
              {args.gcp_project_id}.{args.dataset_id}.item
        WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
            AND store_sales.ss_item_sk = item.i_item_sk
            AND item.i_manufact_id = 128
            AND dt.d_moy=11
        GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        ORDER BY dt.d_year, sum_agg desc, brand_id
        """

    client = bq.Client(project=args.gcp_project_id)
    write_disposition = "WRITE_TRUNCATE" if args.bq_write_mode == "overwrite" else "WRITE_APPEND"
    job_config = bq.QueryJobConfig(
        labels={"usecase": query_label},
        write_disposition=write_disposition,
        destination=f"{args.gcp_project_id}.{args.dataset_id}.query3_result",
    )
    client.query(query, job_config=job_config)
    print(f"Label: {query_label}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--gcp_project_id", type=str, help="GCP project ID.", default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument("--dataset_id", type=str,
                        help="BigQuery dataset containing the tpcds input tables.")
    parser.add_argument("--bq_write_mode", type=str, required=False,
                        help="Overwrite or append.", default="overwrite")
    args = parser.parse_args()


    main(args)




