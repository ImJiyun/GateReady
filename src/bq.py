from google.cloud import bigquery
from config import GCP_PROJECT_ID

_client = None

def get_bq_client():
    global _client
    if _client is None:
        _client = bigquery.Client(project=GCP_PROJECT_ID)
    return _client


def load_df_to_bq(
    df,
    table_id: str,
    write_disposition: str = "WRITE_APPEND"
):
    client = get_bq_client()

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=write_disposition
        )
    )
    job.result()

    return {
        "table_id": table_id,
        "rows": job.output_rows,
        "errors": job.errors
    }