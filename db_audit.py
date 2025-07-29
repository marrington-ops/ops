from google.cloud import bigquery
import pandas as pd

def read_bq_to_dict(project_id: str, dataset_id: str, table_id: str) -> list[dict]:
    table_name = f"{project_id}.{dataset_id}.{table_id}"
    client = bigquery.Client(project=project_id)
    query = f"SELECT table_id, row_count FROM `{table_name}`"

    try:
        query_job = client.query(query)  # Make an API request.
        results = query_job.result()  # Waits for query to finish.
        dict_list = [dict(row.items()) for row in results]
        return dict_list
    except Exception as e:
        print(f"Error reading BigQuery table: {e}")
        return []

def loop_over_tables(results, project_id):

    client = bigquery.Client(project=project_id)

    inc_rc = f"""
                    SELECT  '{tbl}' as tbl_id,
                        CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %z', edp_update_ts) AS DATE) edp_load_dt,
                        Count(*) as inc_row_count
                        FROM `rews-cbre-edp-prod.main.{tbl}`
                        Group by  CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %z', edp_update_ts) AS DATE), edp_update_ts            
            """ 
    try:
        query_job = client.query(inc_rc)  # Make an API request.
        results = query_job.result()  # Waits for query to finish.
        dict_list = [dict(row.items()) for row in results]
        return dict_list
    except Exception as e:
        print(f"Error reading BigQuery table: {e}")
        return []

"""
Politely write the results to the table
Update this section to write to the bq table once it's created.  
After the intial run update the code to to fetch the most recent date
"""

def write_json_to_bq(data: list[dict], project_id: str, dataset_id: str, table_id: str, schema: list[bigquery.SchemaField]):
    """Writes JSON data to a BigQuery table."""
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
    )

    # Convert data to newline-delimited JSON format
    json_string = '\n'.join([json.dumps(row) for row in data])
    data_bytes = json_string.encode('utf-8')
    data_file = io.BytesIO(data_bytes)

    job = client.load_table_from_file(
        data_file, table_ref, job_config=job_config
    )
    job.result()

    print(f"Loaded JSON to {dataset_id}.{table_id}")


if __name__ == '__main__':
    PROJECT_ID = "rews-cbre-edp-prod"
    DATASET_ID = "main"
    TABLE_ID = "__TABLES__"
    results = read_bq_to_dict(PROJECT_ID, DATASET_ID, TABLE_ID)
    
    for li in results:
        tbl = li['table_id']
        data = loop_over_tables(tbl, PROJECT_ID)

