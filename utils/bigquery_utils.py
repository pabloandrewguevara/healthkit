from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from utils.logging_config import logger
import pandas as pd

class BigQueryError(Exception):
    """Custom exception for BigQuery operations."""
    pass

def upload_to_bigquery(df, table_key, schema, project_id, dataset_id, tables):
    """Upload a DataFrame to BigQuery with the specified schema.
    
    Args:
        df: DataFrame to upload
        table_key: Key in tables dict for the target table
        schema: BigQuery schema definition
        project_id: BigQuery project ID
        dataset_id: BigQuery dataset ID
        tables: Dictionary of table names
        
    Raises:
        BigQueryError: If there's an error during the upload process
    """
    try:
        # Validate input parameters
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
        if table_key not in tables:
            raise ValueError(f"Table key '{table_key}' not found in tables configuration")
            
        logger.info(f"Starting upload to BigQuery table {tables[table_key]}")
        
        # Convert DataFrame to Parquet format (in-memory)
        parquet_buffer = BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_buffer)

        # Upload the Parquet data to BigQuery
        table_ref = f"{project_id}.{dataset_id}.{tables[table_key]}"
        logger.debug(f"Table reference: {table_ref}")

        client = bigquery.Client(project=project_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        parquet_buffer.seek(0)  # Reset buffer position
        job = client.load_table_from_file(
            parquet_buffer, table_ref, job_config=job_config
        )

        # Wait for the job to finish
        job.result()
        logger.info(f"Successfully loaded {len(df)} rows into {table_ref} \033[92m✓\033[0m")
        
    except GoogleCloudError as e:
        logger.error(f"Google Cloud error during BigQuery upload: {str(e)}")
        raise BigQueryError(f"Google Cloud error during BigQuery upload: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during BigQuery upload: {str(e)}")
        raise BigQueryError(f"Unexpected error during BigQuery upload: {str(e)}") 