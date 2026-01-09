"""
Example DAG that reads from PostgreSQL and writes results to MinIO/S3.
Uses PostgresHook for PostgreSQL and boto3 for MinIO (S3-compatible storage).
"""
import os
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Default arguments for the DAG
default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'example_minio_dag3',
    default_args=default_args,
    description='Extract from PostgreSQL using PostgresHook and write to MinIO using boto3',
    schedule='@once',  # Airflow 3.0+ uses 'schedule' instead of 'schedule_interval'
    catchup=False,
    tags=['example', 'minio', 'postgresql', 'boto3'],
)


def extract_sales_data(**kwargs):
    """
    Extract sales data from PostgreSQL using PostgresHook
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

    query = """
    SELECT id, product_name, quantity, price, sale_date
    FROM sales_data
    ORDER BY sale_date
    """

    # Use PostgresHook to execute query
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append(dict(zip(columns, row)))

    print(f"Extracted {len(results)} records from PostgreSQL using PostgresHook")
    return results


def save_to_minio(data, **kwargs):
    """
    Save results to MinIO using boto3 client
    """
    import csv
    import io

    # Get MinIO configuration from environment variables
    # These are injected from minio-creds secret
    minio_endpoint = os.environ.get('MINIO_ENDPOINT_URL', 'http://minio.lakehouse-platform:9000')
    minio_access_key = os.environ.get('rootUser', 'lakehouse')
    minio_secret_key = os.environ.get('rootPassword', 'lakehouse')

    # Create boto3 S3 client for MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name='us-east-1',
        verify=False
    )

    # Create CSV content
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=['id', 'product_name', 'quantity', 'price', 'sale_date'])
    writer.writeheader()
    writer.writerows(data)
    csv_content = output.getvalue()

    # Upload to MinIO
    bucket = 'lakehouse-dev-warehouse'
    key = f'sales_data/sales_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_content.encode('utf-8'))

    print(f"Successfully uploaded to s3://{bucket}/{key} using boto3")
    return f"s3://{bucket}/{key}"


# Define tasks
extract_task = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='save_to_minio',
    python_callable=save_to_minio,
    op_args=[extract_task.output],
    dag=dag,
)

# Task dependencies
extract_task >> load_task
