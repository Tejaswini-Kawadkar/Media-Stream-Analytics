from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to start the Glue job
def start_glue_job():
    glue = boto3.client('glue', region_name='ap-southeast-2')  # change region if needed
    response = glue.start_job_run(JobName='media_iceberg_job')  # your Glue job name
    print("Started Glue job run:", response['JobRunId'])

# Define the DAG
with DAG(
    'media_stream_etl',
    default_args=default_args,
    description='Trigger Glue job using boto3 PythonOperator',
    schedule_interval=None,  # Trigger manually or via Lambda
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['glue', 'iceberg'],
) as dag:

    # Define the PythonOperator task
    run_glue_job = PythonOperator(
        task_id='run_iceberg_update',
        python_callable=start_glue_job
    )
