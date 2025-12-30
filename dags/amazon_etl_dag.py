from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor  # T0024: Event-driven triggering
from datetime import datetime, timedelta
import logging

# Import your ETL functions
import sys
sys.path.append("/opt/airflow/scripts")

from Extract import extract
from TransformAmazon import transform_amazon
from Load import load


# ---------------------------------------
# T0027: FAILURE HANDLING CALLBACK
# ---------------------------------------
def on_failure_callback(context):
    """
    T0027: Failure handling strategy
    Called when a task fails after all retries are exhausted.
    Logs detailed error information for debugging.
    
    Args:
        context: Airflow context dict with task instance, execution date, etc.
    """
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    
    error_msg = f"""
    ===== TASK FAILURE ALERT (T0027) =====
    DAG ID: {dag_id}
    Task ID: {task_id}
    Execution Date: {execution_date}
    Exception: {exception}
    Log URL: {task_instance.log_url}
    ======================================
    """
    
    logging.error(error_msg)
    
    # TODO: Add email/Slack notification here
    # Example: send_email(to=['admin@example.com'], subject=f'Task Failed: {task_id}', body=error_msg)
    # Example: send_slack_notification(channel='#alerts', message=error_msg)
    
    print(f"[FAILURE HANDLER] Task {task_id} failed. Check logs for details.")


# ---------------------------------------
# DEFAULT DAG SETTINGS (T0027: Failure Handling)
# ---------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 3,                              # T0027: Retry failed tasks 3 times
    "retry_delay": timedelta(minutes=5),       # T0027: Wait 5 minutes between retries
    "retry_exponential_backoff": True,         # T0027: Exponential backoff (5min, 10min, 20min)
    "max_retry_delay": timedelta(minutes=30),  # T0027: Cap retry delay at 30 minutes
    "on_failure_callback": on_failure_callback, # T0027: Custom failure handler
    "email_on_failure": False,                 # Set to True if email is configured
    "email_on_retry": False,
    "depends_on_past": False,                  # Task doesn't depend on previous run success
}

# ---------------------------------------
# DEFINE DAG FOR AMAZON DATASET
# ---------------------------------------
with DAG(
    dag_id="amazon_etl",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),    # T0026: Start date for backfill/catchup
    schedule_interval=None,              # Triggered only by master_orchestrator (no auto-schedule)
    catchup=True,                        # T0026: Enable backfill for historical runs
    max_active_runs=3,                   # T0026: Limit concurrent backfill runs
    tags=['etl', 'amazon', 'production'], # Tags for filtering in UI
    description="Amazon order ETL pipeline with advanced error handling and backfill support"
):

    # --------- TASK 0: FILE SENSOR (T0024: Event-Driven Triggering) ---------
    wait_for_file = FileSensor(
        task_id="wait_for_amazon_file",
        filepath="/opt/airflow/data/raw/amazon.csv",  # File to monitor
        fs_conn_id="fs_default",                      # Default filesystem connection
        poke_interval=30,                             # Check every 30 seconds
        timeout=600,                                  # Timeout after 10 minutes
        mode="poke",                                  # Poke mode (blocking)
        soft_fail=False,                              # Hard fail if file not found
    )

    # --------- TASK 1: EXTRACT ---------
    def extract_task():
        input_path = "/opt/airflow/data/raw/amazon.csv"   # Amazon dataset
        df = extract(input_path)
        print(f"Extracted {len(df)} rows from amazon.csv")
        print(f"Columns: {df.columns.tolist()}")
        return df.to_json()   # pass dataframe to next task

    extract_op = PythonOperator(
        task_id="extract_amazon_data",
        python_callable=extract_task
    )

    # --------- TASK 2: TRANSFORM ---------
    def transform_task(ti):
        df_json = ti.xcom_pull(task_ids="extract_amazon_data")
        import pandas as pd
        df = pd.read_json(df_json)
        print(f"Transforming {len(df)} rows...")
        df = transform_amazon(df)
        print(f"Transformed data shape: {df.shape}")
        return df.to_json()

    transform_op = PythonOperator(
        task_id="transform_amazon_data",
        python_callable=transform_task
    )

    # --------- TASK 3: LOAD ---------
    def load_task(ti):
        df_json = ti.xcom_pull(task_ids="transform_amazon_data")
        import pandas as pd
        df = pd.read_json(df_json)
        load(
            df,
            csv_path="/opt/airflow/data/processed/amazon_cleaned_data.csv",
            xlsx_path="/opt/airflow/data/processed/amazon_cleaned_data.xlsx",
            load_type="full",  # Options: 'full' or 'incremental'
            bulk_chunk_size=1000  # Bulk load in chunks of 1000 rows
        )
        print(f"Loaded {len(df)} rows to output files")

    load_op = PythonOperator(
        task_id="load_amazon_data",
        python_callable=load_task
    )

    # ORDER OF EXECUTION (T0024: FileSensor triggers pipeline)
    wait_for_file >> extract_op >> transform_op >> load_op
