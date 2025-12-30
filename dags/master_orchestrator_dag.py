from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging


# ---------------------------------------
# T0023: MASTER ORCHESTRATOR DAG
# ---------------------------------------
# This master DAG triggers multiple child DAGs in sequence or parallel
# Use this to coordinate complex multi-pipeline workflows

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

with DAG(
    dag_id="master_orchestrator",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="@daily",          # Runs daily at midnight
    catchup=False,                       # Don't backfill master DAG
    tags=['orchestration', 'master'],
    description="T0023: Master DAG to trigger and coordinate all ETL pipelines"
):

    def start_orchestration():
        """Log start of orchestration"""
        logging.info("=" * 60)
        logging.info("T0023: Master Orchestrator starting pipeline execution")
        logging.info("=" * 60)
        print("[ORCHESTRATOR] Starting multi-pipeline orchestration")

    start_op = PythonOperator(
        task_id="start_orchestration",
        python_callable=start_orchestration
    )

    # --------- TRIGGER AMAZON ETL PIPELINE ---------
    trigger_amazon_etl = TriggerDagRunOperator(
        task_id="trigger_amazon_etl",
        trigger_dag_id="amazon_etl",           # Target DAG ID
        wait_for_completion=True,              # Wait for child DAG to finish
        poke_interval=30,                      # Check status every 30 seconds
        reset_dag_run=True,                    # Reset if already running
        execution_date="{{ ds }}",             # Pass same execution date
        conf={"triggered_by": "master_orchestrator"}  # Pass config to child DAG
    )

    # --------- TRIGGER AMAZON REPORTING PIPELINE ---------
    trigger_amazon_reporting = TriggerDagRunOperator(
        task_id="trigger_amazon_reporting",
        trigger_dag_id="amazon_reporting",     # Reporting DAG
        wait_for_completion=True,              # Wait for report generation
        poke_interval=30,
        reset_dag_run=True,
        execution_date="{{ ds }}",
        conf={"triggered_by": "master_orchestrator"}
    )

    # --------- PLACEHOLDER: TRIGGER FUTURE PIPELINES ---------
    # Example: Add more pipelines here as they are created
    # trigger_customer_etl = TriggerDagRunOperator(
    #     task_id="trigger_customer_etl",
    #     trigger_dag_id="customer_etl",
    #     wait_for_completion=True,
    # )

    def completion_summary(ti):
        """Log completion summary of all pipelines"""
        logging.info("=" * 60)
        logging.info("T0023: All pipelines completed successfully")
        logging.info("=" * 60)
        print("[ORCHESTRATOR] All child DAGs completed")
        # TODO: Add success metrics aggregation here

    complete_op = PythonOperator(
        task_id="orchestration_complete",
        python_callable=completion_summary
    )

    # ---------------------------------------
    # T0023: ORCHESTRATION FLOW
    # ---------------------------------------
    # Sequential execution: Start -> Amazon ETL -> Amazon Reporting -> Complete
    start_op >> trigger_amazon_etl >> trigger_amazon_reporting >> complete_op

    # For parallel execution (when you have multiple independent ETL pipelines):
    # start_op >> [trigger_amazon_etl, trigger_customer_etl] >> trigger_amazon_reporting >> complete_op
