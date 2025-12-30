from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from pytz import timezone
import logging
import pandas as pd

# Import scripts
import sys
sys.path.append("/opt/airflow/scripts")


# ---------------------------------------
# T0025: REPORTING DAG WITH EXTERNAL DEPENDENCY
# ---------------------------------------
# This DAG demonstrates multi-DAG dependency management
# It waits for amazon_etl to complete before generating reports

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

with DAG(
    dag_id="amazon_reporting",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval=None,              # Triggered only by master_orchestrator (no auto-schedule)
    catchup=False,
    tags=['reporting', 'amazon', 'dependent'],
    description="T0025: Reporting DAG with external dependency on amazon_etl"
):

    # --------- T0025: WAIT FOR UPSTREAM DAG ---------
    wait_for_amazon_etl = ExternalTaskSensor(
        task_id="wait_for_amazon_etl_completion",
        external_dag_id="amazon_etl",              # DAG to wait for
        external_task_id="load_amazon_data",       # Specific task to wait for
        allowed_states=["success"],                # Only proceed if task succeeded
        failed_states=["failed", "skipped"],       # Fail if upstream failed
        mode="poke",                               # Poke mode (blocking)
        poke_interval=30,                          # Check every 30 seconds
        timeout=3600,                              # Timeout after 1 hour
        execution_delta=timedelta(hours=0),        # Same execution date as upstream
    )

    # --------- GENERATE DAILY REPORT ---------
    def generate_daily_report(ti):
        """
        T0025: Generate daily report after amazon_etl completes
        Saves report to orders_summary.xlsx as a new sheet
        """
        logging.info("[REPORTING] Generating daily Amazon order report")
        
        try:
            # Read the cleaned data produced by amazon_etl
            df = pd.read_csv("/opt/airflow/data/processed/amazon_cleaned_data.csv")
            
            # Generate summary statistics
            report_data = {
                "total_orders": len(df),
                "total_revenue": df["TotalAmount"].sum() if "TotalAmount" in df.columns else 0,
                "avg_order_value": df["TotalAmount"].mean() if "TotalAmount" in df.columns else 0,
                "unique_customers": df["CustomerID"].nunique() if "CustomerID" in df.columns else 0,
            }
            
            # Get execution date and actual run time in IST (India Standard Time)
            ist = timezone('Asia/Kolkata')
            execution_date = ti.execution_date.strftime('%Y-%m-%d')
            actual_run_time = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')
            
            # Create DataFrame from report
            report_df = pd.DataFrame([report_data])
            report_df.insert(0, 'report_date', execution_date)
            report_df.insert(1, 'report_time', actual_run_time)
            
            logging.info(f"[REPORTING] Report generated at {actual_run_time} IST: {report_data}")
            print(f"[REPORTING] Daily Report:\n{report_data}")
            
            # Save to orders_summary.xlsx as new sheet
            excel_path = "/opt/airflow/data/processed/orders_summary.xlsx"
            
            try:
                # Try to append to existing Excel file
                with pd.ExcelFile(excel_path) as xls:
                    existing_sheets = xls.sheet_names
                
                # Read existing file and add new sheet
                with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                    sheet_name = f"Report_{execution_date}"
                    report_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    logging.info(f"[REPORTING] Report saved to sheet: {sheet_name}")
                    
            except FileNotFoundError:
                # Create new Excel file if it doesn't exist
                with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                    report_df.to_excel(writer, sheet_name=f"Report_{execution_date}", index=False)
                    logging.info(f"[REPORTING] Created new orders_summary.xlsx with report")
            
            # Save to daily_reports.csv (append mode with time column)
            csv_path = "/opt/airflow/data/processed/daily_reports.csv"
            import os
            file_exists = os.path.isfile(csv_path)
            report_df.to_csv(csv_path, mode='a', header=not file_exists, index=False)
            logging.info(f"[REPORTING] Report {'appended to' if file_exists else 'created in'} {csv_path}")
            
            # Save to daily_reports.xlsx (append mode - all reports in one sheet)
            xlsx_path = "/opt/airflow/data/processed/daily_reports.xlsx"
            try:
                # Try to read existing Excel and append
                existing_df = pd.read_excel(xlsx_path, engine='openpyxl')
                combined_df = pd.concat([existing_df, report_df], ignore_index=True)
                combined_df.to_excel(xlsx_path, index=False, engine='openpyxl')
                logging.info(f"[REPORTING] Report appended to {xlsx_path} (now {len(combined_df)} total reports)")
            except FileNotFoundError:
                # Create new Excel file if it doesn't exist
                report_df.to_excel(xlsx_path, index=False, engine='openpyxl')
                logging.info(f"[REPORTING] Created new {xlsx_path} with first report")
            except Exception as e:
                logging.error(f"[REPORTING] Failed to save Excel report: {e}")
                # Continue anyway - CSV was already saved
            
            return report_data
            
        except FileNotFoundError:
            logging.error("[REPORTING] Cleaned data file not found. amazon_etl may have failed.")
            raise
        except Exception as e:
            logging.error(f"[REPORTING] Report generation failed: {e}")
            raise

    report_op = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report
    )

    # --------- SEND REPORT NOTIFICATION ---------
    def send_report_notification(ti):
        """
        Notify stakeholders that report is ready
        """
        report_data = ti.xcom_pull(task_ids="generate_daily_report")
        logging.info(f"[REPORTING] Report ready for distribution: {report_data}")
        print("[REPORTING] Report notification sent (placeholder)")
        
        # TODO: Implement actual notification (email/Slack)
        # Example: send_email(to=['team@company.com'], subject='Daily Amazon Report', body=...)

    notify_op = PythonOperator(
        task_id="send_report_notification",
        python_callable=send_report_notification
    )

    # ---------------------------------------
    # T0025: CROSS-DAG DEPENDENCY FLOW
    # ---------------------------------------
    # Wait for amazon_etl -> Generate report -> Send notification
    wait_for_amazon_etl >> report_op >> notify_op
