import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# -------------------------------------------------------------------
# Logging setup (Airflow captures this automatically)
# -------------------------------------------------------------------
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Make scripts folder visible to Airflow
# -------------------------------------------------------------------
sys.path.append("/opt/airflow/scripts")

from extract import extract_mta_data

# -------------------------------------------------------------------
# Default arguments (production-grade)
# -------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
     "email": ["siddhanthkivade12@gmail.com"],  # <-- replace with your email
    "email_on_failure": True,
    "email_on_retry": False,
}

# -------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------
with DAG(
    dag_id="mta_refined_pipeline_v1",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",  # once per day at 2 AM
    catchup=False,
    tags=["production", "mta", "etl"],
) as dag:

    # ---------------------------------------------------------------
    # Task 1: Extract from MTA API and load to S3 (Bronze)
    # ---------------------------------------------------------------
    extract_and_load_s3 = PythonOperator(
        task_id="extract_mta_and_load_to_s3",
        python_callable=extract_mta_data,
    )

    # ---------------------------------------------------------------
    # Task 2: Load new files from S3 into Snowflake (Bronze)
    # ---------------------------------------------------------------
    load_to_snowflake = SQLExecuteQueryOperator(
        task_id="load_s3_to_snowflake",
        conn_id="snowflake_default",
        sql="""
        COPY INTO TRANSIT_GFTS.BRONZE.RAW_MTA_DATA (RAW_JSON)
        FROM @TRANSIT_GFTS.BRONZE.MTA_BRONZE_STAGE
        FILE_FORMAT = (FORMAT_NAME = TRANSIT_GFTS.BRONZE.MTA_JSON_FORMAT)
        """
        # ⚠️ NO trailing semicolon → avoids "Empty SQL statement" error
    )

    # ---------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------
    extract_and_load_s3 >> load_to_snowflake

