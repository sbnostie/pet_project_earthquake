from airflow import DAG
import duckdb
import pendulum
import logging
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

OWNER = "sb"
DAG_ID = "raw_from_api_to_s3"

LAYER = "raw"
SOURCE = "earthquake"

#s3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

args ={
    "owner": OWNER,
    "start_time": pendulum.datetime(2025, 9, 15, tz="Asia/Yekaterinburg"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"]
    start_date = start_date.subtract(days = 1).format("YYYY-MM-DD")
    end_date = context["data_interval_end"]
    end_date = end_date.format("YYYY-MM-DD")
    #start_time = context["data_interval_start"].to_time_string() #added by sb
    return start_date, end_date#, start_time

def get_and_transfer_api_data_to_s3(**context):
    start_date, end_date= get_dates(**context)
    logging.info(f"🐸Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}')              
        ) TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}.gz.parquet';
        """
        
    )
    con.close()
    logging.info(f"Download for date success: {start_date}")

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    tags=["s3", "raw"],
    #schedule="@hourly",
    schedule="0 */4 * * *",
    description="SHORT_DESCRIPTION",
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = "LONG_DESCRIPTION"

    start = EmptyOperator(
        task_id="start"
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end"
    )

    trigger_secondary_dag = TriggerDagRunOperator(
        task_id='trigger_secondary_dag',
        trigger_dag_id='raw_from_s3_to_pg',  # Идентификатор целевого DAG'a
        #execution_date=datetime(2025, 9, 15),       # Передача текущей даты запуска (можно передать любые переменные контекста)
        reset_dag_run=False               # Флаг перезапуска, если DAG уже выполнялся ранее
    )
    start >> get_and_transfer_api_data_to_s3 >> end >> trigger_secondary_dag