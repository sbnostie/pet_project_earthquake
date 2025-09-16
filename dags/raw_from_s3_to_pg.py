import logging
import duckdb
import pendulum


from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "sb"
DAG_ID = "raw_from_s3_to_pg"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

#s3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

#DuckDB
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
#LONG_DESCRIPTION"""

SHORT_DESCRIPTION = "SHORT_DESCRIPTION"

args ={
    "owner": OWNER,
    "start_time": pendulum.datetime(2025, 9, 15, tz="Asia/Yekaterinburg"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    #start_time = context["data_interval_start"].to_time_string() #added by sb
    return start_date, end_date#, start_time

def get_and_transfer_raw_data_to_ods_pg(**context):
    start_date, end_date = get_dates(**context)
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

        CREATE SECRET dwh_postgres(
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres_db,
            USER 'postgres',
            PASSWORD '{PASSWORD}'

        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            (time, latitude, longitude)
        SELECT 
        (
            time,
            latitude, 
            longitude
        ) FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}.gz.parquet';
        
        """        
    )
    con.close()
    logging.info(f"✅Download for date success: {start_date}")

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    tags=["s3", "raw", "pg", "q"],
    description="SHORT_DESCRIPTION",
    schedule="@hourly",
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = "LONG_DESCRIPTION"

    start = EmptyOperator(
        task_id="start"
    )
    """sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        #external_task_id="end",
        #allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=5,
    )
"""
    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id="end"
    )

start >> get_and_transfer_api_data_to_s3 >> end