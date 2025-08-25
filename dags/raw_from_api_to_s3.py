from airflow import DAG
import duckdb
import pendulum
import logging
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "sb"
DAG_ID = "raw_from_api_to_s3"
