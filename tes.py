#!python3

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

args = {
    'owner': 'ardianrz',
}

dag = DAG(
    dag_id='simple_workflow_de6',
    default_args=args,
    schedule_interval='0 * * * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['digital-skola', 'de6'],
    params={"example_key": "example_value"},
)

start_task = DummyOperator(
    task_id='start_ETL',
    dag=dag,
)

stop_task = DummyOperator(
    task_id='stop_ETL',
    dag=dag,
)
start_task >> stop_task

