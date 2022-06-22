#!python3

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import date, datetime, timedelta

args = {
    'owner': 'ardianrz',
}

dag = DAG(
    dag_id='workflow_batch_de6',
    default_args=args,
    schedule_interval='* 0 * * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['digital-skola', 'de6']
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

postgresql = BashOperator(
    task_id='postgresql-digitalskola',
    bash_command='python3 /home/ardianrz/airflow/dags/postgresql_ingest.py',
    dag=dag
)

api = BashOperator(
    task_id='api-github',
    bash_command='python3 /home/ardianrz/airflow/dags/api_ingest.py',
    dag=dag
)

hadoop = DummyOperator(
    task_id = 'hadoop-datalake',
    dag=dag
)

dwh = DummyOperator(
    task_id = 'datawarehouse-postgresql',
    dag=dag
)

mart1 = DummyOperator(
    task_id = 'mart1',
    dag=dag
)

mart2 = DummyOperator(
    task_id = 'mart2',
    dag=dag
)



stop = DummyOperator(
    task_id='stop',
    dag=dag,
)

start >> [postgresql, api] >> hadoop >> dwh 
dwh >> mart1 >> stop
dwh >> mart2 >> stop
