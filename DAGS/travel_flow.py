
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.python import PythonOperator
# This makes scheduling easy

from datetime import timedelta
from pendulum import today



from Travel_data_a import airflowfunc

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Michellenyabz',
    'start_date': today('UTC').subtract(days=1),
    'email': ['michelle.nyabz@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,

    'retry_delay': timedelta(minutes=5),

}

# define the DAG
dag = DAG(
    dag_id='VISA-EXTRACTION-DAG',
    default_args=default_args,
    description='triggers incase of error',
    schedule=timedelta(days=1),)

task=PythonOperator(
    task_id='complete_etl_task',
    python_callable=airflowfunc,
    dag=dag,
)

task
