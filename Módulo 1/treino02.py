# Primeira DAG com AirFlow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

# Argumentos default
default_args = {
    'owner': 'Rodrigo - minerAI',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 16, 18),  # ano mes dia e hora
    'email': ['rodrigobrusts@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)}

# Vamos definir a DAG - Fluxo
dag = DAG(
    "treino-02",
    description='Básico de Bash Operators e Python Operators',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

get_data = BashOperator(
    task_id='get-data',
    bash_command = 'curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/train.csv',
    dag = dag
)

def calculate_mean_age():
    df = pd.read_csv('~/train.csv')
    med = df.Age.mean()
    return med

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids = 'calcula-idade-media')
    print(f"A idade média no Titanica era {value} anos.")

task_idade_media = PythonOperator(
    task_id = 'calcula-idade-media',
    python_callable = calculate_mean_age,
    dag = dag
)

task_print_idade = PythonOperator(
    task_id = 'mostra-idade',
    python_callable = print_age,
    provide_context = True,
    dag = dag
)

get_data >> task_idade_media >> task_print_idade