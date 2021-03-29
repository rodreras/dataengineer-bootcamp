# Primeira DAG com AirFlow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Argumentos default
default_args = {
    'owner': 'Rodrigo - minerAI',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 16, 18),  # ano mes dia e hora
    'email': ['rodrigobrusts@gmail.com', 'felipe.almeida@minerai.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Vamos definir a DAG - Fluxo
dag = DAG(
    "treino-01",
    description='Básico de Bash Operators e Python Operators',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

# Vamos começar a adicionar tarefas
hello_bash = BashOperator(task_id='Hello_Dash',
    bash_command='echo "Hello AirFlow from bash"',
    dag=dag
                          )


def say_hello():
    print('Hello AirFlow from Python')

hello_python = PythonOperator(
        task_id='Hello_Python',
        python_callable=say_hello,
        dag=dag)

hello_bash >> hello_python