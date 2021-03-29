# Primeira DAG com AirFlow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import zipfile

#CONSTANTES
data_path = '/usr/local/airflow/data/microdatos_enade_2019/2019/3.DADOS/'
arquivo = data_path+'microdados_enade_2019.txt'

# Argumentos default
default_args = {
    'owner': 'Rodrigo - minerAI',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 16, 18),  # ano mes dia e hora
    'email': ['rodrigobrusts@gmail.com', 'felipe.almeida@minerai.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)}

# Vamos definir a DAG - Fluxo
dag = DAG(
    "treino-05",
    description='Paralelismo',
    default_args=default_args,
    schedule_interval="*/10 * * * *"
)

start_processing = BashOperator(
    task_id='start-processing',
    bash_command='echo Start Preprocessing! Vai!',
    dag=dag
)

get_data = BashOperator(
    task_id='get-data',
    bash_command='curl http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip.  -o /usr/local/airflow/data/train.csv',
    dag=dag
)


def unzip_file():
    with zipfile.ZipFile('/usr/local/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
        zipped.extractall('/usr/local/airflow/data/')


unzip_data = PythonOperator(
    task_id='unzip-data',
    python_callable=unzip_file,
    dag=dag
)


def aplica_filtros():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04',
            'QE_I05', 'QE_I08']
    enade = pe.read_csv(arquivo, sep=';', decimal=',', usecols=cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GET > 0)
        ]
    enade.to_csv(data_path + 'enade_filtrado.csv', index=False)


task_aplica_filtro = PythonOperator(
    task_id='aplica_filtro',
    python_callable=aplica_filtros,
    dag=dag
)


# Idade centralizada na média

# Idade centralizada ao quadrado

def constroi_idade_centralizada():
    idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + 'idadecent.csv', index=False)


def constroi_idade_cent_quad():
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadecent['idade2'] = idadecent.idadecent ** 2
    idadecent[['idade2']].to_csv(data_path + 'idadequadrado.csv', index=False)


task_idade_cent = PythonOperator(
    task_id='constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag=dag

task_idade_quad = PythonOperator(
    task_id='constroi_idade_cent_quad',
    python_callable=constroi_idade_cent_quad,
    dag=dag
)


def constroi_est_civil():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QEI01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)


task_est_civil = PythonOperator(
    task_id='constroi_est_civil',
    python_callable=constroi_est_civil,
    dag=dag
)


def constroi_cor():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I02'])
    filtro['cor'] = filtro.QEI02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': '',
        ' ': ''
    })
    filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)


task_cor = PythonOperator(
    task_id='constroi_cor_da_pele',
    python_callable=constroi_cor,
    dag=dag
)


# TASK JOIN

def join_data():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv')
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadequadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')

    final = pd.concat([
        filtro, idadecent, idadequadrado, estcivil, cor
    ], axis=1)

    final.to_csv(data_path + 'enade_tratado.csv', index=False)


task_join = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag
)


def escreve_dw():
    final = pd.read_csv(data_path + 'enade_tratado.csv')
    engine = sqlalchemy.create_engine(
        "mysql+pyodbc://[localhost]/[username])
    final.to_sql('tratado', con=engine, index=False, if_exists='append')

    task_escreve_dw = PythonOperator(
        task_id='escreve_dw',
        python_callable=escreve_dw,
        dag=dag)
    start_processing >> get_data >> unzip_data >> task_aplica_filtro

    task_aplica_filtro >> [task_idade_cent, task_est_civil, task_cor]

    task_idade_quad.set_upstream(task_idade_cent)

    task_join.set_upstream([
        task_est_civil, task_cor, task_idade_quad]
    ])
    task_join >> task_escreve_dw