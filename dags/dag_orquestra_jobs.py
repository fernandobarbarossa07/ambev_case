import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Função que executa um script Python local
def run_script(script_name):
    script_path = os.path.join('/opt/airflow', script_name)
    subprocess.run(['python3', script_path], check=True)

# Definir o DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),  # Data de início do DAG
    'retries': 1,  # Número de tentativas em caso de falha
}

dag = DAG(
    'orquestra_jobs',  # Nome do DAG
    default_args=default_args,
    schedule_interval=None,  # Não agendado automaticamente, só executado sob demanda
    catchup=False,
)

# Tarefas: uma para cada script
task1 = PythonOperator(
    task_id='run_job1',
    python_callable=run_script,
    op_args=['Job1.py'],  # Nome do primeiro script
    dag=dag,
)

task2 = PythonOperator(
    task_id='run_job2',
    python_callable=run_script,
    op_args=['Job2.py'],  # Nome do segundo script
    dag=dag,
)

task3 = PythonOperator(
    task_id='run_job3',
    python_callable=run_script,
    op_args=['Job3.py'],  # Nome do terceiro script
    dag=dag,
)

# Definindo a sequência das tarefas
task1 >> task2 >> task3
