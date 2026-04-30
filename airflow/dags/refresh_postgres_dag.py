"""
DAG: Refresh PostgreSQL from Delta Lake
Schedule: Every 30 minutes
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 27),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'refresh_postgres_serving_layer',
    default_args=default_args,
    description='Refresh PostgreSQL from Delta Lake every 30 minutes',
    schedule_interval='*/30 * * * *',
    catchup=False,
    tags=['production', 'serving-layer'],
)

def load_to_postgres():
    """Run the PostgreSQL load script"""
    import subprocess
    
    script_path = f'/Users/{os.getenv("USER")}/Desktop/ecommerce-streaming-platform/load_to_postgres.py'
    
    result = subprocess.run(
        ['python', script_path],
        capture_output=True,
        text=True,
        cwd=f'/Users/{os.getenv("USER")}/Desktop/ecommerce-streaming-platform'
    )
    
    if result.returncode != 0:
        raise Exception(f"PostgreSQL load failed: {result.stderr}")
    
    print(result.stdout)
    return "✅ PostgreSQL refresh complete"

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

log_task = BashOperator(
    task_id='log_completion',
    bash_command='echo "✅ PostgreSQL refresh completed at $(date)"',
    dag=dag,
)

load_task >> log_task
