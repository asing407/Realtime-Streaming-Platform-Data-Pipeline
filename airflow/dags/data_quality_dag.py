"""
DAG: Data Quality Monitoring
Schedule: Every 4 hours
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 27),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality_checks',
    default_args=default_args,
    description='Monitor data quality and freshness',
    schedule_interval='0 */4 * * *',
    catchup=False,
    tags=['monitoring', 'data-quality'],
)

def check_data_freshness():
    """Alert if data is more than 24 hours old"""
    import psycopg2
    from datetime import datetime, timedelta
    
    conn = psycopg2.connect(
        host="localhost",
        database="ecommerce_analytics",
        user=os.getenv("USER")
    )
    
    cur = conn.cursor()
    cur.execute("SELECT MAX(window_start) FROM funnel_metrics")
    result = cur.fetchone()
    latest_data = result[0]
    
    cur.close()
    conn.close()
    
    if latest_data:
        age = datetime.now() - latest_data.replace(tzinfo=None)
        age_hours = age.total_seconds() / 3600
        print(f"📊 Latest data is {age_hours:.1f} hours old")
        
        if age_hours > 24:
            print(f"⚠️ WARNING: Data is {age_hours:.1f} hours old!")
        else:
            print("✅ Data freshness OK")
    
    return "Freshness check complete"

def validate_data_counts():
    """Check table row counts"""
    import psycopg2
    
    conn = psycopg2.connect(
        host="localhost",
        database="ecommerce_analytics",
        user=os.getenv("USER")
    )
    
    cur = conn.cursor()
    
    tables = ['funnel_metrics', 'trending_products', 'revenue_metrics', 'hourly_summary']
    
    print("\n📊 TABLE ROW COUNTS")
    print("="*50)
    
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"{table:25} | {count:,} rows")
        
        if count == 0:
            print(f"⚠️ WARNING: {table} is empty!")
    
    print("="*50)
    
    cur.close()
    conn.close()
    
    return "Validation complete"

task1 = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

task2 = PythonOperator(
    task_id='validate_data_counts',
    python_callable=validate_data_counts,
    dag=dag,
)

task1 >> task2
