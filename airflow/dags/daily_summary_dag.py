"""
DAG: Daily Business Summary
Schedule: Daily at 2 AM
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
    'daily_business_summary',
    default_args=default_args,
    description='Generate daily business metrics',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['reporting', 'daily'],
)

def generate_daily_report():
    """Generate daily summary"""
    import psycopg2
    
    conn = psycopg2.connect(
        host="localhost",
        database="ecommerce_analytics",
        user=os.getenv("USER")
    )
    
    cur = conn.cursor()
    
    # Get totals
    cur.execute("""
        SELECT 
            SUM(total_events) as events,
            SUM(total_revenue) as revenue,
            SUM(total_orders) as orders,
            AVG(conversion_rate) as conv_rate
        FROM hourly_summary
    """)
    
    result = cur.fetchone()
    
    print("\n" + "="*60)
    print("📊 DAILY BUSINESS SUMMARY")
    print("="*60)
    print(f"Total Events:    {int(result[0] or 0):,}")
    print(f"Total Revenue:   £{float(result[1] or 0):,.2f}")
    print(f"Total Orders:    {int(result[2] or 0):,}")
    print(f"Conversion Rate: {float(result[3] or 0):.2f}%")
    print("="*60)
    
    # Top products
    cur.execute("""
        SELECT product_name, SUM(activity_count) as total
        FROM trending_products
        WHERE product_name IS NOT NULL
        GROUP BY product_name
        ORDER BY total DESC
        LIMIT 5
    """)
    
    print("\n🏆 TOP 5 PRODUCTS")
    print("="*60)
    for idx, row in enumerate(cur.fetchall(), 1):
        print(f"{idx}. {row[0]:30} | {int(row[1]):,} interactions")
    print("="*60 + "\n")
    
    cur.close()
    conn.close()
    
    return "Report generated"

report_task = PythonOperator(
    task_id='generate_daily_report',
    python_callable=generate_daily_report,
    dag=dag,
)
