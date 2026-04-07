import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = '/mnt/h/ua-aid-intelligence-hub'
if PROJECT_DIR not in sys.path:
    sys.path.append(PROJECT_DIR)

from alert_telegram_bot.telegram_report import send_report_task_logic

# Added -u flag for unbuffered stdout/stderr to ensure immediate log capture
PYTHON_EXEC = '/home/drkosher/airflow_project/venv/bin/python -u'

default_args = {
    'owner': 'kosher',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ua_aid_daily_extraction',
    default_args=default_args,
    schedule_interval='30 6 * * *',
    catchup=False,
    tags=['production', 'osint']
) as dag:

    # Scrapers with XCom push enabled to capture row counts from stdout
    t1 = BashOperator(
        task_id='extract_exchange_rates',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_EXEC} scrapers/currency_rates_scraper.py',
        do_xcom_push=True
    )

    t2 = BashOperator(
        task_id='extract_news_context',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_EXEC} scrapers/news/news_scraper.py',
        do_xcom_push=True
    )

    t3 = BashOperator(
        task_id='extract_live_cba',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_EXEC} scrapers/come_back_alive/come_back_alive_live_scraper.py',
        do_xcom_push=True
    )

    t4 = BashOperator(
        task_id='extract_live_united24',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_EXEC} scrapers/united24/united24_live_scraper.py',
        do_xcom_push=True
    )

    # Final reporting task
    # trigger_rule='all_done' ensures the bot sends a report even if a scraper fails
    report_task = PythonOperator(
        task_id='send_final_report',
        python_callable=send_report_task_logic,
        trigger_rule='all_done'
    )

    # Dependency Graph
    t1 >> t2 >> [t3, t4] >> report_task