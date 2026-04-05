import requests
import os
import re
from dotenv import load_dotenv

ENV_PATH = '/mnt/h/ua-aid-intelligence-hub/.env'


def send_report_task_logic(**context):
    """
    Parses XCom fragments from BashOperator stdout.
    Implements regex filtering to extract pure numeric values from logs if necessary.
    """
    load_dotenv(dotenv_path=ENV_PATH)
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    ti = context['ti']
    monitored_tasks = [
        'extract_exchange_rates',
        'extract_news_context',
        'extract_live_cba',
        'extract_live_united24'
    ]

    report_lines = [f"FINAL REPORT: {ti.dag_id}", "------------------"]

    for t_id in monitored_tasks:
        raw_output = ti.xcom_pull(task_ids=t_id)

        # Tech Lead Note: Extracting the last numeric value from potential log noise
        if raw_output:
            # Look for the last number in the string
            numbers = re.findall(r'\d+', str(raw_output))
            count = numbers[-1] if numbers else "0"

            # Simple heuristic: if 'ERROR' is in the last line, mark as failed
            if "ERROR" in str(raw_output).upper():
                status = "❌ ERROR"
            else:
                status = "✅ OK"
        else:
            status = "⚠️ NO DATA"
            count = "0"

        report_lines.append(f"{t_id}: {status} ({count} rows)")

    full_message = "\n".join(report_lines)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": full_message}, timeout=10)