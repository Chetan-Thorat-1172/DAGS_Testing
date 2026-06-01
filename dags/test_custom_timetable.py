"""
Test DAG: Custom Timetables (#31)
=================================
Tests the @weekdays preset schedule (Mon-Fri only).
This DAG should only create runs on business days.

Also tests that `timetable` parameter is parsed correctly.
"""

from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime


def weekday_report(**context):
    import time
    from datetime import date
    today = date.today()
    print(f"Running weekday report for {today.strftime('%A, %B %d, %Y')}")
    print(f"Day of week: {today.weekday()} (0=Mon, 6=Sun)")
    time.sleep(1)
    return {"day": today.isoformat(), "weekday": today.strftime("%A")}


with DAG(
    dag_id="test_weekday_schedule",
    schedule_interval="@weekdays",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Test: @weekdays preset - only runs Mon-Fri",
) as dag:

    PythonOperator(
        task_id="weekday_report",
        python_callable=weekday_report,
    )
