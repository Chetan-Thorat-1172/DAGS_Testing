from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

    with DAG(
        dag_id="tz_utc",
        schedule_interval="0 9 * * *",
        timezone="Asia/Kolkata",
        start_date=datetime(2026, 6, 22),
        catchup=True,
    ) as dag:
        BashOperator(task_id="run", bash_command="echo 'UTC run at {{ .TS }}'")
