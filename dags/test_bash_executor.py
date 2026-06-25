from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator


with DAG(
    dag_id="test_bash_executor",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:
    # 1. Basic echo — stdout captured
    t_echo = BashOperator(
        task_id="t_echo",
        bash_command="echo 'hello from bash'",
    )

    # 2. Multi-line command
    t_multi = BashOperator(
        task_id="t_multi",
        bash_command="echo line1 && echo line2 && echo line3",
    )

    # 3. Template rendering — {{ .DS }} and {{ .TS }}
    t_template = BashOperator(
        task_id="t_template",
        bash_command="echo 'ds={{ .DS }} ts={{ .TS }}'",
    )

    # 4. Exit code 1 — should fail
    t_fail = BashOperator(
        task_id="t_fail",
        bash_command="echo 'about to fail' && exit 1",
        retries=0,
    )

    # 5. Exit code 0 after some work
    t_work = BashOperator(
        task_id="t_work",
        bash_command="for i in 1 2 3 4 5; do echo $i; done",
    )

    # 6. Stderr output — should succeed (stderr doesn't mean failure)
    t_stderr = BashOperator(
        task_id="t_stderr",
        bash_command="echo 'stdout here' && echo 'stderr here' >&2",
    )
