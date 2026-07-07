from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
    SlackAPIPostOperator,
)


with DAG(
    dag_id="test_slack_alert",
    schedule_interval=None,
    start_date=datetime(2026, 7, 7),
    catchup=False,
) as dag:

    do_work = BashOperator(
        task_id="do_work",
        bash_command="echo 'doing work...' && exit 1",
    )

    slack_failure = SlackAPIPostOperator(
        task_id="slack_failure",
        connection_id="slack_alerts",
        text=(
            ":red_circle: *Task Failed*\n"
            "DAG: {{ .DagID }}\n"
            "Run: {{ .RunID }}\n"
            "Date: {{ .DS }}\n"
            "Task do_work failed!"
        ),
        trigger_rule="one_failed",
    )

    slack_success = SlackAPIPostOperator(
        task_id="slack_success",
        connection_id="slack_alerts",
        text=(
            ":large_green_circle: *All Tasks Passed*\n"
            "DAG: {{ .DagID }}\n"
            "Run: {{ .RunID }}\n"
            "Date: {{ .DS }}"
        ),
        trigger_rule="all_success",
    )

    do_work >> [slack_failure, slack_success]
