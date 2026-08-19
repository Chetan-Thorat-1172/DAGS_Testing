from datetime import datetime

with DAG(
    dag_id="dymmy",
    schedule_interval=None,
    start_date=datetime(2026, 7, 13),
    expected_duration_seconds=10,  # DAG-level SLA: 10 seconds
    catchup=True,
    tags=["dummy_tag"],
    on_sla_miss_callback=SmtpNotifier(
        to=["Chetan.Thorat@Pibythree.com"],
        subject="SLA MISS: DAG {{ dag_id }}",
    ),
) as dag:

    t_slow = BashOperator(
        task_id="t_slow",
        bash_command="sleep 30",  # Exceeds the 10-second SLA
    )
