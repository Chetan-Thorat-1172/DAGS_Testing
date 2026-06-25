from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    BashOperator,
    task,
)


# ── Feature 5, 6: TaskFlow with automatic dependency inference ──
with DAG(
    dag_id="test_xcom_flow",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:

    # Feature 1: Basic XCom push (return value auto-pushed)
    def produce_data():
        return "hello_from_produce"

    t_produce = PythonOperator(
        task_id="t_produce",
        python_callable=produce_data,
    )

    # Feature 3: ti.xcom_push and ti.xcom_pull explicitly
    def push_and_pull(**kwargs):
        ti = kwargs["ti"]
        ti.xcom_push(key="custom_key", value="custom_value")

        pulled = ti.xcom_pull(
            task_ids="t_produce",
            key="return_value",
        )

        print(f"Pulled from t_produce: {pulled}", flush=True)
        return f"got: {pulled}"

    t_push_pull = PythonOperator(
        task_id="t_push_pull",
        python_callable=push_and_pull,
    )

    # Feature 4: multiple_outputs=True
    @task(task_id="t_multi_out", multiple_outputs=True)
    def multi_output():
        return {
            "name": "piflow",
            "version": "1.0",
            "status": "active",
        }

    # Feature 5+6: TaskFlow auto-dependency + XComArg.__getitem__
    @task(task_id="t_consume_multi")
    def consume_multi(name, version):
        return f"consumed: {name} v{version}"

    # Feature 8: Context variables
    def use_context(**kwargs):
        ds = kwargs.get("ds", "unknown")
        execution_date = kwargs.get("execution_date", "unknown")
        return f"ds={ds} exec={execution_date}"

    t_context = PythonOperator(
        task_id="t_context",
        python_callable=use_context,
    )

    # Feature 7: Go template in Bash
    t_template = BashOperator(
        task_id="t_template",
        bash_command="echo 'DS={{ .DS }} TS={{ .TS }}'",
    )

    # Wire dependencies
    multi_result = multi_output()

    # Feature 5+6: Auto-wired via XComArg
    consume_multi(
        name=multi_result["name"],
        version=multi_result["version"],
    )

    t_produce >> t_push_pull
