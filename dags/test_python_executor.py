from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def named_function():
    """Mode 1: Named function (no context)"""
    return "named_function_result"


def with_context(**kwargs):
    """Mode 1: Named function receiving context"""
    ds = kwargs.get("ds", "unknown")
    return f"context_received_ds={ds}"


def with_args(x, y):
    """Mode 1: Named function with op_args"""
    return x + y


with DAG(
    dag_id="test_python_executor",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:

    # Mode 1: Named function — no args
    t_named = PythonOperator(
        task_id="t_named",
        python_callable=named_function,
    )

    # Mode 1/2: Function with **kwargs — receives context
    t_context = PythonOperator(
        task_id="t_context",
        python_callable=with_context,
    )

    # Mode 1: Function with op_args
    t_args = PythonOperator(
        task_id="t_args",
        python_callable=with_args,
        op_args=[10, 20],
    )

    # Mode 2: Lambda — inline callable
    t_lambda = PythonOperator(
        task_id="t_lambda",
        python_callable=lambda: "lambda_result",
    )

    # Mode 1: Function that raises exception — should fail
    t_fail = PythonOperator(
        task_id="t_fail",
        python_callable=lambda: 1 / 0,
        retries=0,
    )
