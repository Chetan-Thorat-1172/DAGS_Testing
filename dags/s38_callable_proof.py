"""S38 live proof: callable-resolution (b) + stderr cap (c).

- t_partial : functools.partial python_callable (used to CRASH ingestion at parse)
- t_nested  : nested/closure python_callable (used to FAIL at runtime via getattr)
- t_plain   : plain module-level function (fast 'function' mode, control)
- t_chatty  : prints >64KB to stdout (-> stderr) to exercise the capped tail
"""
import functools
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def _partial_base(prefix):
    return f"{prefix}:partial-ok"


def _make_nested():
    def inner(**kwargs):
        return "nested-ok"
    return inner


def plain_fn():
    return "plain-ok"


def chatty_fn():
    # ~366 KB to stdout (redirected to stderr by the helper); well past the 64KB cap.
    for _ in range(6000):
        print("X" * 60)
    return "chatty-done"


with DAG(
    dag_id="s38_callable_proof",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:

    t_partial = PythonOperator(
        task_id="t_partial",
        python_callable=functools.partial(_partial_base, prefix="S38"),
    )

    t_nested = PythonOperator(
        task_id="t_nested",
        python_callable=_make_nested(),
    )

    t_plain = PythonOperator(
        task_id="t_plain",
        python_callable=plain_fn,
    )

    t_chatty = PythonOperator(
        task_id="t_chatty",
        python_callable=chatty_fn,
    )
