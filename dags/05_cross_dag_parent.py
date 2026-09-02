"""Feature 27 - Cross-DAG trigger.

One DAG starts a run of ANOTHER DAG, and can wait for it to finish.

    prepare  ->  trigger_child  ->  after_child

trigger_child starts 05_cross_dag_child, hands it some conf, and waits.
While waiting the task sits in 'waiting_for_child' - it is NOT holding a
worker slot. after_child only runs once the child run succeeded.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, TriggerDagRunOperator


def prepare():
    print("parent: getting things ready", flush=True)


def after_child():
    print("parent: the child finished, carrying on", flush=True)


with DAG(
    dag_id="05_cross_dag_parent",
    description="Feature 27 - TriggerDagRunOperator",
    schedule=None,
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["session"],
) as dag:

    prepare_task = PythonOperator(task_id="prepare", python_callable=prepare)

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="05_cross_dag_child",   # must exist AND be unpaused
        conf={"batch_date": "{{ .DS }}"},      # Go templating - non-Python operator
        wait_for_completion=True,              # False = fire and forget
        allowed_states=["success"],            # what counts as "the child was fine"
        failed_states=["failed"],              # what ends the wait as a failure
    )

    after_child_task = PythonOperator(task_id="after_child", python_callable=after_child)

    prepare_task >> trigger_child >> after_child_task
