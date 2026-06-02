"""
Test DAG: Scheduling Features (Session 48)
Tests: trigger rules, weight rules, wait_for_downstream, per-task SLA
"""
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator

dag = DAG(
    "test_scheduling_features",
    schedule_interval="*/5 * * * *",
    start_date="2026-06-01T00:00:00",
    catchup=False,
    description="Tests new scheduling features from Session 48",
)

# --- Trigger Rule Tests ---

# Root tasks that will produce different states
always_succeed = BashOperator(
    task_id="always_succeed",
    bash_command="echo success",
    dag=dag,
)

always_fail = BashOperator(
    task_id="always_fail",
    bash_command="exit 1",
    retries=0,
    dag=dag,
)

# one_failed: fires when at least one upstream fails
error_handler = PythonOperator(
    task_id="error_handler",
    trigger_rule="one_failed",
    python_callable=lambda: print("Handling error"),
    dag=dag,
)

# none_skipped: fires only when no upstream was skipped
all_ran_check = PythonOperator(
    task_id="all_ran_check",
    trigger_rule="none_skipped",
    python_callable=lambda: print("All tasks ran (none skipped)"),
    dag=dag,
)

always_succeed >> error_handler
always_fail >> error_handler
always_succeed >> all_ran_check
always_fail >> all_ran_check

# --- Weight Rule Tests ---

# downstream rule: this task gets higher effective priority because it has many dependents
root_task = BashOperator(
    task_id="root_task",
    bash_command="echo root",
    priority_weight=1,
    weight_rule="downstream",
    dag=dag,
)

child_a = BashOperator(
    task_id="child_a",
    bash_command="echo a",
    priority_weight=5,
    dag=dag,
)

child_b = BashOperator(
    task_id="child_b",
    bash_command="echo b",
    priority_weight=5,
    dag=dag,
)

root_task >> child_a
root_task >> child_b

# --- wait_for_downstream Test ---

# This task won't start in the next run until child_a and child_b from the previous run complete
gated_task = BashOperator(
    task_id="gated_task",
    bash_command="echo gated",
    wait_for_downstream=True,
    dag=dag,
)

gated_downstream = BashOperator(
    task_id="gated_downstream",
    bash_command="sleep 10",
    dag=dag,
)

gated_task >> gated_downstream

# --- Per-Task SLA Test ---

# This task has a 60-second SLA — if it hasn't completed 60s after logical_date, SLA miss fires
sla_task = BashOperator(
    task_id="sla_task",
    bash_command="sleep 120",  # deliberately exceeds SLA
    sla=60,
    dag=dag,
)
