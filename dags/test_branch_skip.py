"""
TEST DAG: Branch Skip Logic
============================
Tests that BranchPythonOperator correctly skips non-selected paths
and cascades skips downstream, while trigger_rule="always" tasks survive.

TOPOLOGY:
                    ┌─► path_a ──► path_a_child
                    │
    start ──► branch
                    │
                    └─► path_b ──► path_b_child
                                        │
                                        ▼
                                    join_always  (trigger_rule="always")
                                        │
                                        ▼
                                      finish

EXPECTED BEHAVIOR (branch returns "path_a"):
  - start:         success
  - branch:        success
  - path_a:        success  (selected)
  - path_a_child:  success  (downstream of selected)
  - path_b:        SKIPPED  (not selected)
  - path_b_child:  SKIPPED  (cascade skip from path_b)
  - join_always:   success  (trigger_rule="always" — immune to skip propagation)
  - finish:        success  (downstream of join_always which ran)

VERIFICATION QUERIES (run after dag_run completes):
  SELECT task_id, state FROM task_instance
  WHERE dag_id = 'test_branch_skip' AND run_id = (
    SELECT run_id FROM dag_run WHERE dag_id = 'test_branch_skip'
    ORDER BY execution_date DESC LIMIT 1
  ) ORDER BY task_id;

EXPECTED RESULTS:
  branch        | success
  finish        | success
  join_always   | success
  path_a        | success
  path_a_child  | success
  path_b        | skipped
  path_b_child  | skipped
  start         | success
"""

from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    BashOperator,
    BranchPythonOperator,
)


def pick_path_a(**kwargs):
    """Branch decision: always pick path_a, skip path_b."""
    return "path_a"


def do_work(**kwargs):
    return "done"


with DAG(
    dag_id="test_branch_skip",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "branch", "skip-logic"],
    description="Tests branch skip propagation and trigger_rule=always immunity",
) as dag:

    start = BashOperator(task_id="start", bash_command="echo starting")

    branch = BranchPythonOperator(task_id="branch", python_callable=pick_path_a)

    # --- Selected path ---
    path_a = BashOperator(task_id="path_a", bash_command="echo path A selected")
    path_a_child = BashOperator(task_id="path_a_child", bash_command="echo path A child")

    # --- Non-selected path (should be SKIPPED) ---
    path_b = BashOperator(task_id="path_b", bash_command="echo path B selected")
    path_b_child = BashOperator(task_id="path_b_child", bash_command="echo path B child")

    # --- Join point: trigger_rule="always" survives skip cascade ---
    join_always = PythonOperator(
        task_id="join_always",
        python_callable=do_work,
        trigger_rule="always",
    )

    # --- Final task downstream of join ---
    finish = BashOperator(task_id="finish", bash_command="echo all done")

    # Wire dependencies
    start >> branch
    branch >> path_a >> path_a_child
    branch >> path_b >> path_b_child
    path_b_child >> join_always
    join_always >> finish
