from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="load_test_scale_out_2",
    schedule_interval=None,
    start_date=datetime(2026, 7, 2),
    catchup=False,
    max_active_tasks=200,  # let almost everything be dispatched at once
) as dag:
    tasks = []
    for i in range(1, 301):  # 300 tasks
        t = BashOperator(
            task_id=f"load_task_{i}",
            bash_command=f"echo 'task {i} start'; sleep 8; echo 'task {i} done'",
        )
        tasks.append(t)
