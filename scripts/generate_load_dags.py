#!/usr/bin/env python3
"""
PI-FLOW Load Test DAG Generator

Generates N DAG files for load testing the scheduler, worker pool, and ingestion pipeline.
Uses BashOperator with configurable sleep to isolate system overhead from external dependencies.

Usage:
    python generate_load_dags.py --level 1        # 50 DAGs, 1 task, every min
    python generate_load_dags.py --level 2        # 100 DAGs, 3 tasks (chain), every min
    python generate_load_dags.py --level 3        # 200 DAGs, 5 tasks (diamond), every 2 min
    python generate_load_dags.py --level 4        # 500 DAGs, 1 task, every 5 min
    python generate_load_dags.py --level 5        # 500 DAGs, 3 tasks, every min (stress)
    python generate_load_dags.py --dags 75 --tasks 2 --schedule "*/3 * * * *" --shape chain
"""

import argparse
import os
import shutil

LEVELS = {
    1: {"dags": 50,  "tasks": 1, "schedule": "* * * * *",   "shape": "single",  "sleep": 1, "desc": "Light: 50 DAGs x 1 task, every min"},
    2: {"dags": 100, "tasks": 3, "schedule": "* * * * *",   "shape": "chain",   "sleep": 2, "desc": "Medium: 100 DAGs x 3 tasks (chain), every min"},
    3: {"dags": 200, "tasks": 5, "schedule": "*/2 * * * *", "shape": "diamond", "sleep": 3, "desc": "Heavy: 200 DAGs x 5 tasks (diamond), every 2 min"},
    4: {"dags": 500, "tasks": 1, "schedule": "*/5 * * * *", "shape": "single",  "sleep": 0, "desc": "Burst: 500 DAGs x 1 task (instant), every 5 min"},
    5: {"dags": 500, "tasks": 3, "schedule": "* * * * *",   "shape": "chain",   "sleep": 1, "desc": "Stress: 500 DAGs x 3 tasks (chain), every min"},
}


def gen_single_task_dag(dag_id, schedule, sleep_secs, max_active_runs=3):
    """Single task DAG — minimal overhead, tests scheduler + dispatch throughput."""
    cmd = f"echo done" if sleep_secs == 0 else f"sleep {sleep_secs} && echo done"
    return f'''from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="{dag_id}",
    schedule_interval="{schedule}",
    start_date=datetime(2026, 5, 14),
    catchup=False,
    max_active_runs={max_active_runs},
    description="Load test: single task",
) as dag:
    t1 = BashOperator(task_id="run", bash_command="{cmd}")
'''


def gen_chain_dag(dag_id, schedule, num_tasks, sleep_secs, max_active_runs=3):
    """Linear chain DAG — tests dependency resolution with sequential tasks."""
    cmd = f"echo done" if sleep_secs == 0 else f"sleep {sleep_secs} && echo done"
    lines = [
        'from datetime import datetime',
        'from dag_parser.dynamic.dag_context import DAG, BashOperator',
        '',
        'with DAG(',
        f'    dag_id="{dag_id}",',
        f'    schedule_interval="{schedule}",',
        '    start_date=datetime(2026, 5, 14),',
        '    catchup=False,',
        f'    max_active_runs={max_active_runs},',
        '    description="Load test: chain",',
        ') as dag:',
    ]
    task_vars = []
    for i in range(1, num_tasks + 1):
        var = f"t{i}"
        task_vars.append(var)
        lines.append(f'    {var} = BashOperator(task_id="step_{i}", bash_command="{cmd}")')

    if len(task_vars) > 1:
        lines.append(f'    {" >> ".join(task_vars)}')

    return "\n".join(lines) + "\n"


def gen_diamond_dag(dag_id, schedule, num_tasks, sleep_secs, max_active_runs=3):
    """Diamond DAG — fan-out from start, fan-in to end. Tests concurrent task execution."""
    cmd = f"echo done" if sleep_secs == 0 else f"sleep {sleep_secs} && echo done"
    middle_count = max(1, num_tasks - 2)
    lines = [
        'from datetime import datetime',
        'from dag_parser.dynamic.dag_context import DAG, BashOperator',
        '',
        'with DAG(',
        f'    dag_id="{dag_id}",',
        f'    schedule_interval="{schedule}",',
        '    start_date=datetime(2026, 5, 14),',
        '    catchup=False,',
        f'    max_active_runs={max_active_runs},',
        '    description="Load test: diamond",',
        ') as dag:',
        f'    start = BashOperator(task_id="start", bash_command="{cmd}")',
    ]
    middle_vars = []
    for i in range(1, middle_count + 1):
        var = f"mid_{i}"
        middle_vars.append(var)
        lines.append(f'    {var} = BashOperator(task_id="middle_{i}", bash_command="{cmd}")')

    lines.append(f'    finish = BashOperator(task_id="finish", bash_command="{cmd}", trigger_rule="all_done")')
    lines.append(f'    start >> [{", ".join(middle_vars)}] >> finish')

    return "\n".join(lines) + "\n"


def generate_dags(output_dir, num_dags, num_tasks, schedule, shape, sleep_secs, prefix="lt", max_active_runs=3):
    """Generate DAG files in the output directory."""
    os.makedirs(output_dir, exist_ok=True)

    for i in range(1, num_dags + 1):
        dag_id = f"{prefix}_{i:04d}"
        filename = os.path.join(output_dir, f"{dag_id}.py")

        if shape == "single" or num_tasks == 1:
            content = gen_single_task_dag(dag_id, schedule, sleep_secs, max_active_runs)
        elif shape == "chain":
            content = gen_chain_dag(dag_id, schedule, num_tasks, sleep_secs, max_active_runs)
        elif shape == "diamond":
            content = gen_diamond_dag(dag_id, schedule, num_tasks, sleep_secs, max_active_runs)
        else:
            content = gen_chain_dag(dag_id, schedule, num_tasks, sleep_secs, max_active_runs)

        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)

    print(f"Generated {num_dags} DAGs in {output_dir} (shape={shape}, tasks={num_tasks}, schedule={schedule}, max_active_runs={max_active_runs})")


def clean_load_test_dags(dags_dir, prefix="lt"):
    """Remove all previously generated load test DAGs (files starting with prefix)."""
    count = 0
    for f in os.listdir(dags_dir):
        if f.startswith(prefix + "_") and f.endswith(".py"):
            os.remove(os.path.join(dags_dir, f))
            count += 1
    if count > 0:
        print(f"Cleaned {count} old load test DAGs from {dags_dir}")


def main():
    parser = argparse.ArgumentParser(description="PI-FLOW Load Test DAG Generator")
    parser.add_argument("--level", type=int, choices=[1, 2, 3, 4, 5],
                        help="Predefined load level (1-5)")
    parser.add_argument("--dags", type=int, help="Number of DAGs to generate")
    parser.add_argument("--tasks", type=int, default=1, help="Tasks per DAG")
    parser.add_argument("--schedule", type=str, default="* * * * *", help="Cron schedule")
    parser.add_argument("--shape", type=str, default="chain",
                        choices=["single", "chain", "diamond"], help="DAG graph shape")
    parser.add_argument("--sleep", type=int, default=1, help="Sleep seconds per task")
    parser.add_argument("--max-active-runs", type=int, default=3, help="max_active_runs per DAG (default: 3)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output directory (default: ../dags)")
    parser.add_argument("--clean", action="store_true", help="Remove old load test DAGs first")
    parser.add_argument("--list-levels", action="store_true", help="Show predefined levels")

    args = parser.parse_args()

    if args.list_levels:
        print("Predefined load levels:")
        for k, v in LEVELS.items():
            tasks_per_min = v["dags"] * v["tasks"]
            if "*/2" in v["schedule"]:
                tasks_per_min = tasks_per_min // 2
            elif "*/5" in v["schedule"]:
                tasks_per_min = tasks_per_min // 5
            print(f"  Level {k}: {v['desc']} (~{tasks_per_min} tasks/min)")
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    dags_dir = args.output or os.path.join(script_dir, "..", "dags")
    dags_dir = os.path.abspath(dags_dir)

    if args.clean:
        clean_load_test_dags(dags_dir)

    if args.level:
        cfg = LEVELS[args.level]
        print(f"Level {args.level}: {cfg['desc']}")
        generate_dags(dags_dir, cfg["dags"], cfg["tasks"], cfg["schedule"],
                      cfg["shape"], cfg["sleep"], max_active_runs=args.max_active_runs)
    elif args.dags:
        generate_dags(dags_dir, args.dags, args.tasks, args.schedule,
                      args.shape, args.sleep, max_active_runs=args.max_active_runs)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
