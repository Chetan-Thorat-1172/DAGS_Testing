"""
S30 #3 live proof: cancelling a Python task tears down its WHOLE process group,
so a spawned grandchild does NOT orphan.

spawn_and_wait launches a detached grandchild that appends a counter line every
0.5s to a heartbeat file on the shared logs volume (visible via the
@DAGS_SHARED_VOLUME/logs stage), then the parent sleeps long so we can cancel it
mid-run via a WI-4 heartbeat-cancel (force-fail the running TI). Pre-fix, cancel
(context.Canceled) skipped killProcGroup -> the grandchild orphaned and the file
kept growing. Post-fix, the group is SIGTERM->grace->SIGKILL torn down and the
file STOPS growing.

Trigger: POST /api/dag-runs {"dag_id":"py3_orphan_demo"}
"""
import subprocess
import sys
import time
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def spawn_and_wait(**context):
    run_id = context.get("run_id", "norun")
    hb = "/app/logs/py3_orphan_{}.hb".format(run_id)
    child_code = (
        "import time,sys\n"
        "p=sys.argv[1]\n"
        "i=0\n"
        "while True:\n"
        "    f=open(p,'a'); f.write(str(i)+'\\n'); f.flush(); f.close()\n"
        "    i+=1; time.sleep(0.5)\n"
    )
    subprocess.Popen([sys.executable, "-c", child_code, hb])
    print("[py3] spawned grandchild writing {}".format(hb), file=sys.stderr)
    # Hold the worker slot long enough to be cancelled mid-run.
    time.sleep(600)
    return "done"


with DAG(
    dag_id="py3_orphan_demo",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="S30 #3: cancel -> process group torn down, grandchild stops (no orphan)",
    tags=["s30", "py3", "orphan"],
) as dag:
    spawn = PythonOperator(task_id="spawn", python_callable=spawn_and_wait)
