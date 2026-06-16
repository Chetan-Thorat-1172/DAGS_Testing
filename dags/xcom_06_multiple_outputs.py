"""xcom_06_multiple_outputs.py — Test #6: @task(multiple_outputs=True)

WHAT WE'RE TESTING:
  When a @task function returns a dict AND multiple_outputs=True, each KEY of the
  returned dict is pushed as a SEPARATE XCom entry (not just one 'return_value').
  Downstream tasks can then pull individual keys: result["count"], result["source"].

HOW IT WORKS INTERNALLY:
  1. @task(multiple_outputs=True) sets params["multiple_outputs"] = True
  2. At runtime, _run_task.py (line ~425) checks this flag after execution:
     if params.get("multiple_outputs") and isinstance(return_value, dict):
         for k, v in return_value.items():
             ti.xcom_push(key=k, value=v)
  3. Each dict key → separate xcom row (in addition to the full return_value)
  4. Downstream references like result["count"] translate to:
     XComArg(task_id="summarize", key="count") → resolves via xcom_pull

HOW TO VERIFY:
  - All tasks succeed
  - DB query: SELECT * FROM metadata.xcom WHERE task_id='summarize';
    Expected: 3 rows: key='return_value' (full dict), key='count', key='source'
  - Consumer logs: "Count: 42" and "Source: snowflake"
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import dag, task


@dag(
    dag_id="xcom_06_multiple_outputs",
    schedule=None,
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["xcom", "test"],
)
def pipeline():

    @task(multiple_outputs=True)
    def summarize():
        """Returns a dict; each key becomes its own XCom entry."""
        return {"count": 42, "source": "snowflake", "status": "complete"}

    @task
    def report(count, source):
        print(f"Count: {count}")
        print(f"Source: {source}")
        return f"Reported {count} from {source}"

    result = summarize()
    report(result["count"], result["source"])  # pulls individual keys


pipeline()
