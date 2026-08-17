from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSqlSensor

# T3: a never-true condition. Proves 0 rows is "condition not met" (the sensor
# waits, it does NOT error), and the timeout mapping:
#   soft_fail_skip (soft_fail=True)  -> skipped on timeout
#   hard_fail      (soft_fail=False) -> failed on timeout
# `SELECT 1 WHERE 1 = 0` succeeds with a present total_row_count of 0, so it is a
# valid empty result the sensor keeps waiting on (not the no-result-set error).
# EXPECTED: soft_fail_skip -> skipped, hard_fail -> failed.
with DAG(
    dag_id="dbx_sensor_t3_timeout",
    schedule=None,
    start_date=datetime(2026, 8, 14),
    catchup=False,
    default_args={"connection_id": "databricks_azure", "retries": 0},
    tags=["databricks", "sensor"],
) as dag:
    DatabricksSqlSensor(
        task_id="soft_fail_skip",
        warehouse_id="3e52b555fe3ac722",
        sql="SELECT 1 WHERE 1 = 0",
        poke_interval=10,
        timeout=25,
        soft_fail=True,
    )

    DatabricksSqlSensor(
        task_id="hard_fail",
        warehouse_id="3e52b555fe3ac722",
        sql="SELECT 1 WHERE 1 = 0",
        poke_interval=10,
        timeout=25,
        soft_fail=False,
    )
