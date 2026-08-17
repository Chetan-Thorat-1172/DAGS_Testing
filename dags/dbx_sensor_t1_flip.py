from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    DatabricksSQLStatementsOperator,
    DatabricksSqlSensor,
)

# T1: the condition flips from 0 rows -> >=1 row on its own.
#
# `arm` creates a table holding ready_at = now + 30s. The sensor polls a query
# that returns no rows until ready_at has passed, then exactly one row. Proves:
#   - 0 rows  => "condition not met" (the sensor keeps waiting)
#   - >=1 row => success (the flip)
#   - manifest.total_row_count read, warehouse_id resolution, reschedule mode
# PASS = both tasks succeed.
with DAG(
    dag_id="dbx_sensor_t1_flip",
    schedule=None,
    start_date=datetime(2026, 8, 14),
    catchup=False,
    default_args={"connection_id": "databricks_azure", "retries": 0},
    tags=["databricks", "sensor"],
) as dag:
    arm = DatabricksSQLStatementsOperator(
        task_id="arm",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        statement=(
            "CREATE OR REPLACE TABLE sensor_flip AS "
            "SELECT timestampadd(second, 30, current_timestamp()) AS ready_at"
        ),
        poll_interval=5,
    )

    wait_for_ready = DatabricksSqlSensor(
        task_id="wait_for_ready",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        sql="SELECT 1 FROM sensor_flip WHERE ready_at <= current_timestamp()",
        mode="reschedule",
        poke_interval=10,
        timeout=180,
    )

    arm >> wait_for_ready
