from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSqlSensor

# T2: fail-fast. EXPECTED TO GO RED — that is the pass condition.
#
# Both tasks set a long timeout (180s) but must FAIL within a poke or two,
# proving a terminal SQL error is NOT retried until the sensor timeout:
#   bad_syntax    -> rejected at submit (not retryable) -> terminal fail fast
#   missing_table -> FAILED terminal state after one poll -> terminal fail fast
# The runner additionally checks each task's duration is far below the timeout.
with DAG(
    dag_id="dbx_sensor_t2_failfast",
    schedule=None,
    start_date=datetime(2026, 8, 14),
    catchup=False,
    default_args={"connection_id": "databricks_azure", "retries": 0},
    tags=["databricks", "sensor"],
) as dag:
    DatabricksSqlSensor(
        task_id="bad_syntax",
        warehouse_id="3e52b555fe3ac722",
        sql="SELCT 1",
        poke_interval=10,
        timeout=180,
    )

    DatabricksSqlSensor(
        task_id="missing_table",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        sql="SELECT * FROM a_table_that_does_not_exist",
        poke_interval=10,
        timeout=180,
    )
