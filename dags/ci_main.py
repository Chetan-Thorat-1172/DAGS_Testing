from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    DatabricksCopyIntoOperator,
    DatabricksSQLStatementsOperator,
)

# CI-MAIN: proves CSV load (C1), idempotent rerun / no double-load (C4), and
# force_copy reload (C5) — all self-verifying via assert_true, so a broken result
# fails the task rather than needing a human to read query history.
#
# Sequence:
#   reset        empty table with the file's inferred schema
#   copy1        first COPY INTO           -> N rows
#   snapshot     record N in emp_rowcount
#   copy2        COPY INTO again, no force -> Delta log skips the file, still N
#   assert_dedup count still == N          (fails loudly if it double-loaded)
#   copy3_force  COPY INTO force_copy=True -> reloads the file, now 2N
#   assert_force count == 2N               (fails loudly if force did nothing)
VOL = "/Volumes/maestro_pi/maestro_sql_test/copyinto_src/"

with DAG(
    dag_id="ci_main",
    schedule=None,
    start_date=datetime(2026, 8, 13),
    catchup=False,
    default_args={"connection_id": "databricks_azure", "retries": 0},
    tags=["databricks", "copyinto"],
) as dag:
    reset = DatabricksSQLStatementsOperator(
        task_id="reset_table",
        warehouse_id="3e52b555fe3ac722",
        statement=(
            "CREATE OR REPLACE TABLE maestro_pi.maestro_sql_test.emp_copyinto AS "
            "SELECT * FROM read_files('" + VOL + "', format => 'csv', header => 'true') "
            "WHERE 1=0"
        ),
        poll_interval=5,
    )

    copy1 = DatabricksCopyIntoOperator(
        task_id="copy_first",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        table_name="emp_copyinto",
        file_location=VOL,
        file_format="CSV",
        format_options={"header": "true"},
        poll_interval=5,
    )

    snapshot = DatabricksSQLStatementsOperator(
        task_id="snapshot_count",
        warehouse_id="3e52b555fe3ac722",
        statement=(
            "CREATE OR REPLACE TABLE maestro_pi.maestro_sql_test.emp_rowcount AS "
            "SELECT count(*) AS n FROM maestro_pi.maestro_sql_test.emp_copyinto"
        ),
        poll_interval=5,
    )

    copy2 = DatabricksCopyIntoOperator(
        task_id="copy_again_no_force",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        table_name="emp_copyinto",
        file_location=VOL,
        file_format="CSV",
        format_options={"header": "true"},
        poll_interval=5,
    )

    assert_dedup = DatabricksSQLStatementsOperator(
        task_id="assert_dedup",
        warehouse_id="3e52b555fe3ac722",
        statement=(
            "SELECT assert_true("
            "(SELECT count(*) FROM maestro_pi.maestro_sql_test.emp_copyinto) = "
            "(SELECT n FROM maestro_pi.maestro_sql_test.emp_rowcount), "
            "'dedup failed: rerun without force changed the row count')"
        ),
        poll_interval=5,
    )

    copy3_force = DatabricksCopyIntoOperator(
        task_id="copy_force_reload",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        table_name="emp_copyinto",
        file_location=VOL,
        file_format="CSV",
        format_options={"header": "true"},
        force_copy=True,
        poll_interval=5,
    )

    assert_force = DatabricksSQLStatementsOperator(
        task_id="assert_force_reloaded",
        warehouse_id="3e52b555fe3ac722",
        statement=(
            "SELECT assert_true("
            "(SELECT count(*) FROM maestro_pi.maestro_sql_test.emp_copyinto) = "
            "2 * (SELECT n FROM maestro_pi.maestro_sql_test.emp_rowcount), "
            "'force_copy failed: reload did not add the rows again')"
        ),
        poll_interval=5,
    )

    reset >> copy1 >> snapshot >> copy2 >> assert_dedup >> copy3_force >> assert_force
