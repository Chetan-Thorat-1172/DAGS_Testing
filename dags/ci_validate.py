from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    DatabricksCopyIntoOperator,
    DatabricksSQLStatementsOperator,
)

# CI-VALIDATE: proves validate=True is a dry run (C6) — it must succeed without
# writing any rows. assert_empty fails the DAG if a row slipped through.
VOL = "/Volumes/maestro_pi/maestro_sql_test/copyinto_src/"

with DAG(
    dag_id="ci_validate",
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
            "CREATE OR REPLACE TABLE maestro_pi.maestro_sql_test.emp_validate AS "
            "SELECT * FROM read_files('" + VOL + "', format => 'csv', header => 'true', "
            "inferColumnTypes => 'false') WHERE 1=0"
        ),
        poll_interval=5,
    )

    dry_run = DatabricksCopyIntoOperator(
        task_id="copy_validate_only",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        table_name="emp_validate",
        file_location=VOL,
        file_format="CSV",
        format_options={"header": "true"},
        validate=True,
        poll_interval=5,
    )

    assert_empty = DatabricksSQLStatementsOperator(
        task_id="assert_no_rows_written",
        warehouse_id="3e52b555fe3ac722",
        statement=(
            "SELECT assert_true("
            "(SELECT count(*) FROM maestro_pi.maestro_sql_test.emp_validate) = 0, "
            "'validate=True should not write any rows')"
        ),
        poll_interval=5,
    )

    reset >> dry_run >> assert_empty
