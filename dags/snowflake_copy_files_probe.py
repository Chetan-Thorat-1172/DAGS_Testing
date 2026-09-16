"""Probe: does Snowflake accept FILES and PATTERN together?

The reference provider's own documented example passes both, and its code emits
both clauses. This finds out what Snowflake actually does with that, so the
authoring guide can say something true rather than something assumed.

Expected to end RED only if Snowflake rejects the combination — which is the
answer either way.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    CopyFromExternalStageToSnowflakeOperator,
    SQLExecuteQueryOperator,
)

CONN = "snowflake_conn"
SESSION = {"database": "DEMO", "schema": "PUBLIC"}

with DAG(
    dag_id="snowflake_copy_files_probe",
    description="Does COPY INTO accept FILES and PATTERN together?",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    clear = SQLExecuteQueryOperator(
        task_id="clear",
        sql="TRUNCATE TABLE DEMO.PUBLIC.COPYTEST_TARGET",
        do_xcom_push=False,
    )

    # Exactly the shape of the reference's documented example: an explicit file
    # AND a pattern. part1.csv is 5 rows; the pattern would also match part2.csv.
    both = CopyFromExternalStageToSnowflakeOperator(
        task_id="files_and_pattern",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        prefix="good/",
        files=["part1.csv"],
        pattern=".*part[12][.]csv",
        file_format="COPYTEST_CSV",
        copy_options="FORCE=TRUE",
        **SESSION,
    )

    # Whatever loaded, report it: 5 means FILES won, 10 means PATTERN won.
    count = SQLExecuteQueryOperator(
        task_id="count_loaded",
        sql="SELECT COUNT(*) AS N FROM DEMO.PUBLIC.COPYTEST_TARGET",
        show_return_value_in_logs=True,
        trigger_rule="all_done",
    )

    clear >> both >> count
