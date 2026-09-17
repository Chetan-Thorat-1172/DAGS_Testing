"""Live test: COPY INTO from a real EXTERNAL (S3) stage.

The earlier COPY INTO tests used an internal stage, which exercises the operator
but not a stage backed by cloud storage and cloud credentials. This one reads
from S3 through DEMO.PUBLIC.COPYTEST_S3_STAGE:

    URL  s3://maestro-pi-s3-test/copytest/
    file good/part1.csv   (ID 1..5, Alice..Eve)

The point of the test is what does NOT change. Only `stage` differs from the
internal-stage version — the bucket, the region and the credentials all live in
the stage object, so the operator never sees them and the generated statement is
the same shape either way.

A green run is the assertion.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    CopyFromExternalStageToSnowflakeOperator,
    SnowflakeValueCheckOperator,
    SQLExecuteQueryOperator,
)

CONN = "snowflake_conn"
SESSION = {"database": "DEMO", "schema": "PUBLIC"}
TARGET = "DEMO.PUBLIC.COPYTEST_TARGET"

with DAG(
    dag_id="snowflake_copy_into_s3",
    description="COPY INTO from an external S3 stage",
    tags=["livetest", "snowflake", "s3"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # Proves Snowflake can reach the bucket at all, before anything is loaded.
    list_stage = SQLExecuteQueryOperator(
        task_id="list_s3_stage",
        sql="LIST @DEMO.PUBLIC.COPYTEST_S3_STAGE",
        show_return_value_in_logs=True,
    )

    clear = SQLExecuteQueryOperator(
        task_id="clear",
        sql=f"TRUNCATE TABLE {TARGET}",
        do_xcom_push=False,
    )

    # Identical to the internal-stage task but for the stage name. No bucket, no
    # region, no keys — the stage carries all three.
    load_from_s3 = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_from_s3",
        table="COPYTEST_TARGET",
        stage="COPYTEST_S3_STAGE",
        prefix="good/",
        file_format="COPYTEST_CSV",
        copy_options="FORCE=TRUE",
        **SESSION,
    )

    check_loaded = SnowflakeValueCheckOperator(
        task_id="check_loaded",
        sql=f"SELECT COUNT(*) FROM {TARGET}",
        pass_value=5,
        retry_on_failure=False,
    )

    # The data is right, not merely present.
    check_first_row = SnowflakeValueCheckOperator(
        task_id="check_first_row",
        sql=f"SELECT NAME FROM {TARGET} WHERE ID = 1",
        pass_value="Alice",
        retry_on_failure=False,
    )

    # An explicit file list resolves against the S3 stage the same way.
    clear_2 = SQLExecuteQueryOperator(
        task_id="clear_2",
        sql=f"TRUNCATE TABLE {TARGET}",
        do_xcom_push=False,
    )

    load_named_file = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_named_file",
        table="COPYTEST_TARGET",
        stage="COPYTEST_S3_STAGE",
        files=["good/part1.csv"],
        file_format="COPYTEST_CSV",
        copy_options="FORCE=TRUE",
        **SESSION,
    )

    check_named = SnowflakeValueCheckOperator(
        task_id="check_named",
        sql=f"SELECT COUNT(*) FROM {TARGET}",
        pass_value=5,
        retry_on_failure=False,
    )

    (
        list_stage
        >> clear >> load_from_s3 >> check_loaded >> check_first_row
        >> clear_2 >> load_named_file >> check_named
    )
