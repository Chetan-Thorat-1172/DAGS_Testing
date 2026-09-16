"""Live test: what CopyFromExternalStageToSnowflakeOperator refuses to run.

Every task fails by design — a red run is correct.

COPY INTO names a table, a stage, a prefix and columns. None of those can be a
bind parameter, so they are placed into the statement directly and each one is
checked first. These tasks pass values that would change what the statement
does, and each must be rejected BEFORE anything reaches Snowflake.

The values are templated from DAG params, which is the case that matters: a DAG
author controls what they write, but not what someone types into the trigger
form. The check therefore runs at execution against the rendered value, not at
parse time against `{{ .Params.x }}`.

Verify afterwards that every task failed, and that the log shows a validation
message rather than a Snowflake error:

    SELECT task_id, state, try_number
    FROM metadata.task_instance
    WHERE dag_id = 'snowflake_copy_into_rejects';
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, CopyFromExternalStageToSnowflakeOperator
from dag_parser.dynamic.params import Param

CONN = "snowflake_conn"
SESSION = {"database": "DEMO", "schema": "PUBLIC"}

with DAG(
    dag_id="snowflake_copy_into_rejects",
    description="Values COPY INTO must refuse — every task fails by design",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
    params={
        # Defaults are the hostile values. Each is a shape that would otherwise
        # change the statement rather than the data it loads.
        "bad_table": Param("string", default="COPYTEST_TARGET; DROP TABLE COPYTEST_TARGET"),
        "bad_prefix": Param("string", default="good/ FILES=('other/part3.csv')"),
        "bad_column": Param("string", default="ID) FROM @COPYTEST_STAGE; --"),
        "bad_format": Param("string", default="COPYTEST_CSV; DROP TABLE COPYTEST_TARGET"),
        "bad_options": Param("string", default="FORCE=TRUE; DROP TABLE COPYTEST_TARGET"),
    },
) as dag:

    # A table name carrying a second statement.
    reject_table = CopyFromExternalStageToSnowflakeOperator(
        task_id="reject_table",
        table="{{ .Params.bad_table }}",
        stage="COPYTEST_STAGE",
        file_format="COPYTEST_CSV",
        **SESSION,
    )

    # A prefix is a path in the middle of the statement, so a space alone is
    # enough to append a clause — no semicolon needed.
    reject_prefix = CopyFromExternalStageToSnowflakeOperator(
        task_id="reject_prefix",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        prefix="{{ .Params.bad_prefix }}",
        file_format="COPYTEST_CSV",
        **SESSION,
    )

    # A column name that closes the column list and opens something else.
    reject_column = CopyFromExternalStageToSnowflakeOperator(
        task_id="reject_column",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        columns_array=["{{ .Params.bad_column }}"],
        file_format="COPYTEST_CSV",
        **SESSION,
    )

    # A file format is SQL by design, so the check is narrow — but a bare
    # semicolon still ends the statement.
    reject_file_format = CopyFromExternalStageToSnowflakeOperator(
        task_id="reject_file_format",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        file_format="{{ .Params.bad_format }}",
        **SESSION,
    )

    reject_copy_options = CopyFromExternalStageToSnowflakeOperator(
        task_id="reject_copy_options",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE",
        file_format="COPYTEST_CSV",
        copy_options="{{ .Params.bad_options }}",
        **SESSION,
    )

    # A stage name that would read from somewhere else entirely.
    reject_stage = CopyFromExternalStageToSnowflakeOperator(
        task_id="reject_stage",
        table="COPYTEST_TARGET",
        stage="COPYTEST_STAGE x",
        file_format="COPYTEST_CSV",
        **SESSION,
    )
