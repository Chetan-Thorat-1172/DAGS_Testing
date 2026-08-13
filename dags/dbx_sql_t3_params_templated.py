from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, DatabricksSQLStatementsOperator

# T3: parameter markers and templating — the two different ways a value gets into
# a statement, exercised side by side.
#
# parameterised uses :markers, bound by Databricks. The value itself is templated,
# so {{ .DS }} renders before the payload is built and arrives as a bound
# parameter rather than as interpolated SQL.
#
# templated puts {{ .DS }} directly in the statement text. This works because
# Maestro renders every parameter, including `statement` — but binding is the
# safer habit and the guide says so.
with DAG(
    dag_id="databricks_sql_t3_params_templated",
    schedule=None,
    start_date=datetime(2026, 8, 13),
    catchup=False,
    default_args={
        "connection_id": "databricks_azure",
        "retries": 0,
    },
    tags=["databricks", "sql"],
) as dag:
    parameterised = DatabricksSQLStatementsOperator(
        task_id="parameterised_insert",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        statement="INSERT INTO events VALUES (:id, :label, :ds)",
        parameters=[
            {"name": "id", "value": "100", "type": "BIGINT"},
            {"name": "label", "value": "from_parameters"},
            {"name": "ds", "value": "{{ .DS }}", "type": "DATE"},
        ],
        poll_interval=5,
    )

    templated = DatabricksSQLStatementsOperator(
        task_id="templated_statement",
        warehouse_id="3e52b555fe3ac722",
        catalog="maestro_pi",
        schema="maestro_sql_test",
        statement="DELETE FROM events WHERE label = 'from_parameters' AND ds = '{{ .DS }}'",
        poll_interval=5,
    )

    parameterised >> templated
