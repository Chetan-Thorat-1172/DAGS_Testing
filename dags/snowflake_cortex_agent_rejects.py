"""Live test: what SnowflakeCortexAgentOperator refuses or reports clearly.

Every task fails by design — a red run is correct. What is being verified is
that each failure is reported usefully, and that a name which would change the
request's URL never reaches Snowflake at all.

`database`, `schema` and `agent_name` become path segments of the REST endpoint.
The reference provider interpolates them raw, so a name containing a slash walks
to a different endpoint; here each segment is escaped, which keeps a name
Snowflake would accept working while making one that changes the path
impossible.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, SnowflakeCortexAgentOperator
from dag_parser.dynamic.params import Param

CONN = "snowflake_conn"


def ask(text):
    return [{"role": "user", "content": [{"type": "text", "text": text}]}]


with DAG(
    dag_id="snowflake_cortex_agent_rejects",
    description="Cortex agent failures — every task fails by design",
    tags=["livetest", "snowflake", "cortex"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
    params={
        # A name that would redirect the call to another endpoint.
        "traversal_agent": Param("string", default="../../../../session/v1/login-request"),
        # A template that renders to a Python literal rather than JSON.
        "python_literal": Param("string", default="[{'role': 'user'}]"),
    },
) as dag:

    # A real agent name that does not exist: Snowflake's own error, surfaced.
    missing_agent = SnowflakeCortexAgentOperator(
        task_id="missing_agent",
        database="DEMO",
        schema="PUBLIC",
        agent_name="NO_SUCH_AGENT_XYZ",
        messages=ask("hello"),
        timeout=60,
    )

    # The escaping guard: this must not reach a different endpoint. It will
    # fail, but as a 404 for an oddly-named agent rather than by calling
    # something else.
    traversal_agent = SnowflakeCortexAgentOperator(
        task_id="traversal_agent",
        database="DEMO",
        schema="PUBLIC",
        agent_name="{{ .Params.traversal_agent }}",
        messages=ask("hello"),
        timeout=60,
    )

    # A whole-value template is allowed, but it has to render to JSON. A Python
    # literal is the likely mistake and is named as such.
    messages_not_json = SnowflakeCortexAgentOperator(
        task_id="messages_not_json",
        database="DEMO",
        schema="PUBLIC",
        agent_name="TEST_AGENT",
        messages="{{ .Params.python_literal }}",
        timeout=60,
    )

    # A nonexistent database: the call is well-formed, Snowflake refuses it.
    missing_database = SnowflakeCortexAgentOperator(
        task_id="missing_database",
        database="NO_SUCH_DB_XYZ",
        schema="PUBLIC",
        agent_name="TEST_AGENT",
        messages=ask("hello"),
        timeout=60,
    )
