"""The reference documentation's own example, run against a real agent.

The operator call below is the documented example character for character, with
only the three identifiers pointed at the agent that exists in this account
(the example's DEFAULT_DATABASE.DEFAULT_SCHEMA.default_agent does not). The DAG
arguments are the example's too, including schedule="@once" and catchup=False.

The point is that porting the documented example needs no change to the call
itself — only the import line differs, which is true of every operator here.

`show_xcom` then records exactly what a downstream task receives, so the stored
shape is on the record rather than described.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SnowflakeCortexAgentOperator,
)

SNOWFLAKE_CONN_ID = "snowflake_conn"


def show_xcom(**context):
    """Report the type and shape of what xcom_pull returns."""
    ti = context["ti"]
    raw = ti.xcom_pull(task_ids="run_agent", key="return_value")

    print(f"xcom_pull returned: {type(raw).__name__}")
    print(f"length            : {len(raw) if raw is not None else 0}")
    print(f"value             : {raw}")

    assert raw, "run_agent pushed nothing"
    assert isinstance(raw, str), f"expected a JSON string, got {type(raw).__name__}"

    # A consumer decodes it once and then works with the object.
    payload = json.loads(raw)
    print(f"after json.loads  : {type(payload).__name__}, keys={sorted(payload.keys())}")

    text = "".join(
        b.get("text", "") for b in payload.get("content", [])
        if isinstance(b, dict) and b.get("type") == "text"
    )
    print(f"agent said        : {text!r}")

    assert text.strip(), "the agent returned no text block"
    print("ALL ASSERTIONS PASSED")
    return "ok"


with DAG(
    "snowflake_cortex_agent_docs_example",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["livetest", "snowflake", "cortex"],
    catchup=False,
) as dag:
    # --- the documented example; only the identifiers point at a real agent ---
    run_agent = SnowflakeCortexAgentOperator(
        task_id="run_agent",
        database="DEMO",
        schema="PUBLIC",
        agent_name="TEST_AGENT",
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "What can you help me with?",
                    }
                ],
            }
        ],
    )
    # -------------------------------------------------------------------------

    run_agent >> PythonOperator(
        task_id="show_xcom",
        python_callable=show_xcom,
        provide_context=True,
    )
