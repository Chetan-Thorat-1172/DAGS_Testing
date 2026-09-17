"""Live test: Cortex Agent tool use, and the request-level tool passthrough.

DEMO.PUBLIC.TEST_AGENT has a Cortex Analyst tool configured on the agent itself,
over the semantic view DEMO.PUBLIC.CUSTOMERS_SV. Two different things are being
checked here:

  * `agent_level_tool` asks a data question and lets the agent use the tool it
    already has — so the response should carry evidence of a tool call.
  * `request_level_tool` sends the SAME tool spec in the request, through the
    operator's `tools` and `tool_resources` parameters. Those are pure
    passthrough: the operator serialises them into the request body and never
    inspects them. Until now that was only unit-tested.

The assertions are deliberately loose: the block types Snowflake puts in a
response are its own to define, so this records what came back rather than
demanding a shape. What is asserted is that the call succeeded and produced
text.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SnowflakeCortexAgentOperator,
)

CONN = "snowflake_conn"
AGENT = {"database": "DEMO", "schema": "PUBLIC", "agent_name": "TEST_AGENT"}

# The agent's own tool spec, repeated at request level to prove the passthrough.
TOOLS = [
    {
        "tool_spec": {
            "type": "cortex_analyst_text_to_sql",
            "name": "CustomerAnalyst",
            "description": "Queries customer data including names and regions",
        }
    }
]

TOOL_RESOURCES = {
    "CustomerAnalyst": {
        "semantic_view": "DEMO.PUBLIC.CUSTOMERS_SV",
        "execution_environment": {"type": "warehouse", "warehouse": "COMPUTE_WH"},
    }
}


def ask(text):
    return [{"role": "user", "content": [{"type": "text", "text": text}]}]


def report(**context):
    """Record what each response contained, including which block types appeared."""
    ti = context["ti"]

    for task_id in ("agent_level_tool", "request_level_tool", "with_tool_choice"):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        print(f"\n===== {task_id} =====")
        assert raw, f"{task_id} returned nothing"
        payload = json.loads(raw) if isinstance(raw, str) else raw

        blocks = payload.get("content", [])
        types = [b.get("type") for b in blocks if isinstance(b, dict)]
        print(f"status      : {payload.get('status')}")
        print(f"block types : {types}")

        text = "".join(
            b.get("text", "") for b in blocks
            if isinstance(b, dict) and b.get("type") == "text"
        )
        print(f"text        : {text[:600]!r}")

        # Anything that looks like tool activity, so the evidence is on record.
        for b in blocks:
            if not isinstance(b, dict):
                continue
            if b.get("type") in ("text", "thinking"):
                continue
            print(f"  block {b.get('type')!r}: {json.dumps(b)[:500]}")

        assert payload.get("status") == "completed", f"{task_id}: {payload.get('status')}"
        assert text.strip(), f"{task_id} produced no text"

    print("\nALL ASSERTIONS PASSED")
    return "ok"


with DAG(
    dag_id="snowflake_cortex_agent_tools",
    description="Cortex Agent tool use and request-level tool passthrough",
    tags=["livetest", "snowflake", "cortex"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # The agent already has the Analyst tool, so a data question should make it
    # use it. Nothing tool-related is passed by the operator here.
    agent_level_tool = SnowflakeCortexAgentOperator(
        task_id="agent_level_tool",
        messages=ask("How many customers are there?"),
        timeout=300,
        **AGENT,
    )

    # The same tool spec supplied in the request, through the operator's
    # passthrough parameters. This is what closes the gap those two parameters
    # previously had.
    request_level_tool = SnowflakeCortexAgentOperator(
        task_id="request_level_tool",
        messages=ask("Show me the customer count by region."),
        tools=TOOLS,
        tool_resources=TOOL_RESOURCES,
        timeout=300,
        **AGENT,
    )

    # tool_choice is passed through in the same way.
    with_tool_choice = SnowflakeCortexAgentOperator(
        task_id="with_tool_choice",
        messages=ask("Which customers are in US-WEST?"),
        tools=TOOLS,
        tool_resources=TOOL_RESOURCES,
        tool_choice={"type": "auto"},
        timeout=300,
        **AGENT,
    )

    [agent_level_tool, request_level_tool, with_tool_choice] >> PythonOperator(
        task_id="report",
        python_callable=report,
        provide_context=True,
    )
