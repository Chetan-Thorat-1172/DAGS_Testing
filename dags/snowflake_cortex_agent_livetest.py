"""Live end-to-end test for SnowflakeCortexAgentOperator.

Calls DEMO.PUBLIC.TEST_AGENT, a minimal Cortex Agent with no tools, and reports
the response so its shape is recorded rather than assumed.

The connection authenticates with a programmatic access token. The reference
provider's hook is OAuth-only; this implementation accepts both, so a green run
here is also the first evidence that a PAT is accepted by the Cortex REST
endpoint.

A green run is the assertion. Deliberate failures live in
snowflake_cortex_agent_rejects.py.
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


def ask(text):
    return [{"role": "user", "content": [{"type": "text", "text": text}]}]


def report(**context):
    """Print each response whole, so the payload shape is on the record."""
    ti = context["ti"]

    for task_id in ("basic", "with_instructions", "with_models", "with_timeout"):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        print(f"===== {task_id} =====")
        print(f"raw type: {type(raw).__name__}, {len(raw) if raw else 0} chars")
        print(raw)

        assert raw, f"{task_id} returned nothing"
        payload = json.loads(raw) if isinstance(raw, str) else raw

        # The operator returns the response verbatim, so this only asserts it
        # is the JSON Snowflake sent — not any particular field, which is
        # Snowflake's to define.
        assert isinstance(payload, (dict, list)), f"{task_id}: {type(payload)}"

        # Record the keys and any thread id, which is what a follow-up turn
        # would need.
        if isinstance(payload, dict):
            print(f"keys: {sorted(payload.keys())}")
            for key in ("thread_id", "parent_message_id", "id"):
                if key in payload:
                    print(f"  {key} = {payload[key]!r}")
            # The reference ships a helper that joins the text blocks; the same
            # shape is read here to confirm the response carries one.
            content = payload.get("content")
            if isinstance(content, list):
                text = "".join(
                    b.get("text", "") for b in content
                    if isinstance(b, dict) and b.get("type") == "text"
                )
                print(f"  text: {text[:400]!r}")

    print("ALL ASSERTIONS PASSED")
    return "ok"


with DAG(
    dag_id="snowflake_cortex_agent_livetest",
    description="Calling a Cortex Agent and recording its response",
    tags=["livetest", "snowflake", "cortex"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # The plain case: one question, one answer.
    basic = SnowflakeCortexAgentOperator(
        task_id="basic",
        messages=ask("Reply with exactly the word: pong"),
        **AGENT,
    )

    # instructions is passed through untouched — it needs no external resource,
    # so it proves the passthrough plumbing without a tool to point at.
    with_instructions = SnowflakeCortexAgentOperator(
        task_id="with_instructions",
        messages=ask("What is 2 + 2?"),
        instructions={"response": "Answer with digits only, no words."},
        **AGENT,
    )

    # models likewise.
    with_models = SnowflakeCortexAgentOperator(
        task_id="with_models",
        messages=ask("Name one colour."),
        models={"orchestration": "auto"},
        **AGENT,
    )

    # An explicit timeout, well short of the 600s default.
    with_timeout = SnowflakeCortexAgentOperator(
        task_id="with_timeout",
        messages=ask("Say hello."),
        timeout=120,
        **AGENT,
    )

    [basic, with_instructions, with_models, with_timeout] >> PythonOperator(
        task_id="report",
        python_callable=report,
        provide_context=True,
    )
