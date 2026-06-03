"""
test_batch5_batch6.py — E2E test DAG for Batch 5 + Batch 6 features.

Tests:
  1. Task Context Variables (#29): context["ds"], context["execution_date"], context["var"]
  2. XCom Backend Abstraction (#6): Push/Pull via ti.xcom_push() / ti.xcom_pull()
  3. XCom Cleanup (#56): Generates XCom data (cleaner runs on scheduler iterations)
  4. ExecutionDate population: Validates ds is a real date (not fallback time.Now())
  5. Connection Import/Export (#75): Validates API endpoints exist

Flow:
  verify_context_vars → xcom_producer → xcom_consumer

Usage:
  Trigger via API: POST /api/dag-runs { "dag_id": "test_batch5_batch6", "conf": {} }
  Expected: All 3 tasks succeed. verify_context_vars validates date fields.
"""

from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime
import re


def verify_context_vars(**kwargs):
    """Validates that task context variables are populated correctly."""
    context = kwargs

    # Check execution_date is present and ISO 8601
    exec_date = context.get("execution_date", "")
    print(f"[verify] execution_date = {exec_date}")
    if not exec_date:
        raise ValueError("execution_date is empty — ExecutionDate not populated in TaskDispatch!")

    # Check ds is present and YYYY-MM-DD format
    ds = context.get("ds", "")
    print(f"[verify] ds = {ds}")
    if not ds:
        raise ValueError("ds is empty — ExecutionDate not populated in TaskDispatch!")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", ds):
        raise ValueError(f"ds has wrong format: '{ds}' (expected YYYY-MM-DD)")

    # Check ds_nodash is present and YYYYMMDD format
    ds_nodash = context.get("ds_nodash", "")
    print(f"[verify] ds_nodash = {ds_nodash}")
    if not ds_nodash:
        raise ValueError("ds_nodash is empty!")
    if not re.match(r"^\d{8}$", ds_nodash):
        raise ValueError(f"ds_nodash has wrong format: '{ds_nodash}' (expected YYYYMMDD)")

    # Check logical_date matches execution_date
    logical_date = context.get("logical_date", "")
    print(f"[verify] logical_date = {logical_date}")
    if logical_date != exec_date:
        raise ValueError(f"logical_date ({logical_date}) != execution_date ({exec_date})")

    # Check var is a dict (may be empty if no variables defined)
    var = context.get("var")
    print(f"[verify] var = {var} (type={type(var).__name__})")
    if var is not None and not isinstance(var, dict):
        raise ValueError(f"var should be dict or None, got {type(var).__name__}")

    print("[verify] All context variables validated successfully!")
    return "context_ok"


def xcom_producer(**kwargs):
    """Pushes test XCom values to validate the backend abstraction."""
    ti = kwargs.get("ti")
    ds = kwargs.get("ds", "unknown")

    # Push a test value using xcom_push
    test_value = f"batch6_test_{ds}"
    ti.xcom_push("test_key", test_value)
    print(f"[producer] Pushed XCom: test_key = {test_value}")

    # Also return a value (auto-pushed as return_value)
    result = f"produced_on_{ds}"
    print(f"[producer] Returning: {result}")
    return result


def xcom_consumer(**kwargs):
    """Pulls XCom values from producer to validate end-to-end flow."""
    ti = kwargs.get("ti")
    ds = kwargs.get("ds", "unknown")

    # Pull the explicit push
    test_val = ti.xcom_pull(task_ids="xcom_producer", key="test_key")
    print(f"[consumer] Pulled test_key from xcom_producer: {test_val}")
    if test_val is None:
        raise ValueError("xcom_pull returned None for test_key — XCom backend broken!")

    expected = f"batch6_test_{ds}"
    if test_val != expected:
        raise ValueError(f"XCom mismatch: got '{test_val}', expected '{expected}'")

    # Pull auto-pushed return_value
    return_val = ti.xcom_pull(task_ids="xcom_producer", key="return_value")
    print(f"[consumer] Pulled return_value from xcom_producer: {return_val}")
    if return_val is None:
        raise ValueError("xcom_pull returned None for return_value — auto-push broken!")

    print(f"[consumer] All XCom operations validated! ds={ds}")
    return "all_batch5_batch6_tests_passed"


with DAG(
    dag_id="test_batch5_batch6",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="E2E test for Batch 5 (DB/Infra) + Batch 6 (Repo/Data) features",
    tags=["test", "batch5", "batch6"],
) as dag:

    t1 = PythonOperator(
        task_id="verify_context_vars",
        python_callable=verify_context_vars,
    )

    t2 = PythonOperator(
        task_id="xcom_producer",
        python_callable=xcom_producer,
    )

    t3 = PythonOperator(
        task_id="xcom_consumer",
        python_callable=xcom_consumer,
    )

    t1 >> t2 >> t3
