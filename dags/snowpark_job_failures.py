"""Live test: how a failing Snowpark container job behaves.

Every task fails by design — a red run is correct. What is being verified:

  * a container that exits non-zero fails the task, its logs are still
    collected, and the job is LEFT IN PLACE so it can be inspected;
  * a job that outruns its timeout fails the task rather than holding the
    worker, and drop_on_completion decides whether it is cleaned up;
  * a value that would change the statement never reaches Snowflake.

Uses trigger_rule="all_done" throughout so one failure does not mask the rest.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, SnowparkContainerJobOperator
from dag_parser.dynamic.params import Param

CONN = "snowflake_conn"
SESSION = {"database": "DEMO", "schema": "PUBLIC"}
IMAGE = "/demo/public/spcstest_repo/alpine:3.20"

FAILING_SPEC = f"""spec:
  containers:
    - name: main
      image: {IMAGE}
      command:
        - sh
        - -c
        - echo about to fail; echo some diagnostic output; exit 1
"""

SLOW_SPEC = f"""spec:
  containers:
    - name: main
      image: {IMAGE}
      command:
        - sh
        - -c
        - echo sleeping; sleep 600; echo never reached
"""

with DAG(
    dag_id="snowpark_job_failures",
    description="Failing Snowpark container jobs — every task fails by design",
    tags=["livetest", "snowflake", "spcs"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
    params={
        "bad_pool": Param("string", default="SPCSTEST_POOL; DROP SERVICE X"),
        "bad_stage": Param("string", default="@SPCSTEST_STAGE SPEC = 'other.yaml'"),
    },
) as dag:

    # The container exits 1: the task must fail, the logs must still appear, and
    # the job must be kept for inspection.
    container_fails = SnowparkContainerJobOperator(
        task_id="container_fails",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec_text=FAILING_SPEC,
        poll_interval=10,
        timeout=600,
        **SESSION,
    )

    # The container sleeps for ten minutes; the task waits sixty seconds. It
    # must give up, and — drop_on_completion being on by default — clean up.
    times_out = SnowparkContainerJobOperator(
        task_id="times_out",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec_text=SLOW_SPEC,
        poll_interval=10,
        timeout=60,
        trigger_rule="all_done",
        **SESSION,
    )

    # The same, but asked not to clean up: the job should survive the timeout.
    times_out_kept = SnowparkContainerJobOperator(
        task_id="times_out_kept",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec_text=SLOW_SPEC,
        drop_on_completion=False,
        poll_interval=10,
        timeout=60,
        trigger_rule="all_done",
        **SESSION,
    )

    # A compute pool name carrying a second statement, templated in as a
    # trigger-time value would be. Must be refused before reaching Snowflake.
    reject_pool = SnowparkContainerJobOperator(
        task_id="reject_pool",
        compute_pool="{{ .Params.bad_pool }}",
        container_name="main",
        spec_text=FAILING_SPEC,
        trigger_rule="all_done",
        **SESSION,
    )

    # A stage reference with an appended clause.
    reject_stage = SnowparkContainerJobOperator(
        task_id="reject_stage",
        compute_pool="SPCSTEST_POOL",
        container_name="main",
        spec="job_spec.yaml",
        spec_stage="{{ .Params.bad_stage }}",
        trigger_rule="all_done",
        **SESSION,
    )

    container_fails >> times_out >> times_out_kept >> reject_pool >> reject_stage
