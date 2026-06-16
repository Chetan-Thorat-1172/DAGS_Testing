"""alert_slack_test.py — Test SlackAPIPostOperator (task-based Slack alert).

HOW THIS DAG WORKS:
  - 'do_work' intentionally fails (exit 1)
  - 'slack_on_failure' fires ONLY when do_work fails (trigger_rule="one_failed")
    → posts a failure alert to your Slack channel
  - 'slack_on_success' fires ONLY when everything succeeds (trigger_rule="all_success")
    → posts a success notification (skipped in failure path)

To test success path: change 'do_work' to "echo ok" and re-trigger.

PREREQUISITE:
  1. Create a Slack Incoming Webhook (see step-by-step instructions below)
  2. Create a Connection in PI_FLOW UI:
     - Connection ID: "slack_alerts"
     - Type: "slack"
     - Auth Type: "webhook"
     - Webhook URL: https://hooks.slack.com/services/T.../B.../xxx
  3. SPCS External Access Integration must allow hooks.slack.com:443
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, SlackAPIPostOperator

with DAG(
    dag_id="alert_slack_test",
    schedule=None,
    start_date=datetime(2026, 6, 15),
    catchup=False,
    tags=["alerting", "test", "slack"],
) as dag:

    do_work = BashOperator(
        task_id="do_work",
        bash_command="echo starting work && exit 1",
    )

    slack_on_failure = SlackAPIPostOperator(
        task_id="slack_on_failure",
        connection_id="slack_alerts",
        text=":rotating_light: *PI_FLOW Alert — Task Failed*\n\n*DAG:* {{ .DagID }}\n*Run:* {{ .RunID }}\n*Date:* {{ .DS }}\n\nOne or more tasks failed. Check PI_FLOW UI for details.",
        trigger_rule="one_failed",
    )

    slack_on_success = SlackAPIPostOperator(
        task_id="slack_on_success",
        connection_id="slack_alerts",
        text=":white_check_mark: *PI_FLOW — Run Succeeded*\n\n*DAG:* {{ .DagID }}\n*Run:* {{ .RunID }}\n*Date:* {{ .DS }}\n\nAll tasks completed successfully.",
        trigger_rule="all_success",
    )

    do_work >> [slack_on_failure, slack_on_success]
