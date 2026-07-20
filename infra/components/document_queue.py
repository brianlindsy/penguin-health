"""
Document Queue construct: the daily auto-close Lambda and its schedule.

The queue table itself lives on the Database construct (shared with the
rules engine and admin API for grants). This construct owns only the
auto-close job — a small Lambda that runs once daily, queries GSI2 for
open queue entries idle longer than the org's configured window (default
90 days), and flips them to `auto-closed`.
"""

import os

from aws_cdk import (
    Duration,
    BundlingOptions,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

import config
from components.bundler import DirectoryBundler


class DocumentQueue(Construct):

    def __init__(self, scope: Construct, id: str, *,
                 org_config_table: dynamodb.ITable,
                 document_queue_table: dynamodb.ITable) -> None:
        super().__init__(scope, id)

        lambda_dir = os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "multi-org")
        autoclose_dir = os.path.join(lambda_dir, "queue-autoclose")
        audit_pkg_dir = os.path.join(lambda_dir, "audit")

        self.autoclose_fn = _lambda.Function(self, "QueueAutocloseFn",
            function_name=f"{config.PROJECT_NAME}-queue-autoclose",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                autoclose_dir,
                exclude=["*", "!handler.py"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=DirectoryBundler([
                        (os.path.join(autoclose_dir, "handler.py"), None),
                        (audit_pkg_dir, "audit"),
                    ]),
                ),
            ),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "DOCUMENT_QUEUE_TABLE": document_queue_table.table_name,
                "ORG_CONFIG_TABLE_NAME": org_config_table.table_name,
                "DEFAULT_AUTOCLOSE_DAYS": "90",
            },
        )

        org_config_table.grant_read_data(self.autoclose_fn)
        # Only UpdateItem on the base table is needed for closing entries;
        # the scan itself runs against GSI2.
        document_queue_table.grant(
            self.autoclose_fn,
            "dynamodb:Query",
            "dynamodb:UpdateItem",
        )
        # GSI2 read for the sparse "open + idle" scan.
        document_queue_table.grant(
            self.autoclose_fn,
            "dynamodb:Query",
        )
        # Grant Query on the index resource explicitly — CDK's `.grant()`
        # on the table doesn't extend to /index/* by default.
        from aws_cdk import aws_iam as iam
        self.autoclose_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query"],
            resources=[f"{document_queue_table.table_arn}/index/*"],
        ))

        # ----- Daily schedule -----
        # 08:00 UTC — comfortably before the 10:00-12:00 UTC validation runs,
        # so freshly-closed entries never race with an ingest batch.
        events.Rule(self, "QueueAutocloseSchedule",
            rule_name=f"{config.PROJECT_NAME}-queue-autoclose-daily",
            schedule=events.Schedule.cron(hour="8", minute="0"),
            targets=[targets.LambdaFunction(self.autoclose_fn)],
        )
