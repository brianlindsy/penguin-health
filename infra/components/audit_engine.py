"""
Audit Engine construct: Lambda functions for the multi-org document
processing pipeline (Textract, result handling, rules validation).
"""

import os
from aws_cdk import (
    Duration,
    BundlingOptions,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

import config
from components.bundler import CopyFileBundler, MultiFileBundler, DirectoryBundler


class AuditEngine(Construct):

    def __init__(self, scope: Construct, id: str, *,
                 org_config_table: dynamodb.ITable,
                 validation_results_table: dynamodb.ITable,
                 irp_table: dynamodb.ITable,
                 notifications_topic: sns.ITopic) -> None:
        super().__init__(scope, id)

        lambda_dir = os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "multi-org")
        rules_engine_dir = os.path.join(lambda_dir, "rules-engine")
        csv_splitter_dir = os.path.join(lambda_dir, "csv-splitter")

        # Wildcard ARN for all per-org buckets (penguin-health-*)
        s3_bucket_arn = "arn:aws:s3:::penguin-health-*"

        s3_policy = iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetObjectTagging",
                "s3:PutObjectTagging",
                "s3:CopyObject",
            ],
            resources=[s3_bucket_arn, f"{s3_bucket_arn}/*"],
        )

        # ----- process-raw-charts-multi-org -----
        self.process_fn = _lambda.Function(self, "ProcessRawChartsFn",
            function_name=f"{config.PROJECT_NAME}-process-raw-charts-multi-org",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="process_raw_charts_multi_org.lambda_handler",
            code=_lambda.Code.from_asset(
                lambda_dir,
                exclude=["*", "!process_raw_charts_multi_org.py"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=CopyFileBundler(
                        os.path.join(lambda_dir, "process_raw_charts_multi_org.py")
                    ),
                ),
            ),
            timeout=Duration.seconds(config.LAMBDA_DEFAULT_TIMEOUT_SECONDS),
            memory_size=config.LAMBDA_DEFAULT_MEMORY_MB,
            environment={
                "SNS_TOPIC_ARN": notifications_topic.topic_arn,
                "SNS_ROLE_ARN": config.EXISTING_SNS_ROLE_ARN,
            },
        )

        self.process_fn.add_to_role_policy(s3_policy)
        self.process_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["textract:StartDocumentAnalysis"],
            resources=["*"],
        ))
        self.process_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["iam:PassRole"],
            resources=[config.EXISTING_SNS_ROLE_ARN],
        ))

        # ----- textract-result-handler-multi-org -----
        self.textract_handler_fn = _lambda.Function(self, "TextractResultHandlerFn",
            function_name=f"{config.PROJECT_NAME}-textract-result-handler-multi-org",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="textract_result_handler_multi_org.lambda_handler",
            code=_lambda.Code.from_asset(
                lambda_dir,
                exclude=["*", "!textract_result_handler_multi_org.py", "!rules-engine/multi_org_config.py"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=MultiFileBundler([
                        os.path.join(lambda_dir, "textract_result_handler_multi_org.py"),
                        os.path.join(rules_engine_dir, "multi_org_config.py"),
                    ]),
                ),
            ),
            timeout=Duration.seconds(300),
            memory_size=512,
        )

        self.textract_handler_fn.add_to_role_policy(s3_policy)
        self.textract_handler_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["textract:GetDocumentAnalysis"],
            resources=["*"],
        ))
        org_config_table.grant_read_data(self.textract_handler_fn)

        # Subscribe to SNS topic for Textract completion notifications
        notifications_topic.add_subscription(
            subscriptions.LambdaSubscription(self.textract_handler_fn)
        )

        # ----- rules-engine-rag -----
        # Module files for the rules engine (refactored for maintainability)
        rules_engine_modules = [
            "rules_engine_rag.py",      # Lambda entry point
            "multi_org_config.py",       # DynamoDB org config loading
            "rate_limiter.py",           # Rate limiting for Bedrock API
            "bedrock_client.py",         # Claude model invocation
            "document_validator.py",     # Per-rule LLM validation with multi-threading
            "deterministic_evaluator.py", # Code-based deterministic rule evaluation
            "results_handler.py",        # DynamoDB storage and CSV reports
            "field_extractor.py",        # Text field extraction
        ]

        self.rules_engine_fn = _lambda.Function(self, "RulesEngineRagFn",
            function_name=f"{config.PROJECT_NAME}-rules-engine-rag",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="rules_engine_rag.lambda_handler",
            code=_lambda.Code.from_asset(
                rules_engine_dir,
                exclude=["*"] + [f"!{m}" for m in rules_engine_modules],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=MultiFileBundler([
                        os.path.join(rules_engine_dir, m) for m in rules_engine_modules
                    ]),
                ),
            ),
            timeout=Duration.minutes(15),  # Max Lambda timeout for continuation pattern
            memory_size=512,
        )

        self.rules_engine_fn.add_to_role_policy(s3_policy)
        org_config_table.grant_read_data(self.rules_engine_fn)
        validation_results_table.grant_read_write_data(self.rules_engine_fn)
        irp_table.grant_read_data(self.rules_engine_fn)

        # Bedrock permissions for LLM-based rule evaluation
        self.rules_engine_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"],
        ))
        # AWS Marketplace permissions required for cross-region inference profiles
        self.rules_engine_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["aws-marketplace:ViewSubscriptions", "aws-marketplace:Subscribe"],
            resources=["*"],
        ))
        # Permission to invoke itself for continuation pattern
        # Use ARN pattern to avoid circular dependency
        self.rules_engine_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[
                f"arn:aws:lambda:{config.AWS_REGION}:*:function:{config.PROJECT_NAME}-rules-engine-rag"
            ],
        ))

        # ----- csv-splitter-multi-org -----
        # Splits bulk CSV files uploaded via SFTP into individual chart files
        self.csv_splitter_fn = _lambda.Function(self, "CsvSplitterFn",
            function_name=f"{config.PROJECT_NAME}-csv-splitter-multi-org",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="csv_splitter_multi_org.lambda_handler",
            code=_lambda.Code.from_asset(
                csv_splitter_dir,
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=DirectoryBundler([
                        (os.path.join(csv_splitter_dir, "csv_splitter_multi_org.py"), None),
                        (os.path.join(csv_splitter_dir, "splitters"), "splitters"),
                        (os.path.join(rules_engine_dir, "multi_org_config.py"), None),
                    ]),
                ),
            ),
            timeout=Duration.seconds(60),
            memory_size=256,
        )

        self.csv_splitter_fn.add_to_role_policy(s3_policy)
        org_config_table.grant_read_data(self.csv_splitter_fn)

        # ----- Scheduled validation runs -----
        # Catholic Charities: 11PM EDT = 03:00 UTC (next day)
        events.Rule(self, "CatholicCharitiesValidationSchedule",
            rule_name=f"{config.PROJECT_NAME}-catholic-charities-validation-schedule",
            schedule=events.Schedule.cron(hour="10", minute="0"),
            targets=[
                targets.LambdaFunction(
                    self.rules_engine_fn,
                    event=events.RuleTargetInput.from_object({
                        "organization_id": "catholic-charities-multi-org"
                    })
                )
            ],
        )

        # Circles of Care: 6:30AM EDT = 10:30 UTC
        events.Rule(self, "CirclesOfCareValidationSchedule",
            rule_name=f"{config.PROJECT_NAME}-circles-of-care-validation-schedule",
            schedule=events.Schedule.cron(hour="10", minute="30"),
            targets=[
                targets.LambdaFunction(
                    self.rules_engine_fn,
                    event=events.RuleTargetInput.from_object({
                        "organization_id": "circles-of-care"
                    })
                )
            ],
        )

