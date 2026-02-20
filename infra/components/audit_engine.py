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
)
from constructs import Construct

import config
from components.bundler import CopyFileBundler, MultiFileBundler


class AuditEngine(Construct):

    def __init__(self, scope: Construct, id: str, *,
                 org_config_table: dynamodb.ITable,
                 validation_results_table: dynamodb.ITable,
                 irp_table: dynamodb.ITable,
                 notifications_topic: sns.ITopic) -> None:
        super().__init__(scope, id)

        lambda_dir = os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "multi-org")

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
                exclude=["*", "!textract_result_handler_multi_org.py", "!multi_org_config.py"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=MultiFileBundler([
                        os.path.join(lambda_dir, "textract_result_handler_multi_org.py"),
                        os.path.join(lambda_dir, "multi_org_config.py"),
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
        self.rules_engine_fn = _lambda.Function(self, "RulesEngineRagFn",
            function_name=f"{config.PROJECT_NAME}-rules-engine-rag",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="rules_engine_rag.lambda_handler",
            code=_lambda.Code.from_asset(
                lambda_dir,
                exclude=["*", "!rules_engine_rag.py", "!multi_org_config.py"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=MultiFileBundler([
                        os.path.join(lambda_dir, "rules_engine_rag.py"),
                        os.path.join(lambda_dir, "multi_org_config.py"),
                    ]),
                ),
            ),
            timeout=Duration.seconds(300),
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
