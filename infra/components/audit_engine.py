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
from components.bundler import (
    CopyFileBundler,
    MultiFileBundler,
    DirectoryBundler,
    PipInstallBundler,
)


class AuditEngine(Construct):

    def __init__(self, scope: Construct, id: str, *,
                 org_config_table: dynamodb.ITable,
                 validation_results_table: dynamodb.ITable,
                 notifications_topic: sns.ITopic) -> None:
        super().__init__(scope, id)

        lambda_dir = os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "multi-org")
        rules_engine_dir = os.path.join(lambda_dir, "rules-engine")
        csv_splitter_dir = os.path.join(lambda_dir, "csv-splitter")
        fhir_dir = os.path.join(lambda_dir, "fhir")
        fhir_materializer_dir = os.path.join(lambda_dir, "fhir-materializer")
        notifications_pkg_dir = os.path.join(lambda_dir, "notifications")

        # Wildcard ARN for all per-org PHI buckets (`penguin-health-{org_id}`).
        # NOTE: the JWKS hosting bucket is deliberately named `phealth-fhir-jwks`
        # so it does NOT match this wildcard — a compromise of these Lambdas
        # must not be able to overwrite our JWK Sets and substitute keys.
        # See infra/components/jwks_hosting.py for the rationale.
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
        # Module files for the rules engine (refactored for maintainability).
        # Pinned to Python 3.13 (rest of stack runs 3.14) because fastparquet
        # and its pandas/numpy deps don't yet ship Linux wheels for 3.14.
        rules_engine_modules = [
            "rules_engine_rag.py",      # Lambda entry point
            "multi_org_config.py",       # DynamoDB org config loading
            "rate_limiter.py",           # Rate limiting for Bedrock API
            "bedrock_client.py",         # Claude model invocation
            "claude_cost.py",            # Per-org CloudWatch cost emission
            "document_validator.py",     # Per-rule LLM validation with multi-threading
            "deterministic_evaluator.py", # Code-based deterministic rule evaluation
            "results_handler.py",        # DynamoDB storage and CSV reports
            "field_extractor.py",        # Text field extraction
            "parquet_writer.py",         # End-of-run Parquet snapshot for Athena
        ]

        rules_engine_requirements = [
            # fastparquet pulls pandas, numpy, cramjam transitively via
            # PipInstallBundler (which doesn't pass --no-deps), so listing
            # fastparquet alone is enough.
            "fastparquet==2024.11.0",
        ]

        self.rules_engine_fn = _lambda.Function(self, "RulesEngineRagFn",
            function_name=f"{config.PROJECT_NAME}-rules-engine-rag",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="rules_engine_rag.lambda_handler",
            code=_lambda.Code.from_asset(
                rules_engine_dir,
                exclude=["*"] + [f"!{m}" for m in rules_engine_modules],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_13.bundling_image,
                    local=PipInstallBundler(
                        source_paths=[
                            os.path.join(rules_engine_dir, m)
                            for m in rules_engine_modules
                        ],
                        source_dirs=[(notifications_pkg_dir, "notifications")],
                        requirements=rules_engine_requirements,
                        python_version="3.13",
                    ),
                ),
            ),
            timeout=Duration.minutes(15),  # Max Lambda timeout for continuation pattern
            memory_size=512,
            environment={
                "ORG_CONFIG_TABLE_NAME": org_config_table.table_name,
                "STEDI_TABLE_NAME": "penguin-health-stedi",
                "EMAIL_FROM_ADDRESS": "noreply@penguinhealth.io",
                "EMAIL_REPLY_TO": "noreply@penguinhealth.io",
                "ADMIN_UI_BASE_URL": "https://app.penguinhealth.io",
            },
        )

        self.rules_engine_fn.add_to_role_policy(s3_policy)
        org_config_table.grant_read_data(self.rules_engine_fn)
        validation_results_table.grant_read_write_data(self.rules_engine_fn)

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
        # Per-org Claude cost attribution metrics. Namespace-scoped so this
        # role can't write outside PenguinHealth/LLMCost.
        self.rules_engine_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["cloudwatch:PutMetricData"],
            resources=["*"],
            conditions={
                "StringEquals": {
                    "cloudwatch:namespace": "PenguinHealth/LLMCost"
                }
            },
        ))
        # Permission to invoke itself for continuation pattern
        # Use ARN pattern to avoid circular dependency
        self.rules_engine_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[
                f"arn:aws:lambda:{config.AWS_REGION}:*:function:{config.PROJECT_NAME}-rules-engine-rag"
            ],
        ))
        # Email notifications: SES send + DynamoDB write on penguin-health-stedi
        # for the EMAIL_AUDIT# rows that mirror the existing AUDIT# pattern.
        # Sending identity isn't yet verified; tighten resource to the
        # identity ARN once DNS verification of penguinhealth.io completes.
        self.rules_engine_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["ses:SendEmail", "ses:SendRawEmail"],
            resources=["*"],
        ))
        self.rules_engine_fn.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "dynamodb:PutItem",
                "dynamodb:Query",
                "dynamodb:GetItem",
            ],
            resources=[
                f"arn:aws:dynamodb:{config.AWS_REGION}:*:table/penguin-health-stedi",
                f"arn:aws:dynamodb:{config.AWS_REGION}:*:table/penguin-health-stedi/index/*",
            ],
        ))
        # Read subscriber list (SUBSCRIPTION gsi1pk on penguin-health-org-config).
        self.rules_engine_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query"],
            resources=[f"{org_config_table.table_arn}/index/*"],
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
        # Emit SftpIngestComplete on the default bus for followers (e.g. the
        # FHIR encounter materializer). Scoped to the default event bus only.
        self.csv_splitter_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["events:PutEvents"],
            resources=[
                f"arn:aws:events:{config.AWS_REGION}:*:event-bus/default"
            ],
        ))

        # ----- fhir-encounter-materializer -----
        # Triggered by SftpIngestComplete events from the CSV splitter. For
        # each event, identifies encounter IDs in the newly-ingested rows
        # that aren't yet materialized, fetches the FHIR Encounter resource
        # from the org's FHIR API, writes canonical NDJSON under
        # data/fhir/encounter/ and projected Parquet under
        # analytics/fhir/encounter/. Pinned to Python 3.13 for fastparquet.
        fhir_materializer_source_files = [
            os.path.join(fhir_materializer_dir, "encounter_materializer.py"),
            os.path.join(fhir_materializer_dir, "athena.py"),
            os.path.join(fhir_materializer_dir, "metrics.py"),
            os.path.join(fhir_materializer_dir, "storage.py"),
        ]

        self.fhir_materializer_fn = _lambda.Function(
            self, "FhirEncounterMaterializerFn",
            function_name=f"{config.PROJECT_NAME}-fhir-encounter-materializer",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="encounter_materializer.lambda_handler",
            code=_lambda.Code.from_asset(
                fhir_materializer_dir,
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_13.bundling_image,
                    local=PipInstallBundler(
                        source_paths=fhir_materializer_source_files,
                        source_dirs=[(fhir_dir, "fhir")],
                        requirements=[
                            "fastparquet==2024.11.0",
                            # PyJWT for private_key_jwt client authentication.
                            # The [crypto] extra pulls `cryptography`, which
                            # ships manylinux wheels for Python 3.13.
                            "PyJWT[crypto]==2.10.1",
                        ],
                        python_version="3.13",
                    ),
                ),
            ),
            timeout=Duration.minutes(15),
            memory_size=512,
        )

        self.fhir_materializer_fn.add_to_role_policy(s3_policy)
        org_config_table.grant_read_data(self.fhir_materializer_fn)
        # KMS: the per-org FHIR signing key. The materializer resolves the
        # alias to a key ARN + public key at cold start (kms:DescribeKey +
        # kms:GetPublicKey), then signs JWT assertions via kms:Sign. The
        # private key bytes never enter the Lambda process. Scoped to
        # FHIR-purpose aliases created by provision_fhir_keypair.py. No
        # Secrets Manager involvement — client_id lives on FHIR_CONFIG,
        # and kid + key arn are derived from the alias at runtime.
        self.fhir_materializer_fn.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "kms:Sign",
                "kms:GetPublicKey",
                "kms:DescribeKey",
            ],
            resources=["*"],
            conditions={
                "ForAnyValue:StringLike": {
                    "kms:ResourceAliases": [
                        f"alias/{config.PROJECT_NAME}-fhir-*"
                    ]
                }
            },
        ))
        # Athena query execution against per-org workgroups + Glue table read.
        self.fhir_materializer_fn.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
            ],
            resources=[
                f"arn:aws:athena:{config.AWS_REGION}:*:workgroup/{config.PROJECT_NAME}-analytics-*"
            ],
        ))
        self.fhir_materializer_fn.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
            ],
            resources=["*"],
        ))
        # CloudWatch metrics — counters for skipped/failed/fetched/not-found.
        self.fhir_materializer_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["cloudwatch:PutMetricData"],
            resources=["*"],
            conditions={
                "StringEquals": {
                    "cloudwatch:namespace": "PenguinHealth/FhirMaterializer"
                }
            },
        ))
        # Self-invoke for the >15-min continuation pattern.
        self.fhir_materializer_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[
                f"arn:aws:lambda:{config.AWS_REGION}:*:function:{config.PROJECT_NAME}-fhir-encounter-materializer"
            ],
        ))

        # EventBridge rule: subscribe to SftpIngestComplete events emitted by
        # the CSV splitter. All orgs flow through; the materializer's config
        # gate decides per-org whether to skip or proceed. Keeping the rule
        # permissive (no per-org pattern) avoids the "added a new org but
        # forgot to update the EventBridge rule" silent-failure trap.
        events.Rule(self, "FhirEncounterMaterializerTrigger",
            rule_name=f"{config.PROJECT_NAME}-fhir-encounter-materializer",
            event_pattern=events.EventPattern(
                source=["penguin-health.csv-splitter"],
                detail_type=["SftpIngestComplete"],
            ),
            targets=[targets.LambdaFunction(self.fhir_materializer_fn)],
        )

        # ----- Scheduled validation runs -----
        # Two rules per org so the cron expression itself encodes the
        # Monday-vs-rest split:
        #   Monday    -> validate Sat + Sun + today's (Mon) ingest, since the
        #                weekend's data was never picked up by an earlier run.
        #   Other day -> validate today's ingest only.
        # The relative `date_window` is resolved to concrete dates by the
        # rules engine at run time — cron is when, payload is what to look at.
        for org_id, hour, minute, rule_prefix, label in [
            ("catholic-charities-multi-org", "10", "0",
             "catholic-charities-validation", "CatholicCharities"),
            ("circles-of-care", "11", "15",
             "circles-of-care-validation", "CirclesOfCare"),
        ]:
            # Monday at HH:MM UTC — validates Sat, Sun, and today (Mon).
            events.Rule(self, f"{label}MondayValidationSchedule",
                rule_name=f"{config.PROJECT_NAME}-{rule_prefix}-monday",
                schedule=events.Schedule.cron(hour=hour, minute=minute, week_day="MON"),
                targets=[
                    targets.LambdaFunction(
                        self.rules_engine_fn,
                        event=events.RuleTargetInput.from_object({
                            "organization_id": org_id,
                            "date_window": {"days_back_from_today": [2, 1, 0]},
                        })
                    )
                ],
            )

            # Tue-Fri at HH:MM UTC — validates today's ingest only.
            # Sat/Sun deliveries go unvalidated until Monday's catch-up run.
            events.Rule(self, f"{label}DailyValidationSchedule",
                rule_name=f"{config.PROJECT_NAME}-{rule_prefix}-daily",
                schedule=events.Schedule.cron(hour=hour, minute=minute, week_day="TUE-FRI"),
                targets=[
                    targets.LambdaFunction(
                        self.rules_engine_fn,
                        event=events.RuleTargetInput.from_object({
                            "organization_id": org_id,
                            "date_window": {"days_back_from_today": [0]},
                        })
                    )
                ],
            )

