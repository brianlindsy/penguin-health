"""
Audit Engine construct: Lambda functions for the multi-org document
processing pipeline (Textract, CSV splitting, FHIR encounter
materialization). The per-rule validation loop that used to live here
runs as a Fargate task now — see `components/rules_engine.py`.
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
    DirectoryBundler,
    PipInstallBundler,
)


class AuditEngine(Construct):

    def __init__(self, scope: Construct, id: str, *,
                 org_config_table: dynamodb.ITable,
                 notifications_topic: sns.ITopic) -> None:
        super().__init__(scope, id)

        lambda_dir = os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "multi-org")
        rules_engine_dir = os.path.join(lambda_dir, "rules-engine")
        csv_splitter_dir = os.path.join(lambda_dir, "csv-splitter")
        fhir_dir = os.path.join(lambda_dir, "fhir")
        fhir_materializer_dir = os.path.join(lambda_dir, "fhir-materializer")
        audit_pkg_dir = os.path.join(lambda_dir, "audit")

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
                exclude=["*", "!process_raw_charts_multi_org.py", "!audit/**"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=DirectoryBundler([
                        (os.path.join(lambda_dir, "process_raw_charts_multi_org.py"), None),
                        (audit_pkg_dir, "audit"),
                    ]),
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
                exclude=["*", "!textract_result_handler_multi_org.py",
                         "!rules-engine/multi_org_config.py", "!audit/**"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=DirectoryBundler([
                        (os.path.join(lambda_dir, "textract_result_handler_multi_org.py"), None),
                        (os.path.join(rules_engine_dir, "multi_org_config.py"), None),
                        (audit_pkg_dir, "audit"),
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
                        (audit_pkg_dir, "audit"),
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
                        source_dirs=[
                            (fhir_dir, "fhir"),
                            (audit_pkg_dir, "audit"),
                        ],
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

