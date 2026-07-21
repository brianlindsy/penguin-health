"""
Penguin Health stack — composes all infrastructure constructs.

To add new resources:
  1. Create a new construct in components/ (or add to an existing one)
  2. Instantiate it in this file
  3. Add any CfnOutputs below
"""

from aws_cdk import (
    Stack,
    CfnOutput,
    Tags,
)
from constructs import Construct

import config
from components.database import Database
from components.admin_ui import AdminUi
from components.audit_engine import AuditEngine
from components.audit_layer import AuditLayer
from components.analytics import Analytics
from components.jwks_hosting import JwksHosting
from components.centralreach import CentralReach
from components.rules_engine import RulesEngine
from components.document_queue import DocumentQueue


class PenguinHealthStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Apply project-wide tags to all resources
        for key, value in config.COMMON_TAGS.items():
            Tags.of(self).add(key, value)

        # ----- Database + SNS -----
        db = Database(self, "Database")

        # ----- CentralReach (Fargate + Step Functions + per-org schedules) -----
        # Stand up first so its ECS cluster + VPC can be reused by the
        # rules-engine Fargate component below, and so its state-machine
        # ARN is available for wiring into the admin Lambda.
        centralreach = CentralReach(self, "CentralReach",
            org_config_table=db.org_config_table,
            ingest_cursor_table=db.centralreach_ingest_cursor_table,
        )

        # ----- Rules Engine (Fargate + Step Functions + per-org schedules) -----
        # Reuses the CentralReach VPC + ECS cluster. Task role is
        # independent, so a compromised runner can't reach CR credentials
        # or the ingest cursor table.
        rules_engine = RulesEngine(self, "RulesEngine",
            cluster=centralreach.cluster,
            vpc=centralreach.vpc,
            org_config_table=db.org_config_table,
            validation_results_table=db.validation_results_table,
            narrative_hashes_table=db.narrative_hashes_table,
            document_queue_table=db.document_queue_table,
        )

        # ----- Admin UI -----
        admin_ui = AdminUi(self, "AdminUi",
            org_config_table=db.org_config_table,
            validation_results_table=db.validation_results_table,
            analytics_reports_table=db.analytics_reports_table,
            deep_jobs_table=db.deep_jobs_table,
            stedi_table=db.stedi_table,
            document_queue_table=db.document_queue_table,
            centralreach_state_machine=centralreach.state_machine,
            rules_engine_state_machine=rules_engine.state_machine,
        )

        # ----- Audit Engine -----
        audit_engine = AuditEngine(self, "AuditEngine",
            org_config_table=db.org_config_table,
            notifications_topic=db.notifications_topic,
        )

        # ----- Document Queue auto-close job -----
        document_queue = DocumentQueue(self, "DocumentQueue",
            org_config_table=db.org_config_table,
            document_queue_table=db.document_queue_table,
        )

        # ----- Audit Layer -----
        # HIPAA-compliant audit substrate (S3 Object Lock + Firehose +
        # DDB hot mirror) plus IAM grants to every emitting Lambda. See
        # lambda/multi-org/audit/ for the application-level emitter.
        audit = AuditLayer(self, "AuditLayer",
            emitting_fns=[
                admin_ui.api_function,
                admin_ui.deep_worker_function,
                admin_ui.fhir_eligibility_poller_fn,
                audit_engine.process_fn,
                audit_engine.textract_handler_fn,
                audit_engine.csv_splitter_fn,
                audit_engine.fhir_materializer_fn,
                document_queue.autoclose_fn,
            ],
        )

        # Fargate tasks emit audit events directly via boto3, not through
        # AuditLayer.emitting_fns (which is Lambda-only). The audit DDB
        # table and Firehose stream are both CMK-encrypted, so each task
        # role needs the same grants an emitting Lambda gets: encrypt/
        # decrypt on the audit CMK, PutItem on the DDB table, and
        # PutRecord on the Firehose stream. `_grant_emit` centralizes
        # that; call it for each Fargate task role that emits.
        for task_role in (centralreach.task_role, rules_engine.task_role):
            audit._grant_emit_to_role(task_role)

        # ----- Analytics (Athena + Glue) -----
        analytics = Analytics(self, "Analytics")

        # ----- JWKS hosting for FHIR private_key_jwt -----
        # The S3 bucket holds per-org JWK Set files; CloudFront fronts it.
        # The URLs handed to FHIR vendors look like:
        #     {public_base_url}/{org_id}/jwks.json
        # If JWKS_DOMAIN/JWKS_CERT_ARN are unset, this deploys with a
        # cloudfront.net URL (fine for testing, not for vendor registration
        # because the hostname is then tied to this distribution).
        jwks = JwksHosting(self, "JwksHosting",
            jwks_domain=config.JWKS_DOMAIN,
            cert_arn=config.JWKS_CERT_ARN,
        )

        # ----- Outputs: Admin UI -----
        CfnOutput(self, "UserPoolId",
            value=admin_ui.user_pool.user_pool_id,
            description="Cognito User Pool ID",
        )
        CfnOutput(self, "UserPoolClientId",
            value=admin_ui.app_client.user_pool_client_id,
            description="Cognito App Client ID",
        )
        CfnOutput(self, "ApiUrl",
            value=admin_ui.http_api.url or "",
            description="API Gateway URL",
        )
        CfnOutput(self, "CloudFrontUrl",
            value=f"https://{admin_ui.distribution.distribution_domain_name}",
            description="CloudFront Distribution URL",
        )
        CfnOutput(self, "FrontendBucketName",
            value=admin_ui.frontend_bucket.bucket_name,
            description="S3 bucket for frontend assets",
        )
        CfnOutput(self, "DistributionId",
            value=admin_ui.distribution.distribution_id,
            description="CloudFront Distribution ID (for cache invalidation)",
        )

        # ----- Outputs: Audit Engine -----
        CfnOutput(self, "ProcessRawChartsFnArn",
            value=audit_engine.process_fn.function_arn,
            description="process-raw-charts-multi-org Lambda ARN",
        )
        CfnOutput(self, "TextractHandlerFnArn",
            value=audit_engine.textract_handler_fn.function_arn,
            description="textract-result-handler-multi-org Lambda ARN",
        )
        CfnOutput(self, "CsvSplitterFnArn",
            value=audit_engine.csv_splitter_fn.function_arn,
            description="csv-splitter-multi-org Lambda ARN",
        )
        CfnOutput(self, "NotificationsTopicArn",
            value=db.notifications_topic.topic_arn,
            description="SNS topic ARN for Textract notifications",
        )

        # ----- Outputs: Rules Engine -----
        CfnOutput(self, "RulesEngineStateMachineArn",
            value=rules_engine.state_machine.state_machine_arn,
            description="Step Functions state machine that wraps the rules-engine Fargate task",
        )
        CfnOutput(self, "RulesEngineRunnerImageUri",
            value=rules_engine.image_asset.image_uri,
            description="ECR image URI for the rules-engine Fargate container",
        )
        CfnOutput(self, "RulesEngineLogGroupName",
            value=rules_engine.log_group.log_group_name,
            description="CloudWatch log group for rules-engine Fargate stdout/stderr",
        )

        # ----- Outputs: Audit Layer -----
        CfnOutput(self, "AuditBucketName",
            value=audit.bucket.bucket_name,
            description="WORM (Object Lock Compliance) bucket for audit Parquet",
        )
        CfnOutput(self, "AuditTableName",
            value=audit.table.table_name,
            description="DynamoDB hot mirror for the most recent 90d of audit events",
        )
        CfnOutput(self, "AuditFirehoseName",
            value=audit.stream.delivery_stream_name or "",
            description="Kinesis Firehose delivery stream for audit events",
        )
        CfnOutput(self, "AuditKeyArn",
            value=audit.key.key_arn,
            description="KMS CMK protecting all audit-layer storage",
        )

        # ----- Outputs: Analytics -----
        # One Athena workgroup per org; query results land in each org's
        # own bucket under athena-results/ for PHI isolation.
        for org_id, wg in analytics.workgroups.items():
            safe_id = org_id.replace("-", "")
            CfnOutput(self, f"AthenaWorkGroup{safe_id}",
                value=wg.name,
                description=f"Athena workgroup for {org_id}",
            )

        # ----- Outputs: CentralReach -----
        CfnOutput(self, "CentralReachStateMachineArn",
            value=centralreach.state_machine.state_machine_arn,
            description="Step Functions state machine that wraps the CentralReach Fargate task",
        )
        CfnOutput(self, "CentralReachClusterName",
            value=centralreach.cluster.cluster_name,
            description="ECS Fargate cluster hosting the CentralReach ingest runner",
        )
        CfnOutput(self, "CentralReachRunnerImageUri",
            value=centralreach.image_asset.image_uri,
            description="ECR image URI for the CentralReach ingest container",
        )
        CfnOutput(self, "CentralReachLogGroupName",
            value=centralreach.log_group.log_group_name,
            description="CloudWatch log group for CentralReach ingest stdout/stderr",
        )

        # ----- Outputs: JWKS -----
        CfnOutput(self, "JwksBucketName",
            value=jwks.bucket.bucket_name,
            description="S3 bucket where provision_fhir_keypair.py uploads JWK Sets",
        )
        CfnOutput(self, "JwksBaseUrl",
            value=jwks.public_base_url,
            description="Public base URL for FHIR JWKS — give vendors {base}/{org_id}/jwks.json",
        )
        CfnOutput(self, "JwksDistributionId",
            value=jwks.distribution.distribution_id,
            description="CloudFront Distribution ID (for JWKS cache invalidation on key rotation)",
        )
