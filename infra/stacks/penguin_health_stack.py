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
from components.analytics import Analytics
from components.jwks_hosting import JwksHosting


class PenguinHealthStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Apply project-wide tags to all resources
        for key, value in config.COMMON_TAGS.items():
            Tags.of(self).add(key, value)

        # ----- Database + SNS -----
        db = Database(self, "Database")

        # ----- Admin UI -----
        admin_ui = AdminUi(self, "AdminUi",
            org_config_table=db.org_config_table,
            validation_results_table=db.validation_results_table,
            analytics_reports_table=db.analytics_reports_table,
            deep_jobs_table=db.deep_jobs_table,
            stedi_table=db.stedi_table,
        )

        # ----- Audit Engine -----
        audit_engine = AuditEngine(self, "AuditEngine",
            org_config_table=db.org_config_table,
            validation_results_table=db.validation_results_table,
            irp_table=db.irp_table,
            notifications_topic=db.notifications_topic,
        )

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
        CfnOutput(self, "RulesEngineFnArn",
            value=audit_engine.rules_engine_fn.function_arn,
            description="rules-engine-rag Lambda ARN",
        )
        CfnOutput(self, "CsvSplitterFnArn",
            value=audit_engine.csv_splitter_fn.function_arn,
            description="csv-splitter-multi-org Lambda ARN",
        )
        CfnOutput(self, "NotificationsTopicArn",
            value=db.notifications_topic.topic_arn,
            description="SNS topic ARN for Textract notifications",
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
