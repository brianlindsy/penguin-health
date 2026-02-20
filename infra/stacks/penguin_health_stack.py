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
        )

        # ----- Audit Engine -----
        audit_engine = AuditEngine(self, "AuditEngine",
            org_config_table=db.org_config_table,
            validation_results_table=db.validation_results_table,
            irp_table=db.irp_table,
            notifications_topic=db.notifications_topic,
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
        CfnOutput(self, "NotificationsTopicArn",
            value=db.notifications_topic.topic_arn,
            description="SNS topic ARN for Textract notifications",
        )
