"""
Database construct: DynamoDB tables and SNS topic for Penguin Health.
"""

from aws_cdk import (
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
)
from constructs import Construct

import config


class Database(Construct):

    def __init__(self, scope: Construct, id: str) -> None:
        super().__init__(scope, id)

        # ----- penguin-health-org-config -----
        # Note: GSI2 was removed - it was never queried, only written to RULE# items
        self.org_config_table = dynamodb.Table(self, "OrgConfigTable",
            table_name=f"{config.PROJECT_NAME}-org-config",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.org_config_table.add_global_secondary_index(
            index_name="gsi1",
            partition_key=dynamodb.Attribute(name="gsi1pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="gsi1sk", type=dynamodb.AttributeType.STRING),
        )

        # ----- penguin-health-validation-results -----
        self.validation_results_table = dynamodb.Table(self, "ValidationResultsTable",
            table_name=f"{config.PROJECT_NAME}-validation-results",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.validation_results_table.add_global_secondary_index(
            index_name="gsi1",
            partition_key=dynamodb.Attribute(name="gsi1pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="gsi1sk", type=dynamodb.AttributeType.STRING),
        )
        self.validation_results_table.add_global_secondary_index(
            index_name="gsi2",
            partition_key=dynamodb.Attribute(name="gsi2pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="gsi2sk", type=dynamodb.AttributeType.STRING),
        )

        # ----- penguin-health-analytics-reports -----
        # Saved NL-analytics query snapshots. pk=ORG#{org_id},
        # sk=REPORT#{created_at_iso}#{report_id} so a single query lists
        # an org's reports newest-first via ScanIndexForward=False.
        self.analytics_reports_table = dynamodb.Table(self, "AnalyticsReportsTable",
            table_name=f"{config.PROJECT_NAME}-analytics-reports",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ----- penguin-health-analytics-deep-jobs -----
        # Async deep-analysis job rows. pk=ORG#{org_id}, sk=JOB#{job_id}.
        # TTL on `ttl` (epoch seconds) — jobs self-clean 24h after creation.
        self.deep_jobs_table = dynamodb.Table(self, "AnalyticsDeepJobsTable",
            table_name=f"{config.PROJECT_NAME}-analytics-deep-jobs",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ----- penguin-health-stedi -----
        # Stedi eligibility audit log + daily usage counter.
        # Two row types share the table:
        #   sk=AUDIT#{iso_ts}#{request_id}  — one per Stedi call, immutable, 7y TTL
        #   sk=USAGE#{yyyy-mm-dd}           — one per org per day, atomic counter, 90d TTL
        # GSI1 keyed by patient_hash so the "recent checks for this patient"
        # dedup lookup is O(1) instead of scanning AUDIT# rows.
        self.stedi_table = dynamodb.Table(self, "StediTable",
            table_name=f"{config.PROJECT_NAME}-stedi",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expires_at",
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True,
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.stedi_table.add_global_secondary_index(
            index_name="gsi1",
            partition_key=dynamodb.Attribute(name="gsi1pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="gsi1sk", type=dynamodb.AttributeType.STRING),
        )

        # ----- SNS Topic for Textract notifications -----
        self.notifications_topic = sns.Topic(self, "NotificationsTopic",
            topic_name=f"{config.PROJECT_NAME}-notifications-multi-org",
        )
