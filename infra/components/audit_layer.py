"""
Audit layer: HIPAA-compliant audit event capture for every PHI access and
PHI-bearing write across the app.

Two storage paths share one emitter:

  Lambda (HTTP handler or poller)
      │
      │  audit.emit(...) — non-blocking, never raises
      ▼
  ┌────────────────────────┐        ┌──────────────────────────────┐
  │ DDB hot mirror         │        │ Kinesis Firehose             │
  │ penguin-health-audit   │        │ penguin-health-audit         │
  │ pk=ORG#{org_id}        │        │ DirectPut, 60s/64MB buffer   │
  │ sk=AUDIT#{ts}#{event}  │        │ Parquet via Glue schema      │
  │ gsi1=patient_hash      │        │                              │
  │ TTL=90d  KMS  PITR=ON  │        ▼                              │
  └────────────────────────┘        S3 Object Lock COMPLIANCE 7y    │
                                    KMS CMK, versioned, deny-delete│
                                                                   │
                                    Glue table → Athena            │
                                                                   │
The DDB hot mirror is the synchronous durability guarantee. Firehose is
the WORM archive. A 60-second window of events can live only in Lambda
memory between buffer flushes — that risk is bounded by the synchronous
DDB write, and backfill from DDB → S3 is possible if Firehose ever drops
a record.

WHY THIS EXISTS
  - 45 CFR § 164.312(b) Audit Controls (Required, not Addressable)
  - 45 CFR § 164.308(a)(1)(ii)(D) Information System Activity Review
  - OCR settlements consistently cite modifiable logs as a finding;
    Object Lock Compliance mode is the AWS-native answer.

This component does not provision any application code paths — it only
provisions the durable substrate. The `lambda/multi-org/audit/` package
is the emitter library that calls into it.
"""

import os

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    aws_kms as kms,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct

import config


# Glue database name. Kept separate from the analytics Glue database so
# audit-table grants stay tight and a future cross-account "auditor read
# only" role can target this database without touching analytics PHI.
AUDIT_GLUE_DATABASE = "penguin_health_audit"
AUDIT_GLUE_TABLE = "audit_events"


# Event schema — must stay in sync with lambda/multi-org/audit/schema.py.
# Order matches Firehose's expected ordering; types are Glue/Hive types.
# `event_time` is stored as a string (ISO-8601) so Athena's `from_iso8601_timestamp`
# can parse it; partitioning is on year/month/day strings written by Firehose
# dynamic partitioning, not on the timestamp column itself.
_AUDIT_EVENT_COLUMNS: list[tuple[str, str]] = [
    ("event_id", "string"),
    ("event_time", "string"),
    ("schema_version", "string"),
    ("action", "string"),
    ("outcome", "string"),
    ("purpose_of_use", "string"),
    ("org_id", "string"),
    ("agent_type", "string"),
    ("agent_id", "string"),
    ("agent_email", "string"),
    ("agent_groups", "array<string>"),
    ("client_ip", "string"),
    ("user_agent", "string"),
    ("source_lambda", "string"),
    ("request_id", "string"),
    ("resource_type", "string"),
    ("resource_id", "string"),
    ("patient_hash", "string"),
    ("patient_first_initial", "string"),
    ("patient_last_initial", "string"),
    ("patient_dob", "string"),
    ("member_id_last4", "string"),
    ("payer_id", "string"),
    ("payer_name", "string"),
    ("call_type", "string"),
    ("external_control_number", "string"),
    ("duration_ms", "bigint"),
    ("result_summary", "string"),  # JSON-encoded; flatten via Athena UDFs if needed
    ("http_status", "int"),
    ("error_class", "string"),
]


class AuditLayer(Construct):
    """Durable substrate for the application-level audit log.

    Owns: KMS CMK, S3 bucket (Object Lock Compliance), Firehose delivery
    stream (DirectPut → S3 with Parquet conversion), Glue database + table,
    and the DynamoDB hot-mirror table.

    Pass every Lambda that calls `audit.emit(...)` in `emitting_fns`. The
    construct attaches scoped IAM (Firehose:PutRecord, DDB:PutItem on the
    audit table only, kms:GenerateDataKey/Decrypt on the CMK) and sets the
    two environment variables the emitter reads at runtime.
    """

    # The break-glass role name is referenced (not created) so a separate
    # security workflow can manage MFA-required trust policies and rotation
    # without coupling that lifecycle to this stack. The bucket policy
    # below denies destructive S3 actions to every other principal.
    _BREAK_GLASS_ROLE_NAME = "penguin-health-audit-admin-break-glass"

    def __init__(self, scope: Construct, id: str, *,
                 emitting_fns: list[_lambda.IFunction]) -> None:
        super().__init__(scope, id)

        account = Stack.of(self).account
        region = Stack.of(self).region

        # ----- KMS CMK ------------------------------------------------
        # Single CMK protects the bucket, the DDB table, and Firehose's
        # at-rest encryption. Rotation reduces blast radius from a leaked
        # data key; the CMK itself never leaves KMS.
        self.key = kms.Key(self, "AuditKey",
            alias=f"alias/{config.PROJECT_NAME}-audit",
            description=(
                "Encrypts the penguin-health audit subsystem: WORM S3 "
                "bucket, DynamoDB hot mirror, Firehose stream."
            ),
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ----- S3 WORM bucket ----------------------------------------
        # Object Lock COMPLIANCE mode means even the root account cannot
        # delete or shorten retention before the retention period elapses.
        # This is the integrity story OCR auditors look for.
        self.bucket = s3.Bucket(self, "AuditBucket",
            bucket_name=f"{config.PROJECT_NAME}-audit",
            object_lock_enabled=True,
            object_lock_default_retention=s3.ObjectLockRetention.compliance(
                duration=Duration.days(7 * 365),
            ),
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Deny destructive S3 actions for every principal except the named
        # break-glass role. Object Lock already prevents deletion before
        # retention elapses, but this policy stops anyone from waiting out
        # the retention or bypassing governance once retention does expire.
        break_glass_arn = (
            f"arn:aws:iam::{account}:role/{self._BREAK_GLASS_ROLE_NAME}"
        )
        self.bucket.add_to_resource_policy(iam.PolicyStatement(
            sid="DenyDestructiveActionsExceptBreakGlass",
            effect=iam.Effect.DENY,
            principals=[iam.AnyPrincipal()],
            actions=[
                "s3:DeleteObject",
                "s3:DeleteObjectVersion",
                "s3:PutObjectRetention",
                "s3:PutObjectLegalHold",
                "s3:BypassGovernanceRetention",
                "s3:PutBucketObjectLockConfiguration",
            ],
            resources=[
                self.bucket.bucket_arn,
                f"{self.bucket.bucket_arn}/*",
            ],
            conditions={
                "StringNotEquals": {"aws:PrincipalArn": break_glass_arn},
            },
        ))

        # ----- Glue database + table ---------------------------------
        # The Glue table doubles as Firehose's schema source for Parquet
        # conversion. Adding a column means updating this list AND bumping
        # `schema_version` in lambda/multi-org/audit/schema.py.
        self.glue_database = glue.CfnDatabase(self, "AuditDatabase",
            catalog_id=account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=AUDIT_GLUE_DATABASE,
                description=(
                    "Penguin Health audit events. Source of truth for "
                    "PHI-access investigations and § 164.308(a)(1)(ii)(D) "
                    "activity review."
                ),
            ),
        )
        self.glue_table = glue.CfnTable(self, "AuditEventsTable",
            catalog_id=account,
            database_name=AUDIT_GLUE_DATABASE,
            table_input=glue.CfnTable.TableInputProperty(
                name=AUDIT_GLUE_TABLE,
                description=(
                    "Immutable audit events for every PHI access and "
                    "PHI-bearing write. Schema mirrors "
                    "lambda/multi-org/audit/schema.py — bump "
                    "`schema_version` and update both on changes."
                ),
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    # Partition projection — no MSCK REPAIR needed. Firehose
                    # writes year=/month=/day= prefixes via dynamic partitioning.
                    "projection.enabled": "true",
                    "projection.year.type": "integer",
                    "projection.year.range": "2026,2035",
                    "projection.month.type": "integer",
                    "projection.month.range": "1,12",
                    "projection.month.digits": "2",
                    "projection.day.type": "integer",
                    "projection.day.range": "1,31",
                    "projection.day.digits": "2",
                    "storage.location.template": (
                        f"s3://{self.bucket.bucket_name}/"
                        "year=${year}/month=${month}/day=${day}/"
                    ),
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="year", type="int"),
                    glue.CfnTable.ColumnProperty(name="month", type="int"),
                    glue.CfnTable.ColumnProperty(name="day", type="int"),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{self.bucket.bucket_name}/",
                    input_format=(
                        "org.apache.hadoop.hive.ql.io.parquet."
                        "MapredParquetInputFormat"
                    ),
                    output_format=(
                        "org.apache.hadoop.hive.ql.io.parquet."
                        "MapredParquetOutputFormat"
                    ),
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library=(
                            "org.apache.hadoop.hive.ql.io.parquet.serde."
                            "ParquetHiveSerDe"
                        ),
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name=name, type=type_)
                        for name, type_ in _AUDIT_EVENT_COLUMNS
                    ],
                ),
            ),
        )
        self.glue_table.add_dependency(self.glue_database)

        # ----- Firehose delivery stream ------------------------------
        # DirectPut from Lambda. 60s buffer is the buffering / cost
        # / freshness trade-off — short enough that Athena queries see
        # near-real-time activity, long enough to keep Parquet files
        # sized for efficient scans. KMS CMK on the stream itself; the
        # destination bucket also uses KMS so the data is encrypted
        # twice in transit through this pipeline.
        firehose_role = iam.Role(self, "AuditFirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            description=(
                "Firehose role: writes audit events to the WORM bucket "
                "and reads the Glue table for Parquet schema conversion."
            ),
        )
        self.bucket.grant_write(firehose_role)
        self.key.grant_encrypt_decrypt(firehose_role)
        # Attach Glue permissions as a *managed* policy on the role rather
        # than via `add_to_policy` (which creates an `IAM::Policy`
        # AttachedPolicy resource that is *not* in the Firehose stream's
        # dependency graph). CloudFormation will then provision this
        # policy as part of the role itself, so when Firehose validates
        # the Glue schema during stream creation, the role already has
        # the necessary permissions. The construct attaches itself via
        # `roles=[firehose_role]`; the local name isn't needed.
        iam.ManagedPolicy(self, "AuditFirehoseGluePolicy",
            description="Firehose schema-conversion Glue read access",
            statements=[iam.PolicyStatement(
                actions=[
                    "glue:GetTable",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions",
                ],
                resources=[
                    f"arn:aws:glue:{region}:{account}:catalog",
                    f"arn:aws:glue:{region}:{account}:database/{AUDIT_GLUE_DATABASE}",
                    (
                        f"arn:aws:glue:{region}:{account}:"
                        f"table/{AUDIT_GLUE_DATABASE}/{AUDIT_GLUE_TABLE}"
                    ),
                ],
            )],
            roles=[firehose_role],
        )
        # Firehose writes its own CloudWatch logs for delivery failures —
        # the alternative is silent loss, which is unacceptable for audit.
        log_group_arn = (
            f"arn:aws:logs:{region}:{account}:log-group:"
            f"/aws/kinesisfirehose/{config.PROJECT_NAME}-audit:*"
        )
        firehose_role.add_to_policy(iam.PolicyStatement(
            actions=["logs:CreateLogStream", "logs:PutLogEvents"],
            resources=[log_group_arn],
        ))

        self.stream = firehose.CfnDeliveryStream(self, "AuditFirehose",
            delivery_stream_name=f"{config.PROJECT_NAME}-audit",
            delivery_stream_type="DirectPut",
            delivery_stream_encryption_configuration_input=(
                firehose.CfnDeliveryStream
                .DeliveryStreamEncryptionConfigurationInputProperty(
                    key_type="CUSTOMER_MANAGED_CMK",
                    key_arn=self.key.key_arn,
                )
            ),
            extended_s3_destination_configuration=(
                firehose.CfnDeliveryStream
                .ExtendedS3DestinationConfigurationProperty(
                    bucket_arn=self.bucket.bucket_arn,
                    role_arn=firehose_role.role_arn,
                    # Firehose requires SizeInMBs >= 64 whenever
                    # data-format conversion (Parquet here) is enabled,
                    # so the 5MB size we'd otherwise prefer is not an
                    # option. The 60s interval still bounds Athena
                    # freshness — most buffers will flush on time
                    # rather than size at our event volume.
                    buffering_hints=(
                        firehose.CfnDeliveryStream.BufferingHintsProperty(
                            interval_in_seconds=60,
                            size_in_m_bs=64,
                        )
                    ),
                    compression_format="UNCOMPRESSED",  # Parquet handles compression
                    encryption_configuration=(
                        firehose.CfnDeliveryStream.EncryptionConfigurationProperty(
                            kms_encryption_config=(
                                firehose.CfnDeliveryStream
                                .KMSEncryptionConfigProperty(
                                    awskms_key_arn=self.key.key_arn,
                                )
                            ),
                        )
                    ),
                    cloud_watch_logging_options=(
                        firehose.CfnDeliveryStream
                        .CloudWatchLoggingOptionsProperty(
                            enabled=True,
                            log_group_name=f"/aws/kinesisfirehose/{config.PROJECT_NAME}-audit",
                            log_stream_name="S3Delivery",
                        )
                    ),
                    # Dynamic partitioning: Firehose evaluates the JQ
                    # expression on each record to build the prefix.
                    # MetadataExtraction is the only way to turn record
                    # content into partition keys for Parquet.
                    dynamic_partitioning_configuration=(
                        firehose.CfnDeliveryStream
                        .DynamicPartitioningConfigurationProperty(
                            enabled=True,
                            retry_options=(
                                firehose.CfnDeliveryStream.RetryOptionsProperty(
                                    duration_in_seconds=300,
                                )
                            ),
                        )
                    ),
                    processing_configuration=(
                        firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                            enabled=True,
                            processors=[
                                firehose.CfnDeliveryStream.ProcessorProperty(
                                    type="MetadataExtraction",
                                    parameters=[
                                        firehose.CfnDeliveryStream
                                        .ProcessorParameterProperty(
                                            parameter_name="MetadataExtractionQuery",
                                            parameter_value=(
                                                "{year:.event_time[0:4],"
                                                "month:.event_time[5:7],"
                                                "day:.event_time[8:10]}"
                                            ),
                                        ),
                                        firehose.CfnDeliveryStream
                                        .ProcessorParameterProperty(
                                            parameter_name="JsonParsingEngine",
                                            parameter_value="JQ-1.6",
                                        ),
                                    ],
                                ),
                                # AppendDelimiterToRecord ensures Firehose's
                                # JSON deserializer can split records — without
                                # it, batched puts run together.
                                firehose.CfnDeliveryStream.ProcessorProperty(
                                    type="AppendDelimiterToRecord",
                                    parameters=[
                                        firehose.CfnDeliveryStream
                                        .ProcessorParameterProperty(
                                            parameter_name="Delimiter",
                                            parameter_value="\\n",
                                        ),
                                    ],
                                ),
                            ],
                        )
                    ),
                    data_format_conversion_configuration=(
                        firehose.CfnDeliveryStream
                        .DataFormatConversionConfigurationProperty(
                            enabled=True,
                            input_format_configuration=(
                                firehose.CfnDeliveryStream
                                .InputFormatConfigurationProperty(
                                    deserializer=(
                                        firehose.CfnDeliveryStream.DeserializerProperty(
                                            open_x_json_ser_de=(
                                                firehose.CfnDeliveryStream
                                                .OpenXJsonSerDeProperty()
                                            ),
                                        )
                                    ),
                                )
                            ),
                            output_format_configuration=(
                                firehose.CfnDeliveryStream
                                .OutputFormatConfigurationProperty(
                                    serializer=(
                                        firehose.CfnDeliveryStream.SerializerProperty(
                                            parquet_ser_de=(
                                                firehose.CfnDeliveryStream
                                                .ParquetSerDeProperty(
                                                    compression="SNAPPY",
                                                )
                                            ),
                                        )
                                    ),
                                )
                            ),
                            schema_configuration=(
                                firehose.CfnDeliveryStream
                                .SchemaConfigurationProperty(
                                    catalog_id=account,
                                    database_name=AUDIT_GLUE_DATABASE,
                                    table_name=AUDIT_GLUE_TABLE,
                                    region=region,
                                    role_arn=firehose_role.role_arn,
                                )
                            ),
                        )
                    ),
                    prefix=(
                        "year=!{partitionKeyFromQuery:year}/"
                        "month=!{partitionKeyFromQuery:month}/"
                        "day=!{partitionKeyFromQuery:day}/"
                    ),
                    error_output_prefix=(
                        "errors/!{firehose:error-output-type}/"
                        "year=!{timestamp:yyyy}/month=!{timestamp:MM}/"
                        "day=!{timestamp:dd}/"
                    ),
                )
            ),
        )
        self.stream.add_dependency(self.glue_table)

        # ----- DDB hot mirror ----------------------------------------
        # Schema mirrors penguin-health-stedi's AUDIT# rows exactly so
        # `recent_checks_for_patient` keeps working post-cutover. Lives in
        # this component (not database.py) so the audit subsystem's blast
        # radius is isolated — IAM grants here only ever reference this
        # construct's tables, and a future "delete database.py" refactor
        # cannot drop the audit substrate.
        self.table = dynamodb.Table(self, "AuditTable",
            table_name=f"{config.PROJECT_NAME}-audit",
            partition_key=dynamodb.Attribute(
                name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(
                name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expires_at",
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=self.key,
            point_in_time_recovery_specification=(
                dynamodb.PointInTimeRecoverySpecification(
                    point_in_time_recovery_enabled=True,
                )
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )
        # GSI1: gsi1pk=PATIENT#{org_id}#{patient_hash}, gsi1sk=event_time.
        # Drives the 30-minute eligibility dedup pre-check today, plus
        # "who looked up this patient" investigations going forward.
        self.table.add_global_secondary_index(
            index_name="gsi1",
            partition_key=dynamodb.Attribute(
                name="gsi1pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(
                name="gsi1sk", type=dynamodb.AttributeType.STRING),
        )

        # ----- Grants to emitting Lambdas ---------------------------
        # Scoped to the exact stream and table ARNs — no wildcards. The
        # KMS grant is via `grant_encrypt_decrypt` because the Lambda
        # both writes (DDB PutItem → KMS Encrypt) and reads (DDB Query
        # for dedup → KMS Decrypt).
        for fn in emitting_fns:
            self._grant_emit(fn)

    # ------------------------------------------------------------------
    # IAM
    # ------------------------------------------------------------------

    def _grant_emit(self, fn: _lambda.IFunction) -> None:
        """Attach the minimum-privilege policy a Lambda needs to call
        `audit.emit(...)`. Can also be invoked by the stack after
        construction for Lambdas that are created later than this
        construct (e.g. lazy-built per-feature Lambdas).
        """
        fn.add_to_role_policy(iam.PolicyStatement(
            actions=["firehose:PutRecord", "firehose:PutRecordBatch"],
            resources=[self.stream.attr_arn],
        ))
        fn.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:PutItem"],
            resources=[self.table.table_arn],
        ))
        fn.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query"],
            resources=[
                self.table.table_arn,
                f"{self.table.table_arn}/index/gsi1",
            ],
        ))
        self.key.grant_encrypt_decrypt(fn)
        fn.add_environment("AUDIT_TABLE_NAME", self.table.table_name)
        fn.add_environment("AUDIT_FIREHOSE_NAME",
                           self.stream.delivery_stream_name or
                           f"{config.PROJECT_NAME}-audit")
