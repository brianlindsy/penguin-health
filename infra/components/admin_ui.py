"""
Admin UI construct: Cognito, API Gateway, Lambda, S3, CloudFront
for the organization admin dashboard.
"""

import hashlib
import os
from aws_cdk import (
    AssetHashType,
    Duration,
    RemovalPolicy,
    BundlingOptions,
    aws_cognito as cognito,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_apigatewayv2 as apigwv2,
    aws_events as events,
    aws_events_targets as targets,
)
from aws_cdk.aws_apigatewayv2_integrations import HttpLambdaIntegration
from aws_cdk.aws_apigatewayv2_authorizers import HttpJwtAuthorizer
from constructs import Construct

import config
from components.bundler import (
    DirectoryBundler,
    PipInstallBundler,
)


def _hash_sources(paths):
    """SHA-256 over the contents of every Python file under each path.

    CDK's default asset hashing walks the `Code.from_asset()` source directory,
    so files the local bundler pulls in from *other* directories (e.g.
    lambda/multi-org/stedi) are invisible — edits there don't change the
    asset hash and CDK skips the upload. This helper makes the hash depend
    on every source the bundler actually copies, so any edit triggers a
    redeploy.
    """
    h = hashlib.sha256()
    for path in sorted(paths):
        if os.path.isfile(path):
            h.update(path.encode())
            with open(path, 'rb') as f:
                h.update(f.read())
        elif os.path.isdir(path):
            for root, _dirs, files in os.walk(path):
                # Skip caches and bytecode — they don't affect runtime behavior
                # but their mtimes churn and would defeat the cache.
                if '__pycache__' in root:
                    continue
                for name in sorted(files):
                    if name.endswith('.pyc'):
                        continue
                    fp = os.path.join(root, name)
                    h.update(os.path.relpath(fp, path).encode())
                    with open(fp, 'rb') as f:
                        h.update(f.read())
    return h.hexdigest()


class AdminUi(Construct):

    def __init__(self, scope: Construct, id: str, *,
                 org_config_table: dynamodb.ITable,
                 validation_results_table: dynamodb.ITable,
                 analytics_reports_table: dynamodb.ITable,
                 deep_jobs_table: dynamodb.ITable,
                 stedi_table: dynamodb.ITable,
                 centralreach_state_machine=None) -> None:
        # centralreach_state_machine is optional so the admin UI can be
        # deployed standalone (e.g., a dev stack without the CentralReach
        # ingest path). When provided, the admin Lambda gets the ARN as
        # an env var + IAM grants to call StartExecution /
        # ListExecutions / DescribeExecution on it.
        super().__init__(scope, id)

        # ----- Cognito -----
        self.user_pool = cognito.UserPool(self, "AdminUserPool",
            user_pool_name=f"{config.PROJECT_NAME}-admin-pool",
            self_sign_up_enabled=False,
            sign_in_aliases=cognito.SignInAliases(email=True),
            password_policy=cognito.PasswordPolicy(
                min_length=12,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=True,
            ),
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            removal_policy=RemovalPolicy.RETAIN,
            # Custom attributes for RBAC
            custom_attributes={
                "organization_id": cognito.StringAttribute(
                    mutable=False,  # Immutable - only admins can set via Admin API
                ),
            },
        )

        self.app_client = self.user_pool.add_client("AdminAppClient",
            user_pool_client_name=f"{config.PROJECT_NAME}-admin-app",
            auth_flows=cognito.AuthFlow(
                user_srp=True,
            ),
            id_token_validity=Duration.hours(1),
            access_token_validity=Duration.hours(1),
            refresh_token_validity=Duration.days(30),
            # Note: By NOT specifying read_attributes/write_attributes,
            # Cognito will include all readable attributes in tokens by default.
            # This is safer for compatibility. The custom:organization_id attribute
            # is already set as mutable=False on the User Pool, so users can't modify it.
        )

        cognito.CfnUserPoolGroup(self, "AdminsGroup",
            user_pool_id=self.user_pool.user_pool_id,
            group_name="Admins",
            description="Admin users with full access to organization configuration",
        )

        # ----- Admin API Lambda -----
        lambda_api_dir = os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "api")
        lambda_multi_org_dir = os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "multi-org")
        rules_engine_dir = os.path.join(lambda_multi_org_dir, "rules-engine")
        stedi_pkg_dir = os.path.join(lambda_multi_org_dir, "stedi")
        notifications_pkg_dir = os.path.join(lambda_multi_org_dir, "notifications")
        centralreach_pkg_dir = os.path.join(lambda_multi_org_dir, "centralreach")

        # Three modules live in rules-engine/ but are bundled flat into the
        # admin_api asset so `from bedrock_client import ...` resolves at
        # the asset root the same way it does in the rules-engine Lambda.
        # bedrock_client owns invoke_claude_model + JSON extraction;
        # claude_cost emits per-org CloudWatch cost metrics; rate_limiter
        # is bedrock_client's transitive dependency.
        shared_llm_modules = ["bedrock_client.py", "claude_cost.py", "rate_limiter.py"]

        audit_pkg_dir = os.path.join(lambda_multi_org_dir, "audit")

        admin_api_sources = [
            os.path.join(lambda_api_dir, "admin_api.py"),
            os.path.join(lambda_api_dir, "permissions.py"),
            os.path.join(lambda_api_dir, "analytics_helpers.py"),
            os.path.join(lambda_api_dir, "nl_agent.py"),
            os.path.join(lambda_api_dir, "nl_agent_tools.py"),
            os.path.join(lambda_api_dir, "eligibility_api.py"),
            os.path.join(lambda_api_dir, "eligibility_worklist_api.py"),
            os.path.join(lambda_api_dir, "centralreach_api.py"),
            os.path.join(lambda_api_dir, "sqlparse"),
            stedi_pkg_dir,
            notifications_pkg_dir,
            audit_pkg_dir,
            centralreach_pkg_dir,
        ] + [os.path.join(rules_engine_dir, m) for m in shared_llm_modules]

        self.api_function = _lambda.Function(self, "AdminApiFunction",
            function_name=f"{config.PROJECT_NAME}-admin-api",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="admin_api.lambda_handler",
            code=_lambda.Code.from_asset(
                lambda_api_dir,
                exclude=[
                    "*",
                    "!admin_api.py",
                    "!permissions.py",
                    "!analytics_helpers.py",
                    "!nl_agent.py",
                    "!nl_agent_tools.py",
                    "!eligibility_api.py",
                    "!eligibility_worklist_api.py",
                    "!centralreach_api.py",
                    "!sqlparse/**",
                ],
                asset_hash_type=AssetHashType.CUSTOM,
                asset_hash=_hash_sources(admin_api_sources),
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=DirectoryBundler([
                        (os.path.join(lambda_api_dir, "admin_api.py"), None),
                        (os.path.join(lambda_api_dir, "permissions.py"), None),
                        (os.path.join(lambda_api_dir, "analytics_helpers.py"), None),
                        (os.path.join(lambda_api_dir, "nl_agent.py"), None),
                        (os.path.join(lambda_api_dir, "nl_agent_tools.py"), None),
                        (os.path.join(lambda_api_dir, "eligibility_api.py"), None),
                        (os.path.join(lambda_api_dir, "eligibility_worklist_api.py"), None),
                        (os.path.join(lambda_api_dir, "centralreach_api.py"), None),
                        (os.path.join(lambda_api_dir, "sqlparse"), None),
                        (stedi_pkg_dir, "stedi"),
                        (notifications_pkg_dir, "notifications"),
                        (audit_pkg_dir, "audit"),
                        (centralreach_pkg_dir, "centralreach"),
                    ] + [
                        (os.path.join(rules_engine_dir, m), None)
                        for m in shared_llm_modules
                    ]),
                ),
            ),
            timeout=Duration.seconds(60),  # Longer timeout for LLM calls
            memory_size=config.LAMBDA_DEFAULT_MEMORY_MB,
            environment={
                "DYNAMODB_TABLE": org_config_table.table_name,
                "ORG_CONFIG_TABLE_NAME": org_config_table.table_name,
                "COGNITO_USER_POOL_ID": self.user_pool.user_pool_id,
                "ANALYTICS_REPORTS_TABLE": analytics_reports_table.table_name,
                "DEEP_JOBS_TABLE": deep_jobs_table.table_name,
                "DEEP_WORKER_LAMBDA": f"{config.PROJECT_NAME}-deep-analytics-worker",
                "STEDI_TABLE_NAME": stedi_table.table_name,
                "STEDI_API_KEY_SECRET": "penguin-health/stedi/api-key",
            },
        )

        # Agent intermediate payloads spill into each org's own bucket
        # (`penguin-health-{org_id}`) under the `agent-io/` prefix —
        # same compliance boundary as Athena's `athena-results/` output,
        # never a shared cross-org bucket. Operators should apply a 1-day
        # S3 lifecycle rule on `agent-io/` per org bucket to match the
        # DynamoDB job-item TTL. The worker IAM grant for `s3:PutObject`
        # on `penguin-health-*` (further below) already covers this path.

        # ----- Deep Analytics Worker Lambda -----
        # Same code bundle as the api function, but invoked async by the
        # api lambda to drive the agent loop without the 30s API Gateway
        # HTTP API integration timeout.
        deep_worker_sources = [
            os.path.join(lambda_api_dir, "admin_api.py"),
            os.path.join(lambda_api_dir, "permissions.py"),
            os.path.join(lambda_api_dir, "analytics_helpers.py"),
            os.path.join(lambda_api_dir, "nl_agent.py"),
            os.path.join(lambda_api_dir, "nl_agent_tools.py"),
            os.path.join(lambda_api_dir, "sqlparse"),
            notifications_pkg_dir,
            audit_pkg_dir,
        ] + [os.path.join(rules_engine_dir, m) for m in shared_llm_modules]

        self.deep_worker_function = _lambda.Function(self, "DeepAnalyticsWorkerFunction",
            function_name=f"{config.PROJECT_NAME}-deep-analytics-worker",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="admin_api.deep_worker_handler",
            code=_lambda.Code.from_asset(
                lambda_api_dir,
                exclude=[
                    "*",
                    "!admin_api.py",
                    "!permissions.py",
                    "!analytics_helpers.py",
                    "!nl_agent.py",
                    "!nl_agent_tools.py",
                    "!sqlparse/**",
                ],
                asset_hash_type=AssetHashType.CUSTOM,
                asset_hash=_hash_sources(deep_worker_sources),
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=DirectoryBundler([
                        (os.path.join(lambda_api_dir, "admin_api.py"), None),
                        (os.path.join(lambda_api_dir, "permissions.py"), None),
                        (os.path.join(lambda_api_dir, "analytics_helpers.py"), None),
                        (os.path.join(lambda_api_dir, "nl_agent.py"), None),
                        (os.path.join(lambda_api_dir, "nl_agent_tools.py"), None),
                        (os.path.join(lambda_api_dir, "sqlparse"), None),
                        (notifications_pkg_dir, "notifications"),
                        (audit_pkg_dir, "audit"),
                    ] + [
                        (os.path.join(rules_engine_dir, m), None)
                        for m in shared_llm_modules
                    ]),
                ),
            ),
            # An agent run is ~5-10 Bedrock turns plus per-row extraction
            # batches; 10 min keeps headroom for the longest extraction.
            timeout=Duration.minutes(10),
            memory_size=config.LAMBDA_DEFAULT_MEMORY_MB,
            environment={
                "DEEP_JOBS_TABLE": deep_jobs_table.table_name,
                "ORG_CONFIG_TABLE_NAME": org_config_table.table_name,
            },
        )

        # ----- FHIR Eligibility Poller Lambda -----
        # EventBridge fires this every 15 minutes per opted-in org. The
        # poller queries the org's FHIR API for new Encounters since the
        # last cursor watermark, fetches each referenced Patient, and runs
        # stedi.orchestrator.verify for each. Pinned to Python 3.13 to match
        # the FHIR materializer's PyJWT[crypto] wheel availability.
        fhir_pkg_dir = os.path.join(lambda_multi_org_dir, "fhir")
        fhir_eligibility_poller_sources = [
            stedi_pkg_dir, fhir_pkg_dir, notifications_pkg_dir, audit_pkg_dir,
        ]

        self.fhir_eligibility_poller_fn = _lambda.Function(
            self, "FhirEligibilityPollerFunction",
            function_name=f"{config.PROJECT_NAME}-fhir-eligibility-poller",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="stedi.fhir_eligibility_poller.handler",
            code=_lambda.Code.from_asset(
                lambda_multi_org_dir,
                exclude=["*", "!stedi/**", "!fhir/**", "!notifications/**", "!audit/**"],
                asset_hash_type=AssetHashType.CUSTOM,
                asset_hash=_hash_sources(fhir_eligibility_poller_sources),
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_13.bundling_image,
                    local=PipInstallBundler(
                        source_paths=[],
                        source_dirs=[
                            (stedi_pkg_dir, "stedi"),
                            (fhir_pkg_dir, "fhir"),
                            (notifications_pkg_dir, "notifications"),
                            (audit_pkg_dir, "audit"),
                        ],
                        requirements=[
                            # PyJWT[crypto] for FHIR private_key_jwt auth.
                            # The [crypto] extra pulls `cryptography`, which
                            # ships manylinux wheels for Python 3.13.
                            "PyJWT[crypto]==2.10.1",
                        ],
                        python_version="3.13",
                    ),
                ),
            ),
            # 200 encounters/poll x worst-case (FHIR + Stedi) latency.
            timeout=Duration.minutes(10),
            memory_size=config.LAMBDA_DEFAULT_MEMORY_MB,
            environment={
                "STEDI_TABLE_NAME": stedi_table.table_name,
                "STEDI_API_KEY_SECRET": "penguin-health/stedi/api-key",
                "ORG_CONFIG_TABLE_NAME": org_config_table.table_name,
            },
        )

        # 15-minute schedule. One rule, multiple per-org targets so adding
        # an org = one entry in this list + add_stedi_config.py run with
        # --census-enabled and --encounter-filter-* flags.
        fhir_eligibility_schedule = events.Rule(
            self, "FhirEligibilityPollerSchedule",
            rule_name=f"{config.PROJECT_NAME}-fhir-eligibility-poller",
            schedule=events.Schedule.rate(Duration.minutes(15)),
        )
        for org_id in ["demo"]:
            fhir_eligibility_schedule.add_target(
                targets.LambdaFunction(
                    self.fhir_eligibility_poller_fn,
                    event=events.RuleTargetInput.from_object({
                        "organization_id": org_id,
                    }),
                ),
            )

        org_config_table.grant_read_write_data(self.api_function)
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query", "dynamodb:Scan", "dynamodb:GetItem",
                     "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"],
            resources=[f"{org_config_table.table_arn}/index/*"],
        ))

        validation_results_table.grant_read_write_data(self.api_function)
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query", "dynamodb:Scan", "dynamodb:GetItem",
                     "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"],
            resources=[f"{validation_results_table.table_arn}/index/*"],
        ))

        analytics_reports_table.grant_read_write_data(self.api_function)
        deep_jobs_table.grant_read_write_data(self.api_function)
        deep_jobs_table.grant_read_write_data(self.deep_worker_function)

        # Stedi eligibility: audit log + daily counter table + the GSI used
        # for "recent checks for this patient" dedup lookups.
        stedi_table.grant_read_write_data(self.api_function)
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query"],
            resources=[f"{stedi_table.table_arn}/index/*"],
        ))

        # Stedi API key in Secrets Manager. One key per Stedi account
        # (not per-org) so we scope to a single secret path.
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[
                f"arn:aws:secretsmanager:{config.AWS_REGION}:*:secret:penguin-health/stedi/*"
            ],
        ))

        # FHIR eligibility poller: reads STEDI_CONFIG + FHIR_CONFIG from
        # org_config_table, writes ENCOUNTER_ITEM# + FHIR_POLL_CURSOR +
        # AUDIT# + USAGE# rows to stedi_table, reads the Stedi API key
        # from Secrets Manager, and signs FHIR JWT assertions via KMS.
        org_config_table.grant_read_data(self.fhir_eligibility_poller_fn)
        stedi_table.grant_read_write_data(self.fhir_eligibility_poller_fn)
        self.fhir_eligibility_poller_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query"],
            resources=[
                f"{stedi_table.table_arn}/index/*",
                # Read subscriber lists for opt-in eligibility notifications.
                f"{org_config_table.table_arn}/index/*",
            ],
        ))
        self.fhir_eligibility_poller_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[
                f"arn:aws:secretsmanager:{config.AWS_REGION}:*:secret:penguin-health/stedi/*"
            ],
        ))
        # KMS: per-org FHIR signing key, alias-scoped exactly like the
        # FHIR encounter materializer's grant in audit_engine.py.
        self.fhir_eligibility_poller_fn.add_to_role_policy(iam.PolicyStatement(
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
        # Per-poll counters (encounters fetched, errors, cap hits).
        self.fhir_eligibility_poller_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["cloudwatch:PutMetricData"],
            resources=["*"],
            conditions={
                "StringEquals": {
                    "cloudwatch:namespace": "PenguinHealth/FhirEligibilityPoller"
                }
            },
        ))

        # Bedrock permissions for LLM enhancement endpoints
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=[
                "arn:aws:bedrock:*::foundation-model/anthropic.*",
                "arn:aws:bedrock:*:*:inference-profile/*",
            ],
        ))

        # Worker needs Bedrock (driving the agent loop + per-row extraction).
        self.deep_worker_function.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=[
                "arn:aws:bedrock:*::foundation-model/anthropic.*",
                "arn:aws:bedrock:*:*:inference-profile/*",
            ],
        ))

        # Per-org Claude cost attribution metrics. Namespace-scoped so
        # neither role can write outside PenguinHealth/LLMCost. Both
        # admin API and the agent worker emit; they're each on the hot
        # path for Bedrock calls.
        for fn in (self.api_function, self.deep_worker_function):
            fn.add_to_role_policy(iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "cloudwatch:namespace": "PenguinHealth/LLMCost"
                    }
                },
            ))

        # Worker runs Athena via the run_sql tool — same scope as the api
        # function so per-org workgroup isolation is preserved.
        self.deep_worker_function.add_to_role_policy(iam.PolicyStatement(
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
        self.deep_worker_function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartitions",
            ],
            resources=[
                f"arn:aws:glue:{config.AWS_REGION}:*:catalog",
                f"arn:aws:glue:{config.AWS_REGION}:*:database/penguin_health_analytics",
                f"arn:aws:glue:{config.AWS_REGION}:*:table/penguin_health_analytics/*",
            ],
        ))
        # Worker also reads per-org data buckets (Athena needs s3:GetObject
        # on the source data), writes Athena results to athena-results/,
        # and spills agent intermediate payloads to agent-io/ in the SAME
        # org bucket. Wildcard ARN keeps all of those paths under one
        # compliance-bounded grant.
        self.deep_worker_function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:AbortMultipartUpload",
            ],
            resources=[
                f"arn:aws:s3:::{config.PROJECT_NAME}-*",
                f"arn:aws:s3:::{config.PROJECT_NAME}-*/*",
            ],
        ))

        # Lambda invoke permissions: rules-engine for validation runs,
        # plus the deep-analytics worker which the api function kicks off
        # asynchronously when a deep-analysis job is requested.
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[
                f"arn:aws:lambda:{config.AWS_REGION}:*:function:{config.PROJECT_NAME}-rules-engine-rag",
                self.deep_worker_function.function_arn,
            ],
        ))

        # Analytics: Athena query execution scoped to per-org workgroups.
        self.api_function.add_to_role_policy(iam.PolicyStatement(
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

        # Analytics: Glue metadata reads for the analytics catalog.
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartitions",
            ],
            resources=[
                f"arn:aws:glue:{config.AWS_REGION}:*:catalog",
                f"arn:aws:glue:{config.AWS_REGION}:*:database/penguin_health_analytics",
                f"arn:aws:glue:{config.AWS_REGION}:*:table/penguin_health_analytics/*",
            ],
        ))

        # Analytics: S3 read on per-org data buckets + write to athena-results/ prefix.
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:AbortMultipartUpload",
            ],
            resources=[
                f"arn:aws:s3:::{config.PROJECT_NAME}-*",
                f"arn:aws:s3:::{config.PROJECT_NAME}-*/*",
            ],
        ))

        # ----- Email notifications (SES) -----
        # The admin API itself doesn't currently send mail, but the
        # subscription endpoints + the deep-worker share the same code
        # asset as the validation/eligibility paths; granting ses:SendEmail
        # here keeps the policy in one place. Resource is "*" because the
        # sending identity ARN isn't known to this stack — DNS verification
        # of penguinhealth.io is an out-of-band step. Tighten to the
        # specific identity ARN once it's verified and added to config.
        email_from_address = "noreply@penguinhealth.io"
        admin_ui_base_url = "https://app.penguinhealth.io"
        ses_send_email = iam.PolicyStatement(
            actions=["ses:SendEmail", "ses:SendRawEmail"],
            resources=["*"],
        )
        email_env = {
            "EMAIL_FROM_ADDRESS": email_from_address,
            "EMAIL_REPLY_TO": email_from_address,
            "ADMIN_UI_BASE_URL": admin_ui_base_url,
        }
        for fn in (self.api_function, self.fhir_eligibility_poller_fn):
            fn.add_to_role_policy(ses_send_email)
            for k, v in email_env.items():
                fn.add_environment(k, v)

        # ----- RPA: state-machine ARN + StartExecution / read grants -----
        # Wires `lambda/api/centralreach_api.py` to the Step Functions
        # state machine built by `components/centralreach.py`. Optional:
        # when no CentralReach construct is passed, the API still imports
        # centralreach_api fine (its handlers raise at call-time if the
        # env var is missing) and the four CentralReach routes just
        # return 500 — useful for dev stacks that don't deploy the
        # CentralReach ingest runtime.
        if centralreach_state_machine is not None:
            from aws_cdk import ArnFormat, Stack
            self.api_function.add_environment(
                "CENTRALREACH_STATE_MACHINE_ARN",
                centralreach_state_machine.state_machine_arn,
            )
            # Execution ARN format is `arn:aws:states:REGION:ACCT:execution:NAME:EXEC`
            # (colon between resource type and name, NOT slash). CDK's
            # default arn_format is slash-separated; we explicitly pass
            # COLON_RESOURCE_NAME so the resulting Resource is what IAM
            # will actually evaluate at runtime. Python string ops on
            # the L2 state_machine_arn token can't be used here — the
            # value is unresolved at synth time, so any string munging
            # silently produces broken ARNs.
            stack = Stack.of(self)
            execution_arn_pattern = stack.format_arn(
                service="states",
                resource="execution",
                resource_name=f"{centralreach_state_machine.state_machine_name}:*",
                arn_format=ArnFormat.COLON_RESOURCE_NAME,
            )
            self.api_function.add_to_role_policy(iam.PolicyStatement(
                actions=[
                    "states:StartExecution",
                ],
                resources=[centralreach_state_machine.state_machine_arn],
            ))
            self.api_function.add_to_role_policy(iam.PolicyStatement(
                actions=[
                    "states:ListExecutions",
                    "states:DescribeExecution",
                ],
                resources=[
                    centralreach_state_machine.state_machine_arn,
                    execution_arn_pattern,
                ],
            ))

        # ----- API Gateway HTTP API -----
        jwt_authorizer = HttpJwtAuthorizer(
            "CognitoAuthorizer",
            jwt_issuer=f"https://cognito-idp.{config.AWS_REGION}.amazonaws.com/{self.user_pool.user_pool_id}",
            jwt_audience=[self.app_client.user_pool_client_id],
        )

        integration = HttpLambdaIntegration("AdminApiIntegration", self.api_function)

        self.http_api = apigwv2.HttpApi(self, "AdminHttpApi",
            api_name=f"{config.PROJECT_NAME}-admin-api",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[
                    apigwv2.CorsHttpMethod.GET,
                    apigwv2.CorsHttpMethod.PUT,
                    apigwv2.CorsHttpMethod.POST,
                    apigwv2.CorsHttpMethod.DELETE,
                    apigwv2.CorsHttpMethod.OPTIONS,
                ],
                allow_headers=["Authorization", "Content-Type"],
                max_age=Duration.hours(1),
            ),
        )

        routes = [
            ("GET",  "/api/organizations"),
            ("GET",  "/api/organizations/{orgId}"),
            ("GET",  "/api/organizations/{orgId}/rules"),
            ("GET",  "/api/organizations/{orgId}/rules/{ruleId}"),
            ("PUT",  "/api/organizations/{orgId}/rules/{ruleId}"),
            ("POST", "/api/organizations/{orgId}/rules"),
            ("GET",  "/api/organizations/{orgId}/rules-config"),
            ("PUT",  "/api/organizations/{orgId}/rules-config"),
            ("POST", "/api/organizations/{orgId}/rules/enhance-fields"),
            ("POST", "/api/organizations/{orgId}/rules/enhance-note"),
            ("POST", "/api/organizations/{orgId}/analytics/nl-query"),
            ("POST", "/api/organizations/{orgId}/analytics/nl-query/deep"),
            ("GET",  "/api/organizations/{orgId}/analytics/nl-query/deep/{jobId}"),
            ("POST", "/api/organizations/{orgId}/analytics/reports"),
            ("GET",  "/api/organizations/{orgId}/analytics/reports"),
            ("GET",  "/api/organizations/{orgId}/analytics/reports/{reportId}"),
            ("DELETE", "/api/organizations/{orgId}/analytics/reports/{reportId}"),
            ("GET",  "/api/organizations/{orgId}/validation-runs"),
            ("POST", "/api/organizations/{orgId}/validation-runs"),
            ("GET",  "/api/organizations/{orgId}/validation-runs/{runId}"),
            ("GET",  "/api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}"),
            ("PUT",  "/api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/confirm-finding"),
            ("PUT",  "/api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/mark-resolved"),
            ("PUT",  "/api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/mark-incorrect"),
            ("GET",  "/api/me/permissions"),
            ("GET",  "/api/organizations/{orgId}/subscriptions"),
            ("PUT",  "/api/organizations/{orgId}/subscriptions/{email}"),
            ("GET",  "/api/organizations/{orgId}/users"),
            ("GET",  "/api/organizations/{orgId}/users/{email}"),
            ("PUT",  "/api/organizations/{orgId}/users/{email}"),
            ("DELETE", "/api/organizations/{orgId}/users/{email}"),
            ("GET",  "/api/organizations/{orgId}/programs"),
            ("PUT",  "/api/organizations/{orgId}/programs"),
            ("POST", "/api/organizations/{orgId}/eligibility/verify"),
            ("GET",  "/api/organizations/{orgId}/eligibility/history"),
            ("GET",  "/api/organizations/{orgId}/eligibility/config"),
            ("PUT",  "/api/organizations/{orgId}/eligibility/config"),
            ("GET",  "/api/organizations/{orgId}/eligibility/encounters"),
            ("PUT",  "/api/organizations/{orgId}/eligibility/encounters/{encounterId}/resolve"),
            ("POST", "/api/organizations/{orgId}/eligibility/encounters/{encounterId}/rerun"),
        ]

        for method, path in routes:
            self.http_api.add_routes(
                path=path,
                methods=[getattr(apigwv2.HttpMethod, method)],
                integration=integration,
                authorizer=jwt_authorizer,
            )

        # ----- S3 Bucket for frontend -----
        self.frontend_bucket = s3.Bucket(self, "FrontendBucket",
            bucket_name=f"{config.PROJECT_NAME}-admin-ui",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ----- CloudFront Distribution -----
        api_origin = origins.HttpOrigin(
            f"{self.http_api.http_api_id}.execute-api.{config.AWS_REGION}.amazonaws.com",
        )

        self.distribution = cloudfront.Distribution(self, "AdminDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(self.frontend_bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
            ),
            additional_behaviors={
                "/api/*": cloudfront.BehaviorOptions(
                    origin=api_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                ),
            },
            default_root_object="index.html",
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(0),
                ),
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(0),
                ),
            ],
            minimum_protocol_version=cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
        )
