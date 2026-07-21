"""Rules engine Fargate task + Step Functions + per-org EventBridge schedules.

Owns everything the per-rule LLM validation flow needs at runtime:

  * a Fargate task definition on the shared CentralReach ECS cluster,
    built from `fargate/rules_engine/Dockerfile`;
  * a Step Functions state machine that wraps `RunTask` so per-run
    state is visible in the SFN console;
  * three per-org EventBridge rules that fire the state machine on the
    same cadence the previous Lambda cron used (Monday catch-up + Tue-Fri
    daily).

The admin Lambda (admin_ui.py) talks to the state machine to trigger
manual runs — the wiring lives in admin_ui.py, gated on
`rules_engine_state_machine` being passed in.

VPC/cluster reuse:
  The CentralReach component already owns a private-subnet VPC with a
  NAT gateway and an ECS cluster. The rules-engine task has the same
  egress needs (Bedrock, S3, DDB, SES, EventBridge), so we reuse both
  rather than paying for a second NAT hourly. The task role is
  independent — a compromised rules-engine task cannot read CR
  credentials or use the ingest cursor table.

EventBridge input serialization:
  CDK's `RuleTargetInput` strips top-level `None` values from the
  emitted CloudFormation payload, so we can't send a literal `null`
  for the optional keys (validation_run_id / categories / dates). We
  send the placeholder string `"__NULL__"` instead — the runner's
  `_plain_env` / `_parse_json_env` treat it as absent, same as an
  unset env var. This keeps the SFN Normalize step's `JsonPath`
  lookups working without a separate defaulting Choice state.
"""

from __future__ import annotations

from pathlib import Path

from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_logs as logs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct

import config


# Per-org validation schedules. Same UTC times as the previous Lambda
# cron. Each org has two rules:
#   Monday    -> validates Sat + Sun + Mon (weekend catch-up)
#   Tue-Fri   -> validates today's ingest only
# Sat/Sun deliveries wait for Monday's catch-up.
_PER_ORG_SCHEDULES: list[dict] = [
    {"org_id": "catholic-charities-multi-org", "hour": "10", "minute": "0",
     "rule_prefix": "catholic-charities-validation",
     "label": "CatholicCharities"},
    {"org_id": "circles-of-care", "hour": "11", "minute": "15",
     "rule_prefix": "circles-of-care-validation",
     "label": "CirclesOfCare"},
    # 12:00 UTC = 07:00 EST / 08:00 EDT. Runs 5h after the 02:00 ET
    # CentralReach ingest, so same-day data is available.
    {"org_id": "supportive-care", "hour": "12", "minute": "0",
     "rule_prefix": "supportive-care-validation",
     "label": "SupportiveCare"},
]


class RulesEngine(Construct):

    def __init__(self, scope: Construct, construct_id: str, *,
                 cluster: ecs.ICluster,
                 vpc: ec2.IVpc,
                 org_config_table: dynamodb.ITable,
                 validation_results_table: dynamodb.ITable,
                 narrative_hashes_table: dynamodb.ITable,
                 document_queue_table: dynamodb.ITable) -> None:
        super().__init__(scope, construct_id)

        self.cluster = cluster
        self.vpc = vpc

        # ----- Container image -----------------------------------------
        # CDK builds + pushes the image at deploy time. Build context is
        # the repo root because the Dockerfile pulls from both
        # `fargate/rules_engine/` and `lambda/multi-org/`.
        repo_root = Path(__file__).resolve().parents[2]
        self.image_asset = ecr_assets.DockerImageAsset(
            self, "RulesEngineRunnerImage",
            directory=str(repo_root),
            file="fargate/rules_engine/Dockerfile",
            # Pin the build platform so an Apple-Silicon developer
            # doesn't ship an arm64 image to an x86_64 task.
            platform=ecr_assets.Platform.LINUX_AMD64,
        )

        # ----- Task IAM role -------------------------------------------
        self.task_role = iam.Role(
            self, "RulesEngineTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description="Penguin Health rules engine validation task role",
        )

        # S3: same wildcard the Lambda had. Per-org PHI buckets are named
        # `penguin-health-{org_id}`. The JWKS bucket `phealth-fhir-jwks`
        # deliberately does not match this wildcard.
        s3_bucket_arn = f"arn:aws:s3:::{config.PROJECT_NAME}-*"
        self.task_role.add_to_policy(iam.PolicyStatement(
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
        ))

        # DDB: read org_config, read+write validation_results, narrative
        # hashes (dedup), and document queue. Least-privilege scoping
        # matches the original Lambda role — no Scan/Query on the PHI
        # tables beyond what results_handler / queue_handler need.
        org_config_table.grant_read_data(self.task_role)
        validation_results_table.grant_read_write_data(self.task_role)
        # Read subscriber list (SUBSCRIPTION gsi1pk on org-config).
        self.task_role.add_to_policy(iam.PolicyStatement(
            actions=["dynamodb:Query"],
            resources=[f"{org_config_table.table_arn}/index/*"],
        ))
        narrative_hashes_table.grant(
            self.task_role,
            "dynamodb:GetItem",
            "dynamodb:PutItem",
        )
        document_queue_table.grant(
            self.task_role,
            "dynamodb:GetItem",
            "dynamodb:PutItem",
            "dynamodb:UpdateItem",
        )

        # Bedrock: InvokeModel for LLM rule evaluation. Same scoping the
        # CentralReach task uses.
        self.task_role.add_to_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=[
                "arn:aws:bedrock:*::foundation-model/anthropic.*",
                "arn:aws:bedrock:*:*:inference-profile/*",
            ],
        ))
        # AWS Marketplace subscriptions required for cross-region
        # inference profiles.
        self.task_role.add_to_policy(iam.PolicyStatement(
            actions=["aws-marketplace:ViewSubscriptions",
                     "aws-marketplace:Subscribe"],
            resources=["*"],
        ))

        # CloudWatch metrics: per-org Claude cost attribution, namespace-scoped.
        self.task_role.add_to_policy(iam.PolicyStatement(
            actions=["cloudwatch:PutMetricData"],
            resources=["*"],
            conditions={
                "StringEquals": {
                    "cloudwatch:namespace": "PenguinHealth/LLMCost",
                },
            },
        ))

        # SES: subscriber notification emails. Sending identity isn't
        # yet verified — tighten to the identity ARN once DNS verification
        # of penguinhealth.io completes.
        self.task_role.add_to_policy(iam.PolicyStatement(
            actions=["ses:SendEmail", "ses:SendRawEmail"],
            resources=["*"],
        ))
        # Email audit rows on the stedi table (mirrors AUDIT# pattern).
        self.task_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "dynamodb:PutItem",
                "dynamodb:Query",
                "dynamodb:GetItem",
            ],
            resources=[
                f"arn:aws:dynamodb:{config.AWS_REGION}:*:table/{config.PROJECT_NAME}-stedi",
                f"arn:aws:dynamodb:{config.AWS_REGION}:*:table/{config.PROJECT_NAME}-stedi/index/*",
            ],
        ))

        # Audit emitter writes to the audit DDB table + Firehose. Both
        # are owned by AuditLayer; grants are added there via
        # `_grant_emit` — see the stack wiring.

        # ----- Task definition -----------------------------------------
        self.log_group = logs.LogGroup(
            self, "RulesEngineRunnerLogs",
            log_group_name=f"/aws/ecs/{config.PROJECT_NAME}-rules-engine-runner",
            retention=logs.RetentionDays.THREE_MONTHS,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # 2 vCPU / 4 GB is a starting point tuned for headroom over the
        # Lambda's 512 MB: fastparquet + pandas + parallel per-rule
        # thread pool. Tune after the first weeks of runs.
        self.task_definition = ecs.FargateTaskDefinition(
            self, "RulesEngineRunnerTaskDef",
            cpu=2048,
            memory_limit_mib=4096,
            task_role=self.task_role,
            family=f"{config.PROJECT_NAME}-rules-engine-runner",
        )

        self.container = self.task_definition.add_container(
            "runner",
            image=ecs.ContainerImage.from_docker_image_asset(self.image_asset),
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="run",
                log_group=self.log_group,
            ),
            environment={
                "ORG_CONFIG_TABLE_NAME": org_config_table.table_name,
                "STEDI_TABLE_NAME": f"{config.PROJECT_NAME}-stedi",
                "NARRATIVE_HASH_TABLE": narrative_hashes_table.table_name,
                "DOCUMENT_QUEUE_TABLE": document_queue_table.table_name,
                "QUEUE_WRITE_ENABLED": "true",
                "EMAIL_FROM_ADDRESS": "noreply@penguinhealth.io",
                "EMAIL_REPLY_TO": "noreply@penguinhealth.io",
                "ADMIN_UI_BASE_URL": "https://app.penguinhealth.io",
                # Names the audit actor. Same convention the CentralReach
                # runner uses (SystemPrincipal(RULES_ENGINE_TASK_NAME)).
                "RULES_ENGINE_TASK_NAME": f"{config.PROJECT_NAME}-rules-engine-runner",
                # Audit emitter targets. The Lambda path picks these up
                # from AuditLayer._grant_emit; the Fargate path sets them
                # here on the task definition. Keep the values in sync
                # with the AuditLayer construct.
                "AUDIT_TABLE_NAME": f"{config.PROJECT_NAME}-audit",
                "AUDIT_FIREHOSE_NAME": f"{config.PROJECT_NAME}-audit",
            },
            # ORG_ID, RUN_ID, MODE, CATEGORIES, DATES, DATE_WINDOW are
            # injected per-execution by the Step Functions task below via
            # container overrides.
        )

        # ----- Step Functions state machine ----------------------------
        self.state_machine = self._build_state_machine()

        # ----- Per-org EventBridge schedules ---------------------------
        # Two rules per org so the cron itself encodes the Monday-vs-rest
        # split. `date_window` is passed through as a JSON-encoded env
        # var and resolved to concrete dates by the runner at start.
        for entry in _PER_ORG_SCHEDULES:
            org_id = entry["org_id"]
            label = entry["label"]
            rule_prefix = entry["rule_prefix"]
            hour = entry["hour"]
            minute = entry["minute"]

            # Monday — validates Sat, Sun, and today (Mon).
            # TEMPORARY `-v2` suffix: the AuditEngine construct owned rules
            # with the un-suffixed names before this migration. Deploying
            # with these `-v2` names lets CloudFormation delete the old
            # Lambda-targeted rules and create the new SFN-targeted rules
            # without a name collision. Once deployed, drop the suffix
            # and deploy again to restore the tidy names.
            events.Rule(self, f"{label}MondayValidationSchedule",
                rule_name=f"{config.PROJECT_NAME}-{rule_prefix}-monday-v2",
                schedule=events.Schedule.cron(hour=hour, minute=minute,
                                              week_day="MON"),
                targets=[targets.SfnStateMachine(
                    self.state_machine,
                    input=events.RuleTargetInput.from_object({
                        "organization_id": org_id,
                        "mode": "scheduled",
                        # Placeholder strings survive the JSII round-trip;
                        # bare `null` gets stripped. The SFN Normalize
                        # step converts these back to null before the
                        # runner reads them.
                        "validation_run_id": "__NULL__",
                        "categories": "__NULL__",
                        "dates": "__NULL__",
                        "date_window": {"days_back_from_today": [2, 1, 0]},
                    }),
                )],
            )

            # Tue-Fri — validates today's ingest only.
            # See Monday-rule comment above for the `-v2` suffix rationale.
            events.Rule(self, f"{label}DailyValidationSchedule",
                rule_name=f"{config.PROJECT_NAME}-{rule_prefix}-daily-v2",
                schedule=events.Schedule.cron(hour=hour, minute=minute,
                                              week_day="TUE-FRI"),
                targets=[targets.SfnStateMachine(
                    self.state_machine,
                    input=events.RuleTargetInput.from_object({
                        "organization_id": org_id,
                        "mode": "scheduled",
                        "validation_run_id": "__NULL__",
                        "categories": "__NULL__",
                        "dates": "__NULL__",
                        "date_window": {"days_back_from_today": [0]},
                    }),
                )],
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_state_machine(self) -> sfn.StateMachine:
        """Run the Fargate task with the run's inputs as env overrides.

        State diagram:

            Start
              -> Normalize   (JSON-encode list/dict inputs, default missing
                              optionals to null so JsonPath resolves cleanly)
              -> RunTask     (ECS RunTask, sync)
              -> Succeed

        The admin API passes `validation_run_id` in so the frontend can
        show the id immediately. EventBridge doesn't; the runner mints
        one when `RUN_ID` is the string "null".

        Any failure inside RunTask flows through the state machine's
        CloudWatch logs + execution history; the task itself prints a
        traceback to CloudWatch logs before exiting non-zero.
        """
        # JSON-encode the list/dict inputs so they can travel through the
        # container env. The runner's main.py parses them back with
        # json.loads. States.JsonToString(null) yields the string "null",
        # which the runner treats as absent. String-typed inputs
        # (organization_id, mode, validation_run_id) skip JsonToString so
        # they don't arrive as double-quoted strings inside the container.
        # Every caller (EventBridge rules + admin API) sends all six top-
        # level keys, defaulting the optional ones to null, so JsonPath
        # never fails on a missing key.
        normalize = sfn.Pass(
            self, "NormalizeRunInputs",
            parameters={
                "organization_id.$": "$.organization_id",
                "mode.$": "$.mode",
                # validation_run_id is null when the caller didn't set
                # one; passing a JSON-encoded "null" through lets the
                # runner mint its own id (default behavior).
                "run_id.$": "States.JsonToString($.validation_run_id)",
                "categories_json.$": "States.JsonToString($.categories)",
                "dates_json.$": "States.JsonToString($.dates)",
                "date_window_json.$": "States.JsonToString($.date_window)",
            },
        )

        run_task = sfn_tasks.EcsRunTask(
            self, "RunRulesEngineTask",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=self.cluster,
            task_definition=self.task_definition,
            launch_target=sfn_tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST,
            ),
            subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            assign_public_ip=False,
            container_overrides=[
                sfn_tasks.ContainerOverride(
                    container_definition=self.container,
                    environment=[
                        sfn_tasks.TaskEnvironmentVariable(
                            name="ORG_ID",
                            value=sfn.JsonPath.string_at("$.organization_id")),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="RUN_ID",
                            value=sfn.JsonPath.string_at("$.run_id")),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="MODE",
                            value=sfn.JsonPath.string_at("$.mode")),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="CATEGORIES",
                            value=sfn.JsonPath.string_at("$.categories_json")),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="DATES",
                            value=sfn.JsonPath.string_at("$.dates_json")),
                        sfn_tasks.TaskEnvironmentVariable(
                            name="DATE_WINDOW",
                            value=sfn.JsonPath.string_at("$.date_window_json")),
                    ],
                ),
            ],
            result_path=sfn.JsonPath.DISCARD,
        )

        succeed = sfn.Succeed(self, "ValidationDone")

        normalize.next(run_task)
        run_task.next(succeed)

        return sfn.StateMachine(
            self, "RulesEngineStateMachine",
            state_machine_name=f"{config.PROJECT_NAME}-rules-engine",
            definition_body=sfn.DefinitionBody.from_chainable(normalize),
            # Larger orgs' Monday catch-up historically ran ~30-40 minutes;
            # 4 hours is generous headroom.
            timeout=Duration.hours(4),
            tracing_enabled=True,
        )
