"""
Analytics construct: Athena + Glue resources for ad-hoc analytics over the
per-org S3 data lake.

Two tables per org live in a shared Glue database:

  * charts_{org_underscored}              — external table over
        s3://penguin-health-{org}/data/, OpenCSVSerde,
        partition projection on ingest_date.

  * validation_results_{org_underscored}  — external table over
        s3://penguin-health-{org}/analytics/validation_results/,
        Parquet (Snappy), partition projection on validation_date.

Per-org buckets keep tenants isolated; per-org tables keep queries simple
(every query targets one org's data — no cross-org filter needed).

ISOLATION (SOC 2 / HIPAA): each org gets its own Athena workgroup, and
each workgroup writes query results into the *same* org's bucket under
the `athena-results/` prefix. Athena result CSVs are derivative PHI, so
they must stay inside the source org's compliance boundary — no shared
results bucket. Workgroups have `enforce_work_group_configuration=true`
so callers cannot override the OutputLocation at query-submission time.

The per-org buckets (`penguin-health-{org_id}`) are managed manually
outside this stack; this construct only references them by name.
"""

from aws_cdk import (
    Stack,
    aws_athena as athena,
    aws_glue as glue,
)
from constructs import Construct

import config


# Each org has its own bucket: penguin-health-{org_id}.
# `chart_columns` lists the exact CSV header order; missing trailing columns
# come back as NULL, so we only need to populate this when we have ground
# truth from the splitter or a sample file.
#
# Each org's column list is defined independently — even when two orgs
# happen to share large portions of their source schema. Decoupling here
# means a column rename or insertion for one org never silently changes
# another org's table definition.

ORG_TABLES = {
    "circles-of-care": {
        # CANONICAL_COLUMNS from
        # lambda/multi-org/csv-splitter/splitters/circles_of_care.py
        "chart_columns": [
            "fake_client_id",
            "clientvisit_id",
            "grade",
            "race_desc",
            "ethnicity_desc",
            "sex",
            "marital_status",
            "age_at_service",
            "visittype",
            "plan_id",
            "service_date",
            "episode_id",
            "program_desc",
            "admission_date",
            "discharge_date",
            "icd10_codes",
            "problem_list_order",
            "diagnose_on_visit",
            "fake_client_id2",
            "clientvisit_id2",
            "first_referral",
            "question_text",
            "answer",
            "type",
            "episode_id2",
            "cptcode",
            "first_name",
            "last_name",
            "rate",
            "initial_appt",
            "agegroup",
            "diagnose_on_visit2",
        ],
    },
    "catholic-charities-multi-org": {
        # Header captured directly from a source SFTP CSV (61 columns).
        # Glue/Athena column identifiers can't start with a digit, so the
        # leading `<num>_` prefix is moved to the end as `_<num>` for each
        # column. Order is preserved exactly to match OpenCSVSerde's
        # positional read.
        "chart_columns": [
            "visit_link_50",
            "service_id_1",
            "consumer_name_2",
            "consumer_dob_3",
            "episode_id_4",
            "staff_name_5",
            "staff_id_6",
            "program_7",
            "program_name_7b",
            "service_date_8",
            "start_time_9",
            "end_time_10",
            "revised_start_11",
            "revised_end_12",
            "duration_13",
            "signed_time_14",
            "transferred_time_15",
            "cpt_code_16",
            "client_insurance_order_26s",
            "billing_order_26b",
            "billing_sequence_26c",
            "billing_group_id_26t",
            "billing_group_name_26u",
            "units_17",
            "rate_18",
            "modifier_1_19",
            "modifier_2_20",
            "visit_type_id_21",
            "visit_type_name_21b",
            "location_code_22",
            "location_label_22b",
            "recipient_code_23",
            "recipient_label_23b",
            "approved_24",
            "non_billable_25",
            "authorization_id_26",
            "narrative_simple_27",
            "narrative_rich_28",
            "next_steps_effective_29c",
            "next_steps_form_only_29b",
            "next_steps_29",
            "form_q_and_a_30_31",
            "vitals_bp_32",
            "vitals_pulse_33",
            "vitals_temp_34",
            "vitals_weight_35",
            "plan_start_date_38",
            "plan_end_date_39",
            "plan_status_40",
            "diagnosis_code_primary_41b",
            "external_id_44",
            "insurance_45",
            "has_next_insurance_flag_26o",
            "plan_type_46",
            "plan_goals_only_47b",
            "plan_objectives_only_47c",
            "plan_interventions_only_47d",
            "plan_signed_date_48",
            "plan_signer_49",
            "plan_qp_signer_49g",
            "plan_signed_date_qp_49h",
        ],
    },
    "demo": {
        # Header captured directly from a source SFTP CSV (63 columns).
        # Defined independently of any other org's schema; a rename or
        # insertion in another org's list must not change this one.
        "chart_columns": [
            "visit_link_50",
            "service_id_1",
            "consumer_name_2",
            "consumer_dob_3",
            "episode_id_4",
            "staff_name_5",
            "staff_id_6",
            "program_7",
            "program_name_7b",
            "service_date_8",
            "start_time_9",
            "end_time_10",
            "revised_start_11",
            "revised_end_12",
            "duration_13",
            "signed_time_14",
            "transferred_time_15",
            "cpt_code_16",
            "client_insurance_order_26s",
            "billing_order_26b",
            "billing_sequence_26c",
            "billing_group_id_26t",
            "billing_group_name_26u",
            "units_17",
            "rate_18",
            "modifier_1_19",
            "modifier_2_20",
            "visit_type_id_21",
            "visit_type_name_21b",
            "location_code_22",
            "location_label_22b",
            "recipient_code_23",
            "recipient_label_23b",
            "approved_24",
            "non_billable_25",
            "authorization_id_26",
            "narrative_simple_27",
            "narrative_rich_28",
            "next_steps_effective_29c",
            "next_steps_form_only_29b",
            "next_steps_29",
            "form_q_and_a_30_31",
            "vitals_bp_32",
            "vitals_pulse_33",
            "vitals_temp_34",
            "vitals_weight_35",
            "plan_start_date_38",
            "plan_end_date_39",
            "plan_status_40",
            "diagnosis_code_primary_41b",
            "external_id_44",
            "insurance_45",
            "has_next_insurance_flag_26o",
            "plan_type_46",
            "plan_goals_only_47b",
            "plan_objectives_only_47c",
            "plan_interventions_only_47d",
            "plan_signed_date_48",
            "plan_signer_49",
            "plan_qp_signer_49g",
            "plan_signed_date_qp_49h",
            "tx_plus_intervention_documentation_47g",
            "plan_documentation_47e",
        ],
    },
}


# Validation Parquet schema — one row per (document, rule). Must match the
# fastparquet schema in lambda/multi-org/rules-engine/parquet_writer.py.
VALIDATION_RESULT_COLUMNS = [
    "organization_id",
    "validation_run_id",
    "validation_timestamp",
    "document_id",
    "filename",
    "s3_key",
    "rule_id",
    "rule_name",
    "category",
    "rule_type",
    "status",
    "message",
    "field_date",
    "field_employee_name",
    "field_program",
    "field_cpt_code",
    "field_rate",
    "field_values_json",
]


GLUE_DATABASE_NAME = "penguin_health_analytics"


def _table_suffix(org_id: str) -> str:
    """Glue table identifiers can't contain dashes."""
    return org_id.replace("-", "_")


class Analytics(Construct):

    def __init__(self, scope: Construct, id: str) -> None:
        super().__init__(scope, id)

        account = Stack.of(self).account
        region = Stack.of(self).region

        # ----- Glue database -----
        self.database = glue.CfnDatabase(self, "AnalyticsDatabase",
            catalog_id=account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=GLUE_DATABASE_NAME,
                description=(
                    "Penguin Health analytics — charts (CSV) and "
                    "validation_results (Parquet), per org."
                ),
            ),
        )

        # ----- Per-org Glue tables + Athena workgroup -----
        # One workgroup per org keeps query results PHI-isolated to the
        # source org's bucket. Workgroup names follow
        # `{project}-analytics-{org_id}` so the right one is obvious in
        # the Athena console.
        self.workgroups: dict[str, athena.CfnWorkGroup] = {}
        for org_id, spec in ORG_TABLES.items():
            self._build_org_tables(account, region, org_id, spec)
            self.workgroups[org_id] = self._build_org_workgroup(org_id)

    # ------------------------------------------------------------------
    # Per-org workgroup
    # ------------------------------------------------------------------

    def _build_org_workgroup(self, org_id: str) -> athena.CfnWorkGroup:
        """
        Create a workgroup whose query results land in the org's own bucket.

        Each org's bucket is `penguin-health-{org_id}`; results go under
        `athena-results/`. enforce_work_group_configuration=true prevents
        callers from overriding OutputLocation at query time, which is the
        actual compliance boundary — convention isn't enough.
        """
        bucket = f"{config.PROJECT_NAME}-{org_id}"
        suffix = _table_suffix(org_id)

        wg = athena.CfnWorkGroup(self, f"AnalyticsWorkGroup_{suffix}",
            name=f"{config.PROJECT_NAME}-analytics-{org_id}",
            description=(
                f"Analytics queries over {org_id}'s charts and "
                f"validation_results. Query results land in this org's "
                f"bucket only — never commingled with other orgs' data."
            ),
            state="ENABLED",
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{bucket}/athena-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3",
                    ),
                ),
            ),
        )
        wg.add_dependency(self.database)
        return wg

    # ------------------------------------------------------------------
    # Per-org table construction
    # ------------------------------------------------------------------

    def _build_org_tables(self, account: str, region: str,
                          org_id: str, spec: dict) -> None:
        bucket = f"{config.PROJECT_NAME}-{org_id}"
        suffix = _table_suffix(org_id)

        chart_columns = spec.get("chart_columns")
        if chart_columns:
            charts_table = glue.CfnTable(self, f"ChartsTable_{suffix}",
                catalog_id=account,
                database_name=GLUE_DATABASE_NAME,
                table_input=glue.CfnTable.TableInputProperty(
                    name=f"charts_{suffix}",
                    description=(
                        f"Per-chart CSV files for {org_id} written by the "
                        f"CSV splitter. One file per chart; many rows per file."
                    ),
                    table_type="EXTERNAL_TABLE",
                    parameters={
                        "classification": "csv",
                        "skip.header.line.count": "1",
                        "projection.enabled": "true",
                        "projection.ingest_date.type": "date",
                        "projection.ingest_date.format": "yyyy-MM-dd",
                        "projection.ingest_date.range": "2024-01-01,NOW",
                        "projection.ingest_date.interval": "1",
                        "projection.ingest_date.interval.unit": "DAYS",
                        "storage.location.template": (
                            f"s3://{bucket}/data/${{ingest_date}}/"
                        ),
                    },
                    partition_keys=[
                        glue.CfnTable.ColumnProperty(
                            name="ingest_date", type="string"
                        ),
                    ],
                    storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                        location=f"s3://{bucket}/data/",
                        input_format="org.apache.hadoop.mapred.TextInputFormat",
                        output_format=(
                            "org.apache.hadoop.hive.ql.io."
                            "HiveIgnoreKeyTextOutputFormat"
                        ),
                        serde_info=glue.CfnTable.SerdeInfoProperty(
                            serialization_library=(
                                "org.apache.hadoop.hive.serde2.OpenCSVSerde"
                            ),
                            parameters={
                                "separator": ",",
                                "quoteChar": '"',
                                "escapeChar": "\\",
                            },
                        ),
                        columns=[
                            glue.CfnTable.ColumnProperty(name=c, type="string")
                            for c in chart_columns
                        ],
                    ),
                ),
            )
            charts_table.add_dependency(self.database)

        # Validation results table — one per org, even when the chart
        # schema isn't known yet. The shape is fixed by parquet_writer.py.
        validation_table = glue.CfnTable(self, f"ValidationResultsTable_{suffix}",
            catalog_id=account,
            database_name=GLUE_DATABASE_NAME,
            table_input=glue.CfnTable.TableInputProperty(
                name=f"validation_results_{suffix}",
                description=(
                    f"End-of-run Parquet snapshot of validation results for "
                    f"{org_id}. One row per rule per document. Mutable "
                    f"feedback fields are intentionally NOT included."
                ),
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    "projection.enabled": "true",
                    "projection.validation_date.type": "date",
                    "projection.validation_date.format": "yyyy-MM-dd",
                    "projection.validation_date.range": "2026-05-01,NOW",
                    "projection.validation_date.interval": "1",
                    "projection.validation_date.interval.unit": "DAYS",
                    "storage.location.template": (
                        f"s3://{bucket}/analytics/validation_results/"
                        f"validation_date=${{validation_date}}/"
                    ),
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="validation_date", type="string"
                    ),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=(
                        f"s3://{bucket}/analytics/validation_results/"
                    ),
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
                        glue.CfnTable.ColumnProperty(name=c, type="string")
                        for c in VALIDATION_RESULT_COLUMNS
                    ],
                ),
            ),
        )
        validation_table.add_dependency(self.database)
