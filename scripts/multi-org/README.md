# scripts/multi-org

Operational scripts for provisioning a new organization in the multi-org
platform. Most are idempotent — re-running the same command updates the
existing item rather than creating a duplicate.

## Order of operations for a new org

1. `create-organization.sh --org-id <id>` — creates the S3 bucket, KMS
   key, and `ORG#<id> / METADATA` DynamoDB record. Idempotent; safe to
   re-run while wiring downstream integrations.
2. (Optional integrations, any order) `add_rpa_config.py`,
   `add_fhir_config.py`, `add_stedi_config.py`, `seed_rpa_playbook.py`,
   `add-csv-splitter-trigger.sh`.
3. Seed the org's rules — see "Seeding rules for a new org" below.

## Seeding rules for a new org

Rules are stored as one DynamoDB item per rule on
`penguin-health-org-config` under `pk = ORG#<org_id>, sk = RULE#<rule_id>`.
The admin UI/API can create rules one at a time, but for bulk seeding —
when a new org needs its whole rule set in one go — use a seed JSON file
under `rule-seeds/` plus a per-org Python script that reads it.

Pattern in place today:

- `rule-seeds/supportive-care-aba.json` — source-of-truth JSON for the
  supportive-care org's ABA compliance rules (12 rules; the schema
  matches `lambda/api/admin_api.py` rule items).
- `seed_supportive_care_rules.py` — idempotent boto3 script that reads
  the JSON and `PutItem`s each rule.

```bash
# Preview (no AWS calls):
python scripts/multi-org/seed_supportive_care_rules.py --dry-run

# Apply (writes 12 items to penguin-health-org-config):
python scripts/multi-org/seed_supportive_care_rules.py \
    --org-id supportive-care \
    --region us-east-1
```

To seed a different org, copy the JSON to `rule-seeds/<org-id>-<suite>.json`
and the script to `seed_<org_id>_rules.py`. Keep the seed file's `org_id`
matching the default so `--org-id` is optional.

### Rules engine dependencies

Some rules in the supportive-care suite use deterministic operators added
specifically for this org:

| Rule | Operator | Backing infra |
|---|---|---|
| 1 (narratives individualized) | `narrative_hash_unique` | DynamoDB table `penguin-health-narrative-hashes` (deployed by CDK `AuditEngine`); reads `narrative_hash`/`org_id`/`source_record_id` from `fields` |
| 4 (signed within 5 min of billed end) | `datetime_not_before_minus_minutes` | None |

The `narrative_hash` field is computed at RPA-record build time by
`lambda/multi-org/rpa/record.py:narrative_hash` and surfaced in
`extracted_fields` by `result_writer.build_record`. The rules-engine
Lambda IAM role is granted `GetItem`+`PutItem` (no `Scan`/`Query`) on
the hash table by the CDK construct.

### Rule 11 is disabled by default

The supportive-care seed ships rule 11 ("raw tracked data is included in
the note") as `enabled: false`. It needs the RPA pipeline to extract a
`tracked_data_summary` field alongside the narrative. Once that
extraction lands, flip `enabled` in the seed and re-run the script.
