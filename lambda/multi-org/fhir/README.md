# FHIR Integration

Targeted FHIR query layer for fetching encounter data from per-org EHR
APIs (first: Qualifacts Credible) and materializing it into Athena for
report enrichment. **Not** a sync/warehouse pipeline — fetches only the
resources today's reports need.

## Architecture at a glance

```
SFTP CSV drop                                          Athena reports
       │                                                      ▲
       ▼                                                      │
penguin-health-csv-splitter-multi-org                         │
       │                                                      │
       │ emits EventBridge: penguin-health.csv-splitter /     │
       │                    SftpIngestComplete                │
       ▼                                                      │
penguin-health-fhir-encounter-materializer                    │
       │                                                      │
       ├── Athena: SELECT DISTINCT service_id_1 FROM charts_… │
       ├── Athena: SELECT DISTINCT encounter_id FROM fhir_…   │
       ├── Diff → IDs to fetch                                │
       │                                                      │
       │   ┌──────────────────────────────────────┐           │
       │   │ private_key_jwt (RS384), KMS-signed  │           │
       ├──▶│ token request → Credible token srv   │           │
       │   │ ◀── access_token                     │           │
       │   │ GET /Encounter/{id} → resource       │           │
       │   └──────────────────────────────────────┘           │
       │                                                      │
       ├── write NDJSON to data/fhir/encounter/...            │
       └── write projected Parquet to analytics/fhir/… ───────┘
                                                              │
                                                       JOIN at Athena
                                                       on encounter_id
```

Per-org isolation: every S3 path, Athena workgroup, and KMS key is
namespaced by `org_id`. The materializer never touches another org's
bucket, table, or key.

## Trust model: how requests are authenticated

We use **OAuth2 `client_credentials` + `private_key_jwt` (RS384)**. No
shared secrets exist anywhere. The private key lives in AWS KMS as an
asymmetric key (`RSA_4096`, `SIGN_VERIFY`) and never enters the Lambda
process — `kms:Sign` returns just the signature.

The chain:

1. **Lambda** signs a short-lived JWT assertion (5-min lifetime) using
   `kms:Sign` with `SigningAlgorithm=RSASSA_PKCS1_V1_5_SHA_384`.
2. **Lambda** POSTs the JWT to the vendor's token endpoint with
   `client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer`.
3. **Vendor (Credible)** looks up our `client_id`, finds the registered
   JWKS URL, fetches it.
4. **Vendor** finds the JWK matching the JWT header's `kid`, verifies
   the signature against the RSA public key.
5. **Vendor** returns an `access_token`.

The JWKS URL (`https://keys.penguinhealth.io/{org_id}/jwks.json`) is
public S3 + CloudFront, hosting only public keys. PHI never touches that
bucket.

### Trust anchors

| Anchor | Where it lives | Why it matters |
|---|---|---|
| Private key | KMS asymmetric key behind alias `alias/penguin-health-fhir-{org_id}` | Cannot be exfiltrated; `kms:Sign` is the only access path |
| Vendor's registered JWKS URL | Credible side (`https://keys.penguinhealth.io/{org_id}/jwks.json`) | Bridges our `client_id` → our public key |
| Public key | `s3://phealth-fhir-jwks/{org_id}/jwks.json` via CloudFront | What the vendor verifies against |
| `client_id` | `FHIR_CONFIG` DynamoDB record | Embedded in JWT `iss`/`sub` claims |

There is no Secrets Manager involvement. The KMS alias is the single
source of truth for "which key signs requests for this org."

## Module layout

```
lambda/multi-org/fhir/                # importable library — no Lambda
  fhir_query.py                       # public surface: get_resource, search
  fhir_client.py                      # base client: tokens, pagination, retry
  credible_client.py                  # Credible-specific OAuth2 + JWT signing
  kms_signer.py                       # kms:Sign-based RS384 JWT signer
  kms_resolver.py                     # alias → (kms_key_arn, kid), cached
  config.py                           # load_fhir_config from DynamoDB
  fhir_projections.py                 # project_encounter (FHIR → Parquet row)
  exceptions.py                       # typed errors

lambda/multi-org/fhir-materializer/   # follower Lambda
  encounter_materializer.py           # handler: gate, diff, fetch, write
  athena.py                           # per-org workgroup query helper
  storage.py                          # NDJSON + Parquet writers
  metrics.py                          # CloudWatch counters

infra/components/jwks_hosting.py      # S3 + CloudFront for JWKS URLs
infra/components/audit_engine.py      # materializer Lambda + IAM + EB rule
infra/components/analytics.py         # fhir_encounters_{org_id} Athena table

scripts/multi-org/provision_fhir_keypair.py  # create KMS key, publish JWKS
scripts/multi-org/add_fhir_config.py         # seed FHIR_CONFIG row
```

## Per-org configuration

### `penguin-health-org-config` DynamoDB record

```
pk: ORG#demo
sk: FHIR_CONFIG
{
  "organization_id": "demo",
  "vendor": "credible",
  "base_url": "https://fhir.cbhstg4.crediblebh.com",
  "token_url": "https://sts-duende.cbhstg4.crediblebh.com/connect/token",
  "auth_type": "oauth2_client_credentials",
  "client_authentication": "private_key_jwt",
  "signing_alg": "RS384",
  "scopes": [],
  "client_id": "<from-qualifacts>",
  "kms_alias": "alias/penguin-health-fhir-demo",
  "jwks_url": "https://keys.penguinhealth.io/demo/jwks.json",
  "page_size": 100,
  "concurrency": 4,
  "enabled": true,
  "resource_types": ["Encounter"],
  "fhir_mappings": {
    "encounter": {
      "source_table": "charts_demo",
      "source_column": "service_id_1",
      "fhir_lookup": "by_id"
    }
  }
}
```

### KMS

- Alias: `alias/penguin-health-fhir-{org_id}` → asymmetric key
  (`KeySpec=RSA_4096`, `KeyUsage=SIGN_VERIFY`)
- Tags: `Project=penguin-health`, `OrgId={org_id}`, `Purpose=fhir-private-key-jwt`
- The Lambda's IAM is scoped via `kms:ResourceAliases` to keys with
  alias prefix `alias/penguin-health-fhir-*` — so the materializer can
  only sign with FHIR-purpose keys, never other org keys.

### S3 / CloudFront / DNS

- Bucket: `s3://phealth-fhir-jwks/{org_id}/jwks.json` (private, OAC only)
- CloudFront distribution fronts the bucket at `keys.penguinhealth.io`
- ACM cert in `us-east-1` (CloudFront requirement)
- Bucket name intentionally NOT under `penguin-health-*` so audit-engine
  Lambdas (which have wildcard read/write on per-org buckets) cannot
  overwrite JWK Sets. See [jwks_hosting.py](../../../infra/components/jwks_hosting.py)
  docstring for the full rationale.

### Per-org encounter ID column

The materializer looks up encounter IDs in `charts_{org_id}` using the
column named in `fhir_mappings.encounter.source_column`:

| Org | Column |
|---|---|
| `demo` | `service_id_1` |
| `catholic-charities-multi-org` | `service_id_1` |
| `circles-of-care` | `clientvisit_id` |

## End-to-end flow

When the CSV splitter finishes processing a daily SFTP drop:

1. **CSV splitter** writes per-chart CSVs to `s3://penguin-health-{org_id}/data/{ingest_date}/`
   and emits an EventBridge event:
   ```json
   {
     "source": "penguin-health.csv-splitter",
     "detail-type": "SftpIngestComplete",
     "detail": { "organization_id": "demo", "ingest_date": "2026-05-19" }
   }
   ```
2. **Materializer** wakes up via EventBridge rule. Loads `FHIR_CONFIG`.
   - No `FHIR_CONFIG` row, `enabled: false`, or no `fhir_mappings.encounter`
     → silent skip with `FhirMaterializerSkipped` metric.
   - Otherwise → continue.
3. **Athena diff** (in the per-org workgroup):
   - `SELECT DISTINCT service_id_1 FROM charts_demo WHERE ingest_date = '2026-05-19'`
   - `SELECT DISTINCT encounter_id FROM fhir_encounters_demo WHERE ingest_date <= '2026-05-19'`
   - Difference = encounters to fetch.
4. **Resolve KMS alias** (cold start only, cached): `kms:DescribeKey` +
   `kms:GetPublicKey` → `(kms_key_arn, kid)`. The `kid` is the RFC 7638
   thumbprint of the public key.
5. **Mint token** (cached for ~1 hour):
   - Build JWT claims: `iss`/`sub=client_id`, `aud=token_url`,
     `iat=now`, `exp=now+5min`, `jti=uuid4`.
   - Build header: `alg=RS384`, `typ=JWT`, `kid=<from step 4>`.
   - `kms:Sign(Message=base64url(header)+"."+base64url(payload),
     MessageType=RAW, SigningAlgorithm=RSASSA_PKCS1_V1_5_SHA_384)`.
   - POST to token endpoint with `client_assertion_type=...:jwt-bearer`
     and `client_assertion=<the JWT>`.
   - Cache returned `access_token` in-process for `expires_in - 60s`.
6. **Fetch each encounter**:
   `GET {base_url}/Encounter/{id}` with `Authorization: Bearer <token>`.
   Per-org concurrency cap (default 4 in-flight). Retry on 429/5xx with
   exponential backoff (3 attempts).
7. **Write canonical NDJSON** to
   `s3://penguin-health-{org_id}/data/fhir/encounter/{yyyy}/{mm}/{dd}/{run_id}.part-{leg:04d}.ndjson`
   — one resource per line, ordered by fetch. This is the audit/replay/
   re-projection-source-of-truth artifact.
8. **Project and write Parquet** to
   `s3://penguin-health-{org_id}/analytics/fhir/encounter/ingest_date={ingest_date}/{run_id}.part-{leg:04d}.parquet`.
   Each row carries `ndjson_s3_key` + `ndjson_line_no` pointing back to
   the canonical raw resource.
9. **Emit metrics**: `FhirEncountersFetched` (counter, by org),
   `FhirEncountersNotFound` (counter, by org).
10. **If approaching 15-min timeout**: flush partial files, self-invoke
    with `is_continuation=true`, `remaining_ids=[...]`, `leg=leg+1`. New
    leg writes to fresh `part-{leg:04d}.*` files (no overwrite).

## How reports consume FHIR data

After the materializer runs, reports `JOIN` against the per-org Athena
table:

```sql
SELECT c.service_id_1, c.service_date_8, e.period_start, e.status,
       e.subject_reference
FROM charts_demo c
LEFT JOIN fhir_encounters_demo e
  ON c.service_id_1 = e.encounter_id
WHERE c.ingest_date = '2026-05-19';
```

The `LEFT JOIN` is important — `fhir_lookup_status='not_found'` rows
exist for FHIR fetches that 404'd, but the encounter_id is still
present, so the row exists with the FHIR fields nulled out.

To recover the raw FHIR resource for an encounter:

```sql
SELECT ndjson_s3_key, ndjson_line_no
FROM fhir_encounters_demo
WHERE encounter_id = 'enc-abc';
```

Then read the NDJSON file at that line number.

## Failure modes & observability

The materializer follows the rule: **missing config = silent skip
(intentional), broken config = loud failure (someone has to fix it).**

| Scenario | Behavior | CloudWatch metric |
|---|---|---|
| No `FHIR_CONFIG` row | Skip, no calls | `FhirMaterializerSkipped{reason=no_config}` |
| `enabled: false` | Skip, no calls | `FhirMaterializerSkipped{reason=disabled}` |
| No `fhir_mappings.encounter` | Skip, no calls | `FhirMaterializerSkipped{reason=no_encounter_mapping}` |
| KMS alias doesn't resolve | Loud `FhirAuthError` | `FhirMaterializerFailed{reason=upstream_unavailable}` |
| KMS key disabled / wrong KeyUsage | Loud `FhirAuthError` | `FhirMaterializerFailed{reason=upstream_unavailable}` |
| Vendor token endpoint 400 `invalid_client` | Loud `FhirAuthError` with body | `FhirMaterializerFailed{reason=upstream_unavailable}` |
| Vendor 429 (rate limit) | Retry 3× w/ backoff, then loud | `FhirMaterializerFailed{reason=upstream_unavailable}` |
| Per-encounter 404 | Row written with `fhir_lookup_status="not_found"`, run continues | `FhirEncountersNotFound` |
| Athena query against bad `source_column` | Loud failure | `FhirMaterializerFailed{reason=athena_query_failed}` |

**Alarms to set**: page on `FhirMaterializerFailed > 0` per org. Do NOT
alarm on `FhirMaterializerSkipped` — that's expected for unconfigured orgs.

## Onboarding a new org

End-to-end checklist. Assumes the org already has a `penguin-health-{org_id}`
bucket, a per-org Athena workgroup (created by the `Analytics` CDK
construct), and is receiving SFTP drops.

### 1. Register with Qualifacts (or the FHIR vendor)
Get a `client_id`. Tell them you'll authenticate via `private_key_jwt`
with an RS384-signed JWT, and you'll send them a JWKS URL once your
infrastructure is ready.

### 2. Add the org to the `Analytics` construct (if not already there)
Edit [infra/components/analytics.py](../../../infra/components/analytics.py) `ORG_TABLES` dict to register
`charts_{org_id}` + `fhir_encounters_{org_id}`. `cdk deploy`.

### 3. Provision the KMS key + JWKS file
```bash
python3 scripts/multi-org/provision_fhir_keypair.py \
  --org-id <org_id> \
  --jwks-bucket phealth-fhir-jwks \
  --jwks-domain keys.penguinhealth.io
```
This:
- Creates `alias/penguin-health-fhir-{org_id}` pointing at a new RSA_4096
  KMS key.
- Uploads `s3://phealth-fhir-jwks/{org_id}/jwks.json` containing the
  public JWK.
- Prints the JWKS URL.

### 4. Send the JWKS URL to the vendor
`https://keys.penguinhealth.io/{org_id}/jwks.json`. Ask them to register
it against your `client_id` and confirm when active.

### 5. Seed the FHIR_CONFIG DynamoDB row
```bash
python3 scripts/multi-org/add_fhir_config.py \
  --org-id <org_id> \
  --vendor credible \
  --base-url https://fhir.<env>.crediblebh.com \
  --token-url https://sts-duende.<env>.crediblebh.com/connect/token \
  --client-id <from-vendor> \
  --jwks-url https://keys.penguinhealth.io/<org_id>/jwks.json \
  --source-column <service_id_1 or clientvisit_id>
```

### 6. Smoke test auth
From a shell with AWS creds that can read DynamoDB + use KMS:
```bash
cd lambda
python3 -c "
import sys; sys.path.insert(0, 'multi-org')
from fhir.fhir_query import get_client
client = get_client('<org_id>')
token, expires = client.authenticate()
print(f'OK: token expires in {expires}s')
"
```
If this returns a token: auth path works end-to-end.
If it returns `400 invalid_client`: vendor hasn't activated your JWKS
URL yet (most common) or the wrong `client_id`/`token_url` (less common).

### 7. End-to-end test
Drop a small synthetic CSV in `s3://penguin-health-<org_id>/uploaded-data-sftp/`.
Watch CloudWatch logs for the CSV splitter (emits `SftpIngestComplete`)
and the materializer (resolves alias, fetches encounters, writes Parquet).

Verify in Athena:
```sql
SELECT * FROM fhir_encounters_<org_id_underscores>
WHERE ingest_date = '<today>' LIMIT 5;
```

## Key rotation

> **Status today**: the script supports first-run provisioning fully.
> Rotation is *possible* but not yet zero-downtime — there's a brief
> window between repointing the alias and the vendor refreshing its
> JWKS cache where token requests can fail `invalid_client`.

### Why rotate
- Annual hygiene (HIPAA / SOC 2 expectation).
- Suspected key compromise.
- Personnel change with broad AWS access.
- Vendor request.

### Procedure today (with brief vendor-coordinated downtime)

1. **Coordinate with the vendor**: tell Qualifacts you're rotating keys
   for `client_id X` and ask them to refresh their JWKS cache afterward.
2. **Run the script again**:
   ```bash
   python3 scripts/multi-org/provision_fhir_keypair.py \
     --org-id <org_id> \
     --jwks-bucket phealth-fhir-jwks \
     --jwks-domain keys.penguinhealth.io
   ```
   This creates a *new* KMS key, repoints the alias, and **overwrites**
   the JWKS file with only the new public key.
3. **Vendor refreshes their JWKS cache**. Until they do, in-flight token
   requests signed by the new key will fail `invalid_client` because
   their cache still has the old public key.
4. **Verify**: re-run the smoke test from step 6 of onboarding.
5. **Clean up the old KMS key**: AWS console → KMS → Customer managed
   keys → find the key tagged for this org that is NOT the alias's
   target → schedule deletion (7–30 day waiting period). This step is
   manual today.

### Why this isn't zero-downtime
The JWKS file only ever contains one key. The standard well-behaved
pattern is "publish both old + new during a vendor-cache-TTL overlap
window, then drop the old." The current script overwrites instead.

### Planned improvement
Two new modes for `provision_fhir_keypair.py` (~80 lines of code):
- `--rotate`: read existing JWKS, append new JWK alongside old, create new
  KMS key, repoint alias, publish both keys. Print: "Old key still
  valid until you run `--finalize-rotation`."
- `--finalize-rotation`: read JWKS, identify which JWK matches the current
  alias target, republish JWKS with only that key, schedule old KMS key
  for deletion.

Until built, treat rotation as a planned-maintenance event with vendor
coordination.

## Cost

Per-org steady-state (3 orgs total):

| Resource | Monthly cost (approx) |
|---|---|
| KMS asymmetric key | $1.00/org |
| KMS Sign operations | ~$0.0001 (token cached, ~1 sign per Lambda invocation per hour) |
| Lambda invocations | ~$0.01 (one materializer run per org per day) |
| S3 storage (NDJSON + Parquet) | ~$0.10 (depends on encounter volume) |
| Athena queries | ~$0.05 (per-org workgroup, query-scanned bytes) |
| CloudFront (JWKS) | ~$0.01 (tiny static file, low fetch volume) |

**Total**: ~$1.50/org/month at our current scale.

## Testing

```bash
cd lambda
python3 -m pytest tests/unit/fhir/ -v --no-cov
```

Test surface:
- **`test_fhir_query.py`** — signs real JWTs via moto-KMS, verifies via
  PyJWT against the moto-derived public key. Covers token caching, 401
  refresh, 429 backoff, 404 → `FhirNotFound`, pagination, the
  `private_key_jwt` claim shape, jti uniqueness, missing/disabled/
  unknown-vendor/missing-client_id/missing-alias/unresolvable-alias error
  paths.
- **`test_encounter_materializer.py`** — covers the full handler:
  config gate (skip vs fail), Athena diff, NDJSON + Parquet writes,
  continuation, metrics emission.
- **`test_provision_script.py`** — round-trips the provisioning script
  against moto-S3 + moto-KMS, verifies the JWK matches the KMS public
  key and that Secrets Manager is NOT touched.

**Not tested locally**: KMS alias rotation (`update_alias` against a
fresh target key). Moto has a known bug here; real KMS works correctly.

## Operational runbook

### "Materializer is failing with `invalid_client` against Credible"
1. Confirm the JWKS URL is reachable:
   `curl https://keys.penguinhealth.io/{org_id}/jwks.json`
2. Confirm the published `kid` matches what the Lambda would sign with:
   ```bash
   # published kid
   curl -s https://keys.penguinhealth.io/{org_id}/jwks.json \
     | python3 -c "import json,sys; print(json.load(sys.stdin)['keys'][0]['kid'])"
   # runtime kid (what kms_resolver would produce)
   aws kms get-public-key \
     --key-id alias/penguin-health-fhir-{org_id} \
     --query PublicKey --output text \
     | base64 -d \
     | python3 -c "
       import hashlib, base64, json, sys
       from cryptography.hazmat.primitives import serialization
       pk = serialization.load_der_public_key(sys.stdin.buffer.read())
       n = pk.public_numbers()
       def b64(v):
           b = (v.bit_length()+7)//8
           return base64.urlsafe_b64encode(v.to_bytes(b,'big')).rstrip(b'=').decode()
       c = json.dumps({'e': b64(n.e), 'kty': 'RSA', 'n': b64(n.n)}, separators=(',',':'), sort_keys=True).encode()
       print(base64.urlsafe_b64encode(hashlib.sha256(c).digest()).rstrip(b'=').decode())
     "
   ```
   These two values **must** be identical. If not, re-run `provision_fhir_keypair.py`.
3. If kids match, the vendor's JWKS cache is stale or the JWKS URL was
   not registered against your `client_id`. Email Qualifacts.

### "I see `FhirMaterializerSkipped{reason=no_config}` for an org that should be configured"
The `FHIR_CONFIG` row was never written (or got deleted). Re-run
`add_fhir_config.py`.

### "I want to pause an org without deleting its config"
Run `add_fhir_config.py` with `--disabled`. The materializer will see
`enabled: false` and emit `FhirMaterializerSkipped{reason=disabled}`.

### "I want to query an Encounter that came through but Athena can't find it"
Check `fhir_lookup_status` for that row:
```sql
SELECT encounter_id, fhir_lookup_status, ndjson_s3_key
FROM fhir_encounters_{org_id}
WHERE encounter_id = '<id>';
```
If `not_found`, the vendor returned 404 — Encounter ID exists in SFTP
but not in Credible. If `ok` but no row at all, the materializer
hasn't been triggered yet for that ingest_date.

### "How do I trigger a one-off materializer run without an SFTP drop?"
```bash
aws events put-events --entries '[{
  "Source": "penguin-health.csv-splitter",
  "DetailType": "SftpIngestComplete",
  "Detail": "{\"organization_id\": \"demo\", \"ingest_date\": \"2026-05-19\"}"
}]'
```

## Limitations & deliberately-deferred work

- **One vendor**: only Credible is implemented. Adding a second vendor
  is a new `FhirClient` subclass; the rest of the system is vendor-agnostic.
- **One resource type**: only `Encounter` is materialized. Adding more
  (Patient, Observation, etc.) requires:
  - a new `project_*` function in `fhir_projections.py`,
  - a new `fhir_*_{org_id}` Glue table in `analytics.py`,
  - a new write path in the materializer (or a generalized handler),
  - extending `fhir_mappings` on `FHIR_CONFIG`.
- **No cache layer** on the `fhir_query` module. Every `get_resource`
  call hits the vendor. The materializer's Athena diff prevents repeats
  per ingest_date; ad-hoc callers don't get that.
- **Rotation isn't zero-downtime yet** (see above).
- **No analytics-question materialization pattern**. When someone has a
  concrete analytics question (e.g. "encounters with diagnosis X"), we'd
  build a separate "saved query → S3 → Athena table" path on top of the
  same `fhir_query` library.

## Related files

- [fhir_query.py](fhir_query.py) — the public surface
- [credible_client.py](credible_client.py) — Credible-specific auth
- [kms_signer.py](kms_signer.py) / [kms_resolver.py](kms_resolver.py) — KMS plumbing
- [../fhir-materializer/encounter_materializer.py](../fhir-materializer/encounter_materializer.py) — the follower Lambda
- [../../../infra/components/jwks_hosting.py](../../../infra/components/jwks_hosting.py) — JWKS S3+CloudFront CDK construct
- [../../../infra/components/audit_engine.py](../../../infra/components/audit_engine.py) — materializer Lambda + IAM + EventBridge
- [../../../scripts/multi-org/provision_fhir_keypair.py](../../../scripts/multi-org/provision_fhir_keypair.py) — KMS + JWKS provisioning
- [../../../scripts/multi-org/add_fhir_config.py](../../../scripts/multi-org/add_fhir_config.py) — FHIR_CONFIG seeder
