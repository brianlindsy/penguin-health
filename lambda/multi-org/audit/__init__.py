"""Application-level HIPAA audit log.

Public API:
    from audit import emit, audited, SystemPrincipal, from_event, patient_hash

Two storage paths share one emitter:
  * DynamoDB hot mirror (penguin-health-audit) — synchronous, 90d TTL,
    queryable via the existing `recent_checks_for_patient` semantics.
  * Kinesis Firehose (penguin-health-audit) → S3 Object Lock Compliance
    bucket — asynchronous, 7-year WORM archive, Athena-queryable.

The DDB write is the synchronous durability guarantee. Firehose buffers
up to 60s, so a Lambda crash + Firehose drop in the same window can lose
in-memory events; the DDB hot mirror covers that window and a backfill
Lambda can re-derive missing Firehose records from DDB if needed.

`emit` never raises into the request path. DDB failures emit the
CloudWatch metric `PenguinHealth/Audit AuditEmitFailure`; Firehose
failures emit `FirehosePutFailure`. The decorator always re-raises any
exception from the wrapped handler — only emission errors are swallowed.

See lambda/multi-org/audit/README.md for the full operating manual.
"""

from .actor import SystemPrincipal, from_event
from .decorator import audited
from .emitter import emit
from .schema import patient_hash

__all__ = [
    "audited",
    "emit",
    "from_event",
    "patient_hash",
    "SystemPrincipal",
]
