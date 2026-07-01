"""CentralReach API integration.

See `docs/centralreach-api-integration.md` for the design and
`docs/centralreach-onboarding.md` for the operational checklist.

This module replaces the browser-driven RPA integration end-to-end.
Pipeline shape per entry:

    list_query(date_range) -> for each entry:
        get_preview(entry.Id) -> preview.files[0].id
        get_resource_url(file_id) -> presigned S3 URL
        fetch_url(presigned_url) -> PDF bytes
        write_pdf_to_s3 + emit record

Module layout:

    auth.py           - Authenticator ABC + Session dataclass
    client.py         - HTTP client (cookie jar, retry, defaults)
    config.py         - CENTRALREACH_CONFIG loader
    exceptions.py     - CentralReachError hierarchy
    list_query.py     - POST /crxapi/internal/billing/query
    parameters.py     - resolve_date_range (yesterday-Eastern default)
    pdf_storage.py    - per-org bucket pdfs/ prefix writes
    pipeline.py       - run_ingest() per-entry orchestration
    preview.py        - GET /crxapi/billing/billing-entries/{id}/preview
    rate_limiter.py   - sync min-delay HTTP gate
    record.py         - CentralReachNoteRecord dataclass + validators
    record_builder.py - BillingEntry + PreviewResponse -> record
    resources.py      - POST /api/?resources.getresourceurl + PDF fetch
    result_writer.py  - persist_note: S3 + audit emission
    secrets.py        - Secrets Manager credentials loader
    usage_guard.py    - allowed-hours + blackout dates check

The Fargate entry point lives at `fargate/centralreach_ingest/main.py`;
the CDK infrastructure lives at `infra/components/centralreach.py`; the
admin HTTP API lives at `lambda/api/centralreach_api.py`.
"""
