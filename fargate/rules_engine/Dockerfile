# Rules engine Fargate image.
#
# Runs one validation pass end-to-end for a single (org, run_id). The
# task exits when the pass completes. No always-on service, no daemon
# loop. Long-running enough to remove the 15-minute Lambda ceiling; the
# state machine's 10-hour timeout is the real upper bound.
#
# Image: python:3.11-slim-bookworm (Debian 12 base). Same slim base as
# the CentralReach ingest image; keeps the container ~150MB compressed
# and the ECS pull fast.
FROM python:3.11-slim-bookworm

USER root
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

# All COPY paths in this Dockerfile are resolved against the repo root,
# because `infra/components/rules_engine.py` sets the DockerImageAsset
# build context to the repo root so the same Dockerfile can pull both
# `fargate/rules_engine/` and `lambda/multi-org/`.
COPY fargate/rules_engine/requirements.txt /app/requirements.txt
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r /app/requirements.txt

# Rules engine modules. Copied flat into /app/ so
# `from rules_engine_rag import run_validation` resolves at the sys.path
# root — same layout callers assume today.
COPY lambda/multi-org/rules-engine/bedrock_client.py /app/bedrock_client.py
COPY lambda/multi-org/rules-engine/claude_cost.py /app/claude_cost.py
COPY lambda/multi-org/rules-engine/rate_limiter.py /app/rate_limiter.py
COPY lambda/multi-org/rules-engine/multi_org_config.py /app/multi_org_config.py
COPY lambda/multi-org/rules-engine/document_validator.py /app/document_validator.py
COPY lambda/multi-org/rules-engine/deterministic_evaluator.py /app/deterministic_evaluator.py
COPY lambda/multi-org/rules-engine/results_handler.py /app/results_handler.py
COPY lambda/multi-org/rules-engine/field_extractor.py /app/field_extractor.py
COPY lambda/multi-org/rules-engine/parquet_writer.py /app/parquet_writer.py
COPY lambda/multi-org/rules-engine/queue_handler.py /app/queue_handler.py
COPY lambda/multi-org/rules-engine/rules_engine_rag.py /app/rules_engine_rag.py

# Shared packages the rules engine imports.
COPY lambda/multi-org/audit /app/audit
COPY lambda/multi-org/notifications /app/notifications

ENV PYTHONPATH=/app

# Runner entrypoint.
COPY fargate/rules_engine/main.py /app/main.py

# Run as a non-root user. Create one rather than reusing a base-image
# user since the python:slim base doesn't ship with one.
RUN groupadd -r runner && useradd -r -g runner runner
USER runner

ENTRYPOINT ["python3", "/app/main.py"]
