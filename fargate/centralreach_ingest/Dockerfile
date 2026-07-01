# CentralReach Fargate ingest image.
#
# Pure HTTP+S3 workload — no browser, no Playwright. Slim Python base
# image keeps the container small (~150MB vs ~1.5GB for the rpa image)
# and the cold start fast.
#
# Image: python:3.11-slim-bookworm (Debian 12 base)
# Pin a specific tag — never track `:latest` — so CI builds are
# reproducible.
FROM python:3.11-slim-bookworm

USER root
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

# tzdata is needed for zoneinfo (parameters.py + usage_guard.py both
# resolve org-local timezones). The slim image doesn't include the
# tzdata package by default.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata && \
    rm -rf /var/lib/apt/lists/*

# All COPY paths in this Dockerfile are resolved against the repo root,
# because `infra/components/centralreach.py` sets the DockerImageAsset
# build context to the repo root so the same Dockerfile can pull both
# `fargate/centralreach_ingest/` and `lambda/multi-org/`. Keep every
# COPY's source path rooted at the repo, not at
# `fargate/centralreach_ingest/`.
COPY fargate/centralreach_ingest/requirements.txt /app/requirements.txt
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r /app/requirements.txt

# Lambda code packages we depend on. The runner imports them at
# top-level so the layout must match `lambda/multi-org/...` as seen by
# Python. Mirror the import roots the Lambda Layer ships with.
COPY lambda/multi-org/centralreach /app/centralreach
COPY lambda/multi-org/audit /app/audit

# Bedrock client + its two transitive modules from rules-engine.
# `centralreach.narrative_extractor` calls `invoke_claude_model` for
# ingest-time PDF narrative extraction; it lazy-imports these at
# runtime and expects them at the sys.path root, same layout the
# rules-engine Lambda uses. Keep in sync with `admin_ui.py`'s
# `shared_llm_modules` list.
COPY lambda/multi-org/rules-engine/bedrock_client.py /app/bedrock_client.py
COPY lambda/multi-org/rules-engine/claude_cost.py /app/claude_cost.py
COPY lambda/multi-org/rules-engine/rate_limiter.py /app/rate_limiter.py

ENV PYTHONPATH=/app

# Runner entrypoint.
COPY fargate/centralreach_ingest/main.py /app/main.py

# Run as a non-root user. Create one rather than reusing a base-image
# user since the python:slim base doesn't ship with one.
RUN groupadd -r ingest && useradd -r -g ingest ingest
USER ingest

ENTRYPOINT ["python3", "/app/main.py"]
