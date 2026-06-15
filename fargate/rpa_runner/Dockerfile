# RPA Fargate runner image.
#
# Base: Microsoft's official Playwright image pinned to a known good
# version. The image ships with the Chromium browser binary already
# installed at the system level, so we don't need a separate
# `playwright install` step at build time. Pin a specific tag — never
# track `:latest` — so CI builds are reproducible.
#
# Image: mcr.microsoft.com/playwright:v1.49.0-jammy (Ubuntu 22.04 base)
FROM mcr.microsoft.com/playwright:v1.49.0-jammy

# Run as a non-root user. The Playwright base image already creates a
# `pwuser`; reuse it rather than building our own.
USER root
WORKDIR /app

# System Python — the Playwright image bundles Python 3.10 but NOT pip,
# so install it via apt before pip-installing our pinned wheels.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/*

# All COPY paths in this Dockerfile are resolved against the repo root,
# because `infra/components/rpa.py` sets the DockerImageAsset build
# context to the repo root so the same Dockerfile can pull both
# `fargate/rpa_runner/` and `lambda/multi-org/`. Keep every COPY's
# source path rooted at the repo, not at `fargate/rpa_runner/`.
COPY fargate/rpa_runner/requirements.txt /app/requirements.txt
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r /app/requirements.txt

# Lambda code packages we depend on. The runner imports them at
# top-level so the layout must match `lambda/multi-org/...` as seen by
# Python. Mirror the import roots the Lambda Layer ships with.
COPY lambda/multi-org/rpa /app/rpa
COPY lambda/multi-org/audit /app/audit
ENV PYTHONPATH=/app

# Runner entrypoint.
COPY fargate/rpa_runner/main.py /app/main.py

USER pwuser
ENTRYPOINT ["python3", "/app/main.py"]
