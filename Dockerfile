# =============================================================================
# openddil-logistics-fusion-service — Per-Asset Logistics Fusion (Phase 3.5)
# =============================================================================
# Two-stage build, same pattern as openddil-cm-service.
# =============================================================================

# ---------- Stage 1: Builder ----------
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ make librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv && uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY pyproject.toml .
RUN uv pip compile pyproject.toml -o requirements.txt \
    && uv pip install --no-cache -r requirements.txt

# ---------- Stage 2: Runtime ----------
FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
# Generated proto stubs come in via volume mount at /proto;
# shared bootstrap library at /app/openddil_bootstrap; service source under /app/src.
ENV PYTHONPATH=/proto:/app/src:/app

WORKDIR /app
COPY src /app/src

EXPOSE 9081/tcp

CMD ["python", "/app/src/main.py"]
