# ── Stage 1: dependency resolver ─────────────────────────────────────────────
FROM python:3.11-slim AS builder

# Pull uv binary from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Cache the dependency layer separately from app code
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy the rest and do the final install
COPY . .
RUN uv sync --frozen --no-dev


# ── Stage 2: runtime image ────────────────────────────────────────────────────
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Runtime system tools (curl for healthcheck probing)
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy installed venv + app from builder
COPY --from=builder /app /app

# Persistent data lands here via volume mount
RUN mkdir -p /app/logs /app/data

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 47883 47090 47765

CMD ["uv", "run", "python", "run.py"]
