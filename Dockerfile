FROM python:3.13-slim

WORKDIR /app

# Install uv for fast dependency management (no ghcr.io dependency)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.local/bin:$PATH"

# Copy dependency files first for layer caching
COPY pyproject.toml .
RUN uv sync --no-dev --no-install-project

# Copy source
COPY screener/ screener/
COPY config.yaml .
COPY README.md .
RUN uv sync --no-dev

EXPOSE 8000

# Default: server mode (headless daemon with API)
CMD ["uv", "run", "python", "-m", "screener", "--mode", "server"]
