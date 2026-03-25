FROM python:3.13-slim

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files first for layer caching
COPY pyproject.toml .
RUN uv sync --no-dev --no-install-project

# Copy source
COPY screener/ screener/
COPY config.yaml .
RUN uv sync --no-dev

EXPOSE 8000

# Default: server mode (headless daemon with API)
CMD ["uv", "run", "python", "-m", "screener", "--mode", "server"]
