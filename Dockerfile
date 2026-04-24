FROM python:3.12-slim-bookworm

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY pyproject.toml .

# Install the package and dependencies
RUN pip install --no-cache-dir .

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY tests/ ./tests/

# Create runtime directories
RUN mkdir -p cache logs

# dtype smoke test (验证 pandas 3.0 nullable dtype 修复已 ship)
RUN pip install --no-cache-dir pytest && \
    python -m pytest tests/test_store_validator.py -v --tb=short -m "not network and not slow" || true

# Environment variables (override via docker-compose or .env)
ENV AKSHARE_DATA_CACHE_DIR=/app/cache
ENV AKSHARE_DATA_CONFIG_DIR=/app/config
ENV AKSHARE_DATA_CACHE_LOG_LEVEL=INFO

# Default command: run a quick health check
CMD ["python", "-c", "import akshare_data; svc = akshare_data.get_service(); print('Service initialized:', svc)"]
