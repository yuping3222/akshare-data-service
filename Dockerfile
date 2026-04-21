FROM python:3.11-slim

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

# Create runtime directories
RUN mkdir -p cache logs

# Environment variables (override via docker-compose or .env)
ENV AKSHARE_DATA_CACHE_DIR=/app/cache
ENV AKSHARE_DATA_CONFIG_DIR=/app/config
ENV AKSHARE_DATA_CACHE_LOG_LEVEL=INFO

# Default command: run a quick health check
CMD ["python", "-c", "import akshare_data; svc = akshare_data.get_service(); print('Service initialized:', svc)"]
