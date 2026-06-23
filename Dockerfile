# Single image for all app services; the compose `command` selects which to run.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first (better layer caching), then the package.
COPY pyproject.toml ./
COPY src ./src
RUN pip install .

# Run as non-root.
RUN useradd --create-home --uid 1000 appuser
USER appuser

# Default command; overridden per service in docker-compose.yml.
CMD ["python", "-m", "cryptonorm.services.run_api"]
