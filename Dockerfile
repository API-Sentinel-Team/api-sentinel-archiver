# Dedicated archiver process: evidence/retention archiving on an interval.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY alembic.ini ./
COPY migrations/ ./migrations/
COPY server/ ./server/

RUN useradd -m -u 1000 appuser \
    && mkdir -p /app/data/archives \
    && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-m", "server.services.archiver_service"]
