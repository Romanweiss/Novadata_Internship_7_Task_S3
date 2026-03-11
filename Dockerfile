FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install aioboto3==15.5.0 python-dotenv pandas watchdog pyarrow

COPY src /app/src
COPY scripts /app/scripts
COPY .env.example /app/.env.example
COPY README.md /app/README.md

RUN mkdir -p /app/watch /app/archive /app/logs /app/downloads /app/tmp

CMD ["python", "scripts/run_pipeline.py"]
