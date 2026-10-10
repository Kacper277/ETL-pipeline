FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY sql ./sql

RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import requests,sys; r=requests.get('https://api.open-meteo.com/v1/forecast?latitude=52&longitude=21&daily=rain_sum',timeout=5); sys.exit(0 if r.ok else 1)"

CMD ["python", "-m", "src.pipeline"]
