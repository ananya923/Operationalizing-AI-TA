# Stage 1: Builder - install dependencies
FROM python:3.11-slim as builder
WORKDIR /app
COPY app/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y libgomp1 && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY app/backend/ .

# Copy download script
COPY week-02-assignment/starter/scripts/download_data.py /app/download_data.py

# Create directory for data
RUN mkdir -p /data/processed

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000

CMD ["/bin/bash", "-c", "python /app/download_data.py gs://${GCS_BUCKET} && uvicorn main:app --host 0.0.0.0 --port 8000"]