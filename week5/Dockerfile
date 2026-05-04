# ── Stage 1: Builder — install Python dependencies ───────────────────────────
FROM python:3.11-slim AS builder
WORKDIR /app
COPY week5/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.11-slim
WORKDIR /app

# System deps: curl (health checks), procps (process management)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl procps && \
    rm -rf /var/lib/apt/lists/*

# Install Ollama
RUN curl -fsSL https://ollama.com/install.sh | sh

# Copy installed Python packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY week5/app/ /app/

# Copy TechCorp data (baked into image — no GCS needed)
COPY data/raw/techcorp/ /app/data/raw/techcorp/

# Copy startup script
COPY week5/start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Health check — verifies FastAPI is up
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=5 \
  CMD curl -f http://localhost:8001/health || exit 1

EXPOSE 8001

CMD ["/app/start.sh"]
