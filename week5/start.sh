#!/bin/bash
# start.sh — Launch Ollama sidecar, pull model, then start FastAPI agent

set -e

MODEL=${OLLAMA_MODEL:-"llama3.2:3b"}

echo "==> Starting Ollama server..."
ollama serve &
OLLAMA_PID=$!

# Wait for Ollama to be ready
echo "==> Waiting for Ollama to be ready..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:11434/api/tags > /dev/null 2>&1; then
        echo "==> Ollama is ready."
        break
    fi
    echo "    attempt $i/30..."
    sleep 3
done

# Pull the model (skips if already cached)
echo "==> Pulling model: $MODEL"
ollama pull "$MODEL"
echo "==> Model ready."

# Start FastAPI agent
echo "==> Starting TechCorp agent on port 8001..."
exec uvicorn agent:app --host 0.0.0.0 --port 8001
