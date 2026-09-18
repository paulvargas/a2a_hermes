FROM python:3.12-slim

WORKDIR /app

# Install dependencies first so this layer is cached across source changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source.
COPY . .

RUN chmod +x entrypoint.sh

# Overridden per-service in docker-compose.yml (agentmart-worker, hermes-telegram).
# Default: the web front-end.
ENTRYPOINT ["sh", "entrypoint.sh"]
CMD ["python", "hermes_web.py"]
