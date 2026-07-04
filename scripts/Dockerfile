# Build context is the monorepo root so the shared schemas/ directory is available.
FROM python:3.11-slim

WORKDIR /app

COPY scripts/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/inject_alert.py .
COPY schemas /app/schemas

CMD ["python", "inject_alert.py"]
