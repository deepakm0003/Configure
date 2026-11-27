FROM python:3.11-slim

RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chown -R app:app /app
USER app

# Render provides PORT env var
CMD uvicorn api.app.main:app --host 0.0.0.0 --port ${PORT:-8000}
