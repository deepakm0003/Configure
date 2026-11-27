FROM python:3.11-slim

# Non-root user
RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ ./pipeline/

RUN chown -R app:app /app
USER app

CMD ["python", "-m", "pipeline.main"]
