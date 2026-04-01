FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8000

# Inline shell avoids CRLF issues with docker-entrypoint.sh on Windows checkouts
CMD ["sh", "-c", "cd /app/backend && alembic upgrade head && cd /app && exec uvicorn backend.main:app --host 0.0.0.0 --port 8000"]
