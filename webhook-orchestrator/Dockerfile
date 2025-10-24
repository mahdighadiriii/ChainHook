FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y \
    python3-dev \
    python3-setuptools \
    build-essential \
    libssl-dev \
    libpq-dev \
    libffi-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ../
RUN pip install --upgrade pip && pip install -r ../requirements.txt

COPY webhook-orchestrator/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

COPY . /app
ENV PYTHONPATH=/app/webhook-orchestrator
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8002"]