FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y \
    python3-dev \
    python3-setuptools \
    python3-gevent \
    build-essential \
    libssl-dev \
    libpq-dev \
    libffi-dev \
    gcc \
    libevent-dev \
    vim \
    curl \
    iputils-ping \
    libjpeg-dev

WORKDIR /app

COPY requirements.txt ../
RUN pip install --upgrade pip

COPY event-listener/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

RUN pip install -r ../requirements.txt

COPY . /app


CMD ["uvicorn", "event-listener.src.main:app", "--host", "0.0.0.0", "--port", "8001"]