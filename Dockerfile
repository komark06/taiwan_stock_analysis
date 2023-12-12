FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libmariadb3 \
    libmariadb-dev \
    gcc \
    mariadb-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app/

RUN pip install --upgrade pip && \
    pip install pipenv && \
    pipenv install --deploy

RUN chmod +x start.sh

CMD ["/bin/bash", "/app/start.sh"]

