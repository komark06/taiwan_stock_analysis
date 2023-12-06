FROM python:3.12-slim

WORKDIR /app

COPY . /app/

RUN apt-get update && apt-get install -y \
    libmariadb3 \
    libmariadb-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install pipenv && \
    pipenv install --deploy

CMD ["pipenv", "run", "python3", "run.py"]

