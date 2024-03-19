FROM python:3.12-slim AS Builder

ENV PYTHONDONTWRITEBYTECODE 1

RUN apt-get update && apt-get install -y --no-install-recommends \
    libmariadb3 \
    libmariadb-dev \
    gcc

COPY web/requirements.txt .

RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir --upgrade -r requirements.txt

FROM python:3.12-slim AS Runner

ENV PYTHONDONTWRITEBYTECODE 1

RUN apt-get update && apt-get install -y --no-install-recommends \
    libmariadb3 \
    libmariadb-dev

RUN useradd -m eva

WORKDIR /app

COPY --from=Builder /opt/venv /opt/venv

ENV PATH=/opt/venv/bin:$PATH

COPY web/main.py .

COPY taiwan_stock_analysis ./taiwan_stock_analysis

USER eva

CMD ["uvicorn", "main:app", "--host", "0.0.0.0"]
# CMD ["gunicorn", "main:app","--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]

