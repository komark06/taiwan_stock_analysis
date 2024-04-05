FROM python:3.12-slim AS Builder

ENV PIPENV_VENV_IN_PROJECT=1

COPY Pipfile Pipfile.lock /usr/src/

WORKDIR /usr/src

RUN pip install --upgrade pip && \
    pip install pipenv

RUN apt-get update && apt-get install -y --no-install-recommends \
    libmariadb3 \
    libmariadb-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pipenv sync

FROM python:3.12-slim AS Runner

RUN apt-get update && apt-get install -y --no-install-recommends \
    libmariadb3 \
    libmariadb-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/.venv/ /usr/src/.venv/

ENV PATH=/usr/src/.venv/bin:$PATH

WORKDIR /app

COPY run.py scrapy.cfg .

COPY taiwan_stock_analysis ./taiwan_stock_analysis

# Create a non-root user named "eva"
RUN useradd -m eva

# Switch to the "eva" user
USER eva

CMD ["python", "/app/run.py"]

