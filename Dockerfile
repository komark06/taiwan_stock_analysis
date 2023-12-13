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

RUN /usr/src/.venv/bin/python -c "import mariadb; print(mariadb.__version__)"

FROM python:3.12-slim AS Runner

RUN apt-get update && apt-get install -y --no-install-recommends \
    mariadb-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/.venv/ /usr/src/.venv/

ENV PATH=/usr/src/.venv/bin:$PATH

RUN python -c "import mariadb; print(mariadb.__version__)"

WORKDIR /app

COPY run.py scrapy.cfg  start.sh .

COPY taiwan_stock_analysis ./taiwan_stock_analysis

RUN chmod +x start.sh

# Create a non-root user named "eva"
RUN useradd -m eva

# Switch to the "eva" user
USER eva

CMD ["/bin/bash", "/app/start.sh"]

