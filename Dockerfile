FROM python:3.12-slim

# 設置工作目錄為 /app
WORKDIR /app

# 複製 Pipfiles 到容器的 /app
COPY Pipfile /app/

RUN apt-get update && apt-get install -y \
    libmariadb3 \
    libmariadb-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install pipenv && \
    pipenv install --deploy --ignore-pipfile

# 複製其餘應用程式代碼到容器的 /app
COPY . /app/

CMD ["pipenv", "run", "python3", "run.py"]

