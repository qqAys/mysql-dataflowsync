FROM python:3.12-slim-bookworm

LABEL authors="Jinx@qqAys"

WORKDIR /usr/src/mysql-dataflowsync

COPY . .

RUN pip install -i https://mirrors.aliyun.com/pypi/simple --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "/usr/src/mysql-dataflowsync/dfs-entrypoint.py"]