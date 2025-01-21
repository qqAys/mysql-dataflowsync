# /bin/bash
docker compose down
docker image rm mysql-dataflowsync:latest
docker build -t mysql-dataflowsync:latest .
docker compose up -d
# docker logs