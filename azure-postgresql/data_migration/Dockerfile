# syntax=docker/dockerfile:1
FROM python:3.10.5-buster
WORKDIR /app
COPY parallel_migrate.py ./
USER sync
ENTRYPOINT [ "python", "parallel_migrate.py" ]