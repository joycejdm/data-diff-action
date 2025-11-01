# Dockerfile (v0.5.2)
FROM python:3.10-slim

RUN apt-get update && apt-get install -y git-core unzip
RUN pip install dbt-snowflake data-diff

COPY main.py /main.py
WORKDIR /github/workspace
ENTRYPOINT ["python", "/main.py"]

# FORÇA REBUILD v0.5.2