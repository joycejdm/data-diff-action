# Dockerfile (v0.5.0)
FROM python:3.10-slim

# Instala git E unzip
RUN apt-get update && apt-get install -y git-core unzip

# Instala o dbt, o conector E o "Motor V1V" (data-diff)
RUN pip install dbt-snowflake data-diff

COPY main.py /main.py
WORKDIR /github/workspace
ENTRYPOINT ["python", "/main.py"]