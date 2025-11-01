# Dockerfile (v1.0.0 - O MVP Estável)
FROM python:3.10-slim

# Instala git E unzip (para os artefactos)
RUN apt-get update && apt-get install -y git-core unzip

# Instala dbt-snowflake (NÃO instala o data-diff)
RUN pip install dbt-snowflake

COPY main.py /main.py
WORKDIR /github/workspace
ENTRYPOINT ["python", "/main.py"]