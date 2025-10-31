# Dockerfile (v4.0.0)
FROM python:3.10-slim

# Instala git (necessário para o dbt)
RUN apt-get update && apt-get install -y git-core

# Instala dbt-snowflake (que já inclui o conector e o requests)
# NÃO instala o 'dbt-artifacts-parser'
RUN pip install dbt-snowflake

COPY main.py /main.py
WORKDIR /github/workspace
ENTRYPOINT ["python", "/main.py"]