# Dockerfile (v0.2.0)
FROM python:3.10-slim

# Instala git E unzip (para descompactar artefactos)
RUN apt-get update && apt-get install -y git-core unzip

# Instala dbt-snowflake (que já inclui o conector e o requests)
RUN pip install dbt-snowflake

COPY main.py /main.py
WORKDIR /github/workspace
ENTRYPOINT ["python", "/main.py"]