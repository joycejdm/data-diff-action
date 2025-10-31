# Dockerfile (v3.0.0)
FROM python:3.10-slim

# Instala git (necessário para o dbt)
RUN apt-get update && apt-get install -y git-core

# Instala o dbt, o conector E a nova biblioteca de parsing
RUN pip install dbt-snowflake dbt-artifacts-parser

# Copia o nosso script python
COPY main.py /main.py

# Define o diretório de trabalho
WORKDIR /github/workspace

# Ponto de entrada
ENTRYPOINT ["python", "/main.py"]