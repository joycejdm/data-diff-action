# Dockerfile (v2.1.0)
FROM python:3.10-slim

# Instala git (necessário para o dbt)
RUN apt-get update && apt-get install -y git-core

# Instala dbt-snowflake (que já inclui o conector e o requests)
RUN pip install dbt-snowflake

# Copia o nosso script python
COPY main.py /main.py

# Define o diretório de trabalho (onde o código do "cobaia" será montado)
WORKDIR /github/workspace

# Diga ao container para rodar o nosso script
ENTRYPOINT ["python", "/main.py"]