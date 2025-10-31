# Dockerfile

# Use uma imagem Python leve
FROM python:3.10-slim

# Instale a biblioteca 'requests' para fazer chamadas de API
RUN pip install dbt-snowflake==1.8.2 # Vamos usar uma versão estável

# Copie o nosso script python (que vamos criar) para dentro do container
COPY main.py /main.py

# Diga ao container para rodar o nosso script python quando ele iniciar
ENTRYPOINT ["python", "/main.py"]