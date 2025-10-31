# Dockerfile
FROM python:3.10-slim

# Instala o 'requests' (para postar comentários)
# E o 'dbt-snowflake' (que já traz o conector do Snowflake)
RUN pip install requests dbt-snowflake

# O resto fica igual
COPY main.py /main.py
ENTRYPOINT ["python", "/main.py"]

# FORÇA REBUILD v3