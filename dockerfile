# Dockerfile

FROM apache/airflow:2.7.1

# Installer les dépendances Python nécessaires
RUN pip install --no-cache-dir \
    boto3 \
    huggingface_hub \
    mysql-connector-python \
    elasticsearch \
    awscli \
    uvicorn \
    fastapi

# Si besoin, copier vos DAGs et scripts dans l'image 
# COPY dags /opt/airflow/dags
# COPY build /opt/airflow/build
