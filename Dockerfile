# Image Airflow du pipeline : l'image officielle + le pilote Postgres utilisé par le code.
FROM apache/airflow:3.3.2-python3.11

# Installé en tant qu'utilisateur "airflow" (non administrateur), comme le recommande Airflow.
RUN pip install --no-cache-dir "psycopg[binary]==3.3.6"

# Le code du pipeline est importable par les DAG : "from pipeline.etapes import ...".
ENV PYTHONPATH=/opt/airflow/python
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root python/ /opt/airflow/python/
COPY --chown=airflow:root sql/ /opt/airflow/sql/
