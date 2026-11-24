# Airflow image for the Stage 3 batch pipeline.
#
# dbt is installed in its OWN virtual environment (/opt/dbt_venv) so that its
# dependencies can never clash with the ones Airflow pins. The DAGs call
# /opt/dbt_venv/bin/dbt directly.
FROM apache/airflow:3.3.2

# /opt is not writable for the airflow user, so the venv is built as root
# and made readable for every user (the containers run as your own user id).
USER root

RUN python -m venv /opt/dbt_venv \
    && /opt/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.7 dbt-clickhouse==1.10.4 \
    && chmod -R a+rX /opt/dbt_venv

USER airflow
