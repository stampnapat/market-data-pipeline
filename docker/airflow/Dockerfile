# Custom Airflow image = official image + our Python dependencies.
# Baking deps into the image (instead of pip-installing at container start)
# makes every container identical and startup fast.
FROM apache/airflow:3.3.2-python3.12

# Airflow publishes a constraints file per version: pinned versions of every
# transitive dependency known to work with this Airflow release.
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt --constraint "${CONSTRAINTS_URL}"

# Our ingestion package, installed in editable mode. At runtime ./src is also
# mounted over /opt/pipeline/src, so code changes apply without rebuilding.
COPY --chown=airflow:root pyproject.toml /opt/pipeline/pyproject.toml
COPY --chown=airflow:root src /opt/pipeline/src
RUN pip install --no-cache-dir -e /opt/pipeline --constraint "${CONSTRAINTS_URL}"

# dbt lives in its OWN virtualenv: dbt-core and Airflow pin many of the same
# libraries to different versions, so sharing one environment invites conflicts.
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.12.0" "dbt-postgres~=1.11.0" \
    && chown -R airflow:root /opt/dbt-venv
USER airflow
