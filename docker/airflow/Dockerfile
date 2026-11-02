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
