FROM apache/airflow:2.10-python3.11

USER airflow

# Copy project code
COPY --chown=airflow:root . /opt/airflow/project/

# Install project dependencies
RUN pip install --no-cache-dir -e /opt/airflow/project/

# PYTHONPATH
ENV PYTHONPATH="/opt/airflow/project:${PYTHONPATH}"
