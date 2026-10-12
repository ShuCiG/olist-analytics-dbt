# Airflow 3 + Cosmos. dbt and dlt live in a separate venv (/opt/project-venv)
# built from uv.lock: same versions as local dev and CI, and no dependency clash
# with Airflow's own packages. Cosmos calls dbt from there as a subprocess.
FROM apache/airflow:3.3.2-python3.12

COPY --from=ghcr.io/astral-sh/uv:0.10.4 /uv /usr/local/bin/uv

# Airflow side: Cosmos only, pinned against Airflow's constraint file
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "astronomer-cosmos==1.15.1" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"

# Project side: dbt + dlt from the lock file (dev tools excluded)
ENV UV_PROJECT_ENVIRONMENT=/opt/project-venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy
USER root
# Named-volume mount points must exist and be owned by airflow, or Docker
# creates them root-owned
RUN mkdir -p /opt/project-venv /home/airflow/.cache/kagglehub /home/airflow/.dlt \
    && chown -R airflow: /opt/project-venv /home/airflow/.cache /home/airflow/.dlt
USER airflow
COPY --chown=airflow: pyproject.toml uv.lock /tmp/project/
RUN cd /tmp/project \
    && uv sync --frozen --no-dev --no-install-project --no-cache \
    && rm -rf /tmp/project
