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
RUN mkdir -p /opt/project-venv /opt/project /home/airflow/.cache/kagglehub /home/airflow/.dlt \
    && chown -R airflow: /opt/project-venv /home/airflow/.cache /home/airflow/.dlt \
    && chown airflow:0 /opt/project
USER airflow
COPY --chown=airflow: pyproject.toml uv.lock /tmp/project/
RUN cd /tmp/project \
    && uv sync --frozen --no-dev --no-install-project --no-cache \
    && rm -rf /tmp/project

# The project itself, baked in (prod-like): the image is one immutable version of
# the code, tagged and labelled with its git commit. dbt packages and the manifest
# Cosmos renders the DAGs from are built here, so containers start ready.
# Live edit instead: docker-compose.dev.yml mounts the repo over /opt/project.
# Group 0 + g+w: compose may run the image as another uid in group 0 (AIRFLOW_UID),
# and dbt writes target/ and logs/ next to the project.
COPY --chown=airflow:0 . /opt/project
RUN cd /opt/project/transform \
    && /opt/project-venv/bin/dbt deps \
    && /opt/project-venv/bin/dbt parse --target-path target_airflow \
    && rm -rf logs \
    && chmod -R g+rwX /opt/project
ARG GIT_SHA=unknown
LABEL org.opencontainers.image.source="https://github.com/ShuCiG/olist-analytics-dbt" \
      org.opencontainers.image.revision="${GIT_SHA}"
ENV OLIST_GIT_SHA=${GIT_SHA}
