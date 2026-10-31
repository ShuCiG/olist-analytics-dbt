# Airflow 3 + Cosmos. dbt and dlt live in a separate venv (/opt/project-venv)
# built from uv.lock: same versions as local dev and CI, and no dependency clash
# with Airflow's own packages. Cosmos calls dbt from there as a subprocess.
# Base images by digest (a tag can be re-pushed); Dependabot proposes updates
FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

COPY --from=ghcr.io/astral-sh/uv:0.10.4@sha256:4cac394b6b72846f8a85a7a0e577c6d61d4e17fe2ccee65d9451a8b3c9efb4ac /uv /usr/local/bin/uv

# Airflow side: Cosmos, and sqlglot for the column lineage the producer's callback
# sends (lineage/columns.py, D87); pinned against Airflow's constraint file
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "astronomer-cosmos==1.15.1" \
    sqlglot \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"

# Project side: dbt + dlt from the lock file (dev tools excluded)
ENV UV_PROJECT_ENVIRONMENT=/opt/project-venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy
USER root
# Named-volume mount points must exist and be owned by airflow, or Docker
# creates them root-owned
RUN mkdir -p /opt/project-venv /opt/edr-venv /opt/project /home/airflow/.cache/kagglehub /home/airflow/.dlt /opt/dbt-cache \
    && chown -R airflow: /opt/project-venv /home/airflow/.cache /home/airflow/.dlt /opt/dbt-cache \
    && chown airflow:0 /opt/project /opt/edr-venv
USER airflow
COPY --chown=airflow: pyproject.toml uv.lock /tmp/project/
RUN cd /tmp/project \
    && uv sync --frozen --no-dev --no-install-project --no-cache \
    && rm -rf /tmp/project

# Elementary's CLI (edr) for the report task: its own venv (it would downgrade
# networkx in the project venv, D38), from the hash-pinned lock CD uses too. The
# packages of edr's internal dbt project are installed here, and the venv is group
# writable: edr runs dbt in that project, as whatever uid compose gives the task.
COPY --chown=airflow: .github/edr-requirements.txt /tmp/edr-requirements.txt
RUN uv venv --allow-existing /opt/edr-venv \
    && uv pip install --python /opt/edr-venv --require-hashes --no-cache -r /tmp/edr-requirements.txt \
    && cd /opt/edr-venv/lib/python3.12/site-packages/elementary/monitor/dbt_project \
    && /opt/edr-venv/bin/dbt deps \
    && rm -rf logs /tmp/edr-requirements.txt \
    && chmod -R g+rwX /opt/edr-venv

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
