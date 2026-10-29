"""Elementary reports inside the Airflow UI.

The DAGs' `edr_report` task writes one report per warehouse to
data/elementary/airflow_<warehouse>/index.html (mounted data folder). This plugin:
- serves that folder from the API server at /elementary-reports/ (a FastAPI sub-app:
  Airflow 3 runs plugin apps in its own server, no extra port);
- adds a menu entry per warehouse under Browse that shows the report inside the UI
  (`url_route` makes it an embedded page instead of a link that leaves the UI).

The same files are on their own port too (`elementary-reports` service in
docker-compose.yml). A report that doesn't exist yet (no run built dbt) is a 404.
"""

from __future__ import annotations

from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

REPORTS_DIR = Path("/opt/project/data/elementary")
URL_PREFIX = "/elementary-reports"
WAREHOUSES = {"duckdb": "DuckDB", "bigquery": "BigQuery"}

app = FastAPI(title="Elementary reports")
# check_dir=False: the folder appears with the first report, after the server starts
app.mount("/", StaticFiles(directory=REPORTS_DIR, html=True, check_dir=False), name="reports")


class ElementaryReportsPlugin(AirflowPlugin):
    name = "elementary_reports"
    fastapi_apps = [{"app": app, "url_prefix": URL_PREFIX, "name": "Elementary reports"}]
    external_views = [
        {
            "name": f"Elementary ({label})",
            "href": f"{URL_PREFIX}/airflow_{warehouse}/index.html",
            "url_route": f"elementary-{warehouse}",
            "destination": "nav",
            "category": "browse",
        }
        for warehouse, label in WAREHOUSES.items()
    ]
