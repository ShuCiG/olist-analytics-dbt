"""airflow/Dockerfile: the Airflow version has one source, the base image tag (#311).

Dependabot's docker ecosystem edits only the FROM line. Before this guard a bump to 3.4.0
left `apache-airflow==3.3.2` and the 3.3.2 constraint file in place: a 3.3.2 core on 3.4.0
providers, which nobody had tested. The official base image sets ENV AIRFLOW_VERSION to its
own version, and the pip pin and the constraint URL read that variable. A literal version
creeping back in fails here; airflow/check_dags.py compares the installed core with the
variable inside the built image.
"""

import re
from pathlib import Path

DOCKERFILE = (Path(__file__).resolve().parents[1] / "airflow" / "Dockerfile").read_text(encoding="utf-8")


def test_base_image_is_the_official_airflow_image() -> None:
    # AIRFLOW_VERSION is only set by this image: another FROM would leave the pin empty
    assert re.search(r"^FROM apache/airflow:\d+\.\d+\.\d+-python3\.12@sha256:", DOCKERFILE, flags=re.MULTILINE)


def test_pin_and_constraint_read_the_base_version() -> None:
    assert '"apache-airflow==${AIRFLOW_VERSION}"' in DOCKERFILE
    assert "constraints-${AIRFLOW_VERSION}/constraints-3.12.txt" in DOCKERFILE


def test_no_literal_airflow_version_beside_the_base_tag() -> None:
    assert not re.search(r"apache-airflow==\d", DOCKERFILE)
    assert not re.search(r"constraints-\d+\.\d+\.\d+/", DOCKERFILE)  # not the file name constraints-3.12.txt
    assert not re.search(r"^ARG AIRFLOW_VERSION", DOCKERFILE, flags=re.MULTILINE), "ARG would not override the base ENV"
