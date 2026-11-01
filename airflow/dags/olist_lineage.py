"""OpenLineage for the olist ELT DAGs (D87): one graph in Marquez per warehouse, from
the dlt load through raw tables and every dbt model to the marts.

- Airflow tasks: the OpenLineage provider (in the base image) sends a run event per
  DAG run and task. Transport and namespace: docker-compose.yml.
- dlt_ingest (`RawLoadOperator`): its outputs are the raw tables it loads, named as
  the dbt integration names the sources, so both meet in one dataset node.
- dbt: Cosmos' watcher producer runs one `dbt build` and parses its artifacts into
  one OpenLineage event per model, which the provider would send merged into one
  producer job that reads and writes every table (that large event also timed out
  against Marquez). `stash_dbt_lineage` (the producer's post_execute) keeps them one
  job per model, children of the DAG run, adds column lineage from sqlglot
  (lineage/columns.py) and hands them on by XCom; the `openlineage` task sends them.
  Not from the producer itself: Marquez takes 1-3 min for a build's ~25 events,
  and Airflow kills a task's process 20 s after success (`task_success_overtime`).
  Cosmos' own task events are switched off in docker-compose.yml
  (`emission_policy`): the sensors carry no datasets.
- Dashboard (#197): the dbt exposure `olist_dashboard` becomes a job of its own that
  reads the marts it depends on (lineage/exposures.py), one COMPLETE run per DAG run
  that built dbt, sent with the model events. It says "the dashboard reads these
  tables", not "the dashboard ran". Only the DAG whose marts the live app reads
  (Warehouse.dashboard in olist_elt.py).

Lineage never fails a task: any error is logged and the run goes on. A failed build
sends no model events (its tasks' events still show the failure).
"""

from __future__ import annotations

import logging
import sys
import time

import pendulum
from airflow.providers.standard.operators.bash import BashOperator

# Repo root (in the image): the lineage package is unit-tested in the project venv
sys.path.insert(0, "/opt/project")
from lineage.columns import column_lineage  # noqa: E402
from lineage.exposures import exposure_inputs  # noqa: E402
from lineage.exposures import exposures as exposures  # noqa: E402  (re-exported for olist_elt.py)

log = logging.getLogger(__name__)

EVENTS_XCOM = "openlineage_events"


class RawLoadOperator(BashOperator):
    """BashOperator whose OpenLineage outputs are the raw tables it loads."""

    def __init__(self, *, lineage_namespace: str, raw_tables: list[str], **kwargs) -> None:
        super().__init__(**kwargs)
        self.lineage_namespace = lineage_namespace
        self.raw_tables = raw_tables

    def get_openlineage_facets_on_complete(self, task_instance):
        from airflow.providers.openlineage.extractors import OperatorLineage
        from openlineage.client.event_v2 import Dataset

        return OperatorLineage(
            outputs=[Dataset(namespace=self.lineage_namespace, name=t) for t in self.raw_tables]
        )

    def get_openlineage_facets_on_failure(self, task_instance):
        from airflow.providers.openlineage.extractors import OperatorLineage

        return OperatorLineage()


def _column_lineage_facet(namespace: str, columns: dict[str, list[tuple[str, str]]]):
    from openlineage.client.facet_v2 import column_lineage_dataset as cl

    return cl.ColumnLineageDatasetFacet(
        fields={
            column: cl.Fields(
                inputFields=[
                    cl.InputField(namespace=namespace, name=table, field=field)
                    for table, field in inputs
                ]
            )
            for column, inputs in columns.items()
            if inputs
        }
    )


def stash_dbt_lineage(context, result=None, exposures: list[dict] | None = None) -> None:
    """Producer's post_execute: its build's model events, ready to send, to XCom, plus
    one event per given exposure (manifest entries, lineage/exposures.py)."""
    try:
        _stash_dbt_lineage(context, exposures or [])
    except Exception:
        log.warning("OpenLineage: dbt model events not prepared", exc_info=True)


def _exposure_event(exposure: dict, inputs: list[dict], parent, namespace: str, producer: str) -> dict:
    from openlineage.client.event_v2 import InputDataset, Job, Run, RunEvent, RunState
    from openlineage.client.facet_v2 import documentation_job, job_type_job, ownership_job
    from openlineage.client.serde import Serde
    from openlineage.client.uuid import generate_new_uuid

    owner = exposure.get("owner") or {}
    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=pendulum.now("UTC").isoformat(),
        run=Run(runId=str(generate_new_uuid()), facets={"parent": parent}),
        job=Job(
            namespace=namespace,
            name=exposure["name"],
            facets={
                "jobType": job_type_job.JobTypeJobFacet(
                    processingType="BATCH", integration="DBT", jobType=exposure["type"].upper()
                ),
                "documentation": documentation_job.DocumentationJobFacet(
                    description=" ".join((exposure.get("description") or exposure["name"]).split())
                ),
                **(
                    {
                        "ownership": ownership_job.OwnershipJobFacet(
                            owners=[ownership_job.Owner(name=owner["name"], type="MAINTAINER")]
                        )
                    }
                    if owner.get("name")
                    else {}
                ),
            },
        ),
        inputs=[InputDataset(namespace=i["namespace"], name=i["name"]) for i in inputs],
        producer=producer,
    )
    serialized = Serde.to_dict(event)
    # No standard facet for the app's URL: dbt's own exposure fields, as a custom facet
    serialized["job"]["facets"]["dbt_exposure"] = {
        "_producer": producer,
        "_schemaURL": "https://schemas.getdbt.com/dbt/manifest/v12.json#/definitions/Exposure",
        **{k: exposure[k] for k in ("label", "url", "maturity", "type") if exposure.get(k)},
    }
    return serialized


def _stash_dbt_lineage(context, exposures: list[dict]) -> None:
    from airflow.providers.openlineage import conf
    from airflow.providers.openlineage.plugins import macros
    from openlineage.client.facet_v2 import parent_run, schema_dataset
    from openlineage.client.serde import Serde

    if conf.is_disabled():
        return
    # Set by Cosmos after the build (DbtLocalBaseOperator)
    events = getattr(context["task"], "openlineage_events_completes", None) or []
    # Test jobs left out: their results are on the datasets already (Cosmos' events
    # carry dataQualityAssertions), and a job per tested model doubles the graph
    events = [e for e in events if getattr(e.job.facets.get("jobType"), "jobType", None) != "TEST"]

    sql = {e.outputs[0].name: e.job.facets["sql"] for e in events if e.outputs and "sql" in e.job.facets}
    dialect = next(iter(sql.values())).dialect if sql else None
    traced = column_lineage({name: f.query for name, f in sql.items()}, dialect) if dialect else {}

    ti = context["ti"]
    namespace = macros.lineage_root_job_namespace(ti)
    dag_job, dag_run = macros.lineage_root_job_name(ti), macros.lineage_root_run_id(ti)
    parent = parent_run.ParentRunFacet(
        run=parent_run.Run(runId=dag_run),
        job=parent_run.Job(namespace=namespace, name=dag_job),
        root=parent_run.Root(
            run=parent_run.RootRun(runId=dag_run),
            job=parent_run.RootJob(namespace=namespace, name=dag_job),
        ),
    )
    def list_columns(dataset, columns) -> None:
        # Schema facets list the columns documented in YAML (none for the raw tables);
        # Marquez drops the lineage of a column its dataset doesn't list
        schema = dataset.facets.setdefault("schema", schema_dataset.SchemaDatasetFacet(fields=[]))
        listed = {f.name for f in schema.fields}
        schema.fields.extend(
            schema_dataset.SchemaDatasetFacetFields(name=c) for c in sorted(set(columns) - listed)
        )

    for event in events:
        event.run.facets["parent"] = parent
        for output in event.outputs:
            if output.name not in traced:
                continue
            columns = traced[output.name]
            output.facets["columnLineage"] = _column_lineage_facet(output.namespace, columns)
            list_columns(output, columns)
            for input_ in event.inputs:
                list_columns(input_, [c for inputs in columns.values() for t, c in inputs if t == input_.name])
    serialized = [Serde.to_dict(e) for e in events]
    for exposure in exposures:
        inputs = exposure_inputs(exposure, serialized)
        if inputs:
            serialized.append(_exposure_event(exposure, inputs, parent, namespace, events[0].producer))
    ti.xcom_push(key=EVENTS_XCOM, value=serialized)
    log.info(
        "OpenLineage: %d dbt model events (%d with column lineage), %d exposures",
        len(events), len(traced), len(serialized) - len(events),
    )


def send_dbt_lineage(ti, producer_task_id: str) -> None:
    """`openlineage` task: send the model events the producer prepared."""
    from airflow.providers.openlineage import conf
    from airflow.providers.openlineage.plugins.listener import get_openlineage_listener

    events = ti.xcom_pull(task_ids=producer_task_id, key=EVENTS_XCOM) or []
    if conf.is_disabled() or not events:
        log.info("OpenLineage: nothing to send")
        return
    transport = get_openlineage_listener().adapter.get_or_create_openlineage_client().transport
    start = time.monotonic()
    for sent, event in enumerate(events):
        try:
            # Prepared events are plain dicts: the transport sends them as they are
            transport.emit(event)  # type: ignore[arg-type]
        except Exception:
            # Marquez down: the next events would each wait for the same timeout
            log.warning("OpenLineage: sent %d of %d events, then failed", sent, len(events), exc_info=True)
            return
    log.info("OpenLineage: sent %d dbt model events in %.0f s", len(events), time.monotonic() - start)
