# infra — GCP as code

Terraform for the project's Google Cloud side (decision D46 in `docs/DECISIONS.md`):

| File | What |
|---|---|
| `datasets.tf` | 16 BigQuery datasets: prod (`raw`, `raw_staging`, `staging`, `intermediate`, `marts`, `seeds`, `snapshots`, `elementary`) and the same set for the Airflow DAG (`airflow_*`) |
| `service_accounts.tf` | `dbt-runner`, `dashboard-reader`, `olist-ci` (no keys, see below) |
| `iam.tf` | project roles and dataset read grants, additive (`*_iam_member`) |
| `cost.tf` | monthly billing budget with email alerts, BigQuery "query usage per day" quota |
| `apis.tf` | enabled APIs |
| `state.tf` | the GCS bucket that holds this configuration's state |
| `imports.tf` | adoption of the resources that were made by hand before Terraform |

Out of scope on purpose:
- **Service account keys.** A key made by Terraform is stored in the state in plain text. Keys are created in the console and kept as GitHub / Streamlit secrets.
- **`ci_*` datasets.** Each lives for one PR CI run (`drop_isolated_datasets --target ci`).
- **Tables.** dlt and dbt own everything inside the datasets.

## Run

Uses your own Google account through Application Default Credentials. The CI and dbt service accounts hold no IAM or billing rights, and CI only runs `fmt` and `validate`.

```bash
gcloud auth login --update-adc
gcloud auth application-default set-quota-project olist-analytics-dbt-472315
cd infra
cp terraform.tfvars.example terraform.tfvars   # billing account ID
terraform init
terraform plan                                 # expected: No changes
terraform apply
```

## First run (bootstrap, done once)

The state bucket is declared here too, so the very first run had no bucket to keep its state in:

1. Write `local_override.tf` with `terraform { backend "local" {} }`, then run `terraform init` and `terraform apply`. This imports everything in `imports.tf` and creates the bucket.
2. Delete `local_override.tf` and run `terraform init -migrate-state` to move the state into the bucket.
3. Run `terraform plan`. It should report `No changes`.

`cloudresourcemanager` and `iam` APIs had to be enabled by hand first (`gcloud services enable`): the provider needs them to read the rest.

## Airflow datasets and drift

The Airflow reset (`dbt run-operation drop_isolated_datasets --target airflow`) empties the `airflow_*` datasets (drops every table and view) instead of dropping them. So the datasets and the dashboard's read grant on `airflow_marts` survive a reset, and the next `plan` stays clean.
