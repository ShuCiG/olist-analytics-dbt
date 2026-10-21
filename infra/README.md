# infra — GCP as code

Terraform for the project's Google Cloud side (decision D46 in `docs/DECISIONS.md`):

| File | What |
|---|---|
| `datasets.tf` | 16 BigQuery datasets: prod (`raw`, `raw_staging`, `staging`, `intermediate`, `marts`, `seeds`, `snapshots`, `elementary`) and the same set for the Airflow DAG (`airflow_*`) |
| `service_accounts.tf` | `dbt-runner`, `dashboard-reader`, `olist-ci` (no keys, see below) |
| `wif.tf` | Workload Identity Federation for GitHub Actions: pool, OIDC provider limited to this repo, one impersonation binding per account |
| `iam.tf` | project roles and dataset read grants, additive (`*_iam_member`) |
| `cost.tf` | monthly billing budget with email alerts, BigQuery "query usage per day" quota |
| `apis.tf` | enabled APIs |
| `state.tf` | the GCS bucket that holds this configuration's state |
| `imports.tf` | adoption of the resources that were made by hand before Terraform |

Out of scope on purpose:
- **Service account keys.** A key made by Terraform is stored in the state in plain text. GitHub Actions needs none (`wif.tf`); the local Airflow stack and Streamlit use keys created in the console.
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

## Tests

`terraform test` (`tests/policy.tftest.hcl`) plans the configuration on mock providers. It needs no credentials and makes no GCP calls; CI runs it on every PR. It checks the code, not the live project; for that, run `terraform plan` against GCP. It asserts:
- the datasets are exactly the prod layers plus their `airflow_` twins, all in EU, with no default table or partition expiry;
- dataset grants are read-only (`dataViewer`), no service account holds a basic role (`owner`, `editor`, `viewer`), and `olist-ci` has only `bigquery.user` and reads no `elementary` or `airflow_*` dataset (D45);
- `dashboard-reader` can read the dataset that the dashboard's data usage agreement points to (consumer → contract server → dataset);
- the daily query quota stays within the free tier, the budget has thresholds, the state bucket is versioned and private;
- GitHub can impersonate only `dbt-runner` (from the `prod` environment) and `olist-ci` (from pull requests), by exact subjects of this repository; the provider accepts tokens of this repository only and prod tokens from `main` only; no project-level impersonation roles.

`tests/test_infra.py` (pytest) covers what `terraform test` can't see: there are no service account key resources, and every dataset and bucket has `prevent_destroy`.

```bash
cd infra && terraform init -backend=false && terraform test
```

## First run (bootstrap, done once)

The state bucket is declared here too, so the very first run had no bucket to keep its state in:

1. Write `local_override.tf` with `terraform { backend "local" {} }`, then run `terraform init` and `terraform apply`. This imports everything in `imports.tf` and creates the bucket.
2. Delete `local_override.tf` and run `terraform init -migrate-state` to move the state into the bucket.
3. Run `terraform plan`. It should report `No changes`.

`cloudresourcemanager` and `iam` APIs had to be enabled by hand first (`gcloud services enable`): the provider needs them to read the rest.

## Airflow datasets and drift

The Airflow reset (`dbt run-operation drop_isolated_datasets --target airflow`) empties the `airflow_*` datasets (drops every table and view) instead of dropping them. So the datasets and the dashboard's read grant on `airflow_marts` survive a reset, and the next `plan` stays clean.
