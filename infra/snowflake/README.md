# infra/snowflake — Snowflake account as code

Separate Terraform root for the Snowflake engine (#185), so the GCP root never needs the Snowflake account. State: same GCS bucket as `infra/`, prefix `snowflake`.

| File | What |
|---|---|
| `warehouse.tf` | `OLIST_WH`: X-Small, auto-suspend 60 s, statement timeout 30 min |
| `cost.tf` | account-level resource monitor: 20 credits a month, notify at 50/80 %, suspend at 100 %, cancel queries at 110 % |
| `database.tf` | database `OLIST`; dlt and dbt create the schemas inside |
| `access.tf` | role `OLIST_RUNNER` (warehouse usage, `CREATE SCHEMA` on `OLIST`), key-pair service user `OLIST_SVC` |
| `bootstrap.sql` | one-time `TERRAFORM_SVC` service user Terraform runs as |
| `tests/policy.tftest.hcl` | cost guards and grants, on a mock provider (CI) |

No private keys in the repo or the state: users get public keys only, private keys stay in `~/.snowflake/keys/`.

## Run

```bash
# once: key pairs
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out ~/.snowflake/keys/olist_terraform.p8
openssl pkey -in ~/.snowflake/keys/olist_terraform.p8 -pubout -out ~/.snowflake/keys/olist_terraform.pub
# (same for olist_svc), then bootstrap.sql in Snowsight with the olist_terraform public key

cd infra/snowflake
cp terraform.tfvars.example terraform.tfvars   # names from bootstrap.sql output, olist_svc public key
terraform init
terraform plan -out snowflake.tfplan
terraform apply snowflake.tfplan
```

Names are case-sensitive when the provider quotes them: a user created by the trial signup can be lowercase (`owner_user = "letsko"`).

The GCS backend needs your own Google account (ADC): unset `GOOGLE_APPLICATION_CREDENTIALS` if it points to a service account key.
