# Trino over Delta tables

Query layer over the Delta tables of the Spark work (#357): a fifth engine next to DuckDB, BigQuery, Databricks and
Snowflake, plus column-level access control. Decisions: D136.

| Piece | File |
|---|---|
| Trino 470 + Hive Metastore (Compose) | `docker-compose.yml`, `etc/catalog/delta.properties` |
| Spark writes Delta, Trino registers it | `lake.py` |
| Access rules (file-based) | `etc/rules.json`, `etc/access-control.properties` |
| Stdlib REST client | `client.py` |
| Tests | `test_trino.py` |

```bash
docker compose -f trino_lakehouse/docker-compose.yml up -d --wait     # first start ~5 min
uv run python -m trino_lakehouse.lake --csv-dir <folder with the Olist CSVs>
uv run pytest trino_lakehouse
```

## Access rules

| User | May read |
|---|---|
| `analyst` | `orders` without `customer_id`; `customers` without the identifiers and location columns; all of `order_payments` |
| `admin` | everything; runs `register_table` |

The tests: `sum(payment_value)` through Trino = `sum(fct_orders.paid_amount)` from the dbt build (when
`data/olist.duckdb` has the marts) and = the sum read from the CSV; each PII column is denied to `analyst` by name;
`SHOW COLUMNS` hides them; `admin` reads them.

Limit: the user is the `X-Trino-User` header, not an authenticated identity (see D136, "Not done").
