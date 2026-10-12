{% docs __overview__ %}
# Olist analytics

ELT on the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce):
dlt loads the raw CSV files into DuckDB, dbt builds staging → intermediate → marts (star schema).

- **Facts:** `fct_orders` (one row per order), `fct_order_items` (one row per line item)
- **Dimensions:** `dim_customers`, `dim_sellers`, `dim_products`, `dim_date`
- Mart models have enforced **contracts**: column names and types are checked on every build.

Source code and data-quality notes: [github.com/ShuCiG/olist-analytics-dbt](https://github.com/ShuCiG/olist-analytics-dbt)
{% enddocs %}

{% docs order_id %}
Olist order identifier (32-char hex string).
{% enddocs %}

{% docs customer_unique_id %}
Person identifier. Olist issues a new `customer_id` for every order, so `customer_unique_id`
is the only key that groups orders of the same person.
{% enddocs %}

{% docs order_status %}
Order lifecycle status: `created`, `approved`, `invoiced`, `processing`, `shipped`,
`delivered`, `canceled`, `unavailable`.
{% enddocs %}

{% docs purchase_date %}
Calendar date of the purchase; joins to `dim_date.date_day`.
{% enddocs %}

{% docs order_amount %}
Items price + freight, in BRL. What the order costs according to its line items.
{% enddocs %}

{% docs payment_value %}
Total actually paid across all payments of the order, in BRL. Can differ from `order_amount`
(installment interest, vouchers); differences above 1 BRL are flagged by a warn test.
{% enddocs %}

{% docs zip_code_prefix %}
First 5 digits of the Brazilian CEP (postal code), kept as a string to preserve leading zeros.
{% enddocs %}

{% docs latitude %}
Latitude of the zip-prefix centroid (average of all Olist geolocation points). Null when the
prefix is missing from the geolocation dataset.
{% enddocs %}

{% docs longitude %}
Longitude of the zip-prefix centroid. Null when the prefix is missing from the geolocation dataset.
{% enddocs %}

{% docs loaded_at %}
UTC time of the dlt run that wrote the raw row (TIMESTAMPTZ on DuckDB, TIMESTAMP on BigQuery).
Cursor for incremental models.
{% enddocs %}
