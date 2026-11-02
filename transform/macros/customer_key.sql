{#
    Pseudonym of a person id (D94): hex SHA-256 of a secret key, "|" and the id.
    A plain hash would not do: the Olist ids are public, so anyone could hash them
    all and map every key back to its id. On BigQuery, where policy tags keep the
    id from most readers, the key is the one value in `pii_keys.customer_key`,
    generated inside BigQuery and readable only by the accounts that build: it
    never appears in code, compiled SQL, dbt docs or manifests, only this table
    name does. Not a dbt source on purpose: unit tests would then need a mock for
    it on BigQuery and reject one elsewhere. The other engines serve no reader
    without access to the id and use a fixed development key.
#}
{% macro customer_key(id) -%}
    {{ return(adapter.dispatch('customer_key', 'olist')(id)) }}
{%- endmacro %}

{% macro default__customer_key(id) -%}
    {{ sha256_hex(dbt.concat(["'olist-dev-key|'", id])) }}
{%- endmacro %}

{% macro bigquery__customer_key(id) -%}
    {{ sha256_hex("concat((select secret from `" ~ target.database ~ ".pii_keys.customer_key`), '|', " ~ id ~ ")") }}
{%- endmacro %}

{# Hex SHA-256 of a string expression, the same digest on every engine #}
{% macro sha256_hex(expression) -%}
    {{ return(adapter.dispatch('sha256_hex', 'olist')(expression)) }}
{%- endmacro %}

{% macro default__sha256_hex(expression) -%}
    sha256({{ expression }})
{%- endmacro %}

{% macro bigquery__sha256_hex(expression) -%}
    to_hex(sha256({{ expression }}))
{%- endmacro %}

{% macro snowflake__sha256_hex(expression) -%}
    sha2({{ expression }}, 256)
{%- endmacro %}

{% macro databricks__sha256_hex(expression) -%}
    sha2({{ expression }}, 256)
{%- endmacro %}
