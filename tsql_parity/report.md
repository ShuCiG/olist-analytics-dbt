# T-SQL to Spark parity report

Pinned date `@as_of` = 2018-10-17 (the T-SQL originals used `GETDATE()`).

## Faithful port (the CI gate)

| metric | T-SQL | Spark | difference | verdict |
|---|---:|---:|---|---|
| usp_payment_installments: rows | 103,886 | 103,886 | 0.000 % | match |
| usp_payment_installments: row buckets | | | left only 0, right only 0, values differ 0 | match |
| usp_delivery_metrics: rows | 99,441 | 99,441 | 0.000 % | match |
| usp_delivery_metrics: row buckets | | | left only 0, right only 0, values differ 0 | match |
| usp_orders_without_reviews: rows | 768 | 768 | 0.000 % | match |
| usp_orders_without_reviews: row buckets | | | left only 0, right only 0, values differ 0 | match |
| usp_payment_type_summary: rows | 5 | 5 | 0.000 % | match |
| usp_payment_type_summary: row buckets | | | left only 0, right only 0, values differ 0 | match |

## Naive port (syntax translated only)

The port a reader writes first. Each deviation below is a semantic difference, not a bug:
see `docs/tsql-spark-differences.md`.

| metric | T-SQL | Spark | difference | verdict |
|---|---:|---:|---|---|
| usp_payment_installments: rows | 103,886 | 103,886 | 0.000 % | DIFFERENCE |
| usp_payment_installments: row buckets | | | left only 0, right only 0, values differ 4914 | DIFFERENCE |
| usp_payment_installments.installment_value | | | 4,914 rows differ | DIFFERENCE |
| usp_delivery_metrics: rows | 99,441 | 99,441 | 0.000 % | DIFFERENCE |
| usp_delivery_metrics: row buckets | | | left only 0, right only 0, values differ 1737 | DIFFERENCE |
| usp_delivery_metrics.days_overdue_open | | | 1,737 rows differ | DIFFERENCE |
| usp_orders_without_reviews: rows | 768 | 768 | 0.000 % | match |
| usp_orders_without_reviews: row buckets | | | left only 0, right only 0, values differ 0 | match |
| usp_payment_type_summary: rows | 5 | 5 | 0.000 % | match |
| usp_payment_type_summary: row buckets | | | left only 0, right only 0, values differ 0 | match |

- `usp_payment_installments` example: ('c9b045114b2a561d952e2b7a3e7593b3', 1): installment_value Decimal('32.75') vs Decimal('32.74')
- `usp_payment_installments` example: ('99e4bed65a545aa74dfbdfe233f052a5', 1): installment_value Decimal('29.19') vs Decimal('29.18')
- `usp_payment_installments` example: ('aa9c455b2d9888927e1b006b22980c5e', 1): installment_value Decimal('168.67') vs Decimal('168.66')
- `usp_delivery_metrics` example: ('ba41a24657e04c46f93a0c763f8a5646',): days_overdue_open 505 vs 3465
- `usp_delivery_metrics` example: ('0e3e9e19e36c877fbd3decb94152e837',): days_overdue_open 406 vs 3366
- `usp_delivery_metrics` example: ('a77843f2d4f89986e65fc1a4c1c0957a',): days_overdue_open 195 vs 3155
