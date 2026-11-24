# T-SQL to Spark: semantic differences found

A faithful-looking port of legacy T-SQL to Spark can return silently wrong numbers. The harness in
[`tsql_parity/`](../tsql_parity/) runs four legacy-style stored procedures on SQL Server 2022 and the
same logic in PySpark on Delta Lake, on the same raw Olist CSVs, and compares the outputs
(`tsql_parity/report.md`). Each difference below is a property of the engines, not a bug in either, and has
one test in [`tsql_parity/test_semantics.py`](../tsql_parity/test_semantics.py) that runs the case on both
engines.

| # | Difference | T-SQL | Spark (naive port) | Faithful port | Seen on real data |
|---|---|---|---|---|---|
| 1 | `NOT IN` with a NULL in the subquery | no rows | an anti-join returns rows | `NOT IN` (SQL) or an explicit NULL guard | no NULL in Olist reviews: planted in the test |
| 2 | Rounding, integer division | `ROUND` half away from zero; `int / int` is an integer | `bround` is half-even; `/` is fractional | `round`, `div` | 4,914 of 103,886 instalment values differ by a cent (half-even) |
| 3 | String ordering | `SQL_Latin1_General_CP1_CI_AS`: a, á, b, c | byte order: a, b, c, á | `ORDER BY x COLLATE UNICODE_CI` | test only |
| 4 | `TOP` without `ORDER BY` | the row the plan reaches first: another index, another row | `limit` without `orderBy`: partition order | explicit `ORDER BY` in both | test only |
| 5 | `GETDATE()` inside the calculation | depends on the run day | `current_date()` carried over: the same | the date is a parameter (`@as_of`) | 1,737 orders: overdue days 505 vs 3,465 |
| 6 | Empty string, trailing spaces | `'' = ' '` is true, `LEN('a  ')` is 1 | false, 3 | `rtrim` on both sides | test only |
| 7 | Implicit cast in a comparison | `varchar > 9` converts the column to int | `string > '9'` compares text | explicit `CAST` | test only |
| 8 | Division by zero | error 8134 | error with ANSI on (Spark 4 default), silent NULL with ANSI off | `NULLIF` guard in both | test only |

## Notes per difference

1. **NOT IN.** Three-valued logic: `x NOT IN (a, NULL)` is never true. Spark SQL agrees with T-SQL; a
   DataFrame `left_anti` join does not, because it matches on equality and ignores the NULL. The port keeps the
   SQL form.
2. **Rounding.** `ROUND(2.5, 0)` is 3 in T-SQL and in Spark `round` (HALF_UP). `bround` and Python's `round`
   give 2. With `value / installments` ties at the third decimal are common: 4,914 rows. `7 / 2` is 3 in T-SQL
   and 3.5 in Spark; `div` is the integer division. (Decimal division scale follows the same rule in both
   engines; the first draft of the test assumed it did not and the test caught that.)
3. **Collation.** The database collation decides `ORDER BY`, `=` on case and `MIN`/`MAX` of text. Spark 4 has
   collations (`COLLATE UNICODE_CI`); on older versions emulate with a normalised sort key.
4. **TOP without ORDER BY.** The legacy result is an accident of the plan. The test shows one table giving two
   answers through two indexes. A migration must add the order the original author assumed, and that order
   has to be found by asking, not by reading the code.
5. **GETDATE().** Not reproducible, so a parity run cannot compare it. The procedure takes `@as_of`. The
   naive port uses `current_date()`: equal to `GETDATE()` today, different from a pinned run.
6. **Empty string and trailing spaces.** T-SQL pads for comparison; Spark compares bytes. NULL is NULL in both,
   but how a loader turns an empty CSV field into NULL is the loader's choice: here both sides load an empty
   field as NULL (the Spark CSV reader by default, the Python loader of the reviews by `v != ""`).
7. **Implicit casts.** Data type precedence turns the varchar side into int in T-SQL. A port that keeps both
   sides text compares `'10' < '9'`.
8. **Division by zero.** Spark 3 returned NULL, Spark 4 raises by default (`spark.sql.ansi.enabled`), which
   narrows the gap. A job on a cluster that sets ANSI off still differs.

## What the harness does not cover

- Only four procedures and one dataset; no dynamic SQL, no `MERGE`, no `OUTPUT` clause, no time zones.
- SQL Server runs on Linux in a container (the `BULK INSERT` of the reviews file cannot read newlines inside
  quoted fields there, so that table is loaded from Python).
- Performance is not compared.
