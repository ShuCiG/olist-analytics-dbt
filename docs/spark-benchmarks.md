# Spark tuning: measured, not assumed

Common Spark tuning techniques, measured on Olist order items against a skewed key (`seller_id`).
Several of them do not help on this data; the table says which and why.

Reproduce: `OLIST_CSV_DIR=<folder> bash spark_bench/docker_run.sh` (about 40 minutes with the two later experiments; raw numbers in
[`spark_bench/results.json`](../spark_bench/results.json)).

## Setup

- Spark 4 (`pyspark` from the `tsql` dependency group), `local[4]`, 3 GB driver, in a Linux container on a 4-core
  laptop with 8 GB for Docker. Nothing else was running (the local Airflow stack was stopped for the run).
- Data: the 112,650 order items replicated 50 times = 5.6 M rows (`order_id`, `seller_id`, `price`) as Parquet.
  The key `seller_id` is skewed naturally: 3,095 sellers, the largest holds 1.8 % of the rows.
  A second dataset `items_hot` gives one seller 40 % of the rows, to see what salting and AQE do when the skew is real.
- Each variant: one warm-up run, then the median of 3 timed runs. The query ends in a `noop` write, so the
  full plan runs and nothing is written. Shuffle read and disk spill are summed from the stage metrics of the
  Spark UI REST API and divided by the number of runs.

## Results

Times in seconds, shuffle read in MB per run. Disk spill was 0 MB in every variant: the data fits in memory,
so "spill" never discriminates here (see limits).

### 1. Salting against skew

| dataset | technique | time | shuffle read | verdict |
|---|---|---:|---:|---|
| natural | aggregate by seller, plain | 2.46 | 0.6 | baseline |
| natural | aggregate by seller, salted (16) | 7.51 | 11.4 | **worse, 3x** |
| hot | aggregate by seller, plain | 1.20 | 0.6 | baseline |
| hot | aggregate by seller, salted (16) | 5.11 | 10.7 | **worse, 4x** |
| natural | join sellers (sort-merge), plain | 6.07 | 37.4 | baseline |
| natural | join, salted (16) | 5.26 | 87.9 | 13 % faster, 2.3x the shuffle |
| hot | join sellers (sort-merge), plain | 4.09 | 39.1 | baseline |
| hot | join, salted (16) | 4.97 | 73.3 | **worse**, even with 40 % on one key |

**Negative result.** Salting an aggregate does not help: Spark already aggregates map-side (partial
aggregation), so a hot key sends one row per task to the reducer, not millions. Salting adds a second shuffle
and a second aggregation for nothing. Salting a join did not beat the plain sort-merge join either, even with
40 % of the rows on one key: at 5.6 M rows the hot partition (about 2 M rows) is one task of a few seconds, and
exploding the small side 16 times costs more shuffle than the skew costs time. Salting pays when one task runs
for minutes or does not fit in memory, which this data does not reach.

### 2. Broadcast join vs shuffle join

Fact table: 5.6 M rows. Dimension of D rows, key `order_id` (uniform), forced with a join hint. All variants
build the dimension with `distinct()`, which costs a shuffle of 187 MB per run in every row.

| D rows | broadcast | sort-merge | shuffle hash |
|---:|---:|---:|---:|
| 3,000 | **5.80** | 11.20 | 10.58 |
| 100,000 | **9.93** | 15.02 | 13.10 |
| 1,000,000 | **9.97** | 14.82 | 11.78 |
| 4,000,000 | **12.04** | 23.93 | 23.63 |

Broadcast won at every size up to 4 M rows; **no crossover was found**. The default
`spark.sql.autoBroadcastJoinThreshold` (10 MB) would refuse every dimension above roughly 100k rows here, so on
this setup the default is conservative. Do not carry this over to a cluster: in `local[4]` all tasks share one
JVM, so a broadcast costs no network copy per executor. The case where the join key is skewed and the small side
is a real small dimension is in section 1: broadcast was the fastest option there (1.48 s vs 6.07 s sort-merge).

### 3. Adaptive query execution (AQE)

Query: join items with sellers (forced sort-merge), aggregate twice.

| setting | time | shuffle read |
|---|---:|---:|
| AQE off, 200 shuffle partitions (default) | 4.61 | 37.4 |
| AQE off, 8 partitions (hand-tuned) | 4.03 | 86.3 |
| AQE on, 200 partitions | **3.22** | 37.3 |
| AQE off, 2000 partitions | 19.08 | 27.7 |
| AQE on, 2000 partitions | **3.19** | 27.5 |

AQE helped, mostly by **removing the cost of a bad partition count**: with 2000 partitions it ran in 3.19 s
instead of 19.08 s because it merges small partitions after the shuffle, and it was faster than the
hand-tuned 8. On the skew join (section 1) AQE skew handling (`join AQE skew handling`) took 3.31 s against 6.07 s
for the plain join on the natural data and 2.88 s against 4.09 s on the hot data. The natural data has no real
skew, so the gain is the same partition coalescing, not the splitting of skewed partitions: do not read this as
evidence that skew splitting worked.

### 4. `repartition` vs `coalesce` before a write

Items plus two `sha2` calls per row (a CPU-heavy step before the write), written as Parquet.

| variant | time | shuffle read | output |
|---|---:|---:|---|
| none | 8.75 | 0 | 4 files, 544 MB |
| `coalesce(4)` | 9.05 | 0 | 4 files, 544 MB |
| `repartition(4)` | **30.12** | 677 | 4 files, 549 MB |
| `coalesce(1)` | 21.20 | 0 | 1 file, 544 MB |
| `repartition(1)` | **39.42** | 676 | 1 file, 544 MB |

When the plan already has about as many partitions as the target, `coalesce` is free (no shuffle) and
`repartition` is 3x slower because it shuffles every byte (677 MB) for the same output. **The textbook warning
(coalesce collapses the parallelism of the expensive step before it) was not reproduced**: reading 544 MB of
Parquet at the default 128 MB split size already gives only about 4 partitions, so `coalesce(4)` cut nothing.
It shows only at `coalesce(1)`: 21 s against 9 s, because the `sha2` step ran in one task; `repartition(1)` is
still slower (39 s) since it pays the shuffle and then writes with one task as well.

### 4b. The same write with the source split into 32 partitions

Section 4 did not show the coalesce pitfall because the read produced about as many partitions as the
target. Here `spark.sql.files.maxPartitionBytes=16m` splits the source into 32 partitions (the heavy `sha2`
step runs in 32 tasks on 4 cores), then the write reduces to 4, 2 or 1 partitions.

| variant | time | shuffle read | output |
|---|---:|---:|---|
| none (32 partitions) | 12.51 | 0 | 32 files, 546 MB |
| `coalesce(4)` | 11.74 | 0 | 4 files, 544 MB |
| `repartition(4)` | **22.27** | 677 | 4 files, 549 MB |
| `coalesce(2)` | 12.98 | 0 | 2 files, 544 MB |
| `repartition(2)` | **18.93** | 677 | 2 files, 549 MB |
| `coalesce(1)` | 19.60 | 0 | 1 file, 544 MB |
| `repartition(1)` | 24.07 | 676 | 1 file, 544 MB |

**The pitfall is still not visible down to `coalesce(2)`.** Two tasks on a 4-core machine cost 1.5 s over the
32-task plan (12.98 s vs 11.74 s for `coalesce(4)`, inside the noise). The `sha2` step is not heavy enough to
dominate the write of 544 MB; the textbook case needs a step that takes minutes per partition. It appears at
`coalesce(1)` (19.6 s, +57 % over the unreduced plan), and even there `repartition(1)` is slower (24.1 s)
because it adds a 676 MB shuffle. Practical reading: below the core count, `coalesce` loses parallelism and
`repartition` pays a full shuffle; here the shuffle was always the larger bill.

### 5. `spark.sql.shuffle.partitions` sweep

Query of section 3 (join, two aggregations), 2 to 2000 shuffle partitions, AQE off and on.

| partitions | AQE off | AQE on | shuffle read MB |
|---:|---:|---:|---:|
| 2 | 7.81 | 8.23 | 127 |
| 4 | 6.66 | 5.63 | 106 |
| 8 | 6.21 | 5.96 | 86 |
| 16 | 5.90 | 5.77 | 70 |
| 32 | 7.64 | 4.68 | 57 |
| 64 | **4.97** | **3.98** | 47 |
| 200 (default) | 7.28 | 5.41 | 37 |
| 800 | 19.84 | 5.88 | 30 |
| 2000 | 29.81 | 5.01 | 28 |

Without AQE the curve is a shallow bowl with its bottom at 64 (4.97 s, 32 to 200 within noise of each other)
and a steep right side: 800 partitions cost 4x and 2000 cost 6x the best time, because each of the tasks pays
a fixed scheduling and shuffle-file cost. Too few partitions (2) is only 1.6x worse on 5.6 M rows. With AQE on
the right side disappears (5.9 s at 800, 5.0 s at 2000: small partitions are merged after the shuffle) and the
whole curve sits at 4 to 8 s. The default of 200 is about 1.4x off the optimum without AQE and 1.4x with it;
the sweep cannot separate that from noise (single runs vary by up to 2x, for example a 13.4 s outlier
among 8.0 s runs at AQE on, 2 partitions), so the safe reading is: leave AQE on, and do not set the value in the thousands.

## Verdicts

| technique | verdict on this data |
|---|---|
| salting an aggregate | does not help (3-4x slower): partial aggregation already absorbs skew |
| salting a join | does not help, even with 40 % of rows on one key |
| broadcast join | helps at every size tested (up to 4 M rows); the 10 MB default is conservative on one JVM |
| AQE | helps, mainly by fixing the partition count; skew splitting not proven by this data |
| `coalesce` before write | free when the partition count is already near the target; costs parallelism only for a heavy step and a much lower target |
| `coalesce` with a 32-partition source | free down to 2 partitions on 4 cores; only `coalesce(1)` shows the cost (+57 %) |
| shuffle partitions | 64 best without AQE; 800+ is 4-6x slower; AQE on makes the value almost irrelevant |
| `repartition` before write | 3x slower here; use it when the data must be evenly spread or the heavy step needs more partitions than it has |

## Limits

- One machine, one JVM, in-memory data: no network cost for broadcast, no spill (0 MB everywhere), and tasks of
  seconds. The skew effects that matter in production (a task that runs for minutes, executor out of memory)
  need data this laptop cannot hold. The numbers show what does **not** help at this size, not what helps on a cluster.
- 5.6 M rows are replicated copies of 112,650 items, so every seller has 50 times its real rows; the shape of
  the skew is the real one, the number of distinct orders is not.
- Run-to-run noise is about 10-20 % on a shared laptop; differences under that are not conclusions. The
  4x and 3x results above are far outside it.
- Not done: bucketing, Z-order / file compaction on Delta, other executors than `local[4]`, a write step
  heavy enough to show the coalesce pitfall above one partition.
- Sections 4b and 5 ran on 2026-11-27 with the local Airflow stack stopped; a first attempt of 4b died when the
  Spark UI REST API dropped a response under load, so `stage_metrics` retries and the UI keeps fewer stages.
