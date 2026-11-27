"""Spark performance benchmark on Olist order items (#361).

Measures four tuning techniques against a skewed key (`seller_id`) and records, per variant,
wall time (median of 3 timed runs after a warm-up), shuffle read and disk spill, taken from
the Spark UI REST API (stage metrics). Negative results are kept: the point is evidence.

    OLIST_CSV_DIR=<folder> bash spark_bench/docker_run.sh [--scale N] [--only NAME]
"""

import argparse
import json
import shutil
import statistics
import time
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

HOT_SHARE = 0.4  # share of rows given to one seller in the "hot" dataset
SALT = 16
RUNS = 3
SWEEP = (2, 4, 8, 16, 32, 64, 200, 800, 2000)
MB = 1024 * 1024


@dataclass
class Result:
    experiment: str
    dataset: str
    variant: str
    seconds: float
    shuffle_read_mb: float
    spill_mb: float
    runs: list[float] = field(default_factory=lambda: [])
    note: str = ""


def session(driver_memory: str) -> SparkSession:
    return (
        SparkSession.builder.appName("spark-bench")  # pyright: ignore[reportUnknownMemberType]
        .master("local[4]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.ui.retainedStages", "300")
        .getOrCreate()
    )


def stage_metrics(spark: SparkSession) -> dict[int, tuple[int, int]]:
    """stage id -> (shuffle read bytes, disk spilled bytes) of all completed stages."""
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages?status=complete&details=false"
    for attempt in range(5):  # the UI server drops a response when the CPU is saturated
        try:
            with urllib.request.urlopen(url) as resp:
                stages: list[dict[str, Any]] = json.load(resp)
            return {s["stageId"]: (s["shuffleReadBytes"], s["diskBytesSpilled"]) for s in stages}
        except (json.JSONDecodeError, OSError):
            if attempt == 4:
                raise
            time.sleep(2)
    raise AssertionError("unreachable")


def prepare(spark: SparkSession, csv_dir: Path, data: Path, scale: int) -> None:
    """Order items replicated `scale` times (natural seller distribution) + a hot variant."""
    if data.exists():
        shutil.rmtree(data)
    items = (
        spark.read.option("header", True)
        .csv(str(csv_dir / "olist_order_items_dataset.csv"))
        .select("order_id", "seller_id", F.col("price").cast("double").alias("price"))
    )
    copies = spark.range(scale).withColumnRenamed("id", "copy")
    natural = items.crossJoin(copies).select(
        F.concat_ws("-", "order_id", "copy").alias("order_id"), "seller_id", "price"
    )
    natural.repartition(32).write.parquet(str(data / "items"))
    hot_seller = (
        items.groupBy("seller_id").count().orderBy(F.desc("count")).first()["seller_id"]  # pyright: ignore[reportOptionalSubscript]
    )
    natural = spark.read.parquet(str(data / "items"))
    hot = natural.withColumn(
        "seller_id", F.when(F.rand(7) < HOT_SHARE, F.lit(hot_seller)).otherwise(F.col("seller_id"))
    )
    hot.repartition(32).write.parquet(str(data / "items_hot"))
    sellers = spark.read.option("header", True).csv(str(csv_dir / "olist_sellers_dataset.csv"))
    sellers.select("seller_id", "seller_state").write.parquet(str(data / "sellers"))


Variant = tuple[str, dict[str, str], Callable[[], DataFrame]]


def run_noop(df: DataFrame) -> None:
    df.write.format("noop").mode("overwrite").save()


def measure(
    spark: SparkSession,
    experiment: str,
    dataset: str,
    variant: str,
    conf: dict[str, str],
    build: Callable[[], DataFrame],
) -> Result:
    defaults = {k: spark.conf.get(k, None) for k in conf}  # pyright: ignore[reportUnknownMemberType]
    for k, v in conf.items():
        spark.conf.set(k, v)
    try:
        try:
            run_noop(build())  # warm-up: JIT, file cache, discarded
            before = stage_metrics(spark)
            times: list[float] = []
            for _ in range(RUNS):
                start = time.perf_counter()
                run_noop(build())
                times.append(time.perf_counter() - start)
            after = stage_metrics(spark)
        except Exception as exc:
            return Result(experiment, dataset, variant, float("nan"), 0, 0, note=f"failed: {type(exc).__name__}")
        new = [after[s] for s in after if s not in before]
        shuffle = sum(r for r, _ in new) / RUNS / MB
        spill = sum(sp for _, sp in new) / RUNS / MB
        return Result(
            experiment,
            dataset,
            variant,
            statistics.median(times),
            round(shuffle, 1),
            round(spill, 1),
            [round(t, 2) for t in times],
        )
    finally:
        for k in conf:
            previous = defaults[k]
            if previous is None:
                spark.conf.unset(k)
            else:
                spark.conf.set(k, previous)


def salted_agg(items: DataFrame) -> DataFrame:
    salted = items.withColumn("salt", (F.rand(3) * SALT).cast("int"))
    stage1 = salted.groupBy("seller_id", "salt").agg(F.sum("price").alias("s"), F.count("*").alias("c"))
    return stage1.groupBy("seller_id").agg(F.sum("s").alias("revenue"), F.sum("c").alias("n"))


def salted_join(items: DataFrame, sellers: DataFrame) -> DataFrame:
    left = items.withColumn("salt", (F.rand(3) * SALT).cast("int"))
    right = sellers.withColumn("salt", F.explode(F.array(*[F.lit(i) for i in range(SALT)])))
    return left.join(right, ["seller_id", "salt"]).groupBy("seller_state").agg(F.sum("price"))


def no_broadcast() -> dict[str, str]:
    return {"spark.sql.autoBroadcastJoinThreshold": "-1"}


def experiments(
    spark: SparkSession, data: Path
) -> dict[str, list[tuple[str, str, dict[str, str], Callable[[], DataFrame]]]]:
    """experiment -> [(dataset, variant, conf, builder)]"""
    items = {name: spark.read.parquet(str(data / name)) for name in ("items", "items_hot")}
    sellers = spark.read.parquet(str(data / "sellers"))
    aqe_off = {"spark.sql.adaptive.enabled": "false"}
    aqe_on = {"spark.sql.adaptive.enabled": "true"}
    skew_aqe = {
        **aqe_on,
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": str(32 * MB),
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(32 * MB),
    }

    def agg(name: str) -> DataFrame:
        return items[name].groupBy("seller_id").agg(F.sum("price"), F.count("*"))

    def join(name: str) -> DataFrame:
        return items[name].join(sellers, "seller_id").groupBy("seller_state").agg(F.sum("price"))

    out: dict[str, list[tuple[str, str, dict[str, str], Callable[[], DataFrame]]]] = {}

    # 1. salting against skew: aggregation (partial aggregation already handles skew) and join
    skew: list[tuple[str, str, dict[str, str], Callable[[], DataFrame]]] = []
    for ds in ("items", "items_hot"):
        skew += [
            (ds, "agg plain", aqe_off, lambda ds=ds: agg(ds)),
            (ds, "agg salted", aqe_off, lambda ds=ds: salted_agg(items[ds])),
            (ds, "join sort-merge", {**aqe_off, **no_broadcast()}, lambda ds=ds: join(ds)),
            (ds, "join salted", {**aqe_off, **no_broadcast()}, lambda ds=ds: salted_join(items[ds], sellers)),
            (ds, "join AQE skew handling", {**skew_aqe, **no_broadcast()}, lambda ds=ds: join(ds)),
            (ds, "join broadcast", aqe_off, lambda ds=ds: join(ds)),
        ]
    out["skew"] = skew

    # 2. broadcast vs shuffle join around the threshold: dimension of D rows, key order_id
    bcast: list[tuple[str, str, dict[str, str], Callable[[], DataFrame]]] = []
    base = items["items"]
    for d in (3_000, 100_000, 1_000_000, 4_000_000):

        def dim(d: int = d) -> DataFrame:
            return base.select("order_id").distinct().limit(d).withColumn("flag", F.lit(1))

        def hinted(hint: str, d: int = d) -> DataFrame:
            return base.join(dim(d).hint(hint), "order_id").agg(F.sum("price"))

        bcast += [
            (f"dim {d:,} rows", "broadcast", aqe_off, lambda hinted=hinted: hinted("broadcast")),
            (f"dim {d:,} rows", "sort-merge", aqe_off, lambda hinted=hinted: hinted("merge")),
            (f"dim {d:,} rows", "shuffle hash", aqe_off, lambda hinted=hinted: hinted("shuffle_hash")),
        ]
    out["broadcast"] = bcast

    # 3. AQE on/off: default 200 shuffle partitions vs hand-tuned 8
    def query() -> DataFrame:
        return (
            items["items"]
            .join(sellers.hint("merge"), "seller_id")
            .groupBy("seller_state", "seller_id")
            .agg(F.sum("price").alias("revenue"))
            .groupBy("seller_state")
            .agg(F.max("revenue"), F.count("*"))
        )

    out["aqe"] = [
        ("items", "AQE off, 200 partitions", {**aqe_off, "spark.sql.shuffle.partitions": "200"}, query),
        ("items", "AQE off, 8 partitions (hand-tuned)", {**aqe_off, "spark.sql.shuffle.partitions": "8"}, query),
        ("items", "AQE on, 200 partitions", {**aqe_on, "spark.sql.shuffle.partitions": "200"}, query),
        ("items", "AQE off, 2000 partitions (too many)", {**aqe_off, "spark.sql.shuffle.partitions": "2000"}, query),
        ("items", "AQE on, 2000 partitions", {**aqe_on, "spark.sql.shuffle.partitions": "2000"}, query),
    ]

    # 4. shuffle partitions sweep (#376): same query, AQE off and on, 2 .. 2000 partitions
    sweep: list[tuple[str, str, dict[str, str], Callable[[], DataFrame]]] = []
    for n in SWEEP:
        for label, aqe in (("AQE off", aqe_off), ("AQE on", aqe_on)):
            sweep.append(("items", f"{label}, {n} partitions", {**aqe, "spark.sql.shuffle.partitions": str(n)}, query))
    out["shuffle"] = sweep
    return out


def write_bench(
    spark: SparkSession, data: Path, experiment: str, split_bytes: str | None, targets: tuple[int, ...]
) -> list[Result]:
    """repartition vs coalesce before a Parquet write; sizes of the written output are kept.

    `split_bytes` sets spark.sql.files.maxPartitionBytes for the read: a small value splits the
    source into many partitions, so the heavy step has more tasks than the cluster has cores.
    """
    out_dir = data / "out"
    if split_bytes:
        spark.conf.set("spark.sql.files.maxPartitionBytes", split_bytes)
    try:
        items = spark.read.parquet(str(data / "items"))
        source_parts = items.rdd.getNumPartitions()
        # expensive per-row work BEFORE the reduction: coalesce pulls it down to `target` tasks
        heavy = items.withColumn("h", F.sha2(F.sha2(F.col("order_id"), 256), 256)).select("order_id", "seller_id", "h")
        variants: list[tuple[str, Callable[[], DataFrame]]] = [
            (f"no reduction ({source_parts} partitions)", lambda: heavy)
        ]
        for t in targets:
            variants += [
                (f"coalesce({t})", lambda t=t: heavy.coalesce(t)),
                (f"repartition({t})", lambda t=t: heavy.repartition(t)),
            ]
        results: list[Result] = []
        for name, build in variants:
            times: list[float] = []
            files = 0
            size = 0
            before = stage_metrics(spark)
            for i in range(RUNS + 1):  # first run = warm-up
                shutil.rmtree(out_dir, ignore_errors=True)
                start = time.perf_counter()
                build().write.mode("overwrite").parquet(str(out_dir))
                if i:
                    times.append(time.perf_counter() - start)
                files = len(list(out_dir.glob("*.parquet")))
                size = sum(f.stat().st_size for f in out_dir.glob("*.parquet"))
            after = stage_metrics(spark)
            new = [after[s] for s in after if s not in before]
            results.append(
                Result(
                    experiment,
                    "items + sha2",
                    name,
                    statistics.median(times),
                    round(sum(r for r, _ in new) / (RUNS + 1) / MB, 1),
                    round(sum(s for _, s in new) / (RUNS + 1) / MB, 1),
                    [round(t, 2) for t in times],
                    f"{files} files, {size / MB:.0f} MB, source {source_parts} partitions",
                )
            )
        return results
    finally:
        spark.conf.unset("spark.sql.files.maxPartitionBytes")
        shutil.rmtree(out_dir, ignore_errors=True)


def markdown(results: list[Result]) -> str:
    lines = [
        "| experiment | dataset | technique | time s | shuffle read MB | disk spill MB | note |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    lines += [
        f"| {r.experiment} | {r.dataset} | {r.variant} | {r.seconds:.2f} | {r.shuffle_read_mb} | {r.spill_mb} | {r.note} |"
        for r in results
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-dir", type=Path, required=True)
    parser.add_argument("--scale", type=int, default=50, help="copies of the 112,650 order items")
    parser.add_argument("--memory", default="3g")
    parser.add_argument("--data", type=Path, default=Path("/tmp/spark_bench_data"))
    parser.add_argument("--only", choices=["skew", "broadcast", "aqe", "shuffle", "write", "write_split"])
    parser.add_argument("--out", type=Path, default=Path("spark_bench/results.json"))
    args = parser.parse_args()

    spark = session(args.memory)
    spark.sparkContext.setLogLevel("ERROR")
    prepare(spark, args.csv_dir, args.data, args.scale)
    results: list[Result] = []
    for name, variants in experiments(spark, args.data).items():
        if args.only and args.only != name:
            continue
        for dataset, variant, conf, build in variants:
            r = measure(spark, name, dataset, variant, conf, build)
            print(
                f"{name} | {dataset} | {variant}: {r.seconds:.2f}s shuffle {r.shuffle_read_mb} MB spill {r.spill_mb} MB {r.note}",
                flush=True,
            )
            results.append(r)
    if args.only in (None, "write"):
        results += write_bench(spark, args.data, "write", None, (4, 1))
    if args.only in (None, "write_split"):
        results += write_bench(spark, args.data, "write_split", "16m", (4, 2, 1))
    args.out.write_text(json.dumps([asdict(r) for r in results], indent=1), encoding="utf-8", newline="\n")
    print(markdown(results))


if __name__ == "__main__":
    main()
