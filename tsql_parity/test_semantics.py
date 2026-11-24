"""One test per semantic difference between T-SQL and Spark (#357, docs/tsql-spark-differences.md).

Each test runs the same tiny case on SQL Server and Spark and asserts three things:
the T-SQL answer, that the naive port gives another one, and that the faithful port
matches T-SQL. Needs the Compose SQL Server (`tsql_parity/docker-compose.yml`) and Java.

    uv run pytest tsql_parity
"""

from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tsql_parity import mssql, spark_port


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    s = spark_port.session(tmp_path_factory.mktemp("warehouse"), ansi=True)
    yield s
    s.stop()


@pytest.fixture(scope="module", autouse=True)
def database() -> None:
    mssql.execute("if db_id('parity') is null create database parity", database="master")


def tsql(sql: str) -> list[Any]:
    return [r[0] for r in mssql.query(sql)]


def test_1_not_in_with_null(spark: SparkSession) -> None:
    """NOT IN against a subquery holding a NULL: no rows in T-SQL; an anti-join returns rows."""
    t = tsql(
        "select count(*) from (values ('a'), ('b')) l(x) where x not in (select v from (values ('a'), (null)) r(v))"
    )
    assert t == [0]
    left = spark.createDataFrame([("a",), ("b",)], "x string")
    right = spark.createDataFrame([("a",), (None,)], "v string")
    naive = left.join(right, left.x == right.v, "left_anti").count()
    assert naive == 1
    left.createOrReplaceTempView("l1")
    right.createOrReplaceTempView("r1")
    faithful = spark_port.sql(spark, "select count(*) from l1 where x not in (select v from r1)").first()
    assert faithful is not None and faithful[0] == 0


def test_2_decimal_rounding_and_division_scale(spark: SparkSession) -> None:
    """ROUND is half away from zero in T-SQL (Spark round = HALF_UP, bround = half-even);
    `int / int` is integer division in T-SQL and fractional in Spark."""
    assert tsql("select round(cast(0.125 as decimal(18,3)), 2)") == [Decimal("0.130")]
    row = spark_port.sql(
        spark, "select round(cast(0.125 as decimal(18,3)), 2) r, bround(cast(0.125 as decimal(18,3)), 2) b"
    ).first()
    assert row is not None
    assert row["r"] == Decimal("0.13") and row["b"] == Decimal("0.12")  # naive port = bround

    assert tsql("select 1 / 3, 7 / 2") == [0]  # int / int stays an integer
    naive = spark_port.sql(spark, "select 1 / 3 q").first()
    assert naive is not None and naive["q"] != 0  # Spark `/` is always fractional
    faithful = spark_port.sql(spark, "select 1 div 3 q, 7 div 2 r").first()
    assert faithful is not None and (faithful["q"], faithful["r"]) == (0, 3)


def test_3_string_ordering_collation(spark: SparkSession) -> None:
    """SQL_Latin1_General_CP1_CI_AS sorts a, á, b, c; Spark's default is byte order (á last)."""
    values = ["b", "a", "á", "c"]
    mssql.execute("drop table if exists trap3; create table trap3 (x nvarchar(5))")
    mssql.execute("insert into trap3 values " + ",".join(f"(N'{v}')" for v in values))
    assert tsql("select x from trap3 order by x") == ["a", "á", "b", "c"]
    df = spark.createDataFrame([(v,) for v in values], "x string")
    assert [r.x for r in df.orderBy("x").collect()] == ["a", "b", "c", "á"]
    df.createOrReplaceTempView("trap3")
    faithful = spark_port.sql(spark, "select x from trap3 order by x collate UNICODE_CI").collect()
    assert [r.x for r in faithful] == ["a", "á", "b", "c"]


def test_4_top_without_order_by(spark: SparkSession) -> None:
    """TOP 1 without ORDER BY returns whichever row the plan reaches first."""
    mssql.execute(
        "drop table if exists trap4;"
        " create table trap4 (id int primary key clustered, v int);"
        " insert into trap4 values (1, 30), (2, 10), (3, 20);"
        " create nonclustered index ix_v on trap4 (v)"
    )
    by_clustered = tsql("select top 1 id from trap4 with (index(0))")
    by_v_index = tsql("select top 1 id from trap4 with (index(ix_v))")
    assert by_clustered == [1] and by_v_index == [2]  # same text, two answers
    assert tsql("select top 1 id from trap4 order by v") == [2]
    df = spark.createDataFrame([(1, 30), (2, 10), (3, 20)], "id int, v int")
    assert df.orderBy("v").limit(1).first() == (2, 10)  # faithful: the order is explicit


def test_5_getdate_is_not_reproducible(spark: SparkSession) -> None:
    """GETDATE() makes a result depend on the run day; the port takes the date as a parameter."""
    pinned = date(2018, 10, 17)
    today_t = tsql("select cast(getdate() as date)")[0]
    assert tsql(f"select datediff(day, '{pinned}', getdate())") == [(today_t - pinned).days]
    assert (today_t - pinned).days > 0  # a different number tomorrow
    got = spark.range(1).select(F.datediff(F.lit(date(2018, 10, 27)), F.lit(pinned)).alias("d")).first()
    assert got is not None and got["d"] == 10
    assert tsql("select datediff(day, '2018-10-17', '2018-10-27')") == [10]


def test_6_empty_string_trailing_spaces_and_null(spark: SparkSession) -> None:
    """T-SQL ignores trailing spaces in `=` and LEN; Spark does not. NULL stays NULL in both."""
    assert tsql("select case when '' = ' ' then 1 else 0 end") == [1]
    assert tsql("select len('a  ')") == [1]
    row = spark_port.sql(spark, "select '' = ' ' e, length('a  ') n, rtrim('') = rtrim(' ') f").first()
    assert row is not None
    assert (row["e"], row["n"], row["f"]) == (False, 3, True)  # faithful port: rtrim both sides
    assert tsql("select count(*) from (values (cast(null as varchar(1))), ('')) t(x) where x is null") == [1]
    nulls = spark_port.sql(
        spark, "select count(*) c from values (cast(null as string)), ('') t(x) where x is null"
    ).first()
    assert nulls is not None and nulls["c"] == 1


def test_7_implicit_cast_in_comparison(spark: SparkSession) -> None:
    """varchar > int converts the column to int in T-SQL; a string > string port compares text."""
    mssql.execute(
        "drop table if exists trap7; create table trap7 (v varchar(10));"
        " insert into trap7 values ('10'), ('9'), ('100')"
    )
    assert tsql("select count(*) from trap7 where v > 9") == [2]
    df = spark.createDataFrame([("10",), ("9",), ("100",)], "v string")
    assert df.where(F.col("v") > F.lit("9")).count() == 0  # naive: '10' < '9' as text
    assert df.where(F.col("v").cast("int") > 9).count() == 2  # faithful


def test_8_division_by_zero(spark: SparkSession) -> None:
    """T-SQL raises on x/0; Spark 4 raises with ANSI on (default) and silently gives NULL with ANSI off."""
    with pytest.raises(Exception, match=r"(?i)divide by zero"):
        tsql("select 1 / (select 0)")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        row = spark_port.sql(spark, "select 1 / 0 q").first()
        assert row is not None and row["q"] is None  # naive on a non-ANSI session: silent NULL
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "true")
    with pytest.raises(Exception, match=r"(?i)divide.by.zero|DIVIDE_BY_ZERO"):
        spark_port.sql(spark, "select 1 / 0 q").collect()
    guarded = spark_port.sql(spark, "select 1 / nullif(0, 0) q").first()  # the explicit guard both engines share
    assert guarded is not None and guarded["q"] is None
    assert tsql("select 1 / nullif(0, 0)") == [None]
