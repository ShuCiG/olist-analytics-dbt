"""Matching demo on the Olist dump: orders party x reviews party, one shared metric.

    uv run python -m privacy.demo --csv-dir <folder with the Olist CSVs>

Party `orders` knows customers and their orders; party `reviews` knows the review scores. In Olist both sit under
`customer_unique_id`, which stands in for an e-mail address here. Each side tokenises its own table, the matcher
joins on tokens, and the only thing printed is the average review score per state and order band, checked by k and l.
Salts come from PRIVACY_LINKAGE_SALT / PRIVACY_SALT_ORDERS / PRIVACY_SALT_REVIEWS (hex, 32+ bytes) or are drawn
at random for the run.
"""

import argparse
import os
from collections.abc import Mapping
from pathlib import Path

import duckdb

from privacy.config import load_config
from privacy.matching import match, tokenise_table
from privacy.release import GuardedDataset, Query, Release, Scalar
from privacy.tokens import Party, new_salt, salt_from_env

CUSTOMERS = "olist_customers_dataset.csv"
ORDERS = "olist_orders_dataset.csv"
REVIEWS = "olist_order_reviews_dataset.csv"


def _register(con: duckdb.DuckDBPyConnection, directory: Path, file_name: str, view: str) -> None:
    path = directory / file_name
    if not path.is_file():
        raise FileNotFoundError(path)
    literal = path.as_posix().replace("'", "''")  # CREATE VIEW cannot take a bound parameter
    con.execute(f"create view {view} as select * from read_csv('{literal}', header=true, all_varchar=true)")


def load_parties(directory: Path) -> tuple[list[Mapping[str, Scalar]], list[Mapping[str, Scalar]]]:
    """One row per person for each party: (orders party table, reviews party table)."""
    con = duckdb.connect()
    for file_name, view in ((CUSTOMERS, "customers"), (ORDERS, "orders"), (REVIEWS, "reviews")):
        _register(con, directory, file_name, view)
    orders = con.execute(
        """
        select c.customer_unique_id as person, min(c.customer_state) as state,
               case when count(distinct o.order_id) = 1 then '1' else '2+' end as order_band
        from customers c join orders o using (customer_id)
        group by c.customer_unique_id
        """
    ).fetchall()
    reviews = con.execute(
        """
        select c.customer_unique_id as person, avg(cast(r.review_score as double)) as score,
               min(cast(r.review_score as integer)) as worst_score
        from reviews r join orders o using (order_id) join customers c using (customer_id)
        group by c.customer_unique_id
        """
    ).fetchall()
    return (
        [{"person": p, "state": s, "order_band": b} for p, s, b in orders],
        [{"person": p, "score": sc, "worst_score": w} for p, sc, w in reviews],
    )


def make_parties(env: Mapping[str, str] | None = None) -> tuple[Party, Party]:
    env = os.environ if env is None else env
    linkage = salt_from_env("PRIVACY_LINKAGE_SALT", env) if "PRIVACY_LINKAGE_SALT" in env else new_salt()

    def own(var: str) -> bytes:
        return salt_from_env(var, env) if var in env else new_salt()

    return (
        Party("orders", own("PRIVACY_SALT_ORDERS"), linkage),
        Party("reviews", own("PRIVACY_SALT_REVIEWS"), linkage),
    )


def run_demo(directory: Path, orders_party: Party, reviews_party: Party) -> Release:
    orders_rows, reviews_rows = load_parties(directory)
    left = tokenise_table(orders_party, orders_rows, "person", share=["state", "order_band"])
    right = tokenise_table(reviews_party, reviews_rows, "person", share=["score", "worst_score"])
    dataset = GuardedDataset(match(left, right), load_config())
    return dataset.run(
        Query(
            group_by=("orders_state", "orders_order_band"),
            measure="reviews_score",
            sensitive="reviews_worst_score",
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv-dir", type=Path, required=True)
    args = parser.parse_args()
    release = run_demo(args.csv_dir, *make_parties())
    for row in release.rows:
        print(row)
    print(f"{len(release.rows)} groups released, {release.suppressed_groups} suppressed")


if __name__ == "__main__":
    main()
