"""privacy/: tokens, matching, k-anonymity, l-diversity, and the attacks the module exists to stop."""

import hashlib
import itertools
from pathlib import Path

import pytest

from privacy.config import PrivacyConfig, load_config
from privacy.demo import make_parties, run_demo
from privacy.matching import match, tokenise_table
from privacy.release import GuardedDataset, Query, QueryRefused, Record, Scalar
from privacy.tokens import (
    MIN_SALT_BYTES,
    Party,
    linkage_token,
    new_salt,
    normalise,
    pseudonym,
    salt_from_env,
)

SALT_A = bytes(range(32))
SALT_B = bytes(range(32, 64))
LINKAGE = bytes(range(64, 96))


# --- tokens -----------------------------------------------------------------------------------------------------


def test_same_input_same_token() -> None:
    assert linkage_token("abc123", LINKAGE) == linkage_token("abc123", LINKAGE)
    assert len(linkage_token("abc123", LINKAGE)) == 64


def test_changing_the_salt_changes_the_token() -> None:
    assert linkage_token("abc123", LINKAGE) != linkage_token("abc123", SALT_A)


def test_spelling_does_not_split_a_person() -> None:
    assert linkage_token("  Ana@Example.COM ", LINKAGE) == linkage_token("ana@example.com", LINKAGE)
    assert normalise("ＡＢＣ") == "abc"


def test_linkage_token_and_pseudonym_differ_even_with_one_salt() -> None:
    assert linkage_token("x", SALT_A) != pseudonym("x", SALT_A)


def test_empty_identifier_and_short_salt_are_refused() -> None:
    with pytest.raises(ValueError, match="empty"):
        linkage_token("   ", LINKAGE)
    with pytest.raises(ValueError, match="at least"):
        linkage_token("x", b"short")


def test_party_salt_must_differ_from_linkage_salt() -> None:
    with pytest.raises(ValueError, match="differ"):
        Party("p", LINKAGE, LINKAGE)


def test_salt_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S", SALT_A.hex())
    assert salt_from_env("S") == SALT_A
    monkeypatch.setenv("S", "zz")
    with pytest.raises(ValueError, match="hex"):
        salt_from_env("S")
    monkeypatch.setenv("S", "ab")
    with pytest.raises(ValueError, match="at least"):
        salt_from_env("S")
    monkeypatch.delenv("S")
    with pytest.raises(ValueError, match="not set"):
        salt_from_env("S")


def test_new_salt_has_minimum_length_and_is_random() -> None:
    assert len(new_salt()) == MIN_SALT_BYTES
    assert new_salt() != new_salt()


# --- attack: brute force an identifier with a known schema ---------------------------------------------------------


def test_brute_force_of_a_known_schema_fails_without_the_salt() -> None:
    """The attacker knows the identifier is 'CPF-' + 4 digits and holds every token, but not the salt."""
    people = [f"CPF-{n:04d}" for n in (7, 42, 1999, 5000)]
    tokens = {linkage_token(p, LINKAGE) for p in people}
    candidates = [f"CPF-{n:04d}" for n in range(10_000)]

    # plain hashes of the whole space: what an unsalted token would allow
    assert not tokens & {hashlib.sha256(c.encode()).hexdigest() for c in candidates}
    # the attacker's best guess at a salt (empty, the schema name, a wrong key) recovers nothing either
    for guess in (b"", b"CPF", b"\x00" * 32, SALT_A):
        recovered = {c for c in candidates if _try(c, guess) in tokens}
        assert recovered == set()


def _try(identifier: str, guess: bytes) -> str:
    # the attacker is not stopped by the length rule: call the HMAC the way the module does, with a padded guess
    return linkage_token(identifier, guess.ljust(MIN_SALT_BYTES, b"\x00"))


def test_the_same_brute_force_works_when_the_salt_leaks() -> None:
    """Documents the threat model: the salt is the only secret."""
    tokens = {linkage_token("CPF-1999", LINKAGE)}
    found = {c for c in (f"CPF-{n:04d}" for n in range(10_000)) if linkage_token(c, LINKAGE) in tokens}
    assert found == {"CPF-1999"}


# --- matching -------------------------------------------------------------------------------------------------------


def _parties() -> tuple[Party, Party]:
    return Party("a", SALT_A, LINKAGE), Party("b", SALT_B, LINKAGE)


def test_match_joins_on_tokens_and_shows_no_identifier() -> None:
    a, b = _parties()
    left = tokenise_table(
        a,
        [{"id": "ana", "city": "X", "secret": 1}, {"id": "bob", "city": "Y", "secret": 2}, {"id": "only-a", "city": "Z"}],
        "id",
        share=["city"],
    )
    right = tokenise_table(b, [{"id": "ANA", "score": 5}, {"id": "bob", "score": 3}, {"id": "only-b", "score": 1}], "id", share=["score"])
    joined = match(left, right)
    assert {(r["a_city"], r["b_score"]) for r in joined} == {("X", 5), ("Y", 3)}
    flat = repr(joined) + repr(left) + repr(right)
    for identifier in ("ana", "bob", "only-a", "only-b"):
        assert identifier not in flat.lower()
    assert "secret" not in flat  # a column that is not shared never reaches the matcher


def test_different_party_salts_do_not_join_without_the_linkage_step() -> None:
    a, b = _parties()
    assert a.pseudonym("ana") != b.pseudonym("ana")
    assert a.linkage_token("ana") == b.linkage_token("ana")


def test_wrong_linkage_salt_matches_nobody() -> None:
    a = Party("a", SALT_A, LINKAGE)
    b = Party("b", SALT_B, bytes(range(100, 132)))
    left = tokenise_table(a, [{"id": "ana", "v": 1}], "id", share=["v"])
    right = tokenise_table(b, [{"id": "ana", "w": 2}], "id", share=["w"])
    assert match(left, right) == []


def test_matching_refuses_duplicates_and_shared_identifier() -> None:
    a, _ = _parties()
    with pytest.raises(ValueError, match="more than one row"):
        tokenise_table(a, [{"id": "ana"}, {"id": "Ana "}], "id", share=[])
    with pytest.raises(ValueError, match="cannot be shared"):
        tokenise_table(a, [{"id": "ana"}], "id", share=["id"])


# --- k-anonymity / l-diversity --------------------------------------------------------------------------------------


def population(sizes: dict[str, int], diverse: bool = True) -> list[Record]:
    """Persons per city; `score` cycles 1..3 (diverse) or stays 5."""
    rows: list[Record] = []
    for city, size in sizes.items():
        for i in range(size):
            rows.append({"token": f"{city}-{i}", "city": city, "score": (i % 3) + 1 if diverse else 5, "tier": i % 2})
    return rows


def test_groups_below_k_are_dropped_entirely() -> None:
    data = GuardedDataset(population({"big": 12, "edge": 5, "small": 4, "single": 1}), PrivacyConfig(k=5, l=1))
    release = data.run(Query(group_by=("city",), measure="score"))
    assert {r["city"] for r in release.rows} == {"big", "edge"}
    assert release.suppressed_groups == 2
    assert all("small" not in repr(r) and "single" not in repr(r) for r in release.rows)


def test_a_query_narrowed_to_one_person_is_suppressed() -> None:
    data = GuardedDataset(population({"big": 12}), PrivacyConfig(k=5, l=1))
    release = data.run(Query(filters={"city": "big", "tier": 1, "score": 3}, measure="score"))
    assert release.rows == ()
    assert release.suppressed_groups == 1


def test_a_group_of_distinct_people_but_one_sensitive_value_is_suppressed() -> None:
    data = GuardedDataset(population({"uniform": 10}, diverse=False) + population({"mixed": 10}), PrivacyConfig(k=5, l=2))
    release = data.run(Query(group_by=("city",), sensitive="score"))
    assert [r["city"] for r in release.rows] == ["mixed"]
    assert release.suppressed_groups == 1


def test_release_has_no_totals_and_no_tokens() -> None:
    data = GuardedDataset(population({"a": 6, "b": 3}), PrivacyConfig(k=5, l=1))
    release = data.run(Query(group_by=("city",)))
    assert release.rows == ({"city": "a", "n": 6},)


def test_config_default_and_file(tmp_path: Path) -> None:
    assert load_config() == PrivacyConfig(k=5, l=2)
    bad = tmp_path / "c.toml"
    bad.write_text("k = 1\n")
    with pytest.raises(ValueError, match="at least 2"):
        load_config(bad)
    bad.write_text("k = 5\nz = 1\n")
    with pytest.raises(ValueError, match="unknown keys"):
        load_config(bad)


def test_dataset_input_checks() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        GuardedDataset([{"token": "x"}, {"token": "x"}])
    with pytest.raises(ValueError, match="non-empty"):
        GuardedDataset([{"city": "x"}])
    data = GuardedDataset(population({"a": 6}))
    with pytest.raises(ValueError, match="unknown columns"):
        data.run(Query(group_by=("nope",)))
    with pytest.raises(ValueError, match="cannot be grouped"):
        data.run(Query(group_by=("token",)))
    with pytest.raises(TypeError, match="non-numeric"):
        data.run(Query(measure="city"))


# --- attack: differencing -------------------------------------------------------------------------------------------


def test_second_query_with_one_more_filter_is_refused() -> None:
    """Mean of 8 people, then the mean without the one vip: the two answers would give the vip's score."""
    rows: list[Record] = [{"token": f"p{i}", "vip": int(i == 0), "score": i} for i in range(8)]
    data = GuardedDataset(rows, PrivacyConfig(k=5, l=1))
    assert data.run(Query(measure="score")).rows[0]["n"] == 8
    with pytest.raises(QueryRefused, match="differ by 1"):
        data.run(Query(filters={"vip": 0}, measure="score"))
    # the refused query was not answered and not logged: the first one can still be asked again
    assert data.run(Query(measure="score")).rows[0]["n"] == 8


def test_group_by_then_filtered_group_by_is_refused() -> None:
    rows = [{"token": f"p{i}", "city": "a", "tier": 1 if i == 0 else 0, "score": i} for i in range(8)]
    data = GuardedDataset(rows, PrivacyConfig(k=5, l=1))
    data.run(Query(group_by=("city",), measure="score"))
    with pytest.raises(QueryRefused):
        data.run(Query(group_by=("city",), filters={"tier": 0}, measure="score"))  # 7 vs 8 people


def test_repeating_a_query_and_far_apart_queries_are_fine() -> None:
    data = GuardedDataset(population({"a": 20, "b": 20}), PrivacyConfig(k=5, l=1))
    q = Query(group_by=("city",), measure="score")
    assert data.run(q) == data.run(q)
    assert data.run(Query(filters={"tier": 0}, group_by=("city",), measure="score")).rows[0]["n"] == 10  # gap 10 >= k


def test_no_sequence_of_allowed_queries_isolates_fewer_than_k_people() -> None:
    """Property check on a small population: whatever the order of filters, every pair of released groups
    differs by 0 or at least k people, so no subtraction of two answers describes fewer than k people."""
    k = 5
    rows: list[Record] = [{"token": f"p{i}", "g": i % 2, "h": i % 3, "s": i % 5, "score": i} for i in range(30)]
    queries: list[Query] = []
    values: dict[str, list[Scalar]] = {"g": [0, 1], "h": [0, 1, 2], "s": [0, 1, 2, 3, 4]}
    for r in range(0, 3):
        for cols in itertools.combinations(values, r):
            for combo in itertools.product(*(values[c] for c in cols)):
                queries.append(Query(filters=dict(zip(cols, combo, strict=True)), group_by=("g",), measure="score"))
    for order in (queries, queries[::-1]):
        data = GuardedDataset(rows, PrivacyConfig(k=k, l=1))
        cells: set[frozenset[str]] = set()
        for q in order:
            try:
                release = data.run(q)
            except QueryRefused:
                continue
            for r in release.rows:
                members = frozenset(
                    str(x["token"]) for x in rows if all(x[c] == v for c, v in q.filters.items()) and x["g"] == r["g"]
                )
                cells.add(members)
        for x, y in itertools.combinations(cells, 2):
            assert len(x ^ y) == 0 or len(x ^ y) >= k


# --- demo on a tiny Olist-shaped dump ---------------------------------------------------------------------------------


def write_dump(directory: Path, people: int = 20) -> None:
    (directory / "olist_customers_dataset.csv").write_text(
        "customer_id,customer_unique_id,customer_state\n"
        + "".join(f"c{i},u{i % people},{'SP' if i % people < 17 else 'RJ'}\n" for i in range(people + 4)),
        encoding="utf-8",
    )
    (directory / "olist_orders_dataset.csv").write_text(
        "order_id,customer_id\n" + "".join(f"o{i},c{i}\n" for i in range(people + 4)), encoding="utf-8"
    )
    (directory / "olist_order_reviews_dataset.csv").write_text(
        'review_id,order_id,review_score,review_comment_message\n'
        + "".join(f'r{i},o{i},{(i % 5) + 1},"line one\nline two"\n' for i in range(people + 4)),
        encoding="utf-8",
    )


def test_demo_end_to_end(tmp_path: Path) -> None:
    write_dump(tmp_path)
    orders, reviews = make_parties({})
    release = run_demo(tmp_path, orders, reviews)
    assert [(r["orders_state"], r["orders_order_band"], r["n"]) for r in release.rows] == [("SP", "1", 13)]
    assert release.suppressed_groups == 2  # SP with 2+ orders (4 people) and RJ (3 people)
    assert "u1" not in repr(release)


def test_demo_tokens_are_stable_with_env_salts() -> None:
    env = {"PRIVACY_LINKAGE_SALT": LINKAGE.hex(), "PRIVACY_SALT_ORDERS": SALT_A.hex(), "PRIVACY_SALT_REVIEWS": SALT_B.hex()}
    first, second = make_parties(env), make_parties(env)
    assert first[0].linkage_token("u1") == second[1].linkage_token("u1")
