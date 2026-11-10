"""Review labels: batch files (labels.py) and the API path for new reviews (classify_api.py), no network."""

import csv
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion" / "review_labels"))

import classify_api  # noqa: E402
import labels  # noqa: E402

GROUPS = labels.by_text([
    {"review_id": "r1", "review_score": "5", "review_comment_title": "", "review_comment_message": "bom"},
    {"review_id": "r2", "review_score": "4", "review_comment_title": "", "review_comment_message": "bom"},
    {"review_id": "r3", "review_score": "1", "review_comment_title": "Atraso", "review_comment_message": "não chegou"},
])
BOM, ATRASO = labels.text_key("", "bom"), labels.text_key("Atraso", "não chegou")


@pytest.fixture(autouse=True)
def labels_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(labels, "LABELS_DIR", tmp_path)
    monkeypatch.setattr(labels, "QUEUE_DIR", tmp_path / "no-queue")
    return tmp_path


def label(key: str, **fields: str) -> dict[str, str]:
    return {"key": key, "sentiment": "positive", "topic": "general", "translation_en": "good", **fields}


def written(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def test_one_label_per_text_is_written_for_every_review_id() -> None:
    path = labels.write_labels([label(BOM)], GROUPS, "test")
    assert path is not None
    assert [(r["review_id"], r["sentiment"], r["labeled_by"]) for r in written(path)] == [
        ("r1", "positive", "test"), ("r2", "positive", "test")
    ]


def test_labelled_review_ids_are_not_written_again() -> None:
    labels.write_labels([label(BOM)], GROUPS, "test")
    assert labels.write_labels([label(BOM)], GROUPS, "test") is None


@pytest.mark.parametrize("bad", [{"sentiment": "angry"}, {"topic": "price"}, {"translation_en": " "}])
def test_invalid_label_writes_nothing(bad: dict[str, str], labels_dir: Path) -> None:
    with pytest.raises(ValueError):
        labels.write_labels([label(BOM), label(ATRASO, **bad)], GROUPS, "test")
    assert not list(labels_dir.glob("*.csv"))


def test_unknown_key_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown key"):
        labels.write_labels([label("nope")], GROUPS, "test")


def test_open_texts_skip_a_text_with_any_labelled_review() -> None:
    assert classify_api.open_texts(GROUPS, {"r2"}) == [ATRASO]
    assert classify_api.open_texts(GROUPS, {"r1", "r3"}) == []


def test_request_uses_the_guidelines_and_the_label_schema() -> None:
    req = classify_api.request(ATRASO, GROUPS[ATRASO][0], "GUIDELINES")
    params = req["params"]
    assert req["custom_id"] == ATRASO and params["model"] == "claude-haiku-5-5"
    assert params["system"].startswith("GUIDELINES")
    assert params["output_config"]["format"]["schema"]["properties"]["topic"]["enum"] == sorted(labels.TOPICS)
    assert json.loads(params["messages"][0]["content"]) == {"score": 1, "title": "Atraso", "message": "não chegou"}


def result(custom_id: str, kind: str = "succeeded", stop: str = "end_turn", body: dict[str, str] | None = None) -> Any:
    text = json.dumps(body or {"sentiment": "negative", "topic": "delivery", "translation_en": "Delay"})
    message = SimpleNamespace(stop_reason=stop, content=[SimpleNamespace(type="text", text=text)])
    return SimpleNamespace(custom_id=custom_id, result=SimpleNamespace(type=kind, message=message))


class FakeBatches:
    def __init__(self, results: list[Any]) -> None:
        self.results_list = results
        self.created: list[Any] = []
        self.polls = 0

    def create(self, requests: list[Any]) -> Any:
        self.created = requests
        return SimpleNamespace(id="b1", processing_status="in_progress")

    def retrieve(self, batch_id: str) -> Any:
        self.polls += 1
        return SimpleNamespace(id=batch_id, processing_status="ended", request_counts=SimpleNamespace(processing=0))

    def results(self, batch_id: str) -> list[Any]:
        return self.results_list


def test_batch_results_become_labels_and_failures_are_reported() -> None:
    batches = FakeBatches([result(ATRASO), result(BOM, kind="errored"), result("k3", stop="max_tokens")])
    client = SimpleNamespace(messages=SimpleNamespace(batches=batches))
    items, failed = classify_api.label_with_batch(client, [{"custom_id": ATRASO}], poll_seconds=0)
    assert items == [{"key": ATRASO, "sentiment": "negative", "topic": "delivery", "translation_en": "Delay"}]
    assert failed == [f"{BOM}: errored", "k3: stop_reason max_tokens"]
    assert batches.polls == 1


def test_nothing_open_sends_nothing(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    labels.write_labels([label(BOM), label(ATRASO)], GROUPS, "test")
    reviews = [r for rows in GROUPS.values() for r in rows]
    monkeypatch.setattr(classify_api, "reviews_from_duckdb", lambda *_: reviews)
    monkeypatch.setattr(sys, "argv", ["classify_api.py"])
    monkeypatch.setitem(sys.modules, "anthropic", None)  # importing it would fail the test
    classify_api.main()
    assert "nothing to send" in capsys.readouterr().out


def test_estimate_is_small_for_a_few_texts() -> None:
    assert 0 < classify_api.estimate_usd(GROUPS, [BOM, ATRASO]) < 0.01


REVIEWS = [r for rows in GROUPS.values() for r in rows]


def test_repeated_text_gets_the_label_copied_without_the_api(
    labels_dir: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Only r1 of "bom" labelled, as the hand-checked gold set did: r2 is a new review with a known text
    labels.write_labels([label(BOM)], {BOM: GROUPS[BOM][:1]}, "gold")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "anthropic", None)  # importing it would fail the test
    paths = classify_api.label_new_reviews(REVIEWS, labels_dir)
    assert [(r["review_id"], r["labeled_by"]) for r in written(paths[0])] == [("r2", "gold")]
    out = capsys.readouterr().out
    assert "1 texts without a label" in out and "ANTHROPIC_API_KEY not set" in out
    assert set(labels.read_labels(labels_dir)) == {"r1", "r2"}


def test_new_texts_go_to_one_batch_and_land_as_a_file(labels_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    labels.write_labels([label(BOM)], GROUPS, "gold")
    monkeypatch.setattr(classify_api.time, "sleep", lambda _: None)
    batches = FakeBatches([result(ATRASO)])
    client = SimpleNamespace(messages=SimpleNamespace(batches=batches))
    paths = classify_api.label_new_reviews(REVIEWS, labels_dir, client=client)
    assert [r["custom_id"] for r in batches.created] == [ATRASO]
    assert [(r["review_id"], r["topic"], r["labeled_by"]) for r in written(paths[0])] == [
        ("r3", "delivery", "claude-api-haiku-5-5")
    ]


def test_max_texts_caps_the_batch(labels_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(classify_api.time, "sleep", lambda _: None)
    batches = FakeBatches([])
    client = SimpleNamespace(messages=SimpleNamespace(batches=batches))
    assert classify_api.label_new_reviews(REVIEWS, labels_dir, max_texts=1, client=client) == []
    assert len(batches.created) == 1


def test_dry_run_writes_nothing(labels_dir: Path) -> None:
    labels.write_labels([label(BOM)], {BOM: GROUPS[BOM][:1]}, "gold")
    before = sorted(labels_dir.glob("*.csv"))
    assert classify_api.label_new_reviews(REVIEWS, labels_dir, dry_run=True) == []
    assert sorted(labels_dir.glob("*.csv")) == before
