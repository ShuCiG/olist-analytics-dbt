"""One evaluation run: each question to a fresh headless Claude Code whose only tools are this server's.

Uses the Claude Code subscription, no API spend, so it runs by hand and not in CI (D104).

    uv run python -m metrics_mcp.eval.evaluate --model haiku --out metrics_mcp/eval/results/haiku.json

Gold answers come from the `mf` CLI, a code path the server doesn't share.
"""

import argparse
import csv
import datetime
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import yaml

from metrics_mcp.eval.scoring import grade, parse_final_line

REPO = Path(__file__).resolve().parents[2]
QUESTIONS = Path(__file__).with_name("questions.yml")
SERVER_NAME = "olist-metrics"
TOOLS = ["list_metrics", "list_dimensions", "dimension_values", "query_metrics"]
TIMEOUT_SECONDS = 300

SYSTEM_PROMPT = """You answer business questions about an e-commerce company. Your only source of data is
the olist-metrics tools; if they are not there yet, wait for the server to connect. Use them; never
answer from memory or estimate. Simple arithmetic on returned numbers is fine.

If no metric or dimension answers the question, or the request is something the tools can't do,
refuse: don't substitute a similar metric.

Copy numbers exactly as the tools return them, without rounding; rates stay fractions (0.08, not 8).

End with exactly one line of JSON and nothing after it:
- one number: {"answer": 123.45}
- one name or period, the key as the tool returned it: {"answer": "SP"}
- a value per key, keys as the tool returned them: {"answer": {"SP": 1.5, "RJ": 2.5}}
- refusal: {"refused": true, "reason": "..."}
"""


def gold_rows(gold: dict[str, Any], folder: Path) -> list[dict[str, str]]:
    """The gold query through `mf query`, on the read-only target."""
    out = folder / "gold.csv"
    args = ["--metrics", ",".join(gold["metrics"]), "--csv", str(out)]
    if gold.get("group_by"):
        args += ["--group-by", ",".join(gold["group_by"])]
    if gold.get("start"):
        args += ["--start-time", gold["start"].isoformat()]
    if gold.get("end"):
        args += ["--end-time", gold["end"].isoformat()]
    if gold.get("where"):
        args += ["--where", gold["where"]]
    if gold.get("order"):
        args += ["--order", gold["order"]]
    if gold.get("limit"):
        args += ["--limit", str(gold["limit"])]
    mf = shutil.which("mf", path=str(Path(sys.executable).parent))
    assert mf, "mf not found next to this Python"
    env = {**os.environ, "DBT_TARGET": "mcp", "PYTHONIOENCODING": "utf-8"}
    subprocess.run([mf, "query", *args], cwd=REPO / "transform", env=env, check=True, capture_output=True, encoding="utf-8")
    with out.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def ask(question: str, model: str, folder: Path) -> dict[str, Any]:
    """One question in a fresh session, from an empty folder, with no settings or hooks of the user."""
    config = {
        "mcpServers": {
            SERVER_NAME: {
                "command": sys.executable,
                "args": ["-m", "metrics_mcp.server"],
                "cwd": str(REPO),
                "env": {"PYTHONIOENCODING": "utf-8", "PYTHONPATH": str(REPO)},
            }
        }
    }
    config_file = folder / "mcp.json"
    config_file.write_text(json.dumps(config), encoding="utf-8")
    claude = shutil.which("claude")
    assert claude, "claude CLI not found"
    command = [
        claude, "-p", question,
        "--model", model,
        "--system-prompt", SYSTEM_PROMPT,
        "--tools", "WaitForMcpServers",  # the server is still connecting when the first turn starts
        "--mcp-config", str(config_file),
        "--strict-mcp-config",
        "--allowedTools", ",".join(["WaitForMcpServers", *(f"mcp__{SERVER_NAME}__{tool}" for tool in TOOLS)]),
        "--setting-sources", "project",
        "--no-session-persistence",
        "--output-format", "stream-json",
        "--verbose",
    ]  # fmt: skip
    started = time.monotonic()
    env = {**os.environ, "ENABLE_TOOL_SEARCH": "false"}  # no search step before the server's tools
    done = subprocess.run(command, cwd=folder, env=env, capture_output=True, encoding="utf-8", timeout=TIMEOUT_SECONDS)
    calls: list[dict[str, Any]] = []
    result: dict[str, Any] = {}
    for line in done.stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "assistant":
            calls += [
                {"tool": block["name"].rsplit("__", 1)[-1], "input": block["input"]}
                for block in event["message"]["content"]
                if block.get("type") == "tool_use" and block["name"].startswith(f"mcp__{SERVER_NAME}__")
            ]
        if event.get("type") == "result":
            result = event
    return {
        "final_text": result.get("result", ""),
        "calls": calls,
        "seconds": round(time.monotonic() - started, 1),
        "usage": result.get("usage", {}),
        "exit_code": done.returncode,
        "stderr": done.stderr[-500:],
    }


def rescore(path: Path, questions: list[dict[str, Any]]) -> None:
    """Grade the stored replies again, after a change to the grader; no agent, no limit used."""
    run = json.loads(path.read_text(encoding="utf-8"))
    by_id = {question["id"]: question for question in questions}
    for outcome in run["outcomes"]:
        final = parse_final_line(outcome["final_text"])
        outcome["correct"], outcome["reason"] = grade(by_id[outcome["id"]], outcome["gold"], final, len(outcome["calls"]))
        print(f"{'ok ' if outcome['correct'] else 'BAD'} {outcome['id']}: {outcome['reason']}")
    run["summary"]["correct"] = sum(outcome["correct"] for outcome in run["outcomes"])
    path.write_text(json.dumps(run, indent=1, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(run["summary"], indent=1))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="haiku")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--only", help="comma-separated question ids")
    parser.add_argument("--rescore", action="store_true", help="grade the replies stored in --out again")
    args = parser.parse_args()

    questions = yaml.safe_load(QUESTIONS.read_text(encoding="utf-8"))
    if args.rescore:
        rescore(args.out, questions)
        return
    if args.only:
        questions = [q for q in questions if q["id"] in args.only.split(",")]

    outcomes = []
    with tempfile.TemporaryDirectory() as temp:
        folder = Path(temp)
        for question in questions:
            gold = None if question.get("refuse") else gold_rows(question["gold"], folder)
            reply = ask(question["question"], args.model, folder)
            correct, reason = grade(question, gold, parse_final_line(reply["final_text"]), len(reply["calls"]))
            outcomes.append({"id": question["id"], "question": question["question"], "correct": correct, "reason": reason, "gold": gold, **reply})
            print(f"{'ok ' if correct else 'BAD'} {question['id']}: {reason} ({reply['seconds']} s)", flush=True)

    tokens = sum(o["usage"].get("input_tokens", 0) + o["usage"].get("output_tokens", 0) for o in outcomes)
    summary = {
        "date": datetime.date.today().isoformat(),
        "model": args.model,
        "questions": len(outcomes),
        "correct": sum(o["correct"] for o in outcomes),
        "refusals_expected": sum(1 for q in questions if q.get("refuse")),
        "uncached_tokens": tokens,
        "cache_read_tokens": sum(o["usage"].get("cache_read_input_tokens", 0) for o in outcomes),
        "cache_creation_tokens": sum(o["usage"].get("cache_creation_input_tokens", 0) for o in outcomes),
        "seconds": round(sum(o["seconds"] for o in outcomes), 1),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"summary": summary, "outcomes": outcomes}, indent=1, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
