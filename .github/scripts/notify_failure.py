"""Email the failed jobs of a GitHub Actions run (job `notify-failure` in ci-cd.yml).

Stdlib + the `gh` CLI (preinstalled on runners), no third-party action. For each
failed job: the failed step and a log excerpt — dbt's end-of-run summary when the
log has one (every failed node + message), else the lines up to the first error.

Env:
    GH_TOKEN, GITHUB_REPOSITORY, GITHUB_RUN_ID, GITHUB_SERVER_URL (set by Actions)
    SMTP_CONN     secret, Airflow-style URI, same as SMTP_CONN in airflow/.env:
                  smtp://user%40gmail.com:app-password@smtp.gmail.com:587?disable_ssl=true&from_email=...
    ALERT_EMAILS  repo variable, comma-separated recipients
    EVENT_TITLE   what ran, e.g. "PR #24: feat(airflow): ..." or "push to main"

Local test against a past run:
    GH_TOKEN=$(gh auth token) GITHUB_REPOSITORY=owner/repo GITHUB_RUN_ID=<id> \
    SMTP_CONN=... ALERT_EMAILS=you@example.com python .github/scripts/notify_failure.py
"""

import json
import os
import re
import smtplib
import ssl
import subprocess
from dataclasses import dataclass
from email.message import EmailMessage
from html import escape
from urllib.parse import parse_qs, unquote, urlsplit

EXCERPT_LINES = 30
GH_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z ?")  # Actions log line prefix
DBT_TIME = re.compile(r"^\d{2}:\d{2}:\d{2}(\s+|$)")  # dbt's prefix, alone on blank lines
ANSI = re.compile(r"\x1b\[[0-9;]*m")


@dataclass
class FailedJob:
    name: str
    url: str
    steps: list[str]
    lines: list[str]


def gh_api(path: str) -> str:
    return subprocess.run(
        ["gh", "api", path], check=True, capture_output=True, text=True, encoding="utf-8"
    ).stdout


def excerpt(log: str) -> list[str]:
    """dbt's summary block if present, else the lines before the first `##[error]`."""
    lines = [ANSI.sub("", GH_TIME.sub("", line)).rstrip() for line in log.splitlines()]
    starts = [i for i, line in enumerate(lines) if "Completed with" in DBT_TIME.sub("", line)]
    if starts:
        block: list[str] = []
        done = ""
        for line in lines[starts[-1] :]:
            text = DBT_TIME.sub("", line)
            if text.startswith("Done. PASS="):
                done = text
                break
            if text.strip() and "compiled code at" not in text:
                block.append(text)
        # Errors come first in dbt's block; keep the totals line even when cut
        return block[:EXCERPT_LINES] + ([done] if done else [])
    errors = [i for i, line in enumerate(lines) if line.startswith("##[error]")]
    end = errors[0] + 1 if errors else len(lines)
    tail = [line for line in lines[:end] if line.strip() and not line.startswith("##[group]")]
    return tail[-EXCERPT_LINES:]


def failed_jobs(repo: str, run_id: str) -> list[FailedJob]:
    jobs = json.loads(gh_api(f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100"))["jobs"]
    result = []
    for job in jobs:
        if job["conclusion"] != "failure":
            continue
        steps = [s["name"] for s in job.get("steps", []) if s["conclusion"] == "failure"]
        try:
            lines = excerpt(gh_api(f"repos/{repo}/actions/jobs/{job['id']}/logs"))
        except subprocess.CalledProcessError as err:
            lines = [f"(log not available: {err.stderr.strip()[:200]})"]
        result.append(FailedJob(job["name"], job["html_url"], steps, lines))
    return result


def send(conn_uri: str, to: list[str], subject: str, html: str) -> None:
    """Send with an Airflow-style SMTP URI (same flags: disable_ssl / disable_tls)."""
    uri = urlsplit(conn_uri)
    extra = {k: v[0] for k, v in parse_qs(uri.query).items()}
    user, password = unquote(uri.username or ""), unquote(uri.password or "")
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, extra.get("from_email", user), ", ".join(to)
    msg.set_content("HTML email: see the HTML part.")
    msg.add_alternative(html, subtype="html")
    context = ssl.create_default_context()
    if extra.get("disable_ssl"):
        smtp = smtplib.SMTP(uri.hostname or "", uri.port or 587, timeout=30)
        if not extra.get("disable_tls"):
            smtp.starttls(context=context)
    else:
        smtp = smtplib.SMTP_SSL(uri.hostname or "", uri.port or 465, timeout=30, context=context)
    with smtp:
        if user:
            smtp.login(user, password)
        smtp.send_message(msg)


def main() -> None:
    repo, run_id = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"]
    to = [a.strip() for a in os.environ.get("ALERT_EMAILS", "").split(",") if a.strip()]
    if not to or not os.environ.get("SMTP_CONN"):
        print("ALERT_EMAILS or SMTP_CONN not set: no email")
        return
    run = json.loads(gh_api(f"repos/{repo}/actions/runs/{run_id}"))
    jobs = failed_jobs(repo, run_id)
    title = os.environ.get("EVENT_TITLE") or f"{run['event']} on {run['head_branch']}"

    sections = "".join(
        f'<h3><a href="{j.url}">{escape(j.name)}</a></h3>'
        f"<p>Failed step: <b>{escape(', '.join(j.steps) or '?')}</b></p>"
        f"<pre>{escape(chr(10).join(j.lines))}</pre>"
        for j in jobs
    )
    commit = run["head_commit"] or {}
    html = (
        f"<p><b>{escape(title)}</b> failed in GitHub Actions. "
        f'<a href="{run["html_url"]}">Open the run</a></p>'
        f"<p>Commit <code>{run['head_sha'][:7]}</code> by {escape(run['actor']['login'])}: "
        f"{escape((commit.get('message') or '').splitlines()[0] if commit.get('message') else '')}</p>"
        + (sections or "<p>(no failed job found: cancelled or still running?)</p>")
    )
    names = ", ".join(j.name for j in jobs) or "workflow"
    subject = f"[{repo.split('/')[-1]} CI/CD] FAILED: {names} ({title})"
    send(os.environ["SMTP_CONN"], to, subject, html)
    print(f"email: {subject} -> {', '.join(to)}")


if __name__ == "__main__":
    main()
