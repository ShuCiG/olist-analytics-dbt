"""Email the failed jobs of a GitHub Actions run (job `notify-failure` in ci-cd.yml).

Stdlib only (REST API via urllib, smtplib), no third-party action. For each
failed job: the failed step and a log excerpt — dbt's end-of-run summary when the
log has one (failed nodes + messages; data tests explained with their business
description from the manifest, alerting/dbt_summary.py), else the lines up to the
first error.

Env:
    GH_TOKEN, GITHUB_REPOSITORY, GITHUB_RUN_ID, GITHUB_SERVER_URL (set by Actions)
    SMTP_CONN     secret, Airflow-style URI, same as SMTP_CONN in airflow/.env:
                  smtp://user%40gmail.com:app-password@smtp.gmail.com:587?disable_ssl=true&from_email=...
    ALERT_EMAILS  repo variable, comma-separated recipients
    EVENT_TITLE   what ran, e.g. "PR #24: feat(airflow): ..." or "push to main"
    DBT_MANIFEST  manifest.json for test descriptions (default transform/target/)

Local test against a past run:
    GH_TOKEN=$(gh auth token) GITHUB_REPOSITORY=owner/repo GITHUB_RUN_ID=<id> \
    SMTP_CONN=... ALERT_EMAILS=you@example.com python .github/scripts/notify_failure.py
"""

import json
import os
import re
import smtplib
import ssl
import sys
from dataclasses import dataclass
from email.message import EmailMessage
from html import escape
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

# Repo root: alerting/dbt_summary.py is shared with the Airflow alert emails
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from alerting import dbt_summary, email_html  # noqa: E402

EXCERPT_LINES = 30
GH_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z ?")  # Actions log line prefix


@dataclass
class FailedJob:
    name: str
    url: str
    steps: list[str]
    html: str  # log excerpt, rendered


def gh_api(path: str) -> str:
    """GET from the GitHub REST API (stdlib: `gh api` refuses logs with ANSI codes).

    Job logs redirect to a signed blob URL: the token goes as an unredirected header,
    so it is not forwarded there.
    """
    req = urllib.request.Request(f"https://api.github.com/{path}")
    req.add_unredirected_header("Authorization", f"Bearer {os.environ['GH_TOKEN']}")
    req.add_header("Accept", "application/vnd.github+json")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def excerpt(log: str, docs: dict[str, dbt_summary.TestDoc]) -> str:
    """dbt's summary in business words if the log has one, else the lines up to the
    first `##[error]` (lint, pyright, deploy steps)."""
    lines = [dbt_summary.clean(GH_TIME.sub("", line)) for line in log.splitlines()]
    done, block = dbt_summary.summary(lines)
    if done or block:
        return dbt_summary.to_html(done, block, docs)
    errors = [i for i, line in enumerate(lines) if line.startswith("##[error]")]
    end = errors[0] + 1 if errors else len(lines)
    tail = [line for line in lines[:end] if line.strip() and not line.startswith("##[group]")]
    return email_html.pre(chr(10).join(tail[-EXCERPT_LINES:]), "FAILED")


def failed_jobs(repo: str, run_id: str, docs: dict[str, dbt_summary.TestDoc]) -> list[FailedJob]:
    jobs = json.loads(gh_api(f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100"))["jobs"]
    result = []
    for job in jobs:
        if job["conclusion"] != "failure":
            continue
        steps = [s["name"] for s in job.get("steps", []) if s["conclusion"] == "failure"]
        try:
            html = excerpt(gh_api(f"repos/{repo}/actions/jobs/{job['id']}/logs"), docs)
        except urllib.error.URLError as err:
            html = f"<p>(log not available: {escape(str(err))})</p>"
        result.append(FailedJob(job["name"], job["html_url"], steps, html))
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
    # Test descriptions: the CI dbt job's manifest (artifact), else none
    docs = dbt_summary.test_docs(os.environ.get("DBT_MANIFEST", "transform/target/manifest.json"))
    jobs = failed_jobs(repo, run_id, docs)
    title = os.environ.get("EVENT_TITLE") or f"{run['event']} on {run['head_branch']}"

    sections = "".join(
        email_html.heading(f"{j.name} — failed step: {', '.join(j.steps) or '?'}")
        + j.html
        + f'<p style="margin:4px 0 0">{email_html.button("Job log", j.url)}</p>'
        for j in jobs
    )
    commit = run["head_commit"] or {}
    message = (commit.get("message") or "").splitlines()[0] if commit.get("message") else ""
    intro = (
        f"<b>{escape(title)}</b> failed in GitHub Actions.<br>"
        f'Commit <code>{run["head_sha"][:7]}</code> by {escape(run["actor"]["login"])}: {escape(message)}'
    )
    names = ", ".join(j.name for j in jobs) or "workflow"
    html = email_html.page(
        "FAILED",
        f"{names} failed",
        intro,
        sections or "<p>(no failed job found: cancelled or still running?)</p>",
        links=[("Open the run", run["html_url"])],
        footer=f"{repo} · GitHub Actions · recipients: repo variable ALERT_EMAILS",
    )
    subject = f"[{repo.split('/')[-1]} CI/CD] FAILED: {names} ({title})"
    send(os.environ["SMTP_CONN"], to, subject, html)
    print(f"email: {subject} -> {', '.join(to)}")


if __name__ == "__main__":
    main()
