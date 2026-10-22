"""HTML layout for the alert emails (Airflow and CI): one look for OK / STALE / FAILED / SCHEMA.

Email clients ignore <style> blocks and modern CSS (Gmail strips most of it), so the
layout is tables + inline styles only, 640 px wide, system fonts.
"""

from __future__ import annotations

from html import escape

# status → (accent, light background, label)
STATUS = {
    "OK": ("#1a7f37", "#e9f6ec", "Fresh data"),
    "STALE": ("#9a6700", "#fff6dc", "Stale data"),
    "FAILED": ("#cf222e", "#ffebe9", "Failed"),
    "SCHEMA": ("#0969da", "#ddf4ff", "Schema change"),
}
FONT = "-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif"
MONO = "SFMono-Regular,Consolas,Menlo,monospace"
TEXT, MUTED, BORDER = "#1f2328", "#656d76", "#d0d7de"


def pre(text: str, status: str = "") -> str:
    """Log / error excerpt; tinted with the status color when given."""
    accent, bg, _ = STATUS.get(status, (BORDER, "#f6f8fa", ""))
    return (
        f'<pre style="margin:12px 0;padding:12px 14px;background:{bg};'
        f"border-left:4px solid {accent};border-radius:4px;font:12px/1.5 {MONO};"
        f'color:{TEXT};white-space:pre-wrap;word-break:break-word">{escape(text)}</pre>'
    )


def heading(text: str) -> str:
    return (
        f'<p style="margin:20px 0 8px;font:600 14px {FONT};color:{TEXT}">{escape(text)}</p>'
    )


def paragraph(text: str) -> str:
    return f'<p style="margin:12px 0;font:14px/1.5 {FONT};color:{TEXT}">{escape(text)}</p>'


def findings(items: list[tuple[str, str, str]], status: str) -> str:
    """Cards: (amount badge, description, small caption) per data-test finding."""
    accent, bg, _ = STATUS[status]
    rows = "".join(
        f'<tr><td style="padding:10px 12px;border-bottom:1px solid {BORDER};vertical-align:top;'
        f'white-space:nowrap"><span style="display:inline-block;padding:2px 8px;border-radius:10px;'
        f'background:{bg};color:{accent};font:600 12px {FONT}">{escape(amount)}</span></td>'
        f'<td style="padding:10px 12px 10px 0;border-bottom:1px solid {BORDER};font:13px/1.5 {FONT};'
        f'color:{TEXT}">{escape(text)}<br><span style="font:11px {MONO};color:{MUTED};'
        f'word-break:break-all">'
        f"{escape(caption)}</span></td></tr>"
        for amount, text, caption in items
    )
    return (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'style="border:1px solid {BORDER};border-radius:6px;border-collapse:separate">{rows}</table>'
    )


def details(rows: dict[str, object]) -> str:
    cells = "".join(
        f'<tr><td style="padding:6px 12px 6px 0;font:13px {FONT};color:{MUTED};white-space:nowrap;'
        f'vertical-align:top">{escape(k)}</td><td style="padding:6px 0;font:600 13px {FONT};'
        f'color:{TEXT};word-break:break-word">{escape(str(v))}</td></tr>'
        for k, v in rows.items()
    )
    return f'<table role="presentation" cellspacing="0" cellpadding="0">{cells}</table>'


def button(label: str, url: str, primary: bool = False) -> str:
    style = (
        f"background:{TEXT};color:#ffffff;border:1px solid {TEXT}"
        if primary
        else f"background:#ffffff;color:{TEXT};border:1px solid {BORDER}"
    )
    return (
        f'<a href="{escape(url)}" style="display:inline-block;margin:0 8px 8px 0;padding:8px 14px;'
        f'border-radius:6px;{style};font:600 13px {FONT};text-decoration:none">{escape(label)}</a>'
    )


def page(
    status: str,
    title: str,
    intro: str,
    body: str = "",
    rows: dict[str, object] | None = None,
    links: list[tuple[str, str]] | None = None,
    footer: str = "",
) -> str:
    """Whole email. `intro` and `body` are HTML (callers escape their data)."""
    accent, bg, label = STATUS[status]
    buttons = "".join(button(t, u, primary=i == 0) for i, (t, u) in enumerate(links or []))
    return f"""<!doctype html><html><body style="margin:0;padding:0;background:#f3f4f6">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f4f6">
<tr><td align="center" style="padding:24px 12px">
<table role="presentation" width="640" cellspacing="0" cellpadding="0"
 style="max-width:640px;width:100%;background:#ffffff;border:1px solid {BORDER};border-radius:8px">
<tr><td style="padding:18px 24px;background:{bg};border-bottom:3px solid {accent};
 border-radius:8px 8px 0 0">
<span style="display:inline-block;padding:3px 10px;border-radius:12px;background:{accent};
 color:#ffffff;font:700 11px {FONT};letter-spacing:.5px;text-transform:uppercase">{escape(label)}</span>
<div style="margin-top:8px;font:600 18px/1.35 {FONT};color:{TEXT}">{escape(title)}</div>
</td></tr>
<tr><td style="padding:20px 24px;font:14px/1.55 {FONT};color:{TEXT}">
<div>{intro}</div>
{body}
{heading("Details") + details(rows) if rows else ""}
{f'<div style="margin-top:20px">{buttons}</div>' if buttons else ""}
</td></tr>
<tr><td style="padding:12px 24px;border-top:1px solid {BORDER};font:11px {FONT};color:{MUTED}">
{escape(footer or "olist-analytics-dbt alerts")}</td></tr>
</table></td></tr></table></body></html>"""
