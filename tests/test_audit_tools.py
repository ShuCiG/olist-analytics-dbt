"""/audit helpers (.claude/skills/audit/audit_tools.py): reports must never carry secrets."""

import importlib.util
from pathlib import Path

_PATH = Path(__file__).resolve().parents[1] / ".claude" / "skills" / "audit" / "audit_tools.py"
_spec = importlib.util.spec_from_file_location("audit_tools", _PATH)
assert _spec and _spec.loader
audit_tools = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit_tools)


def test_patterns_are_masked() -> None:
    text = (
        "SMTP_CONN=smtps://me%40gmail.com:abcdwxyzefgh1234@smtp.gmail.com:465\n"
        'password: "hunter2hunter2"\n'
        "token ghp_" + "a" * 36 + "\n"
        '"private_key_id": "0123456789abcdef0123456789abcdef"\n'
        # Marker split so the detect-private-key hook does not flag this fake key
        "-----BEGIN " + "PRIVATE KEY-----\nMIIEvQIBADANBg\n-----END " + "PRIVATE KEY-----\n"
    )
    out, n = audit_tools.redact_text(text)
    assert n == 5
    for secret in ("abcdwxyzefgh1234", "hunter2hunter2", "a" * 36, "0123456789abcdef0123", "MIIEvQIBADANBg"):
        assert secret not in out
    assert "smtp.gmail.com" in out  # context stays readable


def test_literal_values_are_masked_everywhere() -> None:
    out, n = audit_tools.redact_text("seen sup3rS3cretValue twice: sup3rS3cretValue", {"sup3rS3cretValue"})
    assert n == 2
    assert "sup3rS3cretValue" not in out
    assert out.count("sup3…[REDACTED]") == 2


def test_redacting_twice_changes_nothing() -> None:
    once, _ = audit_tools.redact_text("smtps://u:abcdwxyzefgh1234@host", {"abcdwxyzefgh1234"})
    twice, n = audit_tools.redact_text(once, {"abcdwxyzefgh1234"})
    assert (twice, n) == (once, 0)


def test_audit_folder_is_ignored_and_untracked() -> None:
    assert audit_tools.check() == 0
