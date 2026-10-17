"""Throwaway: fails on purpose to trigger the CI failure email (never merge)."""


def test_fail_on_purpose() -> None:
    raise AssertionError("checking the SMTP_CONN secret")
