"""HMAC-SHA256 request signing.

The client sends three headers: `X-Timestamp` (unix seconds), `X-Nonce` (random, used once)
and `X-Signature` (hex). The signature covers, joined by newlines: timestamp, nonce, method,
path with query, and the raw body. Binding method and path stops a signed `GET` from being
replayed as another call; the nonce store stops a captured request from being replayed at
all inside the time window.
"""

import hashlib
import hmac
import re
import secrets
from collections.abc import Mapping

TIMESTAMP_HEADER = "X-Timestamp"
NONCE_HEADER = "X-Nonce"
SIGNATURE_HEADER = "X-Signature"
NONCE_PATTERN = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
SIGNATURE_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class SignatureError(Exception):
    def __init__(self, slug: str, detail: str) -> None:
        super().__init__(detail)
        self.slug = slug
        self.detail = detail


def canonical(timestamp: str, nonce: str, method: str, target: str, body: bytes) -> bytes:
    return b"\n".join([timestamp.encode(), nonce.encode(), method.upper().encode(), target.encode(), body])


def sign(secret: bytes, timestamp: str, nonce: str, method: str, target: str, body: bytes) -> str:
    return hmac.new(secret, canonical(timestamp, nonce, method, target, body), hashlib.sha256).hexdigest()


def signed_headers(
    secret: bytes, method: str, target: str, body: bytes, *, now: float, nonce: str | None = None
) -> dict[str, str]:
    """Headers a client adds to a request (used by the tests and the README example)."""
    timestamp = str(int(now))
    nonce = nonce or secrets.token_urlsafe(16)
    return {
        TIMESTAMP_HEADER: timestamp,
        NONCE_HEADER: nonce,
        SIGNATURE_HEADER: sign(secret, timestamp, nonce, method, target, body),
    }


def verify(
    secret: bytes,
    headers: Mapping[str, str],
    method: str,
    target: str,
    body: bytes,
    *,
    now: float,
    window_seconds: int,
) -> str:
    """Check the signature and the time window; returns the nonce (the caller records it).

    The signature is checked before the nonce is looked at, so a caller without the secret
    cannot fill the nonce store.
    """
    timestamp = headers.get(TIMESTAMP_HEADER)
    nonce = headers.get(NONCE_HEADER)
    signature = headers.get(SIGNATURE_HEADER)
    if not timestamp or not nonce or not signature:
        raise SignatureError(
            "missing-signature", f"{TIMESTAMP_HEADER}, {NONCE_HEADER} and {SIGNATURE_HEADER} are required"
        )
    if not timestamp.isascii() or not timestamp.isdigit():
        raise SignatureError("bad-timestamp", f"{TIMESTAMP_HEADER} must be unix seconds")
    if abs(now - int(timestamp)) > window_seconds:
        raise SignatureError("stale-timestamp", f"{TIMESTAMP_HEADER} is outside the {window_seconds} s window")
    if not NONCE_PATTERN.fullmatch(nonce):
        raise SignatureError("bad-nonce", f"{NONCE_HEADER} must be 16-64 characters of A-Za-z0-9_-")
    expected = sign(secret, timestamp, nonce, method, target, body)
    # Constant time; a malformed value is just a wrong signature
    if not SIGNATURE_PATTERN.fullmatch(signature) or not hmac.compare_digest(
        signature.encode(), expected.encode()
    ):
        raise SignatureError("bad-signature", "the signature does not match the request")
    return nonce
