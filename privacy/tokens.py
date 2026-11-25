"""Salted HMAC tokenisation of identifiers.

Two kinds of token, kept apart by a domain label inside the HMAC message, so a salt reused by mistake still does
not make a pseudonym equal to a linkage token:

- linkage token: HMAC-SHA256(linkage_salt, identifier). Both parties hold the same linkage salt, so the same person
  gets the same token on both sides and the tables can be joined on it. The analyst who receives the joined table
  does not hold the salt.
- pseudonym: HMAC-SHA256(party_salt, identifier). Each party has its own salt, so the pseudonyms of two parties
  cannot be joined with each other, only with the linkage step.
"""

import hmac
import os
import secrets
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256

MIN_SALT_BYTES = 32
_LINKAGE_LABEL = b"olist-privacy/v1/linkage\x00"
_PSEUDONYM_LABEL = b"olist-privacy/v1/pseudonym\x00"


def new_salt() -> bytes:
    """A fresh random salt of the minimum length."""
    return secrets.token_bytes(MIN_SALT_BYTES)


def salt_from_env(name: str, env: Mapping[str, str] | None = None) -> bytes:
    """A hex salt from an environment variable; a missing or short one is an error, never a default."""
    raw = (os.environ if env is None else env).get(name)
    if not raw:
        raise ValueError(f"{name} is not set")
    try:
        salt = bytes.fromhex(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be hex") from exc
    _check_salt(salt, name)
    return salt


def _check_salt(salt: bytes, name: str) -> None:
    if len(salt) < MIN_SALT_BYTES:
        raise ValueError(f"{name} must be at least {MIN_SALT_BYTES} bytes")


def normalise(identifier: str) -> str:
    """Same person, same spelling: Unicode NFKC, trimmed, case-folded. Empty identifiers are refused."""
    text = unicodedata.normalize("NFKC", identifier).strip().casefold()
    if not text:
        raise ValueError("empty identifier cannot be tokenised")
    return text


def _token(salt: bytes, label: bytes, identifier: str) -> str:
    _check_salt(salt, "salt")
    return hmac.new(salt, label + normalise(identifier).encode(), sha256).hexdigest()


def linkage_token(identifier: str, linkage_salt: bytes) -> str:
    return _token(linkage_salt, _LINKAGE_LABEL, identifier)


def pseudonym(identifier: str, party_salt: bytes) -> str:
    return _token(party_salt, _PSEUDONYM_LABEL, identifier)


@dataclass(frozen=True)
class Party:
    """One side of the matching. `linkage_salt` is shared with the other party, `party_salt` is not."""

    name: str
    party_salt: bytes = field(repr=False)
    linkage_salt: bytes = field(repr=False)

    def __post_init__(self) -> None:
        _check_salt(self.party_salt, f"{self.name}: party salt")
        _check_salt(self.linkage_salt, f"{self.name}: linkage salt")
        if hmac.compare_digest(self.party_salt, self.linkage_salt):
            raise ValueError(f"{self.name}: party salt must differ from the linkage salt")

    def linkage_token(self, identifier: str) -> str:
        return linkage_token(identifier, self.linkage_salt)

    def pseudonym(self, identifier: str) -> str:
        return pseudonym(identifier, self.party_salt)
