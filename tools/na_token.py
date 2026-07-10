from __future__ import annotations

import re

_NA_TOKEN_STRIP_RE = re.compile(r"[^a-z0-9]")
_NA_TOKENS = {"na", "notapplicable"}


def is_na_token(value: str) -> bool:
    """True for any spelling of "not applicable" (na, n/a, N/A, not applicable,
    not_applicable, __NOT_APPLICABLE__, ...). Blank is a distinct signal — "not
    yet filled in", a manual-entry todo — and is deliberately NOT treated as NA
    by this function; check blank separately, or use is_blank_or_na() when both
    should be treated the same way."""
    return _NA_TOKEN_STRIP_RE.sub("", value.lower()) in _NA_TOKENS


def is_blank_or_na(value: str) -> bool:
    """True if value is blank (not yet filled in) or an explicit "not
    applicable" spelling (reviewed, does not apply). Both mean "-ignore-" for
    grouping/identity purposes even though they carry different meaning for
    manual-entry QA (blank = todo, NA = reviewed) — callers that need to
    preserve that distinction should check blank and is_na_token() separately
    instead of collapsing them here."""
    stripped = value.strip()
    return not stripped or is_na_token(stripped)
