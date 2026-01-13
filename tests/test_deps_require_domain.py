# tests/test_deps_require_domain.py

import pytest

from core import contracts
from core.deps import Blocked, require_domain


def test_require_domain_missing_upstream_blocks():
    with pytest.raises(Blocked) as ei:
        require_domain({}, "view_filters")
    b = ei.value
    assert b.code == "missing_upstream"
    assert list(b.reasons) == ["missing:view_filters"]
    assert b.upstream == "view_filters"


def test_require_domain_invalid_envelope_blocks():
    with pytest.raises(Blocked) as ei:
        require_domain({"view_filters": None}, "view_filters")
    b = ei.value
    assert b.code == "invalid_upstream_envelope"
    assert list(b.reasons) == ["invalid:view_filters"]


def test_require_domain_upstream_not_acceptable_blocks():
    domains = {
        "view_filters": contracts.new_domain_envelope(
            domain="view_filters",
            domain_version="1",
            status=contracts.DOMAIN_STATUS_BLOCKED,
            block_reasons=["missing:something"],
            diag={},
            records=None,
            hash_value=None,
        )
    }
    with pytest.raises(Blocked) as ei:
        require_domain(domains, "view_filters")
    b = ei.value
    assert b.code == "upstream_not_acceptable"
    assert list(b.reasons) == ["view_filters:blocked"]


def test_require_domain_allows_degraded_by_default():
    domains = {
        "view_filters": contracts.new_domain_envelope(
            domain="view_filters",
            domain_version="1",
            status=contracts.DOMAIN_STATUS_DEGRADED,
            block_reasons=[],
            diag={},
            records=None,
            hash_value=None,
        )
    }
    env = require_domain(domains, "view_filters")
    assert env["status"] == contracts.DOMAIN_STATUS_DEGRADED
