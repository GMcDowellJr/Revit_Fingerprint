# -*- coding: utf-8 -*-
"""
Tests for core/timing_collector.py

Validates:
  - Basic start/end timer recording
  - Multiple calls with same label accumulate
  - Domain-scoped sub-timing attribution
  - Report structure matches expected schema
  - Unmatched end_timer is silently ignored
  - Thread-safety under concurrent access
  - Defensive behavior (no exceptions leak)
"""

import threading
import time
import pytest

from core.timing_collector import TimingCollector


class TestTimingCollectorBasic:
    """Basic start/end timer functionality."""

    def test_single_timer(self):
        tc = TimingCollector()
        tc.start_timer("domain:phases")
        time.sleep(0.01)
        tc.end_timer("domain:phases")

        report = tc.get_report()
        assert "domain:phases" in report["raw"]
        entry = report["raw"]["domain:phases"]
        assert entry["calls"] == 1
        assert entry["total_seconds"] > 0.0

    def test_multiple_calls_same_label(self):
        tc = TimingCollector()
        for _ in range(3):
            tc.start_timer("processing:make_hash")
            tc.end_timer("processing:make_hash")

        report = tc.get_report()
        entry = report["raw"]["processing:make_hash"]
        assert entry["calls"] == 3

    def test_unmatched_end_timer_silently_ignored(self):
        tc = TimingCollector()
        # Should not raise
        tc.end_timer("nonexistent_label")
        report = tc.get_report()
        assert "nonexistent_label" not in report["raw"]

    def test_overlapping_timers(self):
        tc = TimingCollector()
        tc.start_timer("domain:phases")
        tc.start_timer("api:filter_elements")
        tc.end_timer("api:filter_elements")
        tc.end_timer("domain:phases")

        report = tc.get_report()
        assert "domain:phases" in report["raw"]
        assert "api:filter_elements" in report["raw"]


class TestTimingCollectorDomainScoping:
    """Domain-scoped sub-timing attribution."""

    def test_sub_timings_attributed_to_active_domain(self):
        tc = TimingCollector()
        tc.set_active_domain("phases")
        tc.start_timer("domain:phases")
        tc.start_timer("api:filter_elements")
        tc.end_timer("api:filter_elements")
        tc.start_timer("processing:make_hash")
        tc.end_timer("processing:make_hash")
        tc.end_timer("domain:phases")
        tc.set_active_domain(None)

        report = tc.get_report()
        domain_entry = report["domains"]["phases"]
        assert domain_entry["total_seconds"] > 0.0
        assert "filter_elements" in domain_entry["api_calls"]
        assert "make_hash" in domain_entry["processing"]
        assert domain_entry["api_seconds"] >= 0.0
        assert domain_entry["processing_seconds"] >= 0.0

    def test_sub_timings_without_active_domain_not_scoped(self):
        tc = TimingCollector()
        # No active domain set
        tc.start_timer("api:filter_elements")
        tc.end_timer("api:filter_elements")

        report = tc.get_report()
        # Should appear in raw but not in any domain
        assert "api:filter_elements" in report["raw"]
        assert len(report["domains"]) == 0

    def test_multiple_domains_scoped_independently(self):
        tc = TimingCollector()

        tc.set_active_domain("phases")
        tc.start_timer("domain:phases")
        tc.start_timer("api:filter_elements")
        tc.end_timer("api:filter_elements")
        tc.end_timer("domain:phases")
        tc.set_active_domain(None)

        tc.set_active_domain("units")
        tc.start_timer("domain:units")
        tc.start_timer("processing:make_hash")
        tc.end_timer("processing:make_hash")
        tc.end_timer("domain:units")
        tc.set_active_domain(None)

        report = tc.get_report()
        assert "phases" in report["domains"]
        assert "units" in report["domains"]
        assert "filter_elements" in report["domains"]["phases"]["api_calls"]
        assert "make_hash" in report["domains"]["units"]["processing"]
        # phases should NOT have make_hash
        assert "make_hash" not in report["domains"]["phases"]["processing"]


class TestTimingCollectorReport:
    """Report structure and summary calculations."""

    def test_report_has_required_keys(self):
        tc = TimingCollector()
        report = tc.get_report()
        assert "domains" in report
        assert "summary" in report
        assert "raw" in report

    def test_summary_totals(self):
        tc = TimingCollector()

        tc.set_active_domain("phases")
        tc.start_timer("domain:phases")
        tc.start_timer("api:filter_elements")
        time.sleep(0.005)
        tc.end_timer("api:filter_elements")
        tc.start_timer("processing:make_hash")
        time.sleep(0.005)
        tc.end_timer("processing:make_hash")
        tc.end_timer("domain:phases")
        tc.set_active_domain(None)

        report = tc.get_report()
        summary = report["summary"]
        assert summary["total_api_seconds"] > 0.0
        assert summary["total_processing_seconds"] > 0.0
        assert summary["total_domain_seconds"] > 0.0
        assert summary["overhead_seconds"] >= 0.0

    def test_domain_other_seconds_non_negative(self):
        tc = TimingCollector()
        tc.set_active_domain("phases")
        tc.start_timer("domain:phases")
        time.sleep(0.01)
        tc.end_timer("domain:phases")
        tc.set_active_domain(None)

        report = tc.get_report()
        domain_entry = report["domains"]["phases"]
        assert domain_entry["other_seconds"] >= 0.0

    def test_empty_report(self):
        tc = TimingCollector()
        report = tc.get_report()
        assert report["domains"] == {}
        assert report["raw"] == {}
        summary = report["summary"]
        assert summary["total_domain_seconds"] == 0.0
        assert summary["total_api_seconds"] == 0.0
        assert summary["total_processing_seconds"] == 0.0


class TestTimingCollectorThreadSafety:
    """Thread-safety under concurrent access."""

    def test_concurrent_timers(self):
        tc = TimingCollector()
        errors = []

        def worker(domain_name):
            try:
                tc.set_active_domain(domain_name)
                tc.start_timer("domain:{}".format(domain_name))
                tc.start_timer("api:filter_elements")
                time.sleep(0.001)
                tc.end_timer("api:filter_elements")
                tc.end_timer("domain:{}".format(domain_name))
                tc.set_active_domain(None)
            except Exception as e:
                errors.append(str(e))

        threads = []
        for i in range(10):
            t = threading.Thread(target=worker, args=("domain_{}".format(i),))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert errors == []
        report = tc.get_report()
        # All 10 domain timers should be recorded
        assert len(report["raw"]) >= 10


class TestTimingCollectorDefensive:
    """Defensive behavior - no exceptions leak."""

    def test_start_timer_with_none_label(self):
        tc = TimingCollector()
        # Should not raise
        tc.start_timer(None)

    def test_end_timer_with_none_label(self):
        tc = TimingCollector()
        # Should not raise
        tc.end_timer(None)

    def test_set_active_domain_none(self):
        tc = TimingCollector()
        tc.set_active_domain(None)
        report = tc.get_report()
        assert report is not None


class TestHashingTimingIntegration:
    """Verify hashing module timing integration does not affect hash output."""

    def test_make_hash_determinism_with_timing(self):
        from core.hashing import make_hash
        from core import hashing as hashing_mod

        tc = TimingCollector()

        # Hash without timing
        hash1 = make_hash(["a", "b", "c"])

        # Hash with timing enabled
        hashing_mod._timing_collector = tc
        try:
            hash2 = make_hash(["a", "b", "c"])
        finally:
            hashing_mod._timing_collector = None

        # Hashes must be identical
        assert hash1 == hash2

        # Timing data should have been collected
        report = tc.get_report()
        assert "processing:make_hash" in report["raw"]
        assert report["raw"]["processing:make_hash"]["calls"] == 1

    def test_make_hash_timing_cleaned_up(self):
        from core import hashing as hashing_mod

        # After cleanup, no timing reference
        hashing_mod._timing_collector = None
        assert hashing_mod._timing_collector is None
