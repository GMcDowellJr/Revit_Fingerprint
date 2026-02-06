# -*- coding: utf-8 -*-
"""
core/timing_collector.py

Lightweight timing instrumentation for Revit Fingerprint extraction.

Collects domain-level, API-call, and processing timings without affecting
deterministic hashing or fail-soft behavior. All timing operations are
wrapped defensively so failures never propagate to callers.

Thread-safe for future parallelization.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional


class TimingCollector:
    """
    Collects hierarchical timing data for extraction runs.

    Labels follow the convention:
      - ``domain:{name}``       - total time in a domain extractor
      - ``api:{operation}``     - Revit API call timing
      - ``processing:{op}``    - Python processing (hashing, transforms)

    Supports nested timing contexts: timers can overlap and nest arbitrarily.
    Each ``start_timer`` / ``end_timer`` pair records one duration entry.
    Multiple calls with the same label accumulate (list of durations).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # label -> list of recorded durations (seconds)
        self._durations: Dict[str, List[float]] = {}
        # label -> perf_counter start (pending timers)
        self._pending: Dict[str, float] = {}
        # domain-scoped sub-timings: domain_name -> {sublabel -> [durations]}
        self._domain_scoped: Dict[str, Dict[str, List[float]]] = {}
        # Currently active domain (for scoping sub-timings)
        self._active_domain: Optional[str] = None

    def start_timer(self, label: str) -> None:
        """Begin timing a labeled operation."""
        try:
            with self._lock:
                self._pending[str(label)] = time.perf_counter()
        except Exception:
            pass

    def end_timer(self, label: str) -> None:
        """End timing and record duration for a labeled operation."""
        try:
            t_end = time.perf_counter()
            with self._lock:
                label = str(label)
                t_start = self._pending.pop(label, None)
                if t_start is None:
                    return
                elapsed = t_end - t_start
                if label not in self._durations:
                    self._durations[label] = []
                self._durations[label].append(elapsed)

                # If there is an active domain, also record under domain scope
                if self._active_domain and not label.startswith("domain:"):
                    domain = self._active_domain
                    if domain not in self._domain_scoped:
                        self._domain_scoped[domain] = {}
                    scoped = self._domain_scoped[domain]
                    if label not in scoped:
                        scoped[label] = []
                    scoped[label].append(elapsed)
        except Exception:
            pass

    def set_active_domain(self, domain_name: Optional[str]) -> None:
        """Set the currently executing domain for sub-timing scoping."""
        try:
            with self._lock:
                self._active_domain = domain_name
        except Exception:
            pass

    def get_report(self) -> Dict[str, Any]:
        """
        Return structured timing report.

        Returns a dict with:
          - ``total_execution_seconds``: overall extraction time (if ``domain:*`` timers exist)
          - ``domains``: per-domain breakdown with api/processing sub-timings
          - ``summary``: aggregate totals for api, processing, and overhead
          - ``raw``: all recorded timers with call counts and total seconds
        """
        try:
            with self._lock:
                return self._build_report()
        except Exception:
            return {"error": "timing_report_build_failed"}

    def _build_report(self) -> Dict[str, Any]:
        """Build the structured report (must hold lock)."""

        # Compute per-label aggregates
        raw: Dict[str, Dict[str, Any]] = {}
        for label, durations in self._durations.items():
            raw[label] = {
                "calls": len(durations),
                "total_seconds": round(sum(durations), 6),
            }

        # Build per-domain breakdown
        domains: Dict[str, Dict[str, Any]] = {}
        total_domain_seconds = 0.0

        for label, durations in self._durations.items():
            if label.startswith("domain:"):
                domain_name = label[len("domain:"):]
                total_sec = sum(durations)
                total_domain_seconds += total_sec

                domain_entry: Dict[str, Any] = {
                    "total_seconds": round(total_sec, 6),
                    "api_calls": {},
                    "processing": {},
                }

                # Pull in domain-scoped sub-timings
                scoped = self._domain_scoped.get(domain_name, {})
                api_total = 0.0
                proc_total = 0.0

                for sub_label, sub_durations in scoped.items():
                    sub_total = sum(sub_durations)
                    sub_entry = {
                        "calls": len(sub_durations),
                        "total_seconds": round(sub_total, 6),
                    }

                    if sub_label.startswith("api:"):
                        op_name = sub_label[len("api:"):]
                        domain_entry["api_calls"][op_name] = sub_entry
                        api_total += sub_total
                    elif sub_label.startswith("processing:"):
                        op_name = sub_label[len("processing:"):]
                        domain_entry["processing"][op_name] = sub_entry
                        proc_total += sub_total

                domain_entry["api_seconds"] = round(api_total, 6)
                domain_entry["processing_seconds"] = round(proc_total, 6)
                domain_entry["other_seconds"] = round(
                    max(0.0, total_sec - api_total - proc_total), 6
                )

                domains[domain_name] = domain_entry

        # Summary
        total_api = 0.0
        total_processing = 0.0
        for label, durations in self._durations.items():
            total = sum(durations)
            if label.startswith("api:"):
                total_api += total
            elif label.startswith("processing:"):
                total_processing += total

        overhead = max(0.0, total_domain_seconds - total_api - total_processing)

        summary = {
            "total_domain_seconds": round(total_domain_seconds, 6),
            "total_api_seconds": round(total_api, 6),
            "total_processing_seconds": round(total_processing, 6),
            "overhead_seconds": round(overhead, 6),
        }

        report: Dict[str, Any] = {
            "domains": domains,
            "summary": summary,
            "raw": raw,
        }

        return report
