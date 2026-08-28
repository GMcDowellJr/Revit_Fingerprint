# Run A progress and performance diagnostics

Run A identifies source-signature reuse **candidates** without reading cache
payloads. Each candidate is loaded and validated only when its export reaches the
sequential output loop; only accepted entries count as reused. Missing, unreadable,
invalid-JSON, and structurally/identity-rejected entries are reported separately
and safely recomputed. This changes cache I/O scheduling and console diagnostics,
not fingerprints, policies, statuses, analytical CSV schemas, ordering, coverage,
or eligibility rules.

`run_extract_all.py` accepts `--progress-interval-seconds` (positive finite,
default `10`) and `--quiet-progress`. Progress is newline-based, flushed to
stderr, and covers startup, the corpus-wide source-signature barrier, per-file
work, output promotion, and manifest publication. Quiet mode suppresses periodic
heartbeats, not warnings or final summaries. Heartbeats during opaque operations
are best effort: interpreter scheduling, the GIL, and blocked I/O can delay them;
no percentage is claimed.

Invalid UTF-8 cache payloads are classified as unreadable and safely recomputed.
The heartbeat is stopped and joined after success, processing failure,
`SystemExit`, or interruption; the original failure remains the process outcome.

`extract_all.report.json` retains existing keys and adds versioned
`performance_diagnostics`, including combined and phase durations, candidate and
accepted reuse counts, entry loads/fallbacks, actual source files/bytes hashed,
emitted rows, tool version, invalidation reason, placeholders, and orchestrator
total. Fresh parse/flatten/signature/join is intentionally a combined interval;
phase totals are not necessarily disjoint. Optional NameKey runs afterward, is
timed by each runbook, and is not included in this JSON.

This is not a complete cache-safety fix. Interrupted rebuilds can still overwrite
entries while an old manifest survives; mtime/size assumptions and entry
integrity/generation remain; and `0.0.0+nogit` weakens code-change invalidation.
Source checking remains a whole-corpus barrier, without workers, pipelining,
cache-format changes, or NameKey caching.
