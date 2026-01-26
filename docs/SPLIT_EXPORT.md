# Split Export: Index + Details

## Overview

As of this update, the Revit Fingerprint exporter can split its output into two separate files to reduce duplication and improve tool performance:

1. **Index JSON** (`<basename>.index.json`) - Small, canonical file for tool ingestion
2. **Details JSON** (`<basename>.details.json`) - Large file containing all domain payloads with records

The legacy monolithic bundle (`<basename>.json`) is still emitted by default for backward compatibility.

## Motivation

The previous monolithic export repeated schema_version, run_status, hash_mode, and per-domain summaries (name, status, hash, block_reasons, diagnostic flags, counts) across multiple locations:
- Top-level `_contract`
- Top-level `_manifest`
- Top-level `_features`
- Top-level `_meta`
- Per-domain payloads

This created:
- **Large file sizes** due to duplication
- **Slow tool loading** when only contract metadata was needed
- **Maintenance overhead** when updating schema

## Solution

The split export separates concerns:
- **Index** contains canonical contract + identity + pointers (used by comparison and analysis tools)
- **Details** contains full domain payloads (used by similarity tools that need records)
- **Legacy bundle** remains unchanged (backward compatibility)

## File Structure

### Index JSON Structure

```json
{
  "_contract": {
    "schema_version": "2.0",
    "run_status": "ok",
    "run_diag": {
      "errors": [],
      "counters": {
        "domain_total": 10,
        "domain_ok": 9,
        "domain_blocked": 1
      }
    },
    "domains": {
      "domain_name": {
        "domain": "domain_name",
        "domain_version": "1",
        "status": "ok",
        "hash": "abc123...",
        "block_reasons": [],
        "diag": {
          "api_reachable": true,
          "hash_mode": "semantic",
          "count": 42,
          "raw_count": 45,
          "has_v2": true
        }
      }
    }
  },
  "_hash_mode": "semantic",
  "identity": {
    "project_title": "Sample Project",
    "is_workshared": false,
    "revit_version_number": "2024"
  },
  "_meta": {
    "runner": "M5",
    "tool_version": "X.Y.Z",
    "elapsed_seconds_total": 12.34
  },
  "_notes": ["Optional runner notes"],
  "artifacts": {
    "details_href": "<basename>.details.json"
  }
}
```

### Details JSON Structure

```json
{
  "identity": {
    "records": [...],
    "count": 1,
    "raw_count": 1
  },
  "units": {
    "records": [...],
    "count": 15,
    "raw_count": 15
  },
  "line_patterns": {
    "records": [...],
    "count": 8,
    "raw_count": 10
  },
  ... all other domain payloads ...
}
```

## Configuration

Control which files are emitted via environment variables:

### Environment Variables

| Variable | Values | Default | Description |
|----------|--------|---------|-------------|
| `REVIT_FINGERPRINT_EMIT_INDEX` | `1`, `0`, `true`, `false` | `1` | Emit index.json |
| `REVIT_FINGERPRINT_EMIT_DETAILS` | `1`, `0`, `true`, `false` | `1` | Emit details.json |
| `REVIT_FINGERPRINT_EMIT_LEGACY_BUNDLE` | `1`, `0`, `true`, `false` | `1` | Emit legacy bundle.json |

### Example Usage

```bash
# Emit all three files (default)
python runner/run_dynamo.py

# Emit only index and details (skip legacy bundle)
REVIT_FINGERPRINT_EMIT_LEGACY_BUNDLE=0 python runner/run_dynamo.py

# Emit only index (minimal output for contract validation)
REVIT_FINGERPRINT_EMIT_DETAILS=0 \
REVIT_FINGERPRINT_EMIT_LEGACY_BUNDLE=0 \
python runner/run_dynamo.py

# Emit only legacy bundle (preserve old behavior)
REVIT_FINGERPRINT_EMIT_INDEX=0 \
REVIT_FINGERPRINT_EMIT_DETAILS=0 \
python runner/run_dynamo.py
```

## Tool Compatibility

### Tools that work with INDEX only

These tools read contract metadata and don't need full domain payloads:

- `core.manifest.build_manifest()` - Reads `_contract` and `_hash_mode`
- `core.features.build_features()` - Reads counts from `_contract.domains[].diag.count` (new) or falls back to legacy payloads
- `tools/compare_manifest.py` - Uses manifest surface
- `tools/score_drift.py` - Uses manifest surface

**Usage:**
```python
import json
from core.manifest import build_manifest
from core.features import build_features

# Load index
with open("output.index.json") as f:
    index = json.load(f)

# Works with index only
manifest = build_manifest(index, include_identity=True)
features = build_features(index)
```

### Tools that need DETAILS

These tools need full domain payloads with records for similarity metrics:

- `tools/similarity_compare.py` - Uses `signature_multiset_similarity` metric (reads records from domain payloads)

**Usage:**
```bash
# Use .details.json files for record-level comparison
python tools/similarity_compare.py \
  --baseline baseline.details.json \
  --dir /path/to/fingerprints \
  --mode both
```

### Tools that work with LEGACY bundle

All tools work with the legacy bundle as before (it contains everything):

```bash
python tools/similarity_compare.py \
  --baseline baseline.json \
  --dir /path/to/fingerprints \
  --mode both
```

## Key Changes

### 1. Enhanced Contract Diagnostics

Domain diagnostics (`_contract.domains[domain].diag`) now include:

```json
{
  "api_reachable": true,
  "hash_mode": "semantic",
  "count": 42,
  "raw_count": 45,
  "has_v2": true,
  "v2_block_reasons": {...}
}
```

This allows `build_features()` to extract counts without needing the full domain payload.

### 2. Backward Compatible Count Reading

`core/features.py` now:
1. **First** tries to read counts from `_contract.domains[domain].diag.count`
2. **Falls back** to `payload[domain].count` for backward compatibility

This ensures:
- New index-only workflows work
- Old full-payload workflows continue to work
- No breaking changes

### 3. File Naming Convention

Given an output path like `/path/to/output.json`:

- Legacy bundle: `/path/to/output.json`
- Index: `/path/to/output.index.json`
- Details: `/path/to/output.details.json`
- Manifest: `/path/to/output.manifest.json`
- Features: `/path/to/output.features.json`

## Migration Guide

### For Tool Authors

**If your tool only needs contract metadata:**
1. Accept `.index.json` files as input
2. Read from `_contract` and `_hash_mode`
3. If you need counts, read from `_contract.domains[domain].diag.count` first

**If your tool needs full records:**
1. Accept `.details.json` files as input
2. Document that `.index.json` won't work for your tool

**To support both:**
1. Try loading `.index.json` first (fast path)
2. Fall back to `.json` or `.details.json` if records are needed
3. Use the backward-compatible pattern from `core/features.py`

### For Pipeline Authors

**Current behavior (all files emitted):**
```bash
# No changes needed - default emits all files
python runner/run_dynamo.py
```

**Optimized for storage:**
```bash
# Only emit index + details (skip legacy bundle)
export REVIT_FINGERPRINT_EMIT_LEGACY_BUNDLE=0
python runner/run_dynamo.py
```

**Optimized for speed (contract validation only):**
```bash
# Only emit index
export REVIT_FINGERPRINT_EMIT_DETAILS=0
export REVIT_FINGERPRINT_EMIT_LEGACY_BUNDLE=0
python runner/run_dynamo.py
```

## Testing

Run the smoke tests to verify the split export works:

```bash
python tests/test_split_export.py
```

Expected output:
```
Running split export smoke tests...

✓ Index payload structure is correct
✓ build_manifest() works with index payload
✓ build_features() works with index payload (reads counts from contract)
✓ Details payload structure is correct
✓ build_features() backward compatibility with legacy payload works

✅ All smoke tests passed!
```

## Rollback

To revert to legacy-only behavior:

```bash
export REVIT_FINGERPRINT_EMIT_INDEX=0
export REVIT_FINGERPRINT_EMIT_DETAILS=0
export REVIT_FINGERPRINT_EMIT_LEGACY_BUNDLE=1
```

Or remove the environment variables entirely (legacy bundle is emitted by default).

## Future Enhancements

Possible future work:
- Compress details.json (gzip) to reduce storage
- Split details into per-domain files for parallel processing
- Add schema validation for index vs details contracts
- Support merging index + details back into full bundle if needed
