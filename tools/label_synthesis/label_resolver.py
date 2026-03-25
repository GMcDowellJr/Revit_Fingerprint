"""
tools/label_synthesis/label_resolver.py

Five-layer (plus 2.5) pattern label resolution chain.

Resolution order (first match wins):
  1.   Curator override      — pattern_annotations.csv keyed by pattern_id (LOCKED)
  2.   Behavioral synopsis   — domain-specific formatter on identity_items (DETERMINISTIC)
  2.5  Near-duplicate merge  — numeric tolerance collapse across join_hashes in same domain
  3.   Strong modal label    — modal_share >= threshold AND low label entropy
  4.   LLM synthesis         — fragmented labels OR params-only (no label data)
  5.   Rank fallback         — "Variant {rank} of {N}" (EXPLICIT UNCERTAINTY)

Layer 2.5 — Near-Duplicate Merge
---------------------------------
Two patterns are "near-duplicates" when their identity_items differ only in
numeric fields within a small tolerance (e.g. accuracy 0.0624 vs 0.0625 due
to floating-point round-trip through Revit API). The smaller cluster defers
to the larger cluster's label with a "(~)" prefix. This prevents artificial
label fragmentation for what is effectively the same configuration.

find_near_duplicate_merges() is a domain-level pre-pass called before the
cluster label loop in emit_analysis_v21. It returns a merge map
{smaller_join_hash -> canonical_join_hash}. The caller resolves canonical
cluster labels first, then passes the resolved label as near_dup_target for
merged clusters.

Layer 3 Guardrail — Entropy
----------------------------
Modal share alone is insufficient. Two labels splitting 60/40 across only
2 files would pass a 60% threshold but is meaningless. Normalized entropy
measures whether the label population is genuinely concentrated (low entropy)
or coincidentally modal (high entropy with small N). Both conditions must pass.
"""

from __future__ import annotations

import json
import math
import os
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

MODAL_THRESHOLD = 0.60          # modal label must own >= 60% of file appearances
MAX_LABELS_FOR_MODAL = 3        # > 3 distinct labels = fragmented regardless of share
MODAL_MAX_NORM_ENTROPY = 0.5    # normalized entropy must be < 0.5 (concentrated)

NEAR_DUP_REL_TOL = 0.02         # 2% relative tolerance for numeric field comparison
NEAR_DUP_ABS_TOL = 1e-5         # absolute floor to avoid division near zero
NEAR_DUP_MIN_SIZE_RATIO = 0.15  # smaller cluster must be >= 15% of larger to merge


# ---------------------------------------------------------------------------
# Public entry point — per-pattern label resolution
# ---------------------------------------------------------------------------

def resolve_pattern_label(
    *,
    domain: str,
    join_hash: str,
    join_key_schema: str,
    pattern_rank: int,
    pattern_count: int,
    identity_items: Optional[List[Dict[str, Any]]] = None,
    label_population: Optional[List[Dict[str, Any]]] = None,
    annotations: Optional[Dict[str, str]] = None,
    llm_cache: Optional[Dict[str, Any]] = None,
    pattern_id: Optional[str] = None,
    near_dup_target_label: Optional[str] = None,
) -> Tuple[str, str]:
    """Resolve a human-readable label for a pattern.

    Args:
        domain:                 Domain name (e.g. "dimension_types")
        join_hash:              The join_hash for this pattern cluster
        join_key_schema:        Schema string (used in fallback label)
        pattern_rank:           1-based rank within domain (by file presence desc)
        pattern_count:          Total patterns in domain (N in "Variant X of N")
        identity_items:         List of {k, v, q} dicts from a representative record.
                                Must come from phase0_identity_items.csv lookup —
                                NOT from the flat record row itself.
        label_population:       Rows from joinhash_label_population.csv for this join_hash.
                                Each row: {label_q, label_v, files_count}
        annotations:            {pattern_id: canonical_name} from pattern_annotations.csv
        llm_cache:              {join_hash: {recommended, candidates, rationale, reviewed}}
        pattern_id:             Stable pattern_id string (for curator lookup)
        near_dup_target_label:  If not None, this cluster was merged into a near-duplicate
                                canonical cluster whose resolved label is this string.
                                Layer 1 (curator) still fires before this — a curator
                                can override the merge by annotating the specific pattern_id.

    Returns:
        Tuple of (label_string, resolution_source) where source is one of:
        "curator", "synopsis", "near_dup", "modal", "llm", "llm_unreviewed", "fallback"
    """
    # Layer 1: Curator override — immutable, fires even for near-dup merged clusters
    if annotations and pattern_id and pattern_id in annotations:
        return annotations[pattern_id], "curator"

    # Layer 2.5: Near-duplicate — defer to canonical cluster's resolved label
    if near_dup_target_label is not None:
        return f"(~) {near_dup_target_label}", "near_dup"

    # Layer 2: Behavioral synopsis
    if identity_items:
        synopsis = _try_synopsis(domain, identity_items)
        if synopsis:
            return synopsis, "synopsis"

    # Layer 3: Modal label (strong — share AND entropy both checked)
    if label_population:
        label, source = _try_modal(label_population)
        if label:
            return label, source

    # Layer 4: LLM synthesis (read from cache only — never calls API at emit time)
    if llm_cache and join_hash in llm_cache:
        entry = llm_cache[join_hash]
        name = entry.get("recommended") or ""
        if name:
            reviewed = entry.get("reviewed", False)
            return name, ("llm" if reviewed else "llm_unreviewed")

    # Layer 5: Rank fallback — explicit uncertainty signal
    return f"{join_key_schema} — Variant {pattern_rank} of {pattern_count}", "fallback"


# ---------------------------------------------------------------------------
# Layer 2.5: Near-duplicate merge detection (domain-level pre-pass)
# ---------------------------------------------------------------------------

def find_near_duplicate_merges(
    cluster_rows: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Detect near-duplicate clusters and return {smaller_jh -> canonical_jh}.

    Must be called with cluster_rows sorted by files_present DESC so that
    the canonical cluster is always the larger one (index i < j).

    Each cluster_row must contain:
        join_hash:       str
        files_present:   int
        identity_items:  List[{k, v, q}]  (from phase0_identity_items lookup)

    Two clusters are near-duplicates when:
        - Identical set of identity_item keys
        - Non-numeric values match exactly
        - Numeric values within NEAR_DUP_REL_TOL relative tolerance
        - Smaller cluster is >= NEAR_DUP_MIN_SIZE_RATIO * larger cluster size
          (prevents absorbing real outlier variants)
    """
    merge_map: Dict[str, str] = {}
    n = len(cluster_rows)

    for i in range(n):
        ci = cluster_rows[i]
        jh_i = ci.get("join_hash", "")
        if jh_i in merge_map:
            continue

        items_i = ci.get("identity_items") or []
        files_i = int(ci.get("files_present", 0))
        kv_i = _extract_kv_typed(items_i)
        if not kv_i:
            continue

        for j in range(i + 1, n):
            cj = cluster_rows[j]
            jh_j = cj.get("join_hash", "")
            if jh_j in merge_map:
                continue

            items_j = cj.get("identity_items") or []
            files_j = int(cj.get("files_present", 0))
            kv_j = _extract_kv_typed(items_j)
            if not kv_j:
                continue

            # Size ratio guard
            larger = max(files_i, files_j)
            smaller = min(files_i, files_j)
            if larger > 0 and (smaller / larger) < NEAR_DUP_MIN_SIZE_RATIO:
                continue

            if _are_near_duplicates(kv_i, kv_j):
                merge_map[jh_j] = jh_i   # j merges into i (i is larger/earlier)

    return merge_map


def _extract_kv_typed(
    identity_items: List[Dict[str, Any]],
) -> Optional[Dict[str, Tuple[str, bool]]]:
    """
    Build {key: (value_str, is_numeric)} from ok-quality identity_items.
    Returns None if items list is empty.
    """
    result: Dict[str, Tuple[str, bool]] = {}
    for item in identity_items:
        if not isinstance(item, dict):
            continue
        if item.get("q") != "ok":
            continue
        k = item.get("k", "")
        v = item.get("v", None)
        if not k or v is None:
            continue
        v_str = str(v)
        try:
            float(v_str)
            is_numeric = True
        except (ValueError, TypeError):
            is_numeric = False
        result[k] = (v_str, is_numeric)
    return result if result else None


def _are_near_duplicates(
    kv_a: Dict[str, Tuple[str, bool]],
    kv_b: Dict[str, Tuple[str, bool]],
) -> bool:
    """Return True if two kv dicts represent near-duplicate configurations."""
    if set(kv_a.keys()) != set(kv_b.keys()):
        return False
    for k in kv_a:
        v_a, is_num_a = kv_a[k]
        v_b, is_num_b = kv_b[k]
        if is_num_a != is_num_b:
            return False
        if not is_num_a:
            if v_a != v_b:
                return False
        else:
            if not _within_tolerance(float(v_a), float(v_b)):
                return False
    return True


def _within_tolerance(a: float, b: float) -> bool:
    if a == b:
        return True
    denom = max(abs(a), abs(b), NEAR_DUP_ABS_TOL)
    return abs(a - b) / denom <= NEAR_DUP_REL_TOL


# ---------------------------------------------------------------------------
# Layer 2: Synopsis dispatch
# ---------------------------------------------------------------------------

def _try_synopsis(domain: str, identity_items: List[Dict[str, Any]]) -> Optional[str]:
    try:
        formatter = _get_synopsis_formatter(domain)
        if formatter is None:
            return None
        return formatter(identity_items)
    except Exception:
        return None


def _get_synopsis_formatter(domain: str):
    try:
        import importlib
        # sys.path has tools/ prepended by v21_emit, so label_synthesis is top-level
        mod = importlib.import_module(
            f"label_synthesis.synopsis_formatters.{domain}"
        )
        return getattr(mod, "format_synopsis", None)
    except ImportError:
        return None

# ---------------------------------------------------------------------------
# Layer 3: Modal label with entropy guardrail
# ---------------------------------------------------------------------------

def _try_modal(label_population: List[Dict[str, Any]]) -> Tuple[Optional[str], str]:
    """
    Attempt modal label resolution with both share and entropy checks.

    Share check: modal label must own >= MODAL_THRESHOLD of file appearances.
    Entropy check: normalized entropy of label distribution must be < MODAL_MAX_NORM_ENTROPY.
    Both must pass.
    """
    ok_rows = [
        r for r in label_population
        if r.get("label_q", "ok") == "ok"
        and r.get("label_v", "").strip()
    ]
    if not ok_rows:
        return None, ""

    ok_rows = sorted(ok_rows, key=lambda r: -int(r.get("files_count", 0)))
    total_files = sum(int(r.get("files_count", 0)) for r in ok_rows)
    if total_files == 0:
        return None, ""

    modal_label = ok_rows[0]["label_v"].strip()
    modal_share = int(ok_rows[0].get("files_count", 0)) / total_files
    distinct = len(ok_rows)

    shares = [int(r.get("files_count", 0)) / total_files for r in ok_rows]
    entropy = -sum(s * math.log2(s) for s in shares if s > 0)
    max_entropy = math.log2(distinct) if distinct > 1 else 0.0
    norm_entropy = (entropy / max_entropy) if max_entropy > 0 else 0.0

    fragmented = (
        modal_share < MODAL_THRESHOLD
        or distinct > MAX_LABELS_FOR_MODAL
        or norm_entropy > MODAL_MAX_NORM_ENTROPY
    )

    if not fragmented:
        return modal_label, "modal"
    return None, ""


# ---------------------------------------------------------------------------
# Fragmentation detection (used by synthesize_fragmented_labels.py)
# ---------------------------------------------------------------------------

def is_fragmented(label_population_rows: List[Dict[str, Any]]) -> bool:
    """Return True if label population is too fragmented for modal label promotion."""
    ok_rows = [
        r for r in label_population_rows
        if r.get("label_q", "ok") == "ok"
        and r.get("label_v", "").strip()
    ]
    if not ok_rows:
        return True

    ok_rows = sorted(ok_rows, key=lambda r: -int(r.get("files_count", 0)))
    total = sum(int(r.get("files_count", 0)) for r in ok_rows)
    if total == 0:
        return True

    modal_share = int(ok_rows[0].get("files_count", 0)) / total
    distinct = len(ok_rows)
    shares = [int(r.get("files_count", 0)) / total for r in ok_rows]
    entropy = -sum(s * math.log2(s) for s in shares if s > 0)
    max_entropy = math.log2(distinct) if distinct > 1 else 0.0
    norm_entropy = (entropy / max_entropy) if max_entropy > 0 else 0.0

    return (
        modal_share < MODAL_THRESHOLD
        or distinct > MAX_LABELS_FOR_MODAL
        or norm_entropy > MODAL_MAX_NORM_ENTROPY
    )


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def load_llm_cache(cache_path: str) -> Dict[str, Any]:
    if not cache_path or not os.path.exists(cache_path):
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_llm_cache(cache_path: str, cache: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(cache_path)), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, sort_keys=True)


def load_annotations(annotations_path: str) -> Dict[str, str]:
    if not annotations_path or not os.path.exists(annotations_path):
        return {}
    import csv
    result: Dict[str, str] = {}
    try:
        with open(annotations_path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                pid = row.get("pattern_id", "").strip()
                name = row.get("canonical_name", "").strip()
                if pid and name:
                    result[pid] = name
    except Exception:
        pass
    return result


def load_label_population(population_csv: str, domain: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Load joinhash_label_population.csv and index by join_hash.
    Returns: {join_hash: [{label_q, label_v, files_count}, ...]}
    """
    if not population_csv or not os.path.exists(population_csv):
        return {}
    import csv
    result: Dict[str, List[Dict[str, Any]]] = {}
    try:
        with open(population_csv, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("domain", "") != domain:
                    continue
                jh = row.get("join_hash", "").strip()
                if not jh:
                    continue
                result.setdefault(jh, []).append({
                    "label_q": row.get("label_q", "ok"),
                    "label_v": row.get("label_v", ""),
                    "files_count": int(row.get("files_count", 0)),
                })
    except Exception:
        pass
    return result
