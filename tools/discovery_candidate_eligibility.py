"""Central, diagnostic-only eligibility for greedy/Pareto candidates.

This module never participates in extraction, sig/join hashing, or runtime
policy resolution.  Rules are explicit in a separately versioned registry;
there is intentionally no fuzzy ``*id*`` or other name-pattern exclusion.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

DEFAULT_REGISTRY = Path(__file__).resolve().parents[1] / "policies" / "discovery_candidate_eligibility.json"


@dataclass(frozen=True)
class CandidateDecision:
    item: str
    eligible: bool
    classification: str
    reason: str = ""
    canonical_item: str = ""


def load_registry(path: Optional[Path] = None) -> Mapping[str, object]:
    source = path or DEFAULT_REGISTRY
    data = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("schema_version") != "1.0":
        raise ValueError("unsupported discovery candidate eligibility registry")
    return data


def classify_candidate(domain: str, item: str, registry: Optional[Mapping[str, object]] = None) -> CandidateDecision:
    cfg = registry or load_registry()
    global_cfg = cfg.get("global", {}) if isinstance(cfg.get("global"), dict) else {}
    domains = cfg.get("domains", {}) if isinstance(cfg.get("domains"), dict) else {}
    domain_cfg = domains.get(domain, {}) if isinstance(domains.get(domain), dict) else {}

    # Alias declarations are intentionally exact and domain scoped.
    for scope in (global_cfg, domain_cfg):
        aliases = scope.get("aliases", {}) if isinstance(scope.get("aliases"), dict) else {}
        for canonical, declarations in aliases.items():
            for declaration in declarations if isinstance(declarations, list) else []:
                alias = declaration.get("item") if isinstance(declaration, dict) else declaration
                if alias == item:
                    classification = declaration.get("classification", "canonical_alias") if isinstance(declaration, dict) else "canonical_alias"
                    return CandidateDecision(item, False, str(classification), f"alias_of:{canonical}", str(canonical))

    for scope in (global_cfg, domain_cfg):
        exclusions = scope.get("excluded_candidates", []) if isinstance(scope.get("excluded_candidates"), list) else []
        for declaration in exclusions:
            if isinstance(declaration, dict) and declaration.get("item") == item:
                return CandidateDecision(item, False, str(declaration.get("classification", "unknown_requires_domain_review")), str(declaration.get("reason", "explicit_discovery_exclusion")))

    # Exact leaf semantics only: presentation_id/sorting_parameter_id and all
    # other stable semantic IDs remain eligible.
    leaf = item.rsplit(".", 1)[-1]
    leaf_rules = global_cfg.get("excluded_leaf_names", []) if isinstance(global_cfg.get("excluded_leaf_names"), list) else []
    for declaration in leaf_rules:
        if isinstance(declaration, dict) and declaration.get("leaf") == leaf:
            return CandidateDecision(item, False, str(declaration.get("classification")), str(declaration.get("reason")))
    return CandidateDecision(item, True, "semantic_candidate")


def filter_candidates(domain: str, candidates: Sequence[str], registry: Optional[Mapping[str, object]] = None) -> Dict[str, object]:
    raw = sorted({str(x).strip() for x in candidates if str(x).strip()}, key=str.lower)
    decisions = [classify_candidate(domain, item, registry) for item in raw]
    return {
        "raw": raw,
        "eligible": [d.item for d in decisions if d.eligible],
        "excluded": [d for d in decisions if not d.eligible and not d.canonical_item],
        "alias_suppressed": [d for d in decisions if bool(d.canonical_item)],
        "decisions": decisions,
    }


def filter_and_cap_candidates(domain: str, ranked_candidates: Sequence[str], max_fields: int,
                              registry: Optional[Mapping[str, object]] = None) -> Dict[str, object]:
    """Filter a complete ranked surface, then cap only eligible candidates.

    ``ranked_candidates`` must already be ordered by the discovery frequency
    ranking.  Ineligible high-frequency evidence must never consume a cap slot.
    """
    raw = []
    seen = set()
    for value in ranked_candidates:
        item = str(value).strip()
        if item and item not in seen:
            seen.add(item)
            raw.append(item)
    decisions = [classify_candidate(domain, item, registry) for item in raw]
    result = {
        "raw": raw,
        "eligible": [d.item for d in decisions if d.eligible],
        "excluded": [d for d in decisions if not d.eligible and not d.canonical_item],
        "alias_suppressed": [d for d in decisions if bool(d.canonical_item)],
        "decisions": decisions,
    }
    eligible = list(result["eligible"])
    if max_fields > 0:
        eligible = eligible[:max_fields]
    result["eligible"] = eligible
    return result


def diagnostic_fields(result: Mapping[str, object]) -> Dict[str, str]:
    excluded = result.get("excluded", [])
    aliases = result.get("alias_suppressed", [])
    raw = result.get("raw", [])
    eligible = result.get("eligible", [])
    def render(values: Iterable[CandidateDecision]) -> str:
        return "|".join(f"{d.item}::{d.classification}::{d.reason}" for d in values)
    return {
        "candidate_fields_raw": "|".join(raw),
        "candidate_fields_raw_count": str(len(raw)),
        "candidate_fields_excluded": render(excluded),
        "candidate_fields_excluded_count": str(len(excluded)),
        "candidate_fields_alias_suppressed": render(aliases),
        "candidate_fields_alias_suppressed_count": str(len(aliases)),
        "candidate_fields_eligible": "|".join(eligible),
        "candidate_fields_eligible_count": str(len(eligible)),
    }
