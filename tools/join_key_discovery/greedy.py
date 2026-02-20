from __future__ import annotations

from typing import Any, Dict, List, Sequence

from .eval import score_candidate


def _score(metrics: Dict[str, Any]) -> tuple:
    return (
        -float(metrics.get("coverage", 0.0)),
        float(metrics.get("collision_rate", 1.0)),
        float(metrics.get("fragmentation_rate", 1.0)),
        len(metrics.get("selected_fields", [])),
        "|".join(metrics.get("selected_fields", [])),
    )


def discover_greedy(
    domain_records: Sequence[Dict[str, str]],
    domain_identity_items: Dict[str, Dict[str, tuple[str, str]]],
    candidate_fields: Sequence[str],
    cfg: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    cfg = cfg or {}
    collision_threshold = float(cfg.get("collision_threshold", 0.05))
    near_tie_delta = float(cfg.get("near_tie_delta", 0.005))

    remaining = sorted(set(str(f) for f in candidate_fields if str(f).strip()), key=lambda s: s.lower())
    selected: List[str] = []
    diagnostics: List[Dict[str, Any]] = []

    while remaining:
        contenders = []
        for f in remaining:
            cand = sorted(selected + [f], key=lambda s: s.lower())
            m = score_candidate(domain_records, domain_identity_items, cand, cfg)
            contenders.append(m)
        contenders = sorted(contenders, key=_score)
        best = contenders[0]
        diagnostics.append({"step": len(selected) + 1, "best": best, "top3": contenders[:3]})
        if _score(best) >= _score(score_candidate(domain_records, domain_identity_items, selected, cfg)) and selected:
            break
        selected = list(best["selected_fields"])
        for f in list(remaining):
            if f in selected:
                remaining.remove(f)

        if float(best.get("coverage", 0.0)) >= 0.999 and float(best.get("collision_rate", 1.0)) <= collision_threshold:
            break

    final_metrics = score_candidate(domain_records, domain_identity_items, selected, cfg)
    contenders = sorted(
        [score_candidate(domain_records, domain_identity_items, sorted(set(selected + [f]), key=lambda s: s.lower()), cfg) for f in candidate_fields if f not in selected],
        key=_score,
    )

    needs_pareto_reasons: List[str] = []
    if float(final_metrics.get("collision_rate", 1.0)) > collision_threshold and float(final_metrics.get("coverage", 0.0)) >= 0.7:
        needs_pareto_reasons.append("collision_above_threshold")
    if contenders:
        gap = abs(float(contenders[0].get("collision_rate", 1.0)) - float(final_metrics.get("collision_rate", 1.0)))
        if gap <= near_tie_delta:
            needs_pareto_reasons.append("near_tie")

    return {
        "selected_fields": selected,
        "metrics": final_metrics,
        "needs_pareto": bool(needs_pareto_reasons),
        "needs_pareto_reasons": needs_pareto_reasons,
        "top_contenders": contenders[:5],
        "diagnostics": diagnostics,
    }
