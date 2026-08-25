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

    # When cfg["gates"]["required_fields"] is set, build_candidate_join_key_with_details
    # (core/join_key_discovery/eval.py) uses it -- not whatever candidate subset is
    # actually passed in -- to decide which fields go into the composite join key:
    # `base_required = gates.get("required_fields") or selected_fields`. That means
    # every candidate this loop scores below evaluates identically regardless of which
    # field is being tested, so starting from selected=[] and letting the loop "discover"
    # its way to the required baseline doesn't work: the very first tie (which is
    # immediate, since every one-field candidate scores the same) stops the loop after
    # a single, essentially arbitrary field, while metrics were actually computed against
    # the full required baseline the whole time -- a misleading, non-representative
    # result. Seed `selected` with the required fields up front so the reported
    # selected_fields matches what was actually evaluated.
    required_fields = sorted(
        {str(f).strip() for f in (cfg.get("runtime_required_fields") or (cfg.get("gates") or {}).get("required_fields") or []) if str(f).strip()},
        key=lambda s: s.lower(),
    )
    selected: List[str] = list(required_fields)
    remaining = sorted(
        (set(str(f) for f in candidate_fields if str(f).strip()) - set(selected)),
        key=lambda s: s.lower(),
    )
    diagnostics: List[Dict[str, Any]] = []

    # Seeding `selected` above is not enough on its own: score_candidate ->
    # build_candidate_join_key_with_details composes the ACTUAL composite key
    # from `gates.required_fields or selected_fields` -- an OR, not a union --
    # so whenever cfg.gates.required_fields is set, EVERY candidate this loop
    # scores below (selected+f for every remaining f) still gets scored using
    # only the static required baseline, never the candidate actually under
    # test. That makes every contender tie on identical metrics, so the very
    # first iteration's tie-break (`_score(best) >= _score(...selected...)`,
    # which is true here because best's field-count is strictly larger for
    # otherwise-identical metrics) breaks the loop before `selected` is ever
    # updated -- harsh/validate mode's greedy search would report metrics
    # against the required baseline while claiming to have searched beyond it,
    # and could never actually pick up any optional/discovered field on top.
    # `selected` already carries the required fields via the seeding above and
    # only ever grows, so scoring with `gates.required_fields` OMITTED lets
    # `base_required` fall through to `selected_fields` (=cand, already
    # required-inclusive) -- the real candidate under test -- while
    # shape-gating (discriminator_key/shape_requirements) stays active via the
    # rest of `gates`, unaffected.
    scoring_cfg = dict(cfg)
    if "evaluation_mode" not in scoring_cfg:
        scoring_cfg["gates"] = {k: v for k, v in (cfg.get("gates") or {}).items() if k != "required_fields"}

    while remaining:
        contenders = []
        for f in remaining:
            cand = sorted(selected + [f], key=lambda s: s.lower())
            m = score_candidate(domain_records, domain_identity_items, cand, scoring_cfg)
            contenders.append(m)
        contenders = sorted(contenders, key=_score)
        best = contenders[0]
        diagnostics.append({"step": len(selected) + 1, "best": best, "top3": contenders[:3]})
        if _score(best) >= _score(score_candidate(domain_records, domain_identity_items, selected, scoring_cfg)) and selected:
            break
        selected = list(best["selected_fields"])
        for f in list(remaining):
            if f in selected:
                remaining.remove(f)

        if float(best.get("coverage", 0.0)) >= 0.999 and float(best.get("collision_rate", 1.0)) <= collision_threshold:
            break

    final_metrics = score_candidate(domain_records, domain_identity_items, selected, scoring_cfg)
    contenders = sorted(
        [score_candidate(domain_records, domain_identity_items, sorted(set(selected + [f]), key=lambda s: s.lower()), scoring_cfg) for f in candidate_fields if f not in selected],
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
