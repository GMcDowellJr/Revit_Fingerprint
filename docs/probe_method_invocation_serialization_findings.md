# Method Invocation Results — Serialization Gap, Missing Invocation, Ambiguous Quality State

Findings-only pass. No code changes under `tools/probes/`. This document
supersedes nothing in `docs/probe_method_invocation_candidates_verification.md`;
it diagnoses the three data-quality problems observed in `PROBE_INVENTORY.csv`
after that allowlist work shipped.

## Step 0 — what actually landed, and an important correction to the task's premise

**The allowlist PR was not `claude/probe-method-invocation-allowlist`.** No
branch of that name exists. The actual work is on
`claude/method-invocation-candidates-jctnxe`, merged as **PR #395**
(merge commit `b44a4fc`), already on `main` and already included in this
branch (`git log` shows `b44a4fc` is the current HEAD). The commit chain,
in order:

| Commit | What it did |
|---|---|
| `fa8b82c` | Step 0 findings-only pass (docs, no code) — `docs/probe_method_invocation_candidates_verification.md` |
| `7ecc627` | re-verify candidates |
| `7215638` | **feat**: allowlist + invoke 35 methods (34 unique names) across 24 `probe_*.py` files |
| `cba1765` | regenerate probe exports, live re-run against one real document |
| `bad4983` | **fix**: drop `GetValidTypes` from the allowlist (34 → 33 names) after the live re-run showed it fails 100% of the time |
| `8be3630`, `f11ad25` | further export refreshes |

**Architecture decision confirmed:** in-place patching of all 24
`probe_*.py` files, *not* a shared module. Verified directly (not just
trusted from the commit message): `grep -rl "_ALLOWLISTED_REFLECTION_METHODS" tools/probes/*.py`
returns exactly 24 files, each with a byte-identical copy of the dict, the
comment block, `_reflect_try_get`, and `_reflect_contract` (spot-checked
`probe_worksets.py`, `probe_wall_types.py`, `probe_view_templates.py`,
`probe_materials.py` against `probe_identity.py` — same functions, same
content, different line offsets). `probe_dimension_types.py` and
`probe_roof_type_import.py` are the 2 of 26 files with no reflection sweep
at all (confirmed via `grep -L`). The commit message's stated rationale
(every `probe_*.py` is a standalone Dynamo Python-node script with zero
cross-file imports, so a shared module would require an import mechanism
this codebase has never relied on) is sound and matches what's on disk.

**The value-serialization function is `_reflect_contract`, not `_pv()`.**
`_pv()` (defined per-file in `probe_browser_organization.py:143`,
`probe_wall_types.py:339`, `probe_worksets.py:104`, `probe_views.py:253`)
is a *hand-authored* wrapper used by curated, domain-specific
`_add_inventory_obs`/`_observe_synth` call sites — the caller has already
extracted/resolved a scalar value before calling it, and `_pv`'s only job
is coercing anything non-JSON-native to a string as a defensive fallback
(the comment at `probe_browser_organization.py:143-153` documents the
real 2026-08-04 bug this guards: `Workset.UniqueId` is `System.Guid`, not
`System.String`, and `json.dump()` threw mid-write). That bug and that
function are unrelated to the reflection sweep. The function that actually
governs `example_raw`/`example_q` for **method** rows is
**`_reflect_contract`** (`probe_identity.py:663-687`, identical in all 24
files), called from `_run_reflection_sweep` (`probe_identity.py:689-723`).

`_reflect_contract` handles, in order:
1. `None` → `q="missing"`
2. `bool`/`int`/`float`/`str` → `q="ok"`, scalar storage
3. anything with `.IntegerValue` (bare `ElementId`) → `q="ok"`, `storage="ElementId"`
4. anything with `.ToString()` whose result doesn't contain `"Autodesk.Revit"` or `"System."` → `q="ok"`, `storage="None"`, `display=<string>`
5. everything else → `q="unsupported"`, `storage="None"`, `raw=None`, `display=None`

There is **no branch for `.NET` collections or complex objects** — that's
the entire root cause of Problem 1, detailed below.

`Element.GetValidTypes`/`Subelement.GetValidTypes` **are not typos or
key-mismatches** — they're spelled and keyed correctly (matching by method
name only, deliberately, not `(declaring_class, type_label)` — see
Problem 2). They are simply **no longer in the allowlist**: `bad4983`
already removed them. Current state, verified directly:

```
$ python3 -c "... parse _ALLOWLISTED_REFLECTION_METHODS from probe_identity.py ..."
33 entries
'GetValidTypes' in names -> False
```

This is the single most important correction to the task's premise: **Problem
2, as described ("still in the 35-entry allowlist"), no longer describes the
current codebase.** It describes the state PR #395 was diagnosing internally
before its own last commit fixed it. See Problem 2 below for what "fixed"
actually means here (removal, not repair) and whether that's the right call.

---

## Problem 1 — Serialization gap

**Root cause:** `_reflect_contract` (`probe_identity.py:663-687`, identical
in all 24 files) has no branch for `.NET` collections
(`IList<ElementId>`/`ICollection<ElementId>`/`ISet<ElementId>`) or for
complex objects (`Asset`, `FillPattern`, `LinePattern`, `Reference`, View
settings/override objects). It is a **silent fallthrough, not a caught
exception**: the collection/object *is* returned successfully by
`_reflect_try_get` (`probe_identity.py:652-656`, `ok=True`), so no
exception is thrown or caught anywhere in this path. `_reflect_contract`
just runs out of `isinstance`/`hasattr` checks and hits the final
`return {"q": "unsupported", ...}` at line 687. The two closest branches
that *look* like they might catch a collection both fail silently for a
different reason:
- The `.IntegerValue` check (line 674-679) is wrapped in `try/except: pass`
  — a `.NET` `List<ElementId>` has no `.IntegerValue` attribute itself
  (only its *elements* do), so `hasattr` is `False` and it falls through
  cleanly, no exception involved.
- The `.ToString()` check (line 680-686) does fire for these objects
  (`.NET` objects always have `ToString()`), but the returned string for a
  default (non-overridden) `.NET` collection/object `ToString()` is
  something like `"System.Collections.Generic.List\`1[...]"` or
  `"Autodesk.Revit.DB.FillPattern"` — which the guard on line 683
  explicitly excludes (`"Autodesk.Revit" not in s and "System." not in s`).
  That guard exists precisely to reject the default `type.ToString()`
  fallback (a fake-looking but meaningless "value"), and it correctly
  rejects collections/complex objects too, as a side effect.

**Scoped-fix assessment, split by the task's two sub-questions:**

**(a) `IList<ElementId>`/`ICollection<ElementId>`/`ISet<ElementId>` → list of
ints — this is a small, scoped fix.** A single additional branch in
`_reflect_contract`, inserted before the final `unsupported` fallback,
that attempts to iterate `raw_v` and checks each item for `.IntegerValue`,
would cover all 8 of the ElementId-collection methods
(`GetMonitoredLinkElementIds`, `GetMonitoredLocalElementIds`,
`GetSimilarTypes`, `GetFilters`, `GetOrderedFilters`,
`GetReferenceCallouts`, `GetReferenceElevations`, `GetReferenceSections`)
uniformly, with no per-method special-casing needed — it's the same shape
problem in every case. This doesn't touch `_reflect_try_get`'s
control flow or the allowlist; it's additive and localized to one function
duplicated in 24 files (mechanical, not risky, but it is 24 files to touch
identically — same blast radius as the original allowlist patch).

**(b) The 20 complex-object returns (`Asset`, `FillPattern`, `LinePattern`,
`Reference`, View settings/override objects) — confirmed out of scope, and
correctly so.** These don't have a natural scalar/list representation the
way an `ElementId` collection does; serializing them meaningfully would
mean either (i) picking specific sub-properties per object type (a
per-method design decision, not a generic serializer change), or (ii) a
generic `.NET`-object-to-dict reflector, which is a materially bigger
change than (a) and carries real risk of surfacing side-effecting or
extremely deep object graphs (e.g. `Reference` → element back-references).
Rightly deferred.

**Does the property-value path already serialize complex objects? Checked
directly — no, and there is nothing for the method path to reuse.**
`_pv()` (see Step 0 above) never receives a raw `.NET` object — every
caller has already pulled out a specific scalar field beforehand (e.g.
`probe_browser_organization.py`'s `_resolve_workset` resolves a
`WorksetId` via `WorksetTable.GetWorkset()` *before* ever calling `_pv`).
`_pv`'s only "generic" handling is `_coerce()`'s bare
`try: return str(v) except: return None` fallback — a defensive
stringification net, not a structured serializer. It would stringify a
`.NET` collection the same useless way `_reflect_contract`'s `ToString()`
branch already does (and gets excluded for the same "looks like
`System.X`" reason, if that guard were applied — it isn't, in `_pv`, but
the effect is the same uselessness). So there is no existing generic
complex-object serialization path anywhere in this codebase for the
method path to reuse; building the scoped ElementId-list fix in (a) would
be new code, not a port of existing logic.

**Crosswalk impact, confirmed against `find_crosswalk_candidates.py`:**
`_is_elementid_typed()` (`find_crosswalk_candidates.py:86-88`) gates
purely on `example_storage == "ElementId"` — a value `_reflect_contract`
only ever assigns via its scalar branch (line 674-679). None of the 8
ElementId-collection methods can reach that branch today, confirming the
task's framing exactly: those 8 methods invoke successfully, cost nothing,
and are structurally invisible to the crosswalk-candidate finder until (a)
is implemented.

---

## Problem 2 — Missing invocation (`GetValidTypes`)

**Root cause, already found and already addressed by the immediately
preceding PR — not a fresh finding, but re-confirmed independently here
rather than trusted from the commit message:**

`Element.GetValidTypes` was added to the allowlist in `7215638` (35
entries, 34 unique names — `GetValidTypes` is declared on both `Element`
and `Subelement`, collapsed to one name-keyed entry per the "key by name,
not `(declaring_class, name)`" decision — see Step 0). The live re-run in
`cba1765` invoked it for real and found it fails on **100% of 21 sampled
instances** with:

```
TypeError: No method matches given arguments for GetValidTypes: (<class '...'>)
```

— a CLR/pythonnet **binder rejection**, confirmed via a standalone
diagnostic outside the normal probe path (per `bad4983`'s commit message),
not a Revit API exception. `bad4983` ruled out the two more likely
explanations before concluding this: not overload ambiguity (.NET
reflection sees exactly one `GetValidTypes` overload, declaring type
`Autodesk.Revit.DB.Element`) and not a per-class precondition (its RevitAPI
doc page states no exception for the zero-arg form, unlike
`GetCalloutParentId`/`GetExternalFileReference`/
`GetModelToProjectionTransforms`, each of which does — see Problem 3). The
call is rejected by the binder before it ever reaches Revit's
implementation — it will never succeed via the sweep's blanket
`getattr(obj, name)()` invocation convention (`probe_identity.py:653`),
regardless of model, Revit version, or sampled element.

**Current state on this branch, verified directly (not assumed from the
commit message):** `GetValidTypes` is absent from
`_ALLOWLISTED_REFLECTION_METHODS` in all 24 files (33 entries now, was
34). So on current `main`/this branch, `Element.GetValidTypes` and
`Subelement.GetValidTypes` correctly show `<method not invoked>` again —
**via the legitimate "not allowlisted" path**, not because invocation is
silently failing. **The task's framing — "despite being in the 35-entry
allowlist" — describes a state that existed only between `7215638` and
`bad4983`, both already merged.** If the `PROBE_INVENTORY.csv` the task's
author is looking at still shows 35 allowlist entries and 0/21 invoked for
`GetValidTypes`, that CSV predates `bad4983` and is stale relative to
current code — regenerating it (out of scope for this pass, and no live
Revit session is available here either) would show the corrected 33-entry
behavior.

**Is dropping the right fix, or does it need revisiting?** This is a
scoped, low-risk fix as applied (removal is unconditionally safe — it can
only ever suppress a call that was already 100%-failing) but it is a
**workaround, not a resolution** of why the method can't be invoked at
all. `bad4983`'s own text flags the loose end: `GetValidTypes`' doc page
notes a "GetValidTypes Overload" sibling, suggesting the real instance
method may require an argument (plausibly `GetValidTypes(Document)`) that
the sweep's zero-arg-only calling convention structurally cannot supply.
Supporting a non-zero-arg overload would be a genuine design decision (it
changes the "zero-arg, no-side-effect" invariant the whole allowlist
exists to enforce — see the safety comment at
`probe_identity.py:644-650`), not a mechanical fix, and is correctly left
unaddressed for now. Recommendation: treat current removal as adequate
and closed for this pass; do not re-open unless a future need for
`GetValidTypes` data specifically justifies extending the sweep to support
single-argument (`Document`) method calls as a new, separately-reviewed
allowlist category.

---

## Problem 3 — Ambiguous quality state (`example_q == ''`)

**Root cause, traced end-to-end through the aggregation pipeline (not just
the extraction-side sweep):**

1. In `_run_reflection_sweep` (`probe_identity.py:689-723`), when
   `_reflect_try_get` returns `ok=False` (an exception was thrown and
   caught), the code does:
   ```python
   if not ok:
       e["error_count"] += 1
       continue          # probe_identity.py:704-706
   ```
   The actual exception message (`err`, the third return value of
   `_reflect_try_get`) **is discarded here** — never stored on `e`, never
   reaches `records.append(...)` at line 718-722. If a given
   `(type_label, method)` key errors on *every* sampled instance across
   *every* run, `e["example"]` never gets set past its `None` initializer
   (line 700), so the emitted JSON record's `"example"` field is `None`.

2. In `build_probe_inventory.py`'s merge step
   (`_merge_entries_for_domain`, line 294-296):
   ```python
   candidate = rec.get("example")                      # None
   if _example_score(candidate) > _example_score(agg["example"]):
       agg["example"] = candidate
   ```
   `_example_score(None)` returns `-1` (line 74-75). The aggregate
   `agg["example"]` also starts at `None` (`_new_agg()`, line 143) →
   score `-1`. `-1 > -1` is `False`, so `agg["example"]` **never gets
   updated away from `None`**, no matter how many runs or how many
   instances are merged.

3. In `write_csv` (line 399-430): `_fmt_example(None)` (line 393-396)
   returns `{"q": None, "storage": None, "raw": None, "display": None,
   "norm": None}`. `csv.DictWriter` (stdlib) serializes Python `None` as
   an **empty string** in the output file — there is no explicit
   `q="empty"`/`q="never_succeeded"` state written anywhere; it's a
   side effect of `None` round-tripping through `csv`.

This is a genuinely distinct fourth state, structurally: it requires
`ok_count == 0` across every sample ever merged (100% exception rate),
which is different from `q="unsupported"` (requires at least one
*successful* invocation whose return value `_reflect_contract` couldn't
classify) and different from `q="ok"`/`"<method not invoked>"` (see the
sentinel-round-trip note below).

**Confirmed against the raw JSON** (`probes_2025_20260804T181200-65103e.json`,
the only local capture), not just the CSV: every domain row for these
three methods shows `ok_count: 0` with a nonzero `error_count` (52/52,
100/100, 18/18 for `View.GetCalloutParentId`; 1 to 134 samples,
all-error, across 20 domains for `*.GetExternalFileReference`; 52/52 for
`LinePatternElement.GetLinePattern`) — no partial successes anywhere. By
contrast, `View.GetModelToProjectionTransforms` (also allowlisted, not one
of the task's 3, but instructive) shows a **mix**: `unsupported` in two
domains (some views succeed, returning an unhandled `Transform` array —
Problem 1) and `''` in a third (that domain's sampled views all threw) —
confirming the blank state is a data-dependent artifact of "0 successes in
this particular sample," not an inherent property of any one method.

**Shared or distinct across the 3 methods? Both, at different layers —
and this is the important nuance the task asked for:**

- **Mechanically, all 3 share the identical cause**: the discarded-`err`
  + strict-`>` comparison path above. Any allowlisted method that happens
  to fail on 100% of sampled instances will land here, regardless of why
  it fails.
- **At the Revit-API layer, only 2 of the 3 have been independently
  confirmed** — and not by this pass; by `cba1765`'s live re-run, which
  explicitly enumerates **"25 caught, documented exceptions"** as
  `GetCalloutParentId` (×3 domains), `GetExternalFileReference` (×21
  domains — I recomputed this from the raw JSON and it matches exactly:
  20 domains, `materials` contributing 2 rows), and
  `GetModelToProjectionTransforms` (×1) — each stated to match a
  documented `InvalidOperationException` precondition on that method's own
  RevitAPI doc page (e.g. `GetCalloutParentId` throwing unless the view is
  actually a callout view; most sampled views aren't). **`GetLinePattern`
  is not in that enumeration.** I confirmed this arithmetically: 3 + 21 +
  1 = 25, which is exactly the "25 caught" total `cba1765` reports, with
  no room left for `GetLinePattern`'s 1 row — so this isn't a rounding
  gap, `GetLinePattern` was simply never checked against its RevitAPI doc
  page the way the other three were. Its 100%-failure (0/52, one domain,
  `line_patterns`) is structurally identical in the tooling but
  **unconfirmed** as a legitimate precondition versus a real bug (wrong
  declaring-class assumption, wrong overload, anything in the same family
  as the `GetValidTypes` binder issue in Problem 2). Notably,
  `LinePatternElement.GetLinePattern()` is a very plausible real API call
  (unlike `GetValidTypes`, which had a documented "Overload" sibling
  warning) — so a binder-rejection explanation is less likely here, but I
  cannot rule it in or out without either the discarded exception text
  (not recoverable from any artifact on disk) or a live Revit session
  (out of scope for this pass).

**One more distinct, related bug worth flagging under this problem's
umbrella, found while tracing Step 0 item 4 (the "missing invocation"
check) against the live data rather than assumed:** a **non-allowlisted**
method does *not* end up with `example_q == ''`/`"missing"` — it ends up
`q == "ok"`, `raw == "<method not invoked>"`. This is because
`_reflect_try_get`'s "not on the allowlist" branch
(`probe_identity.py:643-651`) returns `(True, "<method not invoked>",
None)` — `ok=True` — and `_reflect_contract` then classifies the literal
placeholder *string* through its normal `str` branch (line 672-673) as a
genuine `q="ok"` value, indistinguishable from real string data. This is
exactly what `cba1765`'s commit message calls out as a separate,
previously-reported (not fixed) gap in `build_probe_inventory.py`'s merge
heuristic: `_example_score` scores this fake "ok" string at the same 135
points as a real invoked value, so a placeholder from an old run can
outscore (or at least tie and thus survive via first-processed-wins) a
genuinely invoked value from a newer run if exports are ever merged
carelessly. I re-confirmed this directly against the current on-disk CSV
(`refl.*.GetValidTypes` rows all show `example_q='ok'`,
`example_raw='<method not invoked>'`) — this is *expected*/correct given
`GetValidTypes` is currently unallowlisted (Problem 2), not a bug in
itself, but it means the `q=='ok'` state is not proof of a real value, and
anyone consuming this CSV needs to check `example_raw` for the sentinel
string, not just `example_q`, before trusting a row.

---

## Recommendation on PR splitting

**Split into two follow-on PRs, not one — the root causes don't share a
fix shape, and bundling them obscures review of the one that's actually
low-risk:**

1. **PR A — `_reflect_contract` ElementId-collection support (Problem 1a
   only).** Small, mechanical, identical 24-file patch (same shape as
   `7215638`): add one branch to iterate `IList/ICollection/ISet<ElementId>`
   into a list of ints. This is the change that actually matters for the
   crosswalk per the task's own framing. Do **not** bundle in complex-object
   serialization (1b) — that's a separate, unscoped design decision, not
   ready for this PR.

2. **PR B — reflection-sweep diagnostics.** Two related, low-risk additions,
   naturally paired because they're both "the sweep is discarding forensic
   detail it should keep": (i) capture and surface at least one example
   exception message per 100%-failing method key (fixes the q=='' opacity
   in Problem 3 going forward — without this, `GetLinePattern` can't be
   independently confirmed as precondition-driven vs. a real bug without a
   live Revit session), and (ii) stop `_reflect_contract` from
   classifying the `"<method not invoked>"` sentinel as a genuine
   `q="ok"` string (e.g. special-case it to a distinct `q` value before it
   ever reaches the contract function, or check for the sentinel by
   identity rather than routing it through the generic string branch).

**Problem 2 needs no further code change in either PR** — it's already
resolved on `main` by `bad4983`, one commit before this task's premise was
written. If it resurfaces, it belongs in a separate, explicitly-scoped PR
that decides whether to support non-zero-arg overloads at all (a real
allowlist-invariant design decision), not a mechanical fix alongside
Problems 1 or 3.

Do not combine PR A and PR B: PR A changes what value crosswalk tooling
can see (a real behavior change to review carefully against the
"complex-object serialization is deferred" scope boundary); PR B only
improves diagnosability of failures that already produce no usable data
today, with no crosswalk-visible behavior change. Reviewing them together
risks the scope-creep the original task explicitly warned against
("don't assume did share one root cause and fix them together without
confirming that first") — they don't, so don't.
