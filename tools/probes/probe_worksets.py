# -*- coding: utf-8 -*-
"""
probe_worksets.py
Dynamo CPython3 — run inside Revit against a workshared project file.

Probes:
  1. Import availability (FilteredWorksetCollector, WorksetKind, WorksetTable)
  2. WorksetKind enum integer values
  3. doc.IsWorkshared gate
  4. FilteredWorksetCollector(doc).OfKind(WorksetKind.UserWorkset) — collector works?
  5. doc.GetWorksetTable() + GetActiveWorksetId()
  6. Per-record field surface: Name, Kind, Id, UniqueId, IsEditable, Owner
  7. WorksetKind integer for each kind constant (UserWorkset, View, FamilyWorkset, StandardWorkset)
  8. Collector counts by kind
  9. Active (default) workset name

Output: JSON findings dict via OUT.
"""

import json
import traceback

findings = {
    "probe": "worksets",
    "status": "ok",
    "imports": {},
    "doc_info": {},
    "workset_kind_ints": {},
    "collector_counts": {},
    "active_workset": {},
    "sample_records": [],
    "errors": [],
    "notes": [],
}

def _safe(v):
    try:
        return str(v)
    except Exception:
        return "<error>"

try:
    from RevitServices.Persistence import DocumentManager
    doc = DocumentManager.Instance.CurrentDBDocument

    # ── 1. Imports ──────────────────────────────────────────────────────────
    try:
        from Autodesk.Revit.DB import FilteredWorksetCollector
        findings["imports"]["FilteredWorksetCollector"] = "ok"
    except Exception as e:
        findings["imports"]["FilteredWorksetCollector"] = "FAILED: " + str(e)
        FilteredWorksetCollector = None

    try:
        from Autodesk.Revit.DB import WorksetKind
        findings["imports"]["WorksetKind"] = "ok"
    except Exception as e:
        findings["imports"]["WorksetKind"] = "FAILED: " + str(e)
        WorksetKind = None

    # WorksetTable comes from GetWorksetTable(), no direct import needed
    findings["imports"]["WorksetTable"] = "via doc.GetWorksetTable() — no direct import"

    # ── 2. WorksetKind enum integer values ──────────────────────────────────
    if WorksetKind is not None:
        for kind_name in ("UserWorkset", "View", "FamilyWorkset", "StandardWorkset"):
            try:
                attr = getattr(WorksetKind, kind_name, None)
                findings["workset_kind_ints"][kind_name] = int(str(attr)) if attr is not None else "NOT_FOUND"
            except Exception as e:
                findings["workset_kind_ints"][kind_name] = "ERROR: " + str(e)
    else:
        findings["workset_kind_ints"]["note"] = "WorksetKind import failed"

    # ── 3. doc.IsWorkshared ─────────────────────────────────────────────────
    try:
        is_ws = doc.IsWorkshared
        findings["doc_info"]["IsWorkshared"] = bool(is_ws)
        findings["doc_info"]["IsWorkshared_type"] = type(is_ws).__name__
    except Exception as e:
        findings["doc_info"]["IsWorkshared"] = "ERROR: " + str(e)
        findings["errors"].append("IsWorkshared: " + str(e))

    # ── 4–8. Collector (only if workshared) ──────────────────────────────────
    if findings["doc_info"].get("IsWorkshared") is True and FilteredWorksetCollector is not None and WorksetKind is not None:

        # Count by each kind
        for kind_name in ("UserWorkset", "View", "FamilyWorkset", "StandardWorkset"):
            try:
                kind_attr = getattr(WorksetKind, kind_name, None)
                if kind_attr is None:
                    findings["collector_counts"][kind_name] = "KIND_NOT_FOUND"
                    continue
                col = list(FilteredWorksetCollector(doc).OfKind(kind_attr))
                findings["collector_counts"][kind_name] = len(col)
            except Exception as e:
                findings["collector_counts"][kind_name] = "ERROR: " + str(e)
                findings["errors"].append("collector_counts[{}]: {}".format(kind_name, str(e)))

        # Active workset via WorksetTable
        try:
            wt = doc.GetWorksetTable()
            findings["active_workset"]["GetWorksetTable_ok"] = True

            try:
                active_id = wt.GetActiveWorksetId()
                findings["active_workset"]["GetActiveWorksetId_ok"] = True
                findings["active_workset"]["active_id_int"] = _safe(getattr(active_id, "IntegerValue", None))
            except Exception as e:
                findings["active_workset"]["GetActiveWorksetId_ok"] = False
                findings["active_workset"]["GetActiveWorksetId_error"] = str(e)
                active_id = None

        except Exception as e:
            findings["active_workset"]["GetWorksetTable_ok"] = False
            findings["active_workset"]["error"] = str(e)
            wt = None
            active_id = None

        # Sample UserWorkset records — field surface
        try:
            user_col = list(FilteredWorksetCollector(doc).OfKind(WorksetKind.UserWorkset))
            sample = []
            for ws in user_col[:8]:
                rec = {}

                # Name
                try:
                    rec["Name"] = _safe(getattr(ws, "Name", None))
                except Exception as e:
                    rec["Name"] = "ERROR: " + str(e)

                # Kind (enum — int conversion)
                try:
                    kind_raw = getattr(ws, "Kind", None)
                    rec["Kind"] = _safe(kind_raw)
                    rec["Kind_int"] = int(str(kind_raw)) if kind_raw is not None else None
                except Exception as e:
                    rec["Kind"] = "ERROR: " + str(e)

                # Id
                try:
                    ws_id = getattr(ws, "Id", None)
                    rec["Id_IntegerValue"] = int(getattr(ws_id, "IntegerValue", -1))
                except Exception as e:
                    rec["Id_IntegerValue"] = "ERROR: " + str(e)

                # UniqueId
                try:
                    rec["UniqueId"] = _safe(getattr(ws, "UniqueId", None))
                except Exception as e:
                    rec["UniqueId"] = "ERROR: " + str(e)

                # IsEditable
                try:
                    rec["IsEditable"] = bool(getattr(ws, "IsEditable", None))
                except Exception as e:
                    rec["IsEditable"] = "ERROR: " + str(e)

                # Owner (who has it checked out — session state)
                try:
                    rec["Owner"] = _safe(getattr(ws, "Owner", None))
                except Exception as e:
                    rec["Owner"] = "ERROR: " + str(e)

                # IsDefaultWorkset (probe whether attribute exists)
                try:
                    val = getattr(ws, "IsDefaultWorkset", "ATTR_NOT_FOUND")
                    rec["IsDefaultWorkset"] = _safe(val) if val != "ATTR_NOT_FOUND" else "ATTR_NOT_FOUND"
                except Exception as e:
                    rec["IsDefaultWorkset"] = "ERROR: " + str(e)

                # Cross-check IsDefault against GetActiveWorksetId
                if active_id is not None:
                    try:
                        rec["is_active_workset"] = (ws.Id == active_id)
                    except Exception as e:
                        rec["is_active_workset"] = "ERROR: " + str(e)

                sample.append(rec)

            findings["sample_records"] = sample

            # Identify active workset name
            if active_id is not None:
                for ws in user_col:
                    try:
                        if ws.Id == active_id:
                            findings["active_workset"]["active_workset_name"] = _safe(getattr(ws, "Name", None))
                            break
                    except Exception:
                        pass

        except Exception as e:
            findings["errors"].append("sample_records: " + str(e))
            findings["notes"].append(traceback.format_exc())

    elif findings["doc_info"].get("IsWorkshared") is False:
        findings["notes"].append("doc.IsWorkshared = False — collector probes skipped (expected for templates/families)")
    else:
        findings["notes"].append("Collector probes skipped — import failure or IsWorkshared error")

except Exception as e:
    findings["status"] = "failed"
    findings["errors"].append("top_level: " + str(e))
    findings["traceback"] = traceback.format_exc()

OUT = json.dumps(findings, indent=2, sort_keys=True)