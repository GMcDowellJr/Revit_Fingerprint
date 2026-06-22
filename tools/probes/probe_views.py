# -*- coding: utf-8 -*-
"""
probe_views.py
Dynamo CPython3 — run inside Revit against a workshared project file.

Probes:
  1.  FilteredElementCollector(doc).OfClass(View) — collector works, total count
  2.  ViewType enum integer values (all members)
  3.  ViewDiscipline enum integer values (all members)
  4.  ViewDetailLevel enum integer values
  5.  View.IsFromLinkedFile — exists? callable? returns bool?
  6.  View.IsCallout — attribute access
  7.  View.IsDependent / GetPrimaryViewId() — dependent view detection
  8.  View.IsTemplate — template vs live view split
  9.  View.ViewTemplateId — exists, InvalidElementId (-1) for no template
  10. View.Scale — value type, schedules/sheets return?
  11. View.Discipline — enum, int conversion works?
  12. View.DetailLevel — enum, int conversion works?
  13. BuiltInParameter.VIEW_PHASE — get_Parameter works? returns ElementId?
  14. BuiltInParameter.VIEW_PHASE_FILTER — same
  15. ViewSchedule — IsTitleblockRevisionSchedule attribute
  16. View counts: by IsTemplate, by ViewType integer, linked vs local
  17. Sample records (non-template, 1 per ViewType bucket)

Output: JSON findings dict via OUT.
"""

import json
import traceback

findings = {
    "probe": "views",
    "status": "ok",
    "imports": {},
    "viewtype_ints": {},
    "viewdiscipline_ints": {},
    "viewdetaillevel_ints": {},
    "bip_probe": {},
    "isFromLinkedFile_probe": {},
    "counts": {},
    "schedule_probe": {},
    "sample_by_viewtype": {},
    "errors": [],
    "notes": [],
}

def _safe(v):
    try:
        return str(v)
    except Exception:
        return "<error>"

def _int_enum(val):
    """Convert a Revit enum value to int via str coercion (CPython3 pattern)."""
    try:
        return int(str(val))
    except Exception:
        return None

try:
    from RevitServices.Persistence import DocumentManager
    doc = DocumentManager.Instance.CurrentDBDocument

    # ── 1. Imports ──────────────────────────────────────────────────────────
    View = None
    ViewSchedule = None
    ViewSheet = None
    ViewType = None
    ViewDiscipline = None
    ViewDetailLevel = None
    BuiltInParameter = None
    FilteredElementCollector = None
    ElementId = None

    try:
        from Autodesk.Revit.DB import FilteredElementCollector, ElementId
        findings["imports"]["FilteredElementCollector"] = "ok"
    except Exception as e:
        findings["imports"]["FilteredElementCollector"] = "FAILED: " + str(e)

    try:
        from Autodesk.Revit.DB import View
        findings["imports"]["View"] = "ok"
    except Exception as e:
        findings["imports"]["View"] = "FAILED: " + str(e)

    try:
        from Autodesk.Revit.DB import ViewSchedule
        findings["imports"]["ViewSchedule"] = "ok"
    except Exception as e:
        findings["imports"]["ViewSchedule"] = "FAILED: " + str(e)

    try:
        from Autodesk.Revit.DB import ViewSheet
        findings["imports"]["ViewSheet"] = "ok"
    except Exception as e:
        findings["imports"]["ViewSheet"] = "FAILED: " + str(e)

    try:
        from Autodesk.Revit.DB import ViewType
        findings["imports"]["ViewType"] = "ok"
    except Exception as e:
        findings["imports"]["ViewType"] = "FAILED: " + str(e)

    try:
        from Autodesk.Revit.DB import ViewDiscipline
        findings["imports"]["ViewDiscipline"] = "ok"
    except Exception as e:
        findings["imports"]["ViewDiscipline"] = "FAILED: " + str(e)

    try:
        from Autodesk.Revit.DB import ViewDetailLevel
        findings["imports"]["ViewDetailLevel"] = "ok"
    except Exception as e:
        findings["imports"]["ViewDetailLevel"] = "FAILED: " + str(e)

    try:
        from Autodesk.Revit.DB import BuiltInParameter
        findings["imports"]["BuiltInParameter"] = "ok"
    except Exception as e:
        findings["imports"]["BuiltInParameter"] = "FAILED: " + str(e)

    # ── 2. ViewType integer values ──────────────────────────────────────────
    if ViewType is not None:
        vt_names = [
            "FloorPlan", "CeilingPlan", "Elevation", "ThreeD", "DrawingSheet",
            "DraftingView", "Legend", "EngineeringPlan", "AreaPlan",
            "Section", "Detail", "CostReport", "LoadsReport", "PresureLossReport",
            "ColumnSchedule", "PanelSchedule", "Schedule", "Walkthrough",
            "Rendering", "SystemBrowser", "ProjectBrowser",
        ]
        for name in vt_names:
            try:
                attr = getattr(ViewType, name, "NOT_FOUND")
                if attr == "NOT_FOUND":
                    findings["viewtype_ints"][name] = "NOT_FOUND"
                else:
                    findings["viewtype_ints"][name] = _int_enum(attr)
            except Exception as e:
                findings["viewtype_ints"][name] = "ERROR: " + str(e)
    else:
        findings["viewtype_ints"]["note"] = "ViewType import failed"

    # ── 3. ViewDiscipline integer values ────────────────────────────────────
    if ViewDiscipline is not None:
        vd_names = [
            "Undefined", "Architectural", "Structural", "Mechanical",
            "Electrical", "Plumbing", "Coordination",
        ]
        for name in vd_names:
            try:
                attr = getattr(ViewDiscipline, name, "NOT_FOUND")
                findings["viewdiscipline_ints"][name] = _int_enum(attr) if attr != "NOT_FOUND" else "NOT_FOUND"
            except Exception as e:
                findings["viewdiscipline_ints"][name] = "ERROR: " + str(e)
    else:
        findings["viewdiscipline_ints"]["note"] = "ViewDiscipline import failed"

    # ── 4. ViewDetailLevel integer values ───────────────────────────────────
    if ViewDetailLevel is not None:
        for name in ("Undefined", "Coarse", "Medium", "Fine"):
            try:
                attr = getattr(ViewDetailLevel, name, "NOT_FOUND")
                findings["viewdetaillevel_ints"][name] = _int_enum(attr) if attr != "NOT_FOUND" else "NOT_FOUND"
            except Exception as e:
                findings["viewdetaillevel_ints"][name] = "ERROR: " + str(e)
    else:
        findings["viewdetaillevel_ints"]["note"] = "ViewDetailLevel import failed"

    # ── 5–16. Collector probes ──────────────────────────────────────────────
    if FilteredElementCollector is not None and View is not None:

        try:
            col = list(FilteredElementCollector(doc).OfClass(View))
            findings["counts"]["total_view_elements"] = len(col)
        except Exception as e:
            findings["errors"].append("collector_ofclass_view: " + str(e))
            col = []

        # Partition by IsTemplate and ViewType
        template_count = 0
        live_count = 0
        linked_count = 0
        no_linked_attr = 0
        counts_by_vt = {}
        no_template_count = 0

        # One sample per viewtype bucket (non-template only)
        samples = {}

        invalid_eid = None
        if ElementId is not None:
            try:
                invalid_eid = ElementId.InvalidElementId
            except Exception:
                pass

        for v in col:
            # IsTemplate
            is_template = False
            try:
                is_template = bool(v.IsTemplate)
            except Exception:
                pass

            if is_template:
                template_count += 1
                continue
            live_count += 1

            # ViewType integer
            vt_int = None
            try:
                vt_int = _int_enum(v.ViewType)
            except Exception:
                pass
            vt_key = str(vt_int) if vt_int is not None else "unknown"
            counts_by_vt[vt_key] = counts_by_vt.get(vt_key, 0) + 1

            # IsFromLinkedFile probe (first time we encounter it)
            if "checked" not in findings["isFromLinkedFile_probe"]:
                findings["isFromLinkedFile_probe"]["checked"] = True
                try:
                    attr = getattr(v, "IsFromLinkedFile", "ATTR_NOT_FOUND")
                    if attr == "ATTR_NOT_FOUND":
                        findings["isFromLinkedFile_probe"]["attribute_exists"] = False
                        findings["isFromLinkedFile_probe"]["note"] = "attribute not found"
                    else:
                        findings["isFromLinkedFile_probe"]["attribute_exists"] = True
                        # Is it callable?
                        if callable(attr):
                            try:
                                result = attr()
                                findings["isFromLinkedFile_probe"]["callable"] = True
                                findings["isFromLinkedFile_probe"]["sample_result"] = bool(result)
                                findings["isFromLinkedFile_probe"]["result_type"] = type(result).__name__
                            except Exception as ce:
                                findings["isFromLinkedFile_probe"]["callable"] = True
                                findings["isFromLinkedFile_probe"]["call_error"] = str(ce)
                        else:
                            findings["isFromLinkedFile_probe"]["callable"] = False
                            findings["isFromLinkedFile_probe"]["value"] = _safe(attr)
                            findings["isFromLinkedFile_probe"]["result_type"] = type(attr).__name__
                except Exception as e:
                    findings["isFromLinkedFile_probe"]["error"] = str(e)

            # Count linked views
            try:
                is_linked_attr = getattr(v, "IsFromLinkedFile", None)
                if is_linked_attr is None:
                    no_linked_attr += 1
                elif callable(is_linked_attr):
                    if is_linked_attr():
                        linked_count += 1
                else:
                    if bool(is_linked_attr):
                        linked_count += 1
            except Exception:
                no_linked_attr += 1

            # Per-viewtype sample (first non-template per vt_key, up to 8 buckets)
            if vt_key not in samples and len(samples) < 10:
                rec = {
                    "ViewType_int": vt_int,
                    "Name": None,
                    "IsCallout": None,
                    "IsDependent": None,
                    "ViewTemplateId_int": None,
                    "has_template": None,
                    "Scale": None,
                    "Scale_type": None,
                    "Discipline_int": None,
                    "DetailLevel_int": None,
                    "phase_bip_ok": None,
                    "phase_value": None,
                    "phase_filter_bip_ok": None,
                    "phase_filter_value": None,
                    "is_schedule": False,
                    "is_sheet": False,
                    "IsFromLinkedFile_result": None,
                }

                # Name
                try:
                    rec["Name"] = _safe(getattr(v, "Name", None))
                except Exception:
                    pass

                # IsCallout
                try:
                    rec["IsCallout"] = bool(getattr(v, "IsCallout", None))
                except Exception as e:
                    rec["IsCallout"] = "ERROR: " + str(e)

                # IsDependent — via GetPrimaryViewId()
                try:
                    pv_id = v.GetPrimaryViewId()
                    if invalid_eid is not None:
                        rec["IsDependent"] = (pv_id != invalid_eid)
                    else:
                        rec["IsDependent"] = (getattr(pv_id, "IntegerValue", -1) != -1)
                    rec["GetPrimaryViewId_ok"] = True
                except Exception as e:
                    rec["IsDependent"] = "ERROR: " + str(e)
                    rec["GetPrimaryViewId_ok"] = False

                # ViewTemplateId
                try:
                    tmpl_id = v.ViewTemplateId
                    tmpl_int = getattr(tmpl_id, "IntegerValue", None)
                    rec["ViewTemplateId_int"] = int(tmpl_int) if tmpl_int is not None else None
                    rec["has_template"] = (tmpl_int is not None and int(tmpl_int) != -1)
                    if rec["has_template"]:
                        try:
                            tmpl_elem = doc.GetElement(tmpl_id)
                            rec["template_name"] = _safe(getattr(tmpl_elem, "Name", None))
                        except Exception:
                            pass
                except Exception as e:
                    rec["ViewTemplateId_int"] = "ERROR: " + str(e)

                # Scale
                try:
                    scale_raw = getattr(v, "Scale", "ATTR_NOT_FOUND")
                    if scale_raw == "ATTR_NOT_FOUND":
                        rec["Scale"] = "ATTR_NOT_FOUND"
                    else:
                        rec["Scale"] = _safe(scale_raw)
                        rec["Scale_type"] = type(scale_raw).__name__
                        try:
                            rec["Scale_int"] = int(scale_raw)
                        except Exception:
                            rec["Scale_int"] = "not_int"
                except Exception as e:
                    rec["Scale"] = "ERROR: " + str(e)

                # Discipline
                try:
                    disc_raw = getattr(v, "Discipline", None)
                    rec["Discipline_int"] = _int_enum(disc_raw)
                    rec["Discipline_raw"] = _safe(disc_raw)
                except Exception as e:
                    rec["Discipline_int"] = "ERROR: " + str(e)

                # DetailLevel
                try:
                    dl_raw = getattr(v, "DetailLevel", None)
                    rec["DetailLevel_int"] = _int_enum(dl_raw)
                    rec["DetailLevel_raw"] = _safe(dl_raw)
                except Exception as e:
                    rec["DetailLevel_int"] = "ERROR: " + str(e)

                # VIEW_PHASE BIP
                if BuiltInParameter is not None:
                    try:
                        phase_bip = getattr(BuiltInParameter, "VIEW_PHASE", None)
                        if phase_bip is None:
                            rec["phase_bip_ok"] = "BIP_NOT_FOUND"
                        else:
                            param = v.get_Parameter(phase_bip)
                            if param is None:
                                rec["phase_bip_ok"] = False
                                rec["phase_value"] = "param_is_None"
                            else:
                                rec["phase_bip_ok"] = True
                                try:
                                    eid = param.AsElementId()
                                    eid_int = getattr(eid, "IntegerValue", None)
                                    rec["phase_value"] = int(eid_int) if eid_int is not None else None
                                    if eid_int and int(eid_int) != -1:
                                        phase_elem = doc.GetElement(eid)
                                        rec["phase_name"] = _safe(getattr(phase_elem, "Name", None))
                                except Exception as pe:
                                    rec["phase_value"] = "AsElementId_error: " + str(pe)
                    except Exception as e:
                        rec["phase_bip_ok"] = "ERROR: " + str(e)

                    # VIEW_PHASE_FILTER BIP
                    try:
                        pf_bip = getattr(BuiltInParameter, "VIEW_PHASE_FILTER", None)
                        if pf_bip is None:
                            rec["phase_filter_bip_ok"] = "BIP_NOT_FOUND"
                        else:
                            param = v.get_Parameter(pf_bip)
                            if param is None:
                                rec["phase_filter_bip_ok"] = False
                                rec["phase_filter_value"] = "param_is_None"
                            else:
                                rec["phase_filter_bip_ok"] = True
                                try:
                                    eid = param.AsElementId()
                                    eid_int = getattr(eid, "IntegerValue", None)
                                    rec["phase_filter_value"] = int(eid_int) if eid_int is not None else None
                                    if eid_int and int(eid_int) != -1:
                                        pf_elem = doc.GetElement(eid)
                                        rec["phase_filter_name"] = _safe(getattr(pf_elem, "Name", None))
                                except Exception as pe:
                                    rec["phase_filter_value"] = "AsElementId_error: " + str(pe)
                    except Exception as e:
                        rec["phase_filter_bip_ok"] = "ERROR: " + str(e)

                # Schedule-specific
                if ViewSchedule is not None:
                    try:
                        if isinstance(v, ViewSchedule):
                            rec["is_schedule"] = True
                            try:
                                rec["IsTitleblockRevisionSchedule"] = bool(v.IsTitleblockRevisionSchedule)
                            except Exception as e:
                                rec["IsTitleblockRevisionSchedule"] = "ERROR: " + str(e)
                    except Exception:
                        pass

                # Sheet-specific
                if ViewSheet is not None:
                    try:
                        if isinstance(v, ViewSheet):
                            rec["is_sheet"] = True
                            try:
                                rec["SheetNumber"] = _safe(getattr(v, "SheetNumber", None))
                            except Exception as e:
                                rec["SheetNumber"] = "ERROR: " + str(e)
                    except Exception:
                        pass

                # No-template tracking
                if rec.get("has_template") is False:
                    no_template_count += 1

                samples[vt_key] = rec

        findings["counts"]["template_count"] = template_count
        findings["counts"]["live_count"] = live_count
        findings["counts"]["linked_count"] = linked_count
        findings["counts"]["no_linked_attr_count"] = no_linked_attr
        findings["counts"]["no_template_assigned_count"] = no_template_count
        findings["counts"]["by_viewtype_int"] = counts_by_vt
        findings["sample_by_viewtype"] = samples

        # Schedule-specific probe: IsTitleblockRevisionSchedule
        if ViewSchedule is not None:
            try:
                sched_col = [v for v in col if not v.IsTemplate]
                titleblock_sched_count = 0
                for v in sched_col:
                    try:
                        if isinstance(v, ViewSchedule) and v.IsTitleblockRevisionSchedule:
                            titleblock_sched_count += 1
                    except Exception:
                        pass
                findings["schedule_probe"]["IsTitleblockRevisionSchedule_count"] = titleblock_sched_count
                findings["schedule_probe"]["status"] = "ok"
            except Exception as e:
                findings["schedule_probe"]["status"] = "ERROR: " + str(e)

    else:
        findings["notes"].append("Collector probes skipped — View or FilteredElementCollector import failed")

except Exception as e:
    findings["status"] = "failed"
    findings["errors"].append("top_level: " + str(e))
    findings["traceback"] = traceback.format_exc()

OUT = json.dumps(findings, indent=2, sort_keys=True)