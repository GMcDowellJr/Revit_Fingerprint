import json
import traceback

# probe_browser_organization_v8
# Run against the TEMPLATE file (not a project file).
# The template should have "Discipline - View Classification" active.
#
# Hypotheses to test:
#   1. BrowserOrganization.Id is the tree root seed, not the grouping parameter id
#      (the observed 4261/2617 values are ParameterElement ids).
#   2. Positive FolderItemInfo.ElementIds != org.Id are shared param definition elements
#   3. doc.GetElement(positive_eid) resolves to a named shared parameter element
#   4. Multi-level grouping: GetFolderItems(shared_param_eid) returns next level's items
#   5. Filter tab data lives somewhere in the org's parameter set
#   6. FolderItemInfo.Name is not reliable enough for canonical names; compare it
#      against RevitLookup-style sources (Element.Name, Definition.Name, and
#      LabelUtils.GetLabelFor for built-ins).
#   7. Repeated FolderItemInfo.ElementId values during recursion are cycle
#      references to the current grouping parameter and must not become children.
#
# Extraction model:
#   FolderItemInfo.ElementId < 0  → BIP (built-in parameter)
#   FolderItemInfo.ElementId == current tree seed or org.Id → cycle/self-reference, skip
#   FolderItemInfo.ElementId > 0, != org.Id → shared parameter element

findings = {
    "probe": "browser_organization_v8",
    "status": "ok",
    "imports": {},
    "doc_is_family": None,
    "org_ids": {},
    "folder_trees": {},
    "filter_probe": {},
    "name_probe": {
        "reference": "reference/revit_lookup uses Element.Name for elements, Parameter.Definition.Name for parameters, and Definition.Name/BuiltInParameter.ToString() for definitions.",
        "folder_item_type_names": [],
        "folder_item_member_samples": {},
        "conclusions": [],
        "warning_counts": {},
    },
    "sort_by_probe": {},
    "errors": [],
    "notes": [],
}

def _safe(v):
    try:
        return str(v)
    except Exception:
        return "<error>"

def _int_enum(val):
    try:
        return int(str(val))
    except Exception:
        return None

def _append_unique(seq, value):
    if value not in seq:
        seq.append(value)

def _increment_count(mapping, key):
    mapping[key] = mapping.get(key, 0) + 1

def _clean_name(value):
    text = _safe(value)
    if text in ("", "None", "<error>", "???"):
        return None
    return text

def _try_get_definition_record(elem):
    """Return RevitLookup-style definition name/GUID data for ParameterElement-like values."""
    rec = {}
    try:
        get_definition = getattr(elem, "GetDefinition", None)
        if get_definition and callable(get_definition):
            definition = get_definition()
            if definition is not None:
                rec["definition_type"] = type(definition).__name__
                definition_name = _clean_name(getattr(definition, "Name", None))
                if definition_name:
                    rec["definition_name"] = definition_name
                try:
                    bip = getattr(definition, "BuiltInParameter", None)
                    if bip is not None:
                        rec["definition_built_in_parameter"] = _safe(bip)
                except Exception:
                    pass
                try:
                    guid = getattr(definition, "GUID", None)
                    if guid:
                        rec["guid"] = _safe(guid)
                        rec["is_shared_param"] = True
                except Exception:
                    pass
    except Exception as e:
        rec["definition_error"] = str(e)
    return rec

def _builtin_label(bip_value):
    """Resolve a built-in parameter label when Revit exposes one."""
    try:
        from Autodesk.Revit.DB import BuiltInParameter, LabelUtils
        bip = System.Enum.ToObject(BuiltInParameter, bip_value)
        label = _clean_name(LabelUtils.GetLabelFor(bip))
        return label
    except Exception as e:
        return None

def _best_name(rec, item_name=None):
    """Pick the most stable display name using the same priority as RevitLookup descriptors."""
    candidates = []
    if item_name:
        candidates.append(("folder_item_name", item_name))
    if rec.get("definition_name"):
        candidates.append(("definition_name", rec["definition_name"]))
    if rec.get("element_name"):
        candidates.append(("element_name", rec["element_name"]))
    if rec.get("bip_label"):
        candidates.append(("bip_label", rec["bip_label"]))
    if rec.get("bip_name"):
        candidates.append(("bip_name", rec["bip_name"]))

    rec["name_candidates"] = [
        {"source": source, "value": value}
        for source, value in candidates
        if _clean_name(value)
    ]
    for candidate in rec["name_candidates"]:
        source = candidate["source"]
        value = candidate["value"]
        # FolderItemInfo.Name has been observed as "???"; prefer actual API
        # descriptors over it unless it is the only usable value.
        if source != "folder_item_name":
            rec["display_name"] = value
            rec["display_name_source"] = source
            return
    if rec["name_candidates"]:
        rec["display_name"] = rec["name_candidates"][0]["value"]
        rec["display_name_source"] = rec["name_candidates"][0]["source"]

def _record_name_warning(eid_int, source):
    key = "FolderItemInfo.Name returned ??? for {}; using {} instead.".format(
        eid_int, source
    )
    _increment_count(findings["name_probe"]["warning_counts"], key)

def _resolve_folder_item(item_eid_int, org_id_int, current_seed_eid_int, doc, bip_lookup):
    """Classify and resolve a FolderItemInfo.ElementId."""
    rec = {"eid_int": item_eid_int}
    if item_eid_int == current_seed_eid_int:
        rec["kind"] = "cycle_reference"
        rec["skip"] = True
    elif item_eid_int == org_id_int:
        rec["kind"] = "self_reference"
        rec["skip"] = True
    elif item_eid_int < 0:
        rec["kind"] = "builtin_parameter"
        rec["bip_int"] = item_eid_int
        rec["bip_name"] = bip_lookup.get(item_eid_int, "UNKNOWN")
        rec["bip_label"] = _builtin_label(item_eid_int)
    else:
        rec["kind"] = "element"
        rec["skip"] = False
        try:
            from Autodesk.Revit.DB import ElementId
            elem = doc.GetElement(ElementId(item_eid_int))
            if elem is not None:
                rec["element_type"] = type(elem).__name__
                rec["element_name"] = _safe(getattr(elem, "Name", None))
                rec.update(_try_get_definition_record(elem))
                # Try direct GUID attribute (InternalDefinition)
                try:
                    guid = getattr(elem, "GUID", None)
                    if guid:
                        rec["guid"] = _safe(guid)
                        rec["is_shared_param"] = True
                except Exception:
                    pass
            else:
                rec["element_type"] = "null"
        except Exception as e:
            rec["element_resolve_error"] = str(e)
    return rec

def _record_folder_item_shape(item):
    """Capture limited reflection data so the next probe can target name APIs precisely."""
    try:
        type_name = "{}.{}".format(type(item).__module__, type(item).__name__)
        _append_unique(findings["name_probe"]["folder_item_type_names"], type_name)
    except Exception:
        type_name = "<unknown>"

    if type_name in findings["name_probe"]["folder_item_member_samples"]:
        return

    sample = {"python_dir_name_members": []}
    try:
        sample["python_dir_name_members"] = sorted(
            [name for name in dir(item) if "name" in name.lower()]
        )[:25]
    except Exception:
        pass
    try:
        import clr
        clr_type = clr.GetClrType(type(item))
        sample["clr_properties"] = sorted(set(p.Name for p in clr_type.GetProperties()))[:80]
        sample["clr_methods_name_related"] = sorted(
            set(m.Name for m in clr_type.GetMethods() if "name" in m.Name.lower())
        )[:80]
    except Exception as e:
        sample["clr_reflect_error"] = str(e)
    findings["name_probe"]["folder_item_member_samples"][type_name] = sample


def _walk_tree(org, org_id_int, doc, bip_lookup, seed_eid_int, depth=0, visited=None):
    """Walk the folder tree, skipping self-references and visited nodes."""
    if visited is None:
        visited = set()
    if depth > 4 or seed_eid_int in visited:
        return {"note": "max_depth_or_visited", "eid": seed_eid_int}
    visited.add(seed_eid_int)

    try:
        from Autodesk.Revit.DB import ElementId
        items = list(org.GetFolderItems(ElementId(seed_eid_int)))
    except Exception as e:
        return {"error": str(e), "eid": seed_eid_int}

    result = {"eid": seed_eid_int, "items": []}
    for item in items[:6]:
        _record_folder_item_shape(item)
        try:
            eid_int = item.ElementId.IntegerValue
        except Exception:
            continue
        resolved = _resolve_folder_item(eid_int, org_id_int, seed_eid_int, doc, bip_lookup)
        item_name = _safe(item.Name)
        resolved["folder_item_name"] = item_name
        _best_name(resolved, item_name)
        if item_name == "???" and resolved.get("display_name"):
            _record_name_warning(eid_int, resolved.get("display_name_source"))

        # Recurse into non-self, non-BIP, non-visited items
        if not resolved.get("skip") and resolved.get("kind") == "element" and eid_int not in visited:
            resolved["subtree"] = _walk_tree(
                org, org_id_int, doc, bip_lookup, eid_int, depth + 1, visited
            )
        result["items"].append(resolved)

    return result


try:
    from RevitServices.Persistence import DocumentManager
    doc = DocumentManager.Instance.CurrentDBDocument

    # Check if this is a family doc (shouldn't be, but good to know)
    try:
        findings["doc_is_family"] = bool(doc.IsFamilyDocument)
    except Exception:
        pass

    BrowserOrganization = None
    try:
        from Autodesk.Revit.DB import BrowserOrganization
        findings["imports"]["BrowserOrganization"] = "ok"
    except Exception as e:
        findings["imports"]["BrowserOrganization"] = "FAILED: " + str(e)
        raise RuntimeError("BrowserOrganization import failed")

    from Autodesk.Revit.DB import ElementId, BuiltInParameter
    try:
        import System
        findings["imports"]["System"] = "ok"
    except Exception as e:
        findings["imports"]["System"] = "FAILED: " + str(e)

    # Build a compact BIP reverse lookup from dir() — keyed by int
    # (In the real extractor, use bip_lookup.json instead)
    bip_lookup = {}
    try:
        for name in dir(BuiltInParameter):
            try:
                attr = getattr(BuiltInParameter, name, None)
                if attr is not None:
                    iv = int(str(attr))
                    if iv < 0:
                        bip_lookup[iv] = name
            except Exception:
                pass
        findings["notes"].append("bip_lookup_size: {}".format(len(bip_lookup)))
    except Exception as e:
        findings["errors"].append("bip_lookup_build: " + str(e))

    # Get the three active orgs
    org_map = {
        "views":     BrowserOrganization.GetCurrentBrowserOrganizationForViews(doc),
        "sheets":    BrowserOrganization.GetCurrentBrowserOrganizationForSheets(doc),
        "schedules": BrowserOrganization.GetCurrentBrowserOrganizationForSchedules(doc),
    }

    # ── Per-org: record org.Id, walk tree, probe sort and filter ────────────
    for cat, org in org_map.items():
        org_id_int = None
        try:
            org_id_int = org.Id.IntegerValue
            findings["org_ids"][cat] = org_id_int
        except Exception as e:
            findings["errors"].append("org_id_{}: {}".format(cat, str(e)))
            continue

        # Sort by (SortingParameterId)
        try:
            sp_int = org.SortingParameterId.IntegerValue
            sp_name = bip_lookup.get(sp_int, None)
            so_int = _int_enum(org.SortingOrder)
            findings["sort_by_probe"][cat] = {
                "SortingParameterId_int": sp_int,
                "SortingParameterId_bip_name": sp_name,
                "SortingOrder_int": so_int,
            }
            # If positive, resolve as element
            if sp_int > 0:
                elem = doc.GetElement(ElementId(sp_int))
                if elem is not None:
                    findings["sort_by_probe"][cat]["SortingParameterId_element_name"] = _safe(
                        getattr(elem, "Name", None)
                    )
        except Exception as e:
            findings["sort_by_probe"][cat] = {"error": str(e)}

        # Walk the folder tree from org.Id as root
        tree = _walk_tree(org, org_id_int, doc, bip_lookup, org_id_int)
        findings["folder_trees"][cat] = tree

    # ── Filter probe: try to find filter configuration ───────────────────────
    # The Filter tab in the UI stores filter conditions somewhere.
    # Try: org.GetParameters("Filter"), look at the "Filter" param's stored value,
    # try reflection for any filter-related methods not yet found.
    views_org = org_map["views"]
    try:
        import clr
        clr_type = clr.GetClrType(type(views_org))
        all_methods = sorted(set(m.Name for m in clr_type.GetMethods()))
        filter_methods = [m for m in all_methods if "filter" in m.lower() or "Filter" in m]
        findings["filter_probe"]["filter_related_methods"] = filter_methods
    except Exception as e:
        findings["filter_probe"]["reflect_error"] = str(e)

    # Try GetParameters("Filter") — does it have a stored value when filter is configured?
    try:
        fp = list(views_org.GetParameters("Filter"))
        if fp:
            p = fp[0]
            findings["filter_probe"]["Filter_param"] = {
                "HasValue": bool(p.HasValue),
                "StorageType_int": _int_enum(p.StorageType),
            }
            if p.HasValue:
                st = _int_enum(p.StorageType)
                if st == 3:
                    findings["filter_probe"]["Filter_param"]["AsString"] = _safe(p.AsString())
                elif st == 1:
                    findings["filter_probe"]["Filter_param"]["AsInteger"] = p.AsInteger()
    except Exception as e:
        findings["filter_probe"]["Filter_param_error"] = str(e)

    if findings["name_probe"]["folder_item_member_samples"]:
        findings["name_probe"]["conclusions"].append(
            "FolderItemInfo exposes only ElementId and Name; Name returned ??? in the observed template, so canonical names must be resolved from ElementId."
        )
    if findings["name_probe"]["warning_counts"]:
        findings["name_probe"]["conclusions"].append(
            "Name fallback is expected: built-ins resolve through LabelUtils.GetLabelFor/BuiltInParameter, and positive ids resolve through ParameterElement.GetDefinition().Name."
        )

except Exception as e:
    findings["status"] = "failed"
    findings["errors"].append("top_level: " + str(e))
    findings["traceback"] = traceback.format_exc()

OUT = json.dumps(findings, indent=2, sort_keys=True)
