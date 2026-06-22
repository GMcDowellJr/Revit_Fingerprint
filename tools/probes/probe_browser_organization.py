import json
import traceback

# probe_browser_organization_v6
# Run against the TEMPLATE file (not a project file).
# The template should have "Discipline - View Classification" active.
#
# Hypotheses to test:
#   1. org.Id.IntegerValue == 4261 (self-reference was the circularity cause)
#   2. Positive FolderItemInfo.ElementIds != org.Id are shared param definition elements
#   3. doc.GetElement(positive_eid) resolves to a named shared parameter element
#   4. Multi-level grouping: GetFolderItems(shared_param_eid) returns next level's items
#   5. Filter tab data lives somewhere in the org's parameter set
#
# Extraction model:
#   FolderItemInfo.ElementId < 0  → BIP (built-in parameter)
#   FolderItemInfo.ElementId == org.Id → self-reference, skip
#   FolderItemInfo.ElementId > 0, != org.Id → shared parameter element

findings = {
    "probe": "browser_organization_v6",
    "status": "ok",
    "imports": {},
    "doc_is_family": None,
    "org_ids": {},
    "folder_trees": {},
    "filter_probe": {},
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

def _resolve_folder_item(item_eid_int, org_id_int, doc, bip_lookup):
    """Classify and resolve a FolderItemInfo.ElementId."""
    rec = {"eid_int": item_eid_int}
    if item_eid_int == org_id_int:
        rec["kind"] = "self_reference"
        rec["skip"] = True
    elif item_eid_int < 0:
        rec["kind"] = "builtin_parameter"
        rec["bip_int"] = item_eid_int
        rec["bip_name"] = bip_lookup.get(item_eid_int, "UNKNOWN")
    else:
        rec["kind"] = "element"
        rec["skip"] = False
        try:
            from Autodesk.Revit.DB import ElementId
            elem = doc.GetElement(ElementId(item_eid_int))
            if elem is not None:
                rec["element_type"] = type(elem).__name__
                rec["element_name"] = _safe(getattr(elem, "Name", None))
                # Check if it has a GUID (shared parameter)
                try:
                    defn = getattr(elem, "GetDefinition", None)
                    if defn and callable(defn):
                        d = defn()
                        guid = getattr(d, "GUID", None)
                        if guid:
                            rec["is_shared_param"] = True
                            rec["guid"] = _safe(guid)
                except Exception:
                    pass
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
        try:
            eid_int = item.ElementId.IntegerValue
        except Exception:
            continue
        resolved = _resolve_folder_item(eid_int, org_id_int, doc, bip_lookup)
        resolved["name"] = _safe(item.Name)

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

except Exception as e:
    findings["status"] = "failed"
    findings["errors"].append("top_level: " + str(e))
    findings["traceback"] = traceback.format_exc()

OUT = json.dumps(findings, indent=2, sort_keys=True)