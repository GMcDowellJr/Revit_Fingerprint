# Dynamo Python (Revit) — Temporary import probe: RoofType
#
# Checks whether RoofType (and the other compound-type additions) are
# importable from Autodesk.Revit.DB in the current Revit/Dynamo environment.
#
# Paste into a Dynamo Python Script node and run; read OUT for results.

results = []

for cls_name in ("RoofType", "FloorType", "CeilingType", "DeckEmbeddingType"):
    try:
        mod = __import__("Autodesk.Revit.DB", fromlist=[cls_name])
        cls = getattr(mod, cls_name, None)
        if cls is None:
            results.append("{}: attribute missing from Autodesk.Revit.DB".format(cls_name))
        else:
            results.append("{} OK".format(cls_name))
    except ImportError as e:
        results.append("{} failed: {}".format(cls_name, e))
    except Exception as e:
        results.append("{} error: {}".format(cls_name, e))

# Original one-liner requested, kept for direct copy-paste verification:
try:
    from Autodesk.Revit.DB import RoofType
    results.append("one-liner: RoofType OK")
except ImportError as e:
    results.append("one-liner: RoofType failed: {}".format(e))

OUT = results
