# Probe export data

Captured Revit probe exports are intentionally not stored in this repository.
They can contain project, organization, user, and workstation identifiers.

Obtain an approved, access-controlled dataset before running the inventory tools
in `tools/probes/`. Generate derived inventories and crosswalk candidates locally
with `build_probe_inventory.py` and `find_crosswalk_candidates.py`; do not commit
those inputs or outputs from this directory.

Deleting files from the current tree does not remove them from Git history. Before
wider distribution, repository owners should separately assess whether history
rewriting and credential or identifier response procedures are required.
