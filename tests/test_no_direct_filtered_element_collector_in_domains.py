# tests/test_no_direct_filtered_element_collector_in_domains.py
import glob
import os


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def test_domains_do_not_reference_filtered_element_collector():
    """
    PR5 policy:
    - Domains must not directly import or reference FilteredElementCollector.
    - They must use core.collect.* APIs instead.
    """
    root = _repo_root()
    domains_dir = os.path.join(root, "domains")
    paths = sorted(glob.glob(os.path.join(domains_dir, "*.py")))
    assert paths, "No domain files found under {}".format(domains_dir)

    offenders = []
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            txt = f.read()
        if "FilteredElementCollector" in txt:
            offenders.append(os.path.basename(p))

    assert not offenders, "Domains must not reference FilteredElementCollector directly:\n- {}".format(
        "\n- ".join(offenders)
    )
