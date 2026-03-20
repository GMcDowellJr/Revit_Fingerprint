from pathlib import Path

from domains import view_templates


def test_view_instances_cache_key_consistency():
    expected = view_templates._VIEW_INSTANCES_CACHE_KEY
    assert expected == "view_instances:View:all"

    vt_text = Path("domains/view_templates.py").read_text(encoding="utf-8")
    vco_text = Path("domains/view_category_overrides.py").read_text(encoding="utf-8")
    vfa_text = Path("domains/view_filter_applications_view_templates.py").read_text(encoding="utf-8")

    assert "cache_key=_VIEW_INSTANCES_CACHE_KEY" in vt_text
    assert "cache_key=_VIEW_INSTANCES_CACHE_KEY" in vco_text
    assert "cache_key=_VIEW_INSTANCES_CACHE_KEY" in vfa_text
    assert 'from domains.view_templates import _VIEW_INSTANCES_CACHE_KEY' in vco_text
    assert 'from domains.view_templates import _VIEW_INSTANCES_CACHE_KEY' in vfa_text
