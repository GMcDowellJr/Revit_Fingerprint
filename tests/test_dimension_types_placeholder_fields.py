import domains.dimension_types as m


def test_attach_placeholder_metadata_ok():
    rec = {}
    m._attach_placeholder_metadata(rec, 42, {42: 3}, "ok")
    assert rec["instance_count"] == 3
    assert rec["instance_count_q"] == "ok"


def test_attach_placeholder_metadata_unreadable():
    rec = {}
    m._attach_placeholder_metadata(rec, None, {}, "unreadable")
    assert rec["instance_count"] is None
    assert rec["instance_count_q"] == "unreadable"
