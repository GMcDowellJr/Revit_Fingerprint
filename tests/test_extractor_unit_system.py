from tools.extractor import _derive_unit_system


def _length_record(unit_type_id, status="ok"):
    items = [{"k": "units.spec", "v": "length"}]
    if unit_type_id is not None:
        items.append({"k": "units.unit_type_id", "v": unit_type_id})
    return {"status": status, "identity_basis": {"items": items}}


def _payload(records):
    return {"units": {"records": records}}


def test_accepts_plural_meters():
    payload = _payload([_length_record("autodesk.unit.unit:meters-1.0.0")])
    assert _derive_unit_system(payload, "run") == "Metric"


def test_continues_after_unrecognized_or_missing_unit_type_id():
    payload = _payload([
        _length_record("autodesk.unit.unit:parsecs-1.0.0"),
        _length_record(None),
        _length_record("autodesk.unit.unit:feetFractionalInches-1.0.1"),
    ])
    assert _derive_unit_system(payload, "run") == "Imperial"


def test_accepts_degraded_records():
    payload = _payload([_length_record("autodesk.unit.unit:millimeters-1.0.0", status="degraded")])
    assert _derive_unit_system(payload, "run") == "Metric"


def test_broader_length_unit_matching():
    metric = _payload([_length_record("autodesk.unit.unit:centimeters-1.0.0")])
    imperial = _payload([_length_record("autodesk.unit.unit:inches-1.0.0")])
    assert _derive_unit_system(metric, "run") == "Metric"
    assert _derive_unit_system(imperial, "run") == "Imperial"
