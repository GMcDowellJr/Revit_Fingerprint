from core import naming


class _DummyPI:
    def __init__(self, unique_id=None, number=None, name=None):
        self.UniqueId = unique_id
        self.Number = number
        self.Name = name


class _DummyApp:
    def __init__(self, version_build="2026.0.1"):
        self.VersionBuild = version_build


class _DummyDoc:
    def __init__(self, *, title, path_name="", project_info=None, app=None):
        self.Title = title
        self.PathName = path_name
        self.ProjectInformation = project_info
        self.Application = app or _DummyApp()


def test_derive_doc_key_uses_unique_document_identity_when_uid_available():
    doc_a = _DummyDoc(
        title="Tower",
        path_name=r"C:\\proj\\tower.rvt",
        project_info=_DummyPI(unique_id="PI-UID-A"),
    )
    doc_b = _DummyDoc(
        title="Tower",
        path_name=r"C:\\proj\\tower.rvt",
        project_info=_DummyPI(unique_id="PI-UID-B"),
    )

    key_a = naming.derive_doc_key(doc_a)
    key_b = naming.derive_doc_key(doc_b)

    assert key_a["doc_identity_short"] != key_b["doc_identity_short"]
    assert key_a["key"] != key_b["key"]


def test_derive_doc_key_fallback_identity_not_tied_to_version_build():
    # Simulate two distinct docs with same title and same Revit build
    # but different paths and no ProjectInformation UID.
    doc_a = _DummyDoc(
        title="Campus",
        path_name=r"C:\\docs\\a\\campus.rvt",
        project_info=_DummyPI(unique_id=None),
        app=_DummyApp(version_build="2026.0.1"),
    )
    doc_b = _DummyDoc(
        title="Campus",
        path_name=r"C:\\docs\\b\\campus.rvt",
        project_info=_DummyPI(unique_id=None),
        app=_DummyApp(version_build="2026.0.1"),
    )

    key_a = naming.derive_doc_key(doc_a)
    key_b = naming.derive_doc_key(doc_b)

    assert key_a["doc_identity_short"] != key_b["doc_identity_short"]
    assert key_a["key"] != key_b["key"]
