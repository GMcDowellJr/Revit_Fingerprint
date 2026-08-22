"""White-box test for the >MAX_TEXT_READ_BYTES surfacing fix.

Writing an actual 30MB+ fixture file just to exercise this path would be
slow and wasteful, so this test imports rc_scan directly and monkeypatches
its read-size cap down to something tiny instead of going through the CLI.
"""
from conftest import write_files

import rc_scan


def test_oversized_text_file_is_surfaced_not_silently_skipped(repo, out, monkeypatch):
    monkeypatch.setattr(rc_scan, "MAX_TEXT_READ_BYTES", 50)
    content = "line one\n" * 20  # comfortably over the 50-byte cap
    write_files(repo, {"big_for_cap_test.txt": content})

    options = rc_scan.ScanOptions(root=repo, output_dir=out)
    result = rc_scan.scan_repository(options)

    row = next(f for f in result.files if f.relative_path == "big_for_cap_test.txt")
    assert row.included is True
    assert row.parse_status == "skipped_too_large"
    # Line count is still obtained via a streaming count, without holding
    # the whole file in memory.
    assert row.line_count == 20
    assert row.chunked is False
    assert not any(c.source_relative_path == "big_for_cap_test.txt" for c in result.chunks)
