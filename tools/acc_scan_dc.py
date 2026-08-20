"""
acc_scan_dc.py — Desktop Connector / network folder scanner

Walks a root folder, finds Revit files, and writes a manifest CSV.
Open the CSV, set include=1 on the rows you want to fingerprint, then
run script.py (BatchExtract button).

Usage:
    python acc_scan_dc.py --root <path> --out <csv_path>
    python acc_scan_dc.py --root "./ACCDocs/ExampleEnterprise" --out "./acc_manifest.csv"
    python acc_scan_dc.py --root <path> --out <csv_path> --version
    python acc_scan_dc.py --root <path> --out <csv_path> --types rvt rfa
    python acc_scan_dc.py --root <path> --out <csv_path> --types all

Arguments:
    --root      Required. Root folder to scan.
    --out       Required. Output CSV path.
    --version   Optional. Read Revit version year from each file's OLE header.
                WARNING: Do NOT use on a Desktop Connector folder if files may
                be online-only stubs — reading any bytes triggers DC to queue
                a download.  Safe on a network share or after acc_sync_dc.py
                has fully hydrated all files.
    --types     Optional. Space-separated list of extensions to include.
                Choices: rvt rte rfa rft all  (default: rvt)
                "all" expands to rvt rte rfa rft.

Columns written:
    include          — set to 1 to include in extraction run (default 0)
    file_type        — rvt / rte / rfa / rft
    project_folder   — top-level folder directly under root
    subfolder        — path between project_folder and the file
    filename         — e.g. 23-1234_ContainerFile.rvt
    relative_path    — project_folder/subfolder/filename (unique key)
    full_path        — absolute local path
    size_mb          — file size rounded to 2 dp
    last_modified    — ISO timestamp from filesystem
    rvt_version      — Revit version year e.g. "2025";
                       empty string if --version not supplied;
                       "stub" if file too small to parse;
                       "unknown" if OLE header unreadable
"""

import os
import csv
import re
import argparse
from datetime import datetime

# ---------------------------------------------------------------------------
# Supported file types
# ---------------------------------------------------------------------------

ALL_TYPES = {"rvt", "rte", "rfa", "rft"}

# ---------------------------------------------------------------------------
# Revit version extraction
# ---------------------------------------------------------------------------
#
# .rvt/.rte/.rfa/.rft files are OLE Compound Documents.
# The "BasicFileInfo" stream contains a UTF-16LE block including a line:
#   Format: 2025\r\n
# We read up to 64KB and search for that pattern as raw bytes.
# Reading bytes on a Desktop Connector stub will trigger a download —
# the --version flag should only be used on fully-hydrated files.

_READ_BYTES = 65536   # 64 KB — covers BasicFileInfo in virtually all files
_STUB_SIZE  = 1 * 1024 * 1024  # files under 1 MB treated as stubs

# "Format: YYYY" in UTF-16LE
_VERSION_RE = re.compile(
    rb"F\x00o\x00r\x00m\x00a\x00t\x00:\x00 \x00"
    rb"(\d\x00\d\x00\d\x00\d\x00)"
)
# Fallback: "Revit YYYY" in UTF-16LE
_REVIT_RE = re.compile(
    rb"R\x00e\x00v\x00i\x00t\x00 \x00"
    rb"(\d\x00\d\x00\d\x00\d\x00)"
)

_OLE_MAGIC = b"\xd0\xcf\x11\xe0"


def read_rvt_version(path, size_bytes):
    """
    Return the Revit version year as a string (e.g. "2025"),
    "stub" if the file is too small, or "unknown" if unreadable.
    Only call this on fully-downloaded files.
    """
    if size_bytes < _STUB_SIZE:
        return "stub"

    try:
        with open(path, "rb") as f:
            data = f.read(_READ_BYTES)
    except OSError:
        return "unknown"

    if not data.startswith(_OLE_MAGIC):
        return "unknown"

    m = _VERSION_RE.search(data) or _REVIT_RE.search(data)
    if m:
        return m.group(1).decode("utf-16-le")

    return "unknown"


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def scan(root, extensions, include_version):
    """
    Walk root, yield one dict per matching file.
    Skips names starting with '~$' (Revit/Office temp locks).
    extensions: set of lowercase strings without dot, e.g. {"rvt", "rfa"}
    """
    root = os.path.abspath(root)

    for dirpath, dirnames, filenames in os.walk(root):
        # Skip hidden and Desktop Connector metadata folders
        dirnames[:] = [
            d for d in sorted(dirnames)
            if not d.startswith(".") and not d.startswith("$")
        ]

        for fn in sorted(filenames):
            if fn.startswith("~$"):
                continue

            ext = os.path.splitext(fn)[1].lstrip(".").lower()
            if ext not in extensions:
                continue

            full_path = os.path.join(dirpath, fn)
            rel       = os.path.relpath(full_path, root)
            parts     = rel.split(os.sep)

            project_folder = parts[0] if len(parts) >= 1 else ""
            subfolder      = os.sep.join(parts[1:-1]) if len(parts) > 2 else ""

            size_mb  = ""
            last_mod = ""
            size_bytes = 0
            try:
                stat       = os.stat(full_path)
                size_bytes = stat.st_size
                size_mb    = round(size_bytes / (1024 * 1024), 2)
                last_mod   = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")
            except OSError:
                pass

            rvt_version = ""
            if include_version:
                rvt_version = read_rvt_version(full_path, size_bytes)

            yield {
                "include":        0,
                "file_type":      ext,
                "project_folder": project_folder,
                "subfolder":      subfolder,
                "filename":       fn,
                "relative_path":  rel.replace(os.sep, "/"),
                "full_path":      full_path,
                "size_mb":        size_mb,
                "last_modified":  last_mod,
                "rvt_version":    rvt_version,
            }


# ---------------------------------------------------------------------------
# Manifest read / write
# ---------------------------------------------------------------------------

def load_existing_includes(out_path):
    """Return {relative_path: include_value} from an existing manifest."""
    if not os.path.isfile(out_path):
        return {}
    result = {}
    with open(out_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rp = row.get("relative_path", "").strip()
            if rp:
                result[rp] = row.get("include", "0").strip()
    return result


def write_manifest(rows, out_path, existing_includes):
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    fieldnames = [
        "include", "file_type", "project_folder", "subfolder", "filename",
        "relative_path", "full_path", "size_mb", "last_modified", "rvt_version",
    ]

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            row["include"] = existing_includes.get(row["relative_path"], row["include"])
            writer.writerow(row)

    return len(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_types(raw):
    """Expand --types argument to a set of lowercase extensions."""
    result = set()
    for t in raw:
        if t.lower() == "all":
            result |= ALL_TYPES
        else:
            t = t.lower().lstrip(".")
            if t not in ALL_TYPES:
                raise argparse.ArgumentTypeError(
                    "Unknown file type {!r}. Choose from: {} all".format(
                        t, " ".join(sorted(ALL_TYPES)))
                )
            result.add(t)
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Scan a folder for Revit files and write a manifest CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--root", required=True,
        metavar="PATH",
        help="Root folder to scan (required)."
    )
    parser.add_argument(
        "--out", required=True,
        metavar="CSV",
        help="Output CSV path (required)."
    )
    parser.add_argument(
        "--version", action="store_true",
        help=(
            "Read Revit version year from each file's OLE header. "
            "WARNING: do not use on Desktop Connector folders with unhydrated stubs — "
            "reading bytes triggers DC to queue a download."
        )
    )
    parser.add_argument(
        "--types", nargs="+", default=["rvt"],
        metavar="EXT",
        help=(
            "File extensions to include. "
            "Choices: rvt rte rfa rft all  (default: rvt). "
            "Example: --types rvt rfa  or  --types all"
        )
    )
    args = parser.parse_args()

    # Validate types
    try:
        extensions = parse_types(args.types)
    except argparse.ArgumentTypeError as e:
        parser.error(str(e))

    root     = args.root
    out_path = args.out

    if not os.path.isdir(root):
        parser.error("Root path not found or not a directory:\n  {}".format(root))

    print("Scanning: {}".format(root))
    print("  File types:     {}".format(", ".join(sorted(extensions))))
    print("  Version check:  {}".format("yes" if args.version else "no (use --version to enable)"))
    if args.version:
        print("  WARNING: --version reads file bytes — only use on fully-downloaded files")
    print("")

    existing_includes = load_existing_includes(out_path)
    if existing_includes:
        preserved = sum(1 for v in existing_includes.values() if v == "1")
        print("  Existing manifest found — preserving {} include flag(s).".format(preserved))

    rows = list(scan(root, extensions, include_version=args.version))

    # Version summary (only when --version supplied)
    if args.version:
        version_counts = {}
        for r in rows:
            v = r.get("rvt_version") or "unknown"
            version_counts[v] = version_counts.get(v, 0) + 1
        print("  Version breakdown:")
        for v in sorted(version_counts):
            print("    {:>10}  {} file(s)".format(v, version_counts[v]))
        print("")

    # File type summary
    type_counts = {}
    for r in rows:
        t = r.get("file_type", "?")
        type_counts[t] = type_counts.get(t, 0) + 1
    for t in sorted(type_counts):
        print("  .{}  {} file(s)".format(t, type_counts[t]))

    count = write_manifest(rows, out_path, existing_includes)
    print("")
    print("  Total: {} file(s)".format(count))
    print("  Written to: {}".format(out_path))
    print("")
    print("Next step: open the CSV, set include=1 on the files you want,")
    print("then run the BatchExtract button in pyRevit.")


if __name__ == "__main__":
    main()