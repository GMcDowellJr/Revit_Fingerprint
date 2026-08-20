"""
acc_sync_dc.py — Desktop Connector pre-sync tool

Reads acc_manifest.csv, identifies online-only stub files (not yet downloaded),
and hydrates them via the Windows Cloud Files API before the fingerprint
extraction run.  Blocks until each file is fully downloaded.

Run this BEFORE pressing the BatchExtract button in pyRevit.

Usage:
    python acc_sync_dc.py --manifest "C:\\path\\to\\acc_manifest.csv"
    python acc_sync_dc.py --manifest ".\acc_manifest.csv" --dry-run        # report what needs syncing, don't download
    python acc_sync_dc.py --manifest ".\acc_manifest.csv" --limit 10       # hydrate at most N files (for testing)
    python acc_sync_dc.py --manifest ".\acc_manifest.csv" --timeout 600    # override per-file timeout in seconds

Requirements:
    Python 3.6+, Windows only.
    No third-party packages — uses ctypes against kernel32 (ships with Windows 10+).

Log output:
    A timestamped sync log is written to the same folder as the manifest CSV.
    Files are categorised as: ok / timeout / error / already_on_disk / missing.
    Re-run the script after fixing timeouts — already-downloaded files are skipped.
"""

import os
import sys
import csv
import ctypes
import argparse
import time
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT  = 300               # seconds per file (5 min); override with --timeout

# ---------------------------------------------------------------------------
# Windows file attribute constants
# ---------------------------------------------------------------------------

FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x00400000
FILE_ATTRIBUTE_RECALL_ON_OPEN        = 0x00040000
FILE_ATTRIBUTE_OFFLINE               = 0x00001000
INVALID_FILE_ATTRIBUTES              = 0xFFFFFFFF

_STUB_MASK = (
    FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS |
    FILE_ATTRIBUTE_RECALL_ON_OPEN |
    FILE_ATTRIBUTE_OFFLINE
)

# ---------------------------------------------------------------------------
# Stub detection
# ---------------------------------------------------------------------------

def is_stub(path):
    """
    Return True if the file is an online-only stub (not fully downloaded).

    Uses Windows cloud-file attributes exclusively — no size fallback.
    The size fallback was removed because it produced false positives on
    small-but-fully-downloaded files (e.g. tiny .rfa/.rte files with a
    green check in Explorer that are legitimately under 1 MB).

    If GetFileAttributesW returns INVALID_FILE_ATTRIBUTES the path is
    not accessible; we treat that as not-a-stub and let the open attempt
    surface the real error.
    """
    try:
        attrs = ctypes.windll.kernel32.GetFileAttributesW(path)
        if attrs == INVALID_FILE_ATTRIBUTES:
            return False  # can't read attrs — not treating as stub
        return bool(attrs & _STUB_MASK)
    except Exception:
        return False

# ---------------------------------------------------------------------------
# Hydration
# ---------------------------------------------------------------------------

# READ_TRIGGER_TIMEOUT: seconds to wait for the initial f.read(4096) trigger.
# Desktop Connector can block this call indefinitely on some files while it
# decides whether to queue a download.  We run the read on a daemon thread
# so we can impose a hard timeout on it independently of the hydration timeout.
READ_TRIGGER_TIMEOUT = 30


def _trigger_read(path, result):
    """Run in a daemon thread — attempts to read 4096 bytes from path."""
    try:
        with open(path, "rb") as f:
            f.read(4096)
        result.append("ok")
    except Exception as exc:
        result.append("error:{}".format(exc))


def hydrate(path, timeout):
    """
    Trigger hydration of a stub by opening the file for read.
    Polls until cloud-file attributes clear or timeout expires.

    The initial read trigger runs on a daemon thread with READ_TRIGGER_TIMEOUT
    so a blocking DC intercept does not hang the whole process.

    Returns a dict with keys:
        status  : "ok" | "timeout" | "error"
        elapsed : float seconds
        size_mb : float (final on-disk size; 0.0 if unreadable)
        error   : str or None
    """
    import threading
    start = time.time()

    # --- Trigger the download with a guarded read --------------------------
    _result = []
    _t = threading.Thread(target=_trigger_read, args=(path, _result), daemon=True)
    _t.start()
    _t.join(timeout=READ_TRIGGER_TIMEOUT)

    if not _result:
        # Read is still blocked — DC is intercepting; treat as a hanging stub
        return {
            "status":  "timeout",
            "elapsed": time.time() - start,
            "size_mb": 0.0,
            "error":   (
                "Read trigger blocked for {}s — DC may be intercepting the file. "
                "Try 'Make Available Offline' in Desktop Connector for this file."
            ).format(READ_TRIGGER_TIMEOUT),
        }

    if _result[0].startswith("error:"):
        return {
            "status":  "error",
            "elapsed": time.time() - start,
            "size_mb": 0.0,
            "error":   _result[0][6:],
        }

    # --- Poll until stub attributes clear or timeout -----------------------
    while True:
        elapsed = time.time() - start
        if not is_stub(path):
            try:
                size_mb = os.path.getsize(path) / (1024 * 1024)
            except OSError:
                size_mb = 0.0
            return {"status": "ok", "elapsed": elapsed, "size_mb": size_mb, "error": None}
        if elapsed >= timeout:
            try:
                size_mb = os.path.getsize(path) / (1024 * 1024)
            except OSError:
                size_mb = 0.0
            return {
                "status":  "timeout",
                "elapsed": elapsed,
                "size_mb": size_mb,
                "error":   "Timed out after {:.0f}s — file may still be downloading".format(elapsed),
            }
        time.sleep(2)

# ---------------------------------------------------------------------------
# Manifest loader
# ---------------------------------------------------------------------------

def load_included_entries(csv_path):
    entries = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("include", "").strip() != "1":
                continue
            relative_path = row.get("relative_path", "").strip()
            full_path     = row.get("full_path", "").strip()
            if not relative_path or not full_path:
                continue
            project = row.get("project_folder", "").strip()
            fname   = row.get("filename", os.path.basename(full_path)).strip()
            entries.append({
                "relative_path": relative_path,
                "full_path":     full_path,
                "display_name":  "{}/{}".format(project, fname) if project else fname,
            })
    return entries

# ---------------------------------------------------------------------------
# Log writer
# ---------------------------------------------------------------------------

def write_log(log_path, run_meta, results):
    """
    Write a persistent timestamped sync log.

    Each result dict must have:
        display_name, full_path, status, elapsed, size_mb, error
    """
    by_status = {}
    for r in results:
        by_status.setdefault(r["status"], []).append(r)

    with open(log_path, "w", encoding="utf-8") as f:

        f.write("ACC Desktop Connector Pre-Sync Log\n")
        f.write("Run:      {}\n".format(run_meta["started"]))
        f.write("Manifest: {}\n".format(run_meta["manifest"]))
        f.write("Timeout:  {}s per file\n".format(run_meta["timeout"]))
        f.write("\n")

        # Per-file detail
        f.write("=" * 70 + "\n")
        f.write("PER-FILE DETAIL\n")
        f.write("=" * 70 + "\n\n")

        for r in results:
            f.write("[{}]  {}\n".format(r["status"].upper(), r["display_name"]))
            f.write("  Path:  {}\n".format(r["full_path"]))
            if r["status"] in ("ok", "timeout", "already_on_disk"):
                f.write("  Time:  {:.1f}s\n".format(r["elapsed"]))
                f.write("  Size:  {:.1f} MB\n".format(r["size_mb"]))
            if r["error"]:
                f.write("  Error: {}\n".format(r["error"]))
            f.write("\n")

        # Summary
        f.write("=" * 70 + "\n")
        f.write("SUMMARY\n")
        f.write("=" * 70 + "\n\n")

        ok_downloaded  = len(by_status.get("ok", []))
        already        = len(by_status.get("already_on_disk", []))
        timed_out      = len(by_status.get("timeout", []))
        errors         = len(by_status.get("error", []))
        missing        = len(by_status.get("missing", []))
        total          = len(results)

        f.write("  Total included:          {}\n".format(total))
        f.write("  Already on disk:         {}\n".format(already))
        f.write("  Downloaded this run:     {}\n".format(ok_downloaded))
        f.write("  Timed out (>{}s):        {}\n".format(run_meta["timeout"], timed_out))
        f.write("  Errors:                  {}\n".format(errors))
        f.write("  Missing from DC cache:   {}\n".format(missing))
        f.write("  Ready for extraction:    {}/{}\n".format(ok_downloaded + already, total))
        f.write("\n")

        if by_status.get("timeout"):
            f.write("TIMED OUT — check Desktop Connector, then re-run:\n")
            for r in by_status["timeout"]:
                f.write("  {:.1f}s  {:.1f} MB  {}\n".format(
                    r["elapsed"], r["size_mb"], r["display_name"]))
            f.write("\n")

        if by_status.get("error"):
            f.write("ERRORS:\n")
            for r in by_status["error"]:
                f.write("  {}  —  {}\n".format(r["display_name"], r["error"]))
            f.write("\n")

        if by_status.get("missing"):
            f.write("MISSING FROM DC CACHE (path not found on disk):\n")
            for r in by_status["missing"]:
                f.write("  {}\n".format(r["display_name"]))
            f.write("\n")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Hydrate Desktop Connector stubs before fingerprint extraction."
    )
    parser.add_argument("--manifest", required=True,
                        help="Path to acc_manifest.csv")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Report what needs syncing without downloading.")
    parser.add_argument("--limit",   type=int, default=None,
                        help="Hydrate at most N stubs (for testing).")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help="Per-file hydration timeout in seconds (default: {}).".format(
                            DEFAULT_TIMEOUT))
    args = parser.parse_args()

    if not os.path.isfile(args.manifest):
        print("ERROR: Manifest not found:\n  {}".format(args.manifest))
        sys.exit(1)

    started  = datetime.now()
    log_dir  = os.path.dirname(os.path.abspath(args.manifest))
    log_path = os.path.join(
        log_dir,
        "sync_{}.log".format(started.strftime("%Y%m%dT%H%M%S"))
    )

    print("ACC Desktop Connector Pre-Sync")
    print("  Manifest: {}".format(args.manifest))
    print("  Timeout:  {}s per file".format(args.timeout))
    print("  Dry run:  {}".format(args.dry_run))
    print("")

    entries = load_included_entries(args.manifest)
    if not entries:
        print("No include=1 entries found in manifest.")
        sys.exit(0)

    # Classify all entries upfront
    results = []
    stubs   = []

    for entry in entries:
        fp = entry["full_path"]
        if not os.path.isfile(fp):
            results.append(dict(entry, status="missing", elapsed=0.0, size_mb=0.0,
                                error="Not found on disk"))
        elif is_stub(fp):
            stubs.append(entry)
        else:
            try:
                size_mb = os.path.getsize(fp) / (1024 * 1024)
            except OSError:
                size_mb = 0.0
            results.append(dict(entry, status="already_on_disk", elapsed=0.0,
                                size_mb=size_mb, error=None))

    already_count  = sum(1 for r in results if r["status"] == "already_on_disk")
    missing_count  = sum(1 for r in results if r["status"] == "missing")

    print("  Total included:   {}".format(len(entries)))
    print("  Already on disk:  {}".format(already_count))
    print("  Stubs to hydrate: {}".format(len(stubs)))
    print("  Missing from DC:  {}".format(missing_count))
    print("")

    if not stubs:
        print("Nothing to sync — all included files are already fully downloaded.")
        write_log(log_path, {"started": started.isoformat(),
                             "manifest": args.manifest,
                             "timeout": args.timeout}, results)
        print("Log: {}".format(log_path))
        sys.exit(0)

    if args.dry_run:
        print("Dry run — stubs that would be hydrated:")
        for e in stubs:
            print("  {}".format(e["display_name"]))
        print("")
        print("Re-run without --dry-run to download.")
        sys.exit(0)

    to_hydrate = stubs[:args.limit] if args.limit else stubs
    if args.limit and len(stubs) > args.limit:
        print("Note: --limit {} applied; {} stub(s) deferred to next run.".format(
            args.limit, len(stubs) - args.limit))
        print("")

    print("Hydrating {} file(s) — keep Desktop Connector running...".format(len(to_hydrate)))
    print("")

    for i, entry in enumerate(to_hydrate):
        print("[{}/{}] {}".format(i + 1, len(to_hydrate), entry["display_name"]))

        r = hydrate(entry["full_path"], timeout=args.timeout)

        if r["status"] == "ok":
            print("  OK       {:.1f}s   {:.1f} MB".format(r["elapsed"], r["size_mb"]))
        elif r["status"] == "timeout":
            print("  TIMEOUT  {:.1f}s   {:.1f} MB (partial?)".format(r["elapsed"], r["size_mb"]))
            print("  Tip: re-run with --timeout {} or check DC connection".format(
                args.timeout * 2))
        else:
            print("  ERROR    {}".format(r["error"]))

        results.append(dict(entry, **r))
        print("")

    # Write persistent log
    run_meta = {"started": started.isoformat(), "manifest": args.manifest,
                "timeout": args.timeout}
    write_log(log_path, run_meta, results)

    # Console summary
    ok_count      = sum(1 for r in results if r["status"] == "ok")
    timeout_count = sum(1 for r in results if r["status"] == "timeout")
    error_count   = sum(1 for r in results if r["status"] == "error")

    print("=" * 60)
    print("Downloaded this run:  {}".format(ok_count))
    print("Already on disk:      {}".format(already_count))
    print("Timed out:            {}".format(timeout_count))
    print("Errors:               {}".format(error_count))
    print("Log:                  {}".format(log_path))
    print("")

    problems = timeout_count + error_count
    if problems:
        print("{} file(s) need attention before extraction.".format(problems))
        print("Re-run this script after resolving, or raise --timeout for large files.")
    else:
        print("All files ready — run BatchExtract in pyRevit.")


if __name__ == "__main__":
    main()
