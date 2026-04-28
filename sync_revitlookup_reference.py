"""
sync_revitlookup_reference.py

Copies RevitLookup descriptor source files into the Fingerprint repo as a
reference layer for extractor validation and domain development.

Fetches directly from the GitHub API — no git clone required.

Usage:
    python sync_revitlookup_reference.py
    python sync_revitlookup_reference.py --output-dir path/to/fingerprint/reference/revit_lookup
    python sync_revitlookup_reference.py --dry-run
    python sync_revitlookup_reference.py --priority-only   # fetch only descriptors mapped to active domains

What it copies:
    - source/RevitLookup/Core/Decomposition/Descriptors/*.cs  (all, or priority subset)
    - source/RevitLookup/Core/Decomposition/DescriptorsMap.cs
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime, timezone

REPO_OWNER = "lookup-foundation"
REPO_NAME  = "RevitLookup"
BRANCH     = "develop"

DESCRIPTORS_DIR = "source/RevitLookup/Core/Decomposition/Descriptors"
DESCRIPTORS_MAP = "source/RevitLookup/Core/Decomposition/DescriptorsMap.cs"

GITHUB_API = "https://api.github.com"
GITHUB_RAW = "https://raw.githubusercontent.com"

# Descriptors that map directly to active Fingerprint domains or near-future ones.
# Used by --priority-only to fetch a minimal useful set first.
# Keys are descriptor filenames; values describe which domain(s) they cover.
PRIORITY_DESCRIPTORS = {
    # --- Active domains ---
    "ElementTypeDescriptor.cs":             "arrowheads, text_types (base type for both)",
    "TextNoteTypeDescriptor.cs":            "text_types",
    "LinePatternElementDescriptor.cs":      "line_patterns",
    "GraphicsStyleDescriptor.cs":           "line_styles",
    "FillPatternElementDescriptor.cs":      "fill_patterns_drafting, fill_patterns_model",
    "CategoryDescriptor.cs":               "object_styles_* (all 4 splits)",
    "DimensionTypeDescriptor.cs":           "dimension_types_* (all 7 splits)",
    "PhaseDescriptor.cs":                   "phases",
    "ElementDescriptor.cs":                 "phase_filters, phase_graphics, general base",
    "DocumentDescriptor.cs":               "identity, units (document-level collections)",
    "ForgeTypeIdDescriptor.cs":            "units (discipline/format/spec access)",
    "ParameterFilterElementDescriptor.cs": "view_filter_definitions",
    "ViewDescriptor.cs":                    "view_filter_applications, view_category_overrides, view_templates",
    "OverrideGraphicSettingsDescriptor.cs": "view_category_overrides (VG settings object)",
    # --- Future / compound layer domains ---
    "WallTypeDescriptor.cs":               "future: wall type layers",
    "CompoundStructureDescriptor.cs":      "future: all compound layer domains (wall/floor/roof/ceiling)",
    "FloorTypeDescriptor.cs":              "future: floor type layers",
    "RoofTypeDescriptor.cs":              "future: roof type layers",
}

README_TEMPLATE = """\
# RevitLookup Descriptor Reference

Source: https://github.com/{owner}/{repo}/tree/{branch}/{descriptors_dir}
Pinned commit: {commit_sha}
Pinned date:   {sync_date}
License:       MIT (https://github.com/{owner}/{repo}/blob/{branch}/License.md)

## Purpose

These files are **reference material only** — not executed by the Fingerprint pipeline.

Each descriptor in `Descriptors/` is a C# class that documents the exact Revit API
traversal RevitLookup uses for a given type (WallType, CompoundStructure, etc.).

See `REVIT_LOOKUP_DOMAIN_MAP.md` for the mapping between Fingerprint domains and
the descriptor files that cover the same Revit API surface.

## When to use

- Writing a new domain extractor: check what API calls are available and what
  guard conditions are needed (null checks, type discrimination, etc.)
- Auditing an existing extractor: compare what you call vs what they call
- Investigating an edge case: see how RevitLookup handles it in the wild
- Planning a future domain: check descriptor availability in DescriptorsMap.cs

## Important distinction

RevitLookup descriptors call *everything* for display purposes.
Fingerprint extractors call only configuration-stable signals.
The API traversal is reference; the selection of calls is your judgment.

## Keeping up to date

Re-run `sync_revitlookup_reference.py` from the repo root to refresh.
The script is idempotent — re-running when already at HEAD is a no-op.
Use `--priority-only` to fetch just the descriptors mapped to active domains.
"""


def github_get(path: str, token: str = None) -> dict:
    url = f"{GITHUB_API}{path}"
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github.v3+json")
    req.add_header("User-Agent", "fingerprint-sync-script")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  GitHub API error {e.code} for {url}")
        print(f"  {body[:300]}")
        sys.exit(1)


def fetch_raw(owner: str, repo: str, branch: str, path: str, token: str = None):
    url = f"{GITHUB_RAW}/{owner}/{repo}/{branch}/{path}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "fingerprint-sync-script")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code}")
        return None


def get_current_commit_sha(owner: str, repo: str, branch: str, token: str = None) -> str:
    data = github_get(f"/repos/{owner}/{repo}/branches/{branch}", token)
    return data["commit"]["sha"]


def list_all_cs_files(owner: str, repo: str, branch: str, prefix: str, token: str = None) -> list:
    data = github_get(f"/repos/{owner}/{repo}/git/trees/{branch}?recursive=1", token)
    return [
        {"path": item["path"], "sha": item.get("sha")}
        for item in data.get("tree", [])
        if item.get("type") == "blob"
        and item["path"].startswith(prefix)
        and item["path"].endswith(".cs")
    ]


def sync(output_dir: Path, token: str = None, dry_run: bool = False, priority_only: bool = False) -> None:
    mode = "priority descriptors only" if priority_only else "all descriptors"
    print(f"RevitLookup descriptor sync  [{mode}]")
    print(f"  Repo:       {REPO_OWNER}/{REPO_NAME}  branch={BRANCH}")
    print(f"  Output dir: {output_dir}")
    print(f"  Dry run:    {dry_run}")
    print()

    print("Fetching current HEAD commit SHA...")
    commit_sha = get_current_commit_sha(REPO_OWNER, REPO_NAME, BRANCH, token)
    print(f"  Commit: {commit_sha[:12]}...")
    print()

    # Idempotency check
    manifest_path = output_dir / ".sync_manifest.json"
    if manifest_path.exists() and not dry_run:
        try:
            existing = json.loads(manifest_path.read_text())
            if existing.get("commit_sha") == commit_sha and existing.get("priority_only") == priority_only:
                print("Already up to date — nothing to do.")
                print(f"(Pinned at {existing.get('sync_date', 'unknown')})")
                return
        except Exception:
            pass

    print(f"Listing .cs files under {DESCRIPTORS_DIR} ...")
    all_cs = list_all_cs_files(REPO_OWNER, REPO_NAME, BRANCH, DESCRIPTORS_DIR, token)

    if priority_only:
        descriptor_files = [f for f in all_cs if Path(f["path"]).name in PRIORITY_DESCRIPTORS]
        print(f"  {len(all_cs)} total .cs files found, {len(descriptor_files)} match priority list")
    else:
        descriptor_files = all_cs
        print(f"  {len(all_cs)} descriptor .cs files found")

    # Always include DescriptorsMap
    all_files = descriptor_files + [{"path": DESCRIPTORS_MAP, "sha": None}]
    print()

    written   = 0
    failed    = 0
    not_found = []

    for item in all_files:
        repo_path = item["path"]
        filename  = Path(repo_path).name
        relative  = repo_path.replace("source/RevitLookup/Core/Decomposition/", "")
        dest      = output_dir / relative

        if dry_run:
            note = f"  # {PRIORITY_DESCRIPTORS[filename]}" if filename in PRIORITY_DESCRIPTORS else ""
            print(f"  [dry-run] {relative}{note}")
            continue

        print(f"  {relative} ...", end=" ", flush=True)
        time.sleep(0.05)

        content = fetch_raw(REPO_OWNER, REPO_NAME, BRANCH, repo_path, token)
        if content is None:
            print("NOT FOUND (may not exist in this version yet)")
            not_found.append(filename)
            failed += 1
            continue

        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        print(f"ok ({len(content):,} bytes)")
        written += 1

    if dry_run:
        print(f"\nDry run complete — {len(all_files)} files would be attempted.")
        return

    # Write README
    (output_dir / "README.md").write_text(
        README_TEMPLATE.format(
            owner=REPO_OWNER,
            repo=REPO_NAME,
            branch=BRANCH,
            descriptors_dir=DESCRIPTORS_DIR,
            commit_sha=commit_sha,
            sync_date=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        ),
        encoding="utf-8",
    )

    # Write manifest
    manifest_path.write_text(
        json.dumps({
            "commit_sha":    commit_sha,
            "sync_date":     datetime.now(timezone.utc).isoformat(),
            "repo":          f"{REPO_OWNER}/{REPO_NAME}",
            "branch":        BRANCH,
            "priority_only": priority_only,
            "files_written": written,
            "files_not_found": not_found,
        }, indent=2),
        encoding="utf-8",
    )

    print()
    print(f"Sync complete:")
    print(f"  Written:   {written}")
    if not_found:
        print(f"  Not found: {failed}  {not_found}")
        print()
        print("  Note: 'not found' descriptors may not exist in this RevitLookup version yet.")
        print("  Re-run after a RevitLookup update to pick them up.")
    print(f"  Pinned:    {commit_sha[:12]}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output-dir",
        default="reference/revit_lookup",
        help="Destination folder relative to cwd (default: reference/revit_lookup)",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("GITHUB_TOKEN"),
        help="GitHub personal access token, or set GITHUB_TOKEN env var. "
             "Optional but avoids rate limiting.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without writing anything.",
    )
    parser.add_argument(
        "--priority-only",
        action="store_true",
        help="Fetch only the ~18 descriptors mapped to active Fingerprint domains. "
             "Faster for setup; re-run without flag to get all ~50 descriptors.",
    )
    args = parser.parse_args()

    sync(
        output_dir=Path(args.output_dir),
        token=args.token,
        dry_run=args.dry_run,
        priority_only=args.priority_only,
    )


if __name__ == "__main__":
    main()
