#!/usr/bin/env python3
"""repo_context.py — local, read-only repository-context generator.

Scans a repository or folder and prepares it for a document-grounded LLM
(e.g. Microsoft 365 Copilot) without uploading, transmitting, executing,
importing, or modifying any scanned code.

Usage:
    repo_context.py scan ROOT [options]
    repo_context.py packet ROOT [options]
    repo_context.py validate OUTPUT_DIRECTORY [options]

Run with -h / --help, or see README.md in a generated output directory.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Running this file directly (``python repo_context.py ...``) puts its own
# directory on sys.path[0], so plain absolute imports of sibling modules
# work without turning this into a package.
import rc_scan
import rc_manifest
import rc_writers
import rc_tree
import rc_overview
import rc_packet
import rc_validate
from rc_common import TOOL_VERSION


def _resolve_output_dir(root: Path, output_arg: str) -> tuple[Path, set]:
    output_dir = Path(output_arg)
    output_resolved = output_dir.resolve()
    root_resolved = root.resolve()

    if output_resolved == root_resolved:
        print("error: --output must not be the repository root itself", file=sys.stderr)
        sys.exit(2)

    extra_exclude_dirs: set = set()
    try:
        rel = output_resolved.relative_to(root_resolved)
    except ValueError:
        rel = None
    if rel is not None and rel.parts:
        extra_exclude_dirs.add(rel.parts[0])

    return output_dir, extra_exclude_dirs


def cmd_scan(args: argparse.Namespace) -> int:
    root = Path(args.root)
    if not root.exists() or not root.is_dir():
        print(f"error: ROOT is not a directory: {root}", file=sys.stderr)
        return 2

    output_dir, auto_exclude = _resolve_output_dir(root, args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "chunks").mkdir(parents=True, exist_ok=True)
    (output_dir / "packets").mkdir(parents=True, exist_ok=True)

    extra_exclude_dirs = auto_exclude | set(args.exclude_dir or [])

    options = rc_scan.ScanOptions(
        root=root,
        output_dir=output_dir,
        extra_exclude_dirs=extra_exclude_dirs,
        exclude_globs=list(args.exclude_glob or []),
        include_globs=list(args.include_glob or []),
        include_secrets=args.include_secrets,
        chunk_line_threshold=args.chunk_line_threshold,
        chunk_char_threshold=args.chunk_char_threshold,
        chunk_target_lines=args.chunk_target_lines,
        chunk_overlap_lines=args.chunk_overlap_lines,
        show_excluded_dirs=args.show_excluded_dirs,
        verbose=args.verbose,
        force=args.force,
        redact_secrets=not args.no_redact_secrets,
    )
    options.chunk_reuse_provider = rc_manifest.make_chunk_reuse_provider(output_dir, options, args.force)
    reuse_active = options.chunk_reuse_provider is not None

    started_at = rc_manifest.utc_now_iso()
    if args.verbose:
        print(f"repo_context.py v{TOOL_VERSION}: scanning {root.resolve()}")

    result = rc_scan.scan_repository(options)

    rc_writers.write_all_tables(output_dir, result)
    rc_tree.write_tree(output_dir, result, root.resolve().name,
                        max_depth=args.max_tree_depth, show_excluded_dirs=args.show_excluded_dirs)

    overview_text = rc_overview.generate_overview_md(result, root.resolve().name, started_at)
    from rc_common import atomic_write_text
    atomic_write_text(output_dir / "repository_overview.md", overview_text)
    atomic_write_text(output_dir / "README.md", rc_overview.generate_readme_md())

    rc_manifest.write_manifest(
        output_dir, options, result, root, started_at, reuse_active,
        extra={"excluded_directories_sample": [
            {"path": p, "reason": r} for p, _, r in result.dir_exclusions[:50]
        ]},
    )

    if args.verbose:
        included = sum(1 for f in result.files if f.included)
        print(f"repo_context.py: done. {included} files included, "
              f"{len(result.chunks)} chunks, {len(result.symbols)} symbols.")

    return 0


def cmd_packet(args: argparse.Namespace) -> int:
    root = Path(args.root)
    if not root.exists() or not root.is_dir():
        print(f"error: ROOT is not a directory: {root}", file=sys.stderr)
        return 2

    output_dir = Path(args.output)
    if not (output_dir / "file_inventory.csv").exists():
        print(f"error: no prior scan found in {output_dir}. Run `repo_context.py scan` first.", file=sys.stderr)
        return 1

    if not any([args.file, args.symbol, args.search, args.line, args.changed]):
        print("error: packet requires at least one of --file, --symbol, --search, --line, --changed",
              file=sys.stderr)
        return 2

    opts = rc_packet.PacketOptions(
        root=root, output_dir=output_dir,
        file=args.file, symbol=args.symbol, search=args.search, line=args.line,
        changed=list(args.changed or []),
        caller_depth=args.caller_depth, callee_depth=args.callee_depth,
        max_files=args.max_files, max_lines=args.max_lines, max_characters=args.max_characters,
        all_matches=args.all_matches, name=args.name,
    )
    packet_path = rc_packet.generate_packet(opts)
    print(f"wrote {packet_path}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_directory)
    res = rc_validate.validate_output_dir(output_dir, allow_absolute_paths=args.allow_absolute_paths)
    print(rc_validate.format_report(res, output_dir))
    return 0 if res.ok else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="repo_context.py", description=__doc__.split("\n\n")[0])
    parser.add_argument("--version", action="version", version=f"repo_context.py {TOOL_VERSION}")
    sub = parser.add_subparsers(dest="command", required=True)

    p_scan = sub.add_parser("scan", help="Scan a repository and generate context outputs")
    p_scan.add_argument("root", help="Repository or folder root to scan")
    p_scan.add_argument("--output", required=True, help="Output directory for generated context")
    p_scan.add_argument("--force", action="store_true", help="Force a full rebuild (ignore incremental reuse)")
    p_scan.add_argument("--verbose", action="store_true", help="Print progress information")
    p_scan.add_argument("--exclude-dir", action="append", default=[],
                         help="Additional directory name to always exclude (repeatable)")
    p_scan.add_argument("--exclude-glob", action="append", default=[],
                         help="Additional glob (matched against the repo-relative path) to exclude (repeatable)")
    p_scan.add_argument("--include-glob", action="append", default=[],
                         help="Glob that force-includes matching files even if they would "
                              "otherwise be excluded by --exclude-glob or the default secret patterns (repeatable)")
    p_scan.add_argument("--include-secrets", action="store_true",
                         help="Explicitly disable the default secret-file exclusion patterns "
                              "(.env, *.pem, id_rsa, etc.) -- may expose sensitive files in the output")
    p_scan.add_argument("--chunk-line-threshold", type=int, default=1000)
    p_scan.add_argument("--chunk-char-threshold", type=int, default=80_000)
    p_scan.add_argument("--chunk-target-lines", type=int, default=400)
    p_scan.add_argument("--chunk-overlap-lines", type=int, default=10)
    p_scan.add_argument("--max-tree-depth", type=int, default=None, help="Maximum depth for repository_tree.txt")
    p_scan.add_argument("--show-excluded-dirs", action="store_true",
                         help="Mark excluded directories in the tree without recursing into them")
    p_scan.add_argument("--no-redact-secrets", action="store_true",
                         help="Disable best-effort secret-line redaction in generated chunks")
    p_scan.set_defaults(func=cmd_scan)

    p_packet = sub.add_parser("packet", help="Generate a targeted context packet")
    p_packet.add_argument("root", help="Repository or folder root (must match a prior scan)")
    p_packet.add_argument("--output", required=True, help="Output directory from a prior `scan`")
    p_packet.add_argument("--file", help="Repository-relative path to a file")
    p_packet.add_argument("--symbol", help="Qualified or simple symbol name")
    p_packet.add_argument("--search", help="Literal text to search for across included text files")
    p_packet.add_argument("--line", help="RELATIVE_PATH:LINE")
    p_packet.add_argument("--changed", nargs="+", help="One or more repository-relative file paths")
    p_packet.add_argument("--caller-depth", type=int, default=1)
    p_packet.add_argument("--callee-depth", type=int, default=1)
    p_packet.add_argument("--max-files", type=int, default=8)
    p_packet.add_argument("--max-lines", type=int, default=1200)
    p_packet.add_argument("--max-characters", type=int, default=60_000)
    p_packet.add_argument("--all-matches", action="store_true",
                           help="Include every candidate when a --symbol request is ambiguous")
    p_packet.add_argument("--name", help="Override the generated packet's output filename stem")
    p_packet.set_defaults(func=cmd_packet)

    p_validate = sub.add_parser("validate", help="Validate a previously generated output directory")
    p_validate.add_argument("output_directory", help="Output directory produced by `scan`")
    p_validate.add_argument("--allow-absolute-paths", action="store_true",
                             help="Diagnostic: do not flag absolute paths leaked into generated output")
    p_validate.set_defaults(func=cmd_validate)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
