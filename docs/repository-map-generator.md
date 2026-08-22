# Repository map generator

`tools/gen_map.py` creates deterministic, dependency-free navigation maps for
the Python source beneath any repository or folder. It does not assume package
names, fixed entrypoint filenames, or a particular directory structure.

From this repository root, refresh the checked-in maps with:

```bash
python tools/gen_map.py .
```

To scan another folder or place the generated files elsewhere:

```bash
python tools/gen_map.py /path/to/source --output-dir /path/to/maps
```

The output prefix defaults to a sanitized form of the scanned folder name and
can be overridden with `--prefix`. The three outputs are:

- `<prefix>_code_map_authoritative.md` — per-file imports and definitions.
- `<prefix>_trace_map.md` — approximate, name-based static call traces.
- `<prefix>_symbol_index.md` — definitions and approximate callsites.

Use `--exclude-dir NAME` (repeatable) for project-specific generated, legacy,
or vendor directories. Common caches, virtual environments, VCS metadata,
build outputs, and hidden directories are excluded automatically. Syntax or
encoding failures are recorded as parse warnings instead of aborting the scan.
