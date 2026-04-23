#!/usr/bin/env python3
"""Compare view template fingerprint records between two monolithic JSON exports."""

import argparse
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.lib.diff_engine import run_comparison
from tools.lib.vt_profile import make_vt_profile


def _parse_args():
    parser = argparse.ArgumentParser(description="Compare view template records between two fingerprint JSON files.")
    parser.add_argument("--file_a", required=True, help="Path to fingerprint JSON for file A")
    parser.add_argument("--file_b", required=True, help="Path to fingerprint JSON for file B")
    parser.add_argument("--out_dir", required=True, help="Directory where comparison outputs are written")
    parser.add_argument("--label_a", default=None, help="Display label for file A in logs")
    parser.add_argument("--label_b", default=None, help="Display label for file B in logs")
    parser.add_argument("--name_map", default=None, help="Optional JSON map of normalized names from file A to file B")
    parser.add_argument("--include_same", action="store_true", help="Include same-value items in details output")
    args = parser.parse_args()
    args.name_map_path = args.name_map
    return args


def main():
    args = _parse_args()
    profile = make_vt_profile()
    run_comparison(profile, args)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        if isinstance(exc, ValueError):
            print(str(exc), file=sys.stderr)
            raise SystemExit(2)
        print("ERROR: unexpected failure in compare_view_templates", file=sys.stderr)
        traceback.print_exc()
        raise
