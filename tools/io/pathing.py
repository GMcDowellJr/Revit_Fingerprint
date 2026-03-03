from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_out_dir() -> Path:
    return repo_root() / "out"


def resolve_assets_dir() -> Path:
    return repo_root() / "assets"
