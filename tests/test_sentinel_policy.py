# tests/test_sentinel_policy.py
import glob
import os
import re


ALLOWED = {"<MISSING>", "<UNREADABLE>", "<NOT_APPLICABLE>", "<NONE>", "<UNRESOLVED>"}


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def test_domains_do_not_emit_extra_angle_bracket_tokens():
    """
    Enforces PR3 sentinel policy:

    - Domains may not contain any "<Token>" literals other than:
        <MISSING>, <UNREADABLE>, <NOT_APPLICABLE>

    This catches embedded cases like "foo=<None>" just as well as standalone "<None>".
    """
    root = _repo_root()
    domains_dir = os.path.join(root, "domains")
    paths = sorted(glob.glob(os.path.join(domains_dir, "*.py")))

    assert paths, "No domain files found under {}".format(domains_dir)

    bad = []
    token_re = re.compile(r"<[A-Za-z0-9_]+>")

    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            txt = f.read()

        for m in token_re.finditer(txt):
            tok = m.group(0)
            if tok not in ALLOWED:
                # keep it compact but actionable: file, token, and a short context window
                start = max(0, m.start() - 30)
                end = min(len(txt), m.end() + 30)
                ctx = txt[start:end].replace("\n", "\\n")
                bad.append((os.path.basename(p), tok, ctx))

    assert not bad, "Found non-policy '<...>' tokens in domains:\n{}".format(
        "\n".join(["- {}: {}  [{}]".format(f, t, ctx) for (f, t, ctx) in bad])
    )
