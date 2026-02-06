# -*- coding: utf-8 -*-
"""
Core hashing utilities (pure Python, no Revit API).

Provides deterministic, stable hash generation using .NET MD5
for IronPython compatibility.
"""

# Module-level timing collector reference. Set by the runner to enable
# ``processing:make_hash`` instrumentation. Never affects hash output.
_timing_collector = None


def safe_str(x):
    """
    Convert any value to a string representation safely.

    Handles both str and unicode in IronPython environments.
    Returns "<unrepr>" for values that cannot be converted.
    """
    try:
        return str(x)
    except Exception as e:
        try:
            return unicode(x)
        except Exception as e:
            return u"<unrepr>"

def make_hash(values):
    """
    Deterministic hash based on a sequence of strings.

    Streaming/incremental implementation to avoid building a huge joined
    preimage in memory.

    Semantics intentionally match:
        preimage = "|".join(safe_str(v) for v in values)
        md5(preimage.encode("utf-8")).hexdigest()

    Supports two runtimes:
      - Revit/pythonnet: use CLR MD5 (streaming)
      - Plain CPython (pytest): fall back to hashlib (streaming)
    """
    # Timing instrumentation (never affects hash output)
    _tc = _timing_collector
    if _tc is not None:
        try:
            _tc.start_timer("processing:make_hash")
        except Exception:
            pass

    try:
        return _make_hash_impl(values)
    finally:
        if _tc is not None:
            try:
                _tc.end_timer("processing:make_hash")
            except Exception:
                pass


def _make_hash_impl(values):
    """Inner hash implementation (separated for timing wrapper clarity)."""
    # First try CLR backend (Revit / pythonnet). If not available, use hashlib.
    try:
        from System.Text import Encoding  # type: ignore
        from System.Security.Cryptography import MD5  # type: ignore
    except (ImportError, ModuleNotFoundError):
        Encoding = None
        MD5 = None

    if Encoding is not None and MD5 is not None:
        # CLR streaming MD5 via TransformBlock/TransformFinalBlock
        md5 = MD5.Create()
        first = True

        for v in values:
            s = safe_str(v)

            if not first:
                sep = Encoding.UTF8.GetBytes(u"|")
                md5.TransformBlock(sep, 0, sep.Length, sep, 0)
            else:
                first = False

            chunk = Encoding.UTF8.GetBytes(s)
            md5.TransformBlock(chunk, 0, chunk.Length, chunk, 0)

        # Finalize
        empty = Encoding.UTF8.GetBytes(u"")
        md5.TransformFinalBlock(empty, 0, 0)
        hash_bytes = md5.Hash
        return "".join(["{0:02x}".format(b) for b in hash_bytes])

    # CPython fallback: hashlib (also incremental)
    import hashlib

    h = hashlib.md5()
    first = True

    for v in values:
        s = safe_str(v)

        if not first:
            h.update(b"|")
        else:
            first = False

        h.update(s.encode("utf-8"))

    return h.hexdigest()
