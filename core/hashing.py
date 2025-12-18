# -*- coding: utf-8 -*-
"""
Core hashing utilities (pure Python, no Revit API).

Provides deterministic, stable hash generation using .NET MD5
for IronPython compatibility.
"""

def safe_str(x):
    """
    Convert any value to a string representation safely.

    Handles both str and unicode in IronPython environments.
    Returns "<unrepr>" for values that cannot be converted.
    """
    try:
        return str(x)
    except:
        try:
            return unicode(x)
        except:
            return u"<unrepr>"


def make_hash(values):
    """
    Deterministic hash based on a sequence of strings.
    Uses .NET MD5 to avoid IronPython limitations.

    Args:
        values: Iterable of values to hash (will be converted to strings)

    Returns:
        Hexadecimal MD5 hash string (32 characters)
    """
    from System.Text import Encoding
    from System.Security.Cryptography import MD5

    joined = u"|".join([safe_str(v) for v in values])
    data = Encoding.UTF8.GetBytes(joined)
    md5 = MD5.Create()
    hash_bytes = md5.ComputeHash(data)
    return "".join(["{0:02x}".format(b) for b in hash_bytes])
