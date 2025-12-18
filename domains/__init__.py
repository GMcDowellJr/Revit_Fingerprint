# -*- coding: utf-8 -*-
"""
Domain extractors package.

Each domain module exposes a single extract(doc, ctx) function
that returns a fingerprint dictionary for that domain.

Domains do not import each other - cross-domain references
flow only through the context (ctx) dictionary.
"""
