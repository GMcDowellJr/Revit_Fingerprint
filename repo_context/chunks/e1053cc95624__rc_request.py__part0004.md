# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 4 of 4
- Original line range: 1277-1283
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _res_to_dict
- Source SHA-256: da6b351bdc8071f0313b339e641c5fdb991fa445c5ab75e6b857884c94d04dea
- Starts inside symbol: no
- Ends inside symbol: no

```
  1277| 
  1278| 
  1279| def _res_to_dict(r: SelectorResolution) -> dict:
  1280|     return {
  1281|         "selector_type": r.selector_type, "requested": r.requested,
  1282|         "status": r.status, "detail": r.detail, "candidates": r.candidates,
  1283|     }
```
