# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 4 of 4
- Original line range: 1336-1342
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _res_to_dict
- Source SHA-256: e5af5f07850e5e55e3c59afdede5ca2e2d0d048df70a923234802e541f9466b2
- Starts inside symbol: no
- Ends inside symbol: no

```
  1336| 
  1337| 
  1338| def _res_to_dict(r: SelectorResolution) -> dict:
  1339|     return {
  1340|         "selector_type": r.selector_type, "requested": r.requested,
  1341|         "status": r.status, "detail": r.detail, "candidates": r.candidates,
  1342|     }
```
