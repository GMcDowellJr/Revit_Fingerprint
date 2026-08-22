# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 4 of 4
- Original line range: 1235-1241
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _res_to_dict
- Source SHA-256: 82e6de1cc1d8a6782ab71ce65e4e91a7f422b049d64883de0a14e381c517b7c3
- Starts inside symbol: no
- Ends inside symbol: no

```
  1235| 
  1236| 
  1237| def _res_to_dict(r: SelectorResolution) -> dict:
  1238|     return {
  1239|         "selector_type": r.selector_type, "requested": r.requested,
  1240|         "status": r.status, "detail": r.detail, "candidates": r.candidates,
  1241|     }
```
