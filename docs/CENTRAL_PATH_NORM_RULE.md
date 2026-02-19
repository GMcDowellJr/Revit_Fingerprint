# central_path_norm Rule (v2.1)

Normalization pipeline used for `central_path_norm`:

1. Trim whitespace.
2. Replace backslashes with forward slashes.
3. Collapse repeated slashes.
4. Lowercase.
5. Strip drive-letter prefix (`c:/` -> `/`).
6. Rewrite `/users/<name>/` to `/users/<user>/`.
7. Remove volatile segments anywhere in path:
   - `documents`, `desktop`, `downloads`, `onedrive*`,
   - `appdata`, `local`, `roaming`, `autodesk`, `revit`, `cache`.
8. Collapse adjacent repeated segments.
9. Remove trailing slash.

No tail-window truncation is applied.
