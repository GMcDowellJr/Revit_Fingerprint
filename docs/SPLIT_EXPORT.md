# Split export removed

Split export (`.index.json` + `.details.json`) has been removed.

Revit Fingerprint now emits a single monolithic export per run:

- `<basename>.json`

The monolithic JSON includes all metadata and all domain payloads with records.
