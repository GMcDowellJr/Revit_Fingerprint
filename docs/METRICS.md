# Metrics

## HHI Metric Definitions

All HHI metrics in the v2.1 analysis pipeline use an explicitly defined universe.
HHI values are only computed from closed share vectors (shares sum to 1.0).

### 1) `hhi_domain_presence`
- **Grain:** `domain`
- **Numerator:** `files_present` for a pattern
- **Denominator:** `sum(files_present across patterns in the domain)`
- **Universe type:** presence-event distribution (not a file distribution)
- **Unknown handling:** no explicit unknown bucket (unknown records are not presence events)
- **Interpretation caveat:** multi-pattern files contribute to multiple presence events and can overweight mixed files by design.

### 2) `hhi_domain_dominance`
- **Grain:** `domain`
- **Numerator:** count of files where a pattern is the **unique** dominant pattern
- **Denominator:** number of files with a unique dominant pattern
- **Tie rule:** tied top patterns are excluded from the dominance universe
- **Excluded files behavior:** files with ties and files with no valid dominant pattern do not contribute to dominance shares
- **Interpretation:** concentration of unique dominant winners across files

### 3) `hhi_domain_records`
- **Grain:** `domain`
- **Numerator:** record count per pattern, plus an explicit unknown/unassigned record bucket
- **Denominator:** total records in the domain
- **Unknown bucket definition:** records not assigned to any resolved pattern (e.g., missing join hash or unresolved assignment)
- **Closed-universe statement:** shares are constructed to sum to 1.0

### 4) `hhi_file_records`
- **Grain:** `export_run_id × domain`
- **Numerator:** record count per pattern within file-domain, plus explicit unknown/unassigned bucket
- **Denominator:** total records in that file-domain
- **Unknown bucket definition:** same rule as domain records (not assigned to any resolved pattern)
- **Alignment with `hhi_domain_records`:** same unknown handling and closed-universe requirement

### 5) Effective cluster variants
- **Fields:** `eff_clusters_*`
- **Definition:** `1 / HHI`
- **Null behavior:** null when HHI is null/undefined/invalid
- **Interpretation:** effective number of patterns implied by concentration

## Metric Design Principles

- **Closed universe required:** HHI is computed only when shares form a closed universe.
- **No silent exclusions:** unresolved/unassigned records are modeled explicitly where record universes are used.
- **Unknowns explicit:** record-based HHI metrics include an unknown/unassigned bucket when needed.
- **Dominance requires uniqueness:** only files with a unique dominant winner participate in dominance HHI.
- **Semantic separation:** presence, dominance, and records concentration are separate metrics and are not interchangeable.

## Legacy Fields

- **Legacy field:** `phase2_authority_pattern.hhi` (and its paired `effective_cluster_count`)
- **Why it exists:** backward compatibility for existing Power BI transforms.
- **Why it is ambiguous:** it is a domain-level record concentration repeated at pattern grain.
- **Deprecation plan:** retained for compatibility; explicit domain/file HHI fields are the preferred metrics for new analysis.
