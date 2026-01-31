"""
Standards Governance Report Generator

Analyzes fingerprint exports to:
1. Detect baseline drift (categories with non-canonical settings)
2. Find unnecessary overrides (template overrides canonical baseline)
3. Identify common patterns (template names across projects)
4. Generate remediation recommendations

Usage:
    python tools/governance/standards_governance_report.py export1.json export2.json ...
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class ProjectExport:
    name: str
    path: Path
    data: Dict[str, Any]


class StandardsGovernanceAnalyzer:
    def __init__(self) -> None:
        self.projects: List[ProjectExport] = []
        self.violations: List[Dict[str, Any]] = []
        self.patterns: List[Dict[str, Any]] = []

    def load_exports(self, export_paths: Iterable[str]) -> None:
        """Load multiple project exports."""
        for export_path in export_paths:
            path = Path(export_path)
            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            project_name = (
                data.get("domains", {})
                .get("identity", {})
                .get("project_name", path.stem)
            )
            self.projects.append(ProjectExport(name=project_name, path=path, data=data))

    def analyze_baseline_drift(self) -> None:
        """
        Find categories with inconsistent global baselines.

        Canonical baseline = most common sig_hash for a category across projects.
        Violation = project has non-canonical baseline.
        """
        category_baselines: Dict[str, List[Dict[str, str]]] = defaultdict(list)

        for project in self.projects:
            records = (
                project.data.get("domains", {})
                .get("object_styles", {})
                .get("records", [])
            )
            for rec in records:
                row_key = _get_identity_value(rec, "obj_style.row_key")
                sig_hash = rec.get("sig_hash")
                if row_key and sig_hash:
                    category_baselines[row_key].append(
                        {"project": project.name, "sig_hash": sig_hash}
                    )

        for category, baselines in category_baselines.items():
            sig_hash_counts = Counter(entry["sig_hash"] for entry in baselines)
            if len(sig_hash_counts) <= 1:
                continue
            canonical_sig_hash = sig_hash_counts.most_common(1)[0][0]
            for baseline in baselines:
                if baseline["sig_hash"] != canonical_sig_hash:
                    self.violations.append(
                        {
                            "type": "non_canonical_baseline",
                            "category": category,
                            "project": baseline["project"],
                            "current_sig_hash": baseline["sig_hash"],
                            "canonical_sig_hash": canonical_sig_hash,
                            "severity": "medium",
                            "recommendation": (
                                f"Align '{category}' baseline to canonical standard."
                            ),
                        }
                    )

    def analyze_template_overrides(self) -> None:
        """
        Find templates that override canonical baseline unnecessarily.

        Unnecessary = override matches canonical baseline (redundant).
        """
        canonical_baselines = self._get_canonical_baselines()

        for project in self.projects:
            templates = (
                project.data.get("domains", {})
                .get("view_templates", {})
                .get("records", [])
            )
            for template in templates:
                template_name = template.get("label", "Unknown")
                items = template.get("identity_basis", {}).get("items", [])
                for item in items:
                    item_key = item.get("k", "")
                    if "category_overrides" not in item_key:
                        continue
                    if not item_key.endswith("baseline_sig_hash"):
                        continue
                    baseline_sig_hash = item.get("v")
                    category_key = item_key.replace(
                        "baseline_sig_hash", "category_path"
                    )
                    category_item = next(
                        (candidate for candidate in items if candidate.get("k") == category_key),
                        None,
                    )
                    if not category_item:
                        continue
                    category = category_item.get("v")
                    canonical = canonical_baselines.get(category)
                    if canonical and baseline_sig_hash == canonical:
                        self.violations.append(
                            {
                                "type": "unnecessary_override",
                                "template": template_name,
                                "project": project.name,
                                "category": category,
                                "severity": "low",
                                "recommendation": (
                                    "Remove override - already matches canonical baseline."
                                ),
                            }
                        )

    def identify_common_patterns(self) -> None:
        """
        Find common template patterns across projects.

        Pattern = template name that appears in multiple projects with similar behavior.
        """
        template_patterns: Dict[str, List[Dict[str, str]]] = defaultdict(list)

        for project in self.projects:
            templates = (
                project.data.get("domains", {})
                .get("view_templates", {})
                .get("records", [])
            )
            for template in templates:
                template_name = template.get("label", "Unknown")
                sig_hash = template.get("sig_hash")
                template_patterns[template_name].append(
                    {"project": project.name, "sig_hash": sig_hash}
                )

        for name, instances in template_patterns.items():
            if len(instances) < 2:
                continue
            sig_hashes = {instance["sig_hash"] for instance in instances}
            if len(sig_hashes) == 1:
                self.patterns.append(
                    {
                        "name": name,
                        "projects": len(instances),
                        "consistency": "perfect",
                        "sig_hash": next(iter(sig_hashes)),
                    }
                )
            else:
                self.patterns.append(
                    {
                        "name": name,
                        "projects": len(instances),
                        "consistency": "drift",
                        "variants": len(sig_hashes),
                    }
                )

    def generate_report(self, output_path: str = "governance_report.html") -> None:
        """Generate HTML report."""
        output = Path(output_path)
        summary = self._build_summary()
        html = _build_html_report(self.projects, self.violations, self.patterns, summary)

        output.write_text(html, encoding="utf-8")
        print(f"Report generated: {output.resolve()}")

    def _get_canonical_baselines(self) -> Dict[str, str]:
        """Helper: Get canonical baseline sig_hash for each category."""
        category_baselines: Dict[str, List[str]] = defaultdict(list)

        for project in self.projects:
            records = (
                project.data.get("domains", {})
                .get("object_styles", {})
                .get("records", [])
            )
            for rec in records:
                row_key = _get_identity_value(rec, "obj_style.row_key")
                sig_hash = rec.get("sig_hash")
                if row_key and sig_hash:
                    category_baselines[row_key].append(sig_hash)

        return {
            category: Counter(sig_hashes).most_common(1)[0][0]
            for category, sig_hashes in category_baselines.items()
        }

    def _build_summary(self) -> Dict[str, int]:
        return {
            "projects": len(self.projects),
            "baseline_drift": sum(
                1 for v in self.violations if v["type"] == "non_canonical_baseline"
            ),
            "unnecessary_overrides": sum(
                1 for v in self.violations if v["type"] == "unnecessary_override"
            ),
            "patterns": len(self.patterns),
        }


def _get_identity_value(record: Dict[str, Any], key: str) -> Optional[str]:
    items = record.get("identity_basis", {}).get("items", [])
    for item in items:
        if item.get("k") == key:
            return item.get("v")
    return None


def _build_html_report(
    projects: List[ProjectExport],
    violations: List[Dict[str, Any]],
    patterns: List[Dict[str, Any]],
    summary: Dict[str, int],
) -> str:
    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    baseline_violations = [
        v for v in violations if v["type"] == "non_canonical_baseline"
    ]
    override_violations = [
        v for v in violations if v["type"] == "unnecessary_override"
    ]
    project_list = ", ".join(project.name for project in projects) or "None"

    rows_baseline = "\n".join(
        _row_template(
            v["project"],
            v["category"],
            v["severity"],
            v["recommendation"],
        )
        for v in baseline_violations
    )
    rows_overrides = "\n".join(
        _row_template(
            v["project"],
            f"{v['template']} → {v['category']}",
            v["severity"],
            v["recommendation"],
        )
        for v in override_violations
    )
    rows_patterns = "\n".join(
        _pattern_row_template(pattern) for pattern in patterns
    )

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8" />
    <title>Standards Governance Report</title>
    <style>
        body {{
            font-family: "Segoe UI", Arial, sans-serif;
            margin: 32px;
            color: #2c3e50;
            background-color: #f8fafc;
        }}
        .header {{
            background: #ffffff;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 2px 8px rgba(15, 23, 42, 0.08);
        }}
        h1 {{
            margin: 0 0 8px 0;
            font-size: 28px;
        }}
        .subtitle {{
            color: #64748b;
            margin: 0;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
            margin-top: 20px;
        }}
        .summary-card {{
            background: #ffffff;
            border-radius: 10px;
            padding: 16px;
            border: 1px solid #e2e8f0;
        }}
        .summary-card h3 {{
            margin: 0 0 6px 0;
            font-size: 14px;
            color: #64748b;
            font-weight: 600;
        }}
        .summary-card p {{
            margin: 0;
            font-size: 22px;
            font-weight: 700;
        }}
        h2 {{
            margin-top: 32px;
            color: #1e293b;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: #ffffff;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 1px 4px rgba(15, 23, 42, 0.08);
        }}
        th, td {{
            padding: 12px 14px;
            border-bottom: 1px solid #e2e8f0;
            text-align: left;
        }}
        th {{
            background-color: #0f172a;
            color: #ffffff;
            font-size: 13px;
            letter-spacing: 0.02em;
        }}
        .severity-high {{
            color: #dc2626;
            font-weight: 700;
        }}
        .severity-medium {{
            color: #f97316;
            font-weight: 700;
        }}
        .severity-low {{
            color: #64748b;
            font-weight: 700;
        }}
        .perfect {{
            color: #16a34a;
            font-weight: 700;
        }}
        .drift {{
            color: #ea580c;
            font-weight: 700;
        }}
        .empty {{
            padding: 16px;
            color: #64748b;
            font-style: italic;
            background: #ffffff;
            border-radius: 8px;
            border: 1px dashed #cbd5f5;
        }}
    </style>
</head>
<body>
    <section class="header">
        <h1>Standards Governance Report</h1>
        <p class="subtitle">Generated: {generated_at}</p>
        <p class="subtitle">Projects analyzed: {summary['projects']} ({project_list})</p>
        <div class="summary-grid">
            <div class="summary-card">
                <h3>Baseline Drift</h3>
                <p>{summary['baseline_drift']}</p>
            </div>
            <div class="summary-card">
                <h3>Unnecessary Overrides</h3>
                <p>{summary['unnecessary_overrides']}</p>
            </div>
            <div class="summary-card">
                <h3>Template Patterns</h3>
                <p>{summary['patterns']}</p>
            </div>
        </div>
    </section>

    <h2>Baseline Drift Violations ({len(baseline_violations)})</h2>
    {build_table(rows_baseline)}

    <h2>Unnecessary Template Overrides ({len(override_violations)})</h2>
    {build_table(rows_overrides)}

    <h2>Common Template Patterns ({len(patterns)})</h2>
    {build_pattern_table(rows_patterns)}
</body>
</html>
"""


def _row_template(project: str, category: str, severity: str, recommendation: str) -> str:
    return (
        "<tr>"
        f"<td>{project}</td>"
        f"<td>{category}</td>"
        f"<td class=\"severity-{severity}\">{severity.upper()}</td>"
        f"<td>{recommendation}</td>"
        "</tr>"
    )


def _pattern_row_template(pattern: Dict[str, Any]) -> str:
    if pattern["consistency"] == "perfect":
        detail = f"Sig: {pattern['sig_hash']}"
    else:
        detail = f"{pattern['variants']} variants"
    return (
        "<tr>"
        f"<td>{pattern['name']}</td>"
        f"<td>{pattern['projects']}</td>"
        f"<td class=\"{pattern['consistency']}\">{pattern['consistency'].upper()}</td>"
        f"<td>{detail}</td>"
        "</tr>"
    )


def build_table(rows: str) -> str:
    if not rows:
        return "<div class=\"empty\">No violations detected.</div>"
    return f"""<table>
        <tr>
            <th>Project</th>
            <th>Category</th>
            <th>Severity</th>
            <th>Recommendation</th>
        </tr>
        {rows}
    </table>"""


def build_pattern_table(rows: str) -> str:
    if not rows:
        return "<div class=\"empty\">No common patterns detected.</div>"
    return f"""<table>
        <tr>
            <th>Template Name</th>
            <th>Projects</th>
            <th>Consistency</th>
            <th>Details</th>
        </tr>
        {rows}
    </table>"""


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "Usage: python tools/governance/standards_governance_report.py "
            "<export1.json> <export2.json> ..."
        )
        sys.exit(1)

    analyzer = StandardsGovernanceAnalyzer()
    analyzer.load_exports(sys.argv[1:])

    print("Analyzing baseline drift...")
    analyzer.analyze_baseline_drift()

    print("Analyzing template overrides...")
    analyzer.analyze_template_overrides()

    print("Identifying common patterns...")
    analyzer.identify_common_patterns()

    print(f"Violations found: {len(analyzer.violations)}")
    print(f"Patterns identified: {len(analyzer.patterns)}")

    analyzer.generate_report()


if __name__ == "__main__":
    main()
