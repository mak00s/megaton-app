"""Site mapping audit helpers.

Cross-check tag-config mapping definitions against GA4/AA observed values.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from pathlib import Path
import re
from typing import Any

import pandas as pd


def parse_mapping_markdown(
    md_path: str | Path,
    *,
    allowed_sections: Collection[str] | None = None,
) -> dict[str, str]:
    """Parse markdown table that stores path-pattern to site-name mapping."""
    mapping: dict[str, str] = {}
    text = Path(md_path).read_text(encoding="utf-8")
    active_section = allowed_sections is None

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("### "):
            active_section = allowed_sections is None or stripped in allowed_sections
            continue
        if "未分類" in stripped and stripped.startswith("#"):
            break
        if not active_section or not stripped.startswith("|") or stripped.startswith("|---"):
            continue

        protected = re.sub(r"`[^`]+`", lambda m: m.group().replace("|", "\x00"), stripped)
        protected = protected.replace(r"\|", "\x00")
        cols = [
            c.strip().strip("`").replace("\x00", "|").replace(r"\|", "|")
            for c in protected.split("|")
        ]
        cols = [c for c in cols if c]
        if len(cols) < 2:
            continue
        if cols[0] in {"URL パターン", "ホスト / パス"}:
            continue
        mapping[cols[0]] = cols[1]

    return mapping


def build_site_mapping_report(
    *,
    mapping: Mapping[str, str],
    ga4_data: pd.DataFrame,
    unclassified_pages: pd.DataFrame,
    aa_data: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Build normalized site-mapping audit report payload."""
    ga4 = ga4_data.copy()
    if ga4.empty:
        ga4 = pd.DataFrame(columns=["host", "site", "sessions"])

    ga4["site"] = ga4.get("site", "").fillna("").astype(str)
    ga4["sessions"] = pd.to_numeric(ga4.get("sessions", 0), errors="coerce").fillna(0).astype(int)

    site_sessions = (
        ga4.groupby("site", dropna=False)["sessions"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )

    unclassified_hosts = (
        ga4[ga4["site"].isin(["", "(not set)"])]
        .groupby("host", dropna=False)["sessions"]
        .sum()
        .sort_values(ascending=False)
        .to_dict()
    )

    total_sessions = int(sum(site_sessions.values()))
    unclassified_sessions = int(site_sessions.get("(not set)", 0) + site_sessions.get("", 0))

    tag_site_names = {
        v.strip()
        for k, v in mapping.items()
        if isinstance(k, str)
        and isinstance(v, str)
        and v.strip()
        and k != "__default__"
    }
    ga4_site_names = {
        site.strip()
        for site in site_sessions.keys()
        if isinstance(site, str) and site.strip() and site != "(not set)"
    }

    pages_df = unclassified_pages if unclassified_pages is not None else pd.DataFrame()

    report: dict[str, Any] = {
        "total_sessions": total_sessions,
        "unclassified_sessions": unclassified_sessions,
        "unclassified_pct": round((unclassified_sessions / total_sessions) * 100, 1) if total_sessions else 0.0,
        "mapping_count": len(mapping),
        "tag_site_count": len(tag_site_names),
        "ga4_site_count": len(ga4_site_names),
        "site_sessions": site_sessions,
        "in_tag_no_ga4": sorted(tag_site_names - ga4_site_names),
        "in_ga4_no_tag": sorted(ga4_site_names - tag_site_names),
        "unclassified_hosts": unclassified_hosts,
        "unclassified_top_pages": pages_df.to_dict("records"),
    }

    if aa_data is not None:
        aa = aa_data.copy()
        if aa.empty:
            aa = pd.DataFrame(columns=["site", "metric_value"])
        aa["site"] = aa.get("site", "").fillna("").astype(str)
        aa["metric_value"] = pd.to_numeric(aa.get("metric_value", 0), errors="coerce").fillna(0.0)

        aa_site_metric = (
            aa.groupby("site", dropna=False)["metric_value"]
            .sum()
            .sort_values(ascending=False)
            .to_dict()
        )
        aa_site_names = {
            site.strip()
            for site in aa_site_metric.keys()
            if isinstance(site, str) and site.strip() and site != "(none)"
        }

        report.update(
            {
                "aa_site_count": len(aa_site_names),
                "aa_site_metric": aa_site_metric,
                "in_tag_no_aa": sorted(tag_site_names - aa_site_names),
                "in_aa_no_tag": sorted(aa_site_names - tag_site_names),
                "in_ga4_no_aa": sorted(ga4_site_names - aa_site_names),
                "in_aa_no_ga4": sorted(aa_site_names - ga4_site_names),
            }
        )

    return report
