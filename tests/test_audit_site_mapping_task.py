from __future__ import annotations

import pandas as pd

from megaton_lib.audit.tasks.site_mapping import build_site_mapping_report


def test_build_site_mapping_report_with_aa() -> None:
    mapping = {
        "^/jp/": "JP",
        "^/en/": "EN",
    }
    ga4_data = pd.DataFrame(
        {
            "host": ["example.com", "example.com", "example.com"],
            "site": ["JP", "(not set)", "FR"],
            "sessions": [100, 10, 20],
        }
    )
    unclassified_pages = pd.DataFrame(
        {
            "host": ["example.com"],
            "path": ["/unknown"],
            "sessions": [10],
        }
    )
    aa_data = pd.DataFrame(
        {
            "site": ["JP", "EN"],
            "metric_value": [95, 50],
        }
    )

    report = build_site_mapping_report(
        mapping=mapping,
        ga4_data=ga4_data,
        unclassified_pages=unclassified_pages,
        aa_data=aa_data,
    )

    assert report["total_sessions"] == 130
    assert report["unclassified_sessions"] == 10
    assert "EN" in report["in_tag_no_ga4"]
    assert "FR" in report["in_ga4_no_tag"]
    assert "FR" in report["in_ga4_no_aa"]
    assert report["unclassified_top_pages"][0]["path"] == "/unknown"
