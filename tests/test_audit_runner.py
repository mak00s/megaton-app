from __future__ import annotations

import pandas as pd

from megaton_lib.audit.config import AuditProjectConfig, Ga4Config, GtmConfig, TagSourceConfig
from megaton_lib.audit.runner import AuditRunner


def test_run_site_mapping_with_monkeypatched_providers(monkeypatch) -> None:
    cfg = AuditProjectConfig(
        project_id="corp",
        tag_source=TagSourceConfig(source="gtm", gtm=GtmConfig(container_public_id="GTM-TEST")),
        ga4=Ga4Config(property_id="123"),
        aa=None,
    )

    runner = AuditRunner(cfg)

    monkeypatch.setattr(
        "megaton_lib.audit.runner.fetch_gtm_mapping",
        lambda _cfg: ({"^/jp/": "JP"}, {"provider": "gtm", "mapping_count": 1}),
    )
    monkeypatch.setattr(
        "megaton_lib.audit.runner.fetch_site_sessions",
        lambda **kwargs: pd.DataFrame(
            {
                "host": ["example.com", "example.com"],
                "site": ["JP", "(not set)"],
                "sessions": [100, 5],
            }
        ),
    )
    monkeypatch.setattr(
        "megaton_lib.audit.runner.fetch_unclassified_pages",
        lambda **kwargs: pd.DataFrame(
            {
                "host": ["example.com"],
                "path": ["/missing"],
                "sessions": [5],
            }
        ),
    )

    report = runner.run_site_mapping(days=7, with_aa=False)

    assert report["project_id"] == "corp"
    assert report["tag_source"] == "gtm"
    assert report["total_sessions"] == 105
    assert report["aa_enabled"] is False
    assert report["site_sessions"]["JP"] == 100
