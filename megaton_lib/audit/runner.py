"""Audit runner orchestrating providers and tasks."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

from megaton_lib.audit.config import AuditProjectConfig
from megaton_lib.audit.providers.analytics import fetch_aa_site_metric, fetch_site_sessions, fetch_unclassified_pages
from megaton_lib.audit.providers.tag_config import fetch_adobe_tags_mapping, fetch_gtm_mapping
from megaton_lib.audit.reporters import write_dict_csv, write_report_json
from megaton_lib.audit.tasks import build_site_mapping_report, parse_mapping_markdown


class AuditRunner:
    """Run reusable audit tasks from one project config."""

    def __init__(self, config: AuditProjectConfig):
        self.config = config

    @staticmethod
    def _resolve_date_range(
        *,
        days: int,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> tuple[str, str]:
        if start_date and end_date:
            return start_date, end_date

        if days < 1:
            raise ValueError("days must be >= 1")

        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        return start.isoformat(), end.isoformat()

    def _fetch_tag_mapping(self) -> tuple[dict[str, str], dict[str, Any]]:
        source = self.config.tag_source.source
        if source == "gtm":
            if not self.config.tag_source.gtm:
                raise RuntimeError("tag_source.gtm config is missing")
            return fetch_gtm_mapping(self.config.tag_source.gtm)

        if source == "adobe_tags":
            if not self.config.tag_source.adobe_tags:
                raise RuntimeError("tag_source.adobe_tags config is missing")
            return fetch_adobe_tags_mapping(self.config.tag_source.adobe_tags)

        raise RuntimeError(f"Unsupported tag source: {source}")

    def _fetch_tag_mapping_with_fallback(self) -> tuple[dict[str, str], dict[str, Any]]:
        try:
            return self._fetch_tag_mapping()
        except Exception as exc:
            fallback = self.config.fallback_mapping_path
            if not fallback:
                raise
            path = Path(fallback)
            if not path.exists():
                raise RuntimeError(
                    f"Tag mapping fetch failed and fallback file is missing: {fallback}",
                ) from exc

            mapping = parse_mapping_markdown(str(path))
            if not mapping:
                raise RuntimeError(
                    f"Tag mapping fetch failed and fallback file is empty: {fallback}",
                ) from exc
            return mapping, {
                "provider": "fallback_markdown",
                "fallback_mapping_path": str(path),
                "mapping_count": len(mapping),
                "error": str(exc),
            }

    def export_tag_mapping(self, *, output_dir: str | Path) -> dict[str, Any]:
        """Export current tag mapping to output directory."""
        mapping, metadata = self._fetch_tag_mapping_with_fallback()
        payload = {
            "project_id": self.config.project_id,
            "run_date": date.today().isoformat(),
            "tag_source": self.config.tag_source.source,
            "metadata": metadata,
            "mapping": mapping,
        }
        artifact = write_report_json(payload, output_dir=output_dir, file_stem=f"tag_mapping_{self.config.project_id}")
        payload["artifacts"] = {"json": str(artifact)}
        return payload

    def run_site_mapping(
        self,
        *,
        days: int = 30,
        start_date: str | None = None,
        end_date: str | None = None,
        with_aa: bool = True,
        output_dir: str | Path | None = None,
    ) -> dict[str, Any]:
        """Run site mapping audit."""
        start, end = self._resolve_date_range(days=days, start_date=start_date, end_date=end_date)

        mapping, tag_metadata = self._fetch_tag_mapping_with_fallback()
        ga4_data = fetch_site_sessions(config=self.config.ga4, start_date=start, end_date=end)
        unclassified_pages = fetch_unclassified_pages(
            config=self.config.ga4,
            start_date=start,
            end_date=end,
            top_n=20,
        )

        aa_data = None
        if with_aa and self.config.aa is not None:
            aa_data = fetch_aa_site_metric(
                config=self.config.aa,
                start_date=start,
                end_date=end,
            )

        report = build_site_mapping_report(
            mapping=mapping,
            ga4_data=ga4_data,
            unclassified_pages=unclassified_pages,
            aa_data=aa_data,
        )
        report.update(
            {
                "project_id": self.config.project_id,
                "run_date": date.today().isoformat(),
                "period_start": start,
                "period_end": end,
                "days": days,
                "tag_source": self.config.tag_source.source,
                "tag_metadata": tag_metadata,
                "ga4_property_id": self.config.ga4.property_id,
                "aa_enabled": aa_data is not None,
                "aa_rsid": self.config.aa.rsid if self.config.aa else None,
            }
        )

        if output_dir:
            artifacts: dict[str, str] = {}
            json_path = write_report_json(
                report,
                output_dir=output_dir,
                file_stem=f"site_mapping_audit_{self.config.project_id}",
            )
            artifacts["json"] = str(json_path)
            if isinstance(report.get("site_sessions"), dict):
                sessions_csv = write_dict_csv(
                    report["site_sessions"],
                    output_dir=output_dir,
                    file_stem=f"site_sessions_{self.config.project_id}",
                    key_name="site",
                    value_name="sessions",
                )
                artifacts["site_sessions_csv"] = str(sessions_csv)
            if isinstance(report.get("aa_site_metric"), dict):
                aa_csv = write_dict_csv(
                    report["aa_site_metric"],
                    output_dir=output_dir,
                    file_stem=f"aa_site_metric_{self.config.project_id}",
                    key_name="site",
                    value_name="metric_value",
                )
                artifacts["aa_site_metric_csv"] = str(aa_csv)
            report["artifacts"] = artifacts

        return report
