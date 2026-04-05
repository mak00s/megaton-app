"""Analytics data providers for audits."""

from __future__ import annotations


def __getattr__(name: str):
    if name in {"AdobeAnalyticsClient", "fetch_aa_site_metric"}:
        from .aa import AdobeAnalyticsClient, fetch_aa_site_metric

        return {
            "AdobeAnalyticsClient": AdobeAnalyticsClient,
            "fetch_aa_site_metric": fetch_aa_site_metric,
        }[name]
    if name in {"fetch_site_sessions", "fetch_unclassified_pages"}:
        from .ga4 import fetch_site_sessions, fetch_unclassified_pages

        return {
            "fetch_site_sessions": fetch_site_sessions,
            "fetch_unclassified_pages": fetch_unclassified_pages,
        }[name]
    if name in {"ClassificationsClient", "print_verify_results"}:
        from .classifications import ClassificationsClient, print_verify_results

        return {
            "ClassificationsClient": ClassificationsClient,
            "print_verify_results": print_verify_results,
        }[name]
    if name in {"AdobeDataWarehouseClient"}:
        from .dw import AdobeDataWarehouseClient

        return {
            "AdobeDataWarehouseClient": AdobeDataWarehouseClient,
        }[name]
    raise AttributeError(name)

__all__ = [
    "AdobeAnalyticsClient",
    "AdobeDataWarehouseClient",
    "ClassificationsClient",
    "print_verify_results",
    "fetch_aa_site_metric",
    "fetch_site_sessions",
    "fetch_unclassified_pages",
]
