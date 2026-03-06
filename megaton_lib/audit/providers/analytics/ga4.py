"""GA4 provider for audit tasks."""

from __future__ import annotations

import pandas as pd

from megaton_lib.audit.config import Ga4Config
from megaton_lib.megaton_client import query_ga4


def fetch_site_sessions(*, config: Ga4Config, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch GA4 site x host sessions for audit."""
    df = query_ga4(
        property_id=config.property_id,
        start_date=start_date,
        end_date=end_date,
        dimensions=[
            (config.host_dimension, "host"),
            (config.site_dimension, "site"),
        ],
        metrics=[(config.sessions_metric, "sessions")],
    )
    if df.empty:
        return pd.DataFrame(columns=["host", "site", "sessions"])

    out = df.copy()
    out["host"] = out.get("host", "").fillna("").astype(str)
    out["site"] = out.get("site", "").fillna("").astype(str)
    out["sessions"] = pd.to_numeric(out.get("sessions", 0), errors="coerce").fillna(0).astype(int)
    return out[["host", "site", "sessions"]]


def fetch_unclassified_pages(
    *,
    config: Ga4Config,
    start_date: str,
    end_date: str,
    top_n: int = 20,
) -> pd.DataFrame:
    """Fetch top pages where site dimension is not set."""
    df = query_ga4(
        property_id=config.property_id,
        start_date=start_date,
        end_date=end_date,
        dimensions=[
            (config.host_dimension, "host"),
            (config.page_dimension, "path"),
        ],
        metrics=[(config.sessions_metric, "sessions")],
        filter_d=f"{config.site_dimension}==(not set)",
        limit=max(1000, top_n * 5),
    )
    if df.empty:
        return pd.DataFrame(columns=["host", "path", "sessions"])

    out = df.copy()
    out["host"] = out.get("host", "").fillna("").astype(str)
    out["path"] = out.get("path", "").fillna("").astype(str)
    out["sessions"] = pd.to_numeric(out.get("sessions", 0), errors="coerce").fillna(0).astype(int)
    out = out.sort_values("sessions", ascending=False).head(top_n)
    return out[["host", "path", "sessions"]].reset_index(drop=True)
