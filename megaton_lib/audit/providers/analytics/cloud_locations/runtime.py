"""Runtime helpers for Adobe Cloud Locations client setup."""

from __future__ import annotations

from megaton_lib.audit.providers.analytics.dw.runtime import build_adobe_auth

from .api import AdobeCloudLocationsClient


def build_cloud_locations_client(
    *,
    company_id: str,
    creds_file: str = "",
    client_id: str = "",
    client_secret: str = "",
    org_id: str = "",
    scopes: str = "",
    token_cache_file: str = "",
) -> AdobeCloudLocationsClient:
    """Build a Cloud Locations client from Adobe credential inputs."""
    auth = build_adobe_auth(
        creds_file=creds_file,
        client_id=client_id,
        client_secret=client_secret,
        org_id=org_id,
        scopes=scopes,
        token_cache_file=token_cache_file,
    )
    return AdobeCloudLocationsClient(auth=auth, company_id=company_id)
