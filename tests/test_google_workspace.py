from __future__ import annotations

from megaton_lib import google_workspace


def test_build_service_uses_prepared_credentials(monkeypatch):
    seen = {}

    def _build(api_name, version, **kwargs):
        seen.update({"api_name": api_name, "version": version, **kwargs})
        return {"service": api_name}

    monkeypatch.setattr(google_workspace, "build", _build)
    creds = object()

    service = google_workspace.build_service(
        "gmail",
        "v1",
        credentials=creds,
        static_discovery=False,
    )

    assert service == {"service": "gmail"}
    assert seen == {
        "api_name": "gmail",
        "version": "v1",
        "credentials": creds,
        "cache_discovery": False,
        "static_discovery": False,
    }
