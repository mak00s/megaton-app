"""Adobe Target Recommendations API provider.

Lazy-loaded submodules:

- ``client`` — ``AdobeTargetClient`` (authenticated HTTP)
- ``recs`` — ``export_recs``, ``apply_recs``
- ``feeds`` — ``export_feeds``
- ``getoffer_scope`` — ``detect_getoffer_scope``, ``export_getoffer_scope``
"""

from __future__ import annotations

__all__ = [
    "AdobeTargetClient",
    "export_recs",
    "apply_recs",
    "export_feeds",
    "detect_getoffer_scope",
    "export_getoffer_scope",
]

_LAZY_MAP: dict[str, str] = {
    "AdobeTargetClient": ".client",
    "export_recs": ".recs",
    "apply_recs": ".recs",
    "export_feeds": ".feeds",
    "detect_getoffer_scope": ".getoffer_scope",
    "export_getoffer_scope": ".getoffer_scope",
}


def __getattr__(name: str):
    mod_name = _LAZY_MAP.get(name)
    if mod_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    mod = importlib.import_module(mod_name, __name__)
    return getattr(mod, name)
