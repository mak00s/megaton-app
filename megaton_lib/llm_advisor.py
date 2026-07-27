"""LLM advisor providers with built-in call recording.

Shared chat-completion layer for projects that run scheduled LLM reviews
(minkabu advice など). Providers expose one duck-typed surface::

    advisor.name / .model / .temperature
    advisor.chat(*, system, user, max_tokens) -> str
    advisor.call_records  # list[dict] — one record per chat() call

Every ``chat()`` appends a call record capturing the exact prompts, raw
response, response/generation ID, resolved model, token usage, finish reason,
and error (if any). ``megaton_lib.llm_audit`` persists those records as a
portable audit trail; the two modules are independent but designed together.

SDKs are imported lazily — install ``anthropic`` / ``openai`` (or the
``megaton-app[llm]`` extra) only for the provider you use. ``ADVISOR_MODEL``
overrides the per-provider default model.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Protocol

logger = logging.getLogger(__name__)

DEFAULT_ADVISOR_TEMPERATURE = 0.1


class Advisor(Protocol):
    name: str

    def chat(self, *, system: str, user: str, max_tokens: int = 600) -> str: ...


# --- Call recording ----------------------------------------------------------


def serializable_mapping(value: object) -> dict:
    """Best-effort dict view of SDK usage objects (pydantic / to_dict / __dict__)."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        data = model_dump()
        return data if isinstance(data, dict) else {}
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        data = to_dict()
        return data if isinstance(data, dict) else {}
    data = getattr(value, "__dict__", None)
    return dict(data) if isinstance(data, dict) else {}


def start_call_record(
    advisor: object,
    *,
    system: str,
    user: str,
    max_tokens: int,
) -> dict:
    """Append a fresh call record to ``advisor.call_records`` and return it."""
    record = {
        "started_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "finished_at": "",
        "provider": str(getattr(advisor, "name", "") or ""),
        "requested_model": str(getattr(advisor, "model", "") or ""),
        "response_id": "",
        "response_model": "",
        "max_tokens": max_tokens,
        "temperature": getattr(advisor, "temperature", None),
        "system_prompt": system,
        "user_prompt": user,
        "raw_response": "",
        "usage": {},
        "finish_reason": "",
        "error": "",
    }
    records = getattr(advisor, "call_records", None)
    if records is None:
        records = []
        advisor.call_records = records
    records.append(record)
    return record


def finish_call_record(
    record: dict,
    *,
    response: object | None = None,
    raw_response: str = "",
    error: BaseException | None = None,
) -> None:
    """Fill a call record from an SDK response (Anthropic or OpenAI shape) or error."""
    record["finished_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    record["raw_response"] = raw_response
    if error is not None:
        record["error"] = f"{type(error).__name__}: {error}"[:1000]
        return
    record["response_id"] = str(getattr(response, "id", "") or "")
    record["response_model"] = str(getattr(response, "model", "") or "")
    record["usage"] = serializable_mapping(getattr(response, "usage", None))
    record["finish_reason"] = str(
        getattr(response, "stop_reason", "")
        or getattr(response, "finish_reason", "")
        or ""
    )
    choices = getattr(response, "choices", None)
    if not record["finish_reason"] and choices:
        record["finish_reason"] = str(getattr(choices[0], "finish_reason", "") or "")


# --- Providers ---------------------------------------------------------------


class AnthropicAdvisor:
    name = "anthropic"
    _default_model = "claude-opus-5"

    def __init__(self, *, model: str | None = None, temperature: float = DEFAULT_ADVISOR_TEMPERATURE):
        self.model = model or os.environ.get("ADVISOR_MODEL") or self._default_model
        self.temperature = temperature
        self.call_records: list[dict] = []

    def chat(self, *, system: str, user: str, max_tokens: int = 600) -> str:
        record = start_call_record(self, system=system, user=user, max_tokens=max_tokens)
        try:
            import anthropic  # type: ignore
        except ImportError as exc:
            finish_call_record(record, error=exc)
            raise RuntimeError(
                "anthropic not installed; install megaton-app[llm] or anthropic"
            ) from exc

        try:
            client = anthropic.Anthropic()
            msg = client.messages.create(
                model=self.model,
                system=system,
                max_tokens=max_tokens,
                temperature=self.temperature,
                messages=[{"role": "user", "content": user}],
            )
            parts = [b.text for b in msg.content if getattr(b, "type", None) == "text"]
            raw = "".join(parts).strip()
            finish_call_record(record, response=msg, raw_response=raw)
            return raw
        except Exception as exc:
            finish_call_record(record, error=exc)
            raise


class OpenAIAdvisor:
    name = "openai"
    _default_model = "gpt-4o-mini"
    _base_url: str | None = None  # None → OpenAI default endpoint
    _api_key_env = "OPENAI_API_KEY"

    def __init__(self, *, model: str | None = None, temperature: float = DEFAULT_ADVISOR_TEMPERATURE):
        self.model = model or os.environ.get("ADVISOR_MODEL") or self._default_model
        self.temperature = temperature
        self.call_records: list[dict] = []

    def _client(self):
        try:
            from openai import OpenAI  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "openai not installed; install megaton-app[llm] or openai"
            ) from exc
        kwargs: dict = {}
        if self._base_url:
            kwargs["base_url"] = self._base_url
        api_key = os.environ.get(self._api_key_env)
        if api_key:
            kwargs["api_key"] = api_key
        return OpenAI(**kwargs)

    def chat(self, *, system: str, user: str, max_tokens: int = 600) -> str:
        record = start_call_record(self, system=system, user=user, max_tokens=max_tokens)
        try:
            client = self._client()
            resp = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                max_tokens=max_tokens,
                temperature=self.temperature,
            )
            raw = (resp.choices[0].message.content or "").strip()
            finish_call_record(record, response=resp, raw_response=raw)
            return raw
        except Exception as exc:
            finish_call_record(record, error=exc)
            raise


class OpenRouterAdvisor(OpenAIAdvisor):
    """OpenRouter via its OpenAI-compatible endpoint.

    Auth with ``OPENROUTER_API_KEY``; ``base_url`` is fixed so it never
    collides with a real OpenAI key/endpoint. ``ADVISOR_MODEL`` takes an
    OpenRouter slug (e.g. ``anthropic/claude-opus-5``, ``google/gemini-2.5-pro``).
    """

    name = "openrouter"
    _default_model = "anthropic/claude-opus-5"
    _base_url = "https://openrouter.ai/api/v1"
    _api_key_env = "OPENROUTER_API_KEY"


class DisabledAdvisor:
    """No-op advisor for runs without an API key configured."""

    name = "disabled"

    def chat(self, *, system: str, user: str, max_tokens: int = 600) -> str:
        return ""


# --- Factory ----------------------------------------------------------------


def get_advisor(
    provider: str,
    *,
    model: str | None = None,
    temperature: float = DEFAULT_ADVISOR_TEMPERATURE,
) -> Advisor:
    p = (provider or "disabled").lower()
    if p in ("disabled", "none", ""):
        return DisabledAdvisor()
    builders = {
        "anthropic": AnthropicAdvisor,
        "openai": OpenAIAdvisor,
        "openrouter": OpenRouterAdvisor,
    }
    builder = builders.get(p)
    if builder is None:
        raise ValueError(f"Unknown advisor provider: {provider}")
    # Construction can fail when a provider is selected but its SDK/key isn't
    # present (e.g. a scheduled job sets ADVISOR_PROVIDER but installs no AI
    # extra and passes no key). Degrade to disabled rather than crash the run.
    try:
        return builder(model=model, temperature=temperature)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[advisor] %s unavailable (%s); using disabled", p, exc)
        return DisabledAdvisor()
