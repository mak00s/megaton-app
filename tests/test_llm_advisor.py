"""llm_advisor: provider factory and call recording."""

from __future__ import annotations

from megaton_lib.llm_advisor import (
    DisabledAdvisor,
    OpenAIAdvisor,
    OpenRouterAdvisor,
    finish_call_record,
    get_advisor,
    serializable_mapping,
    start_call_record,
)


def test_get_advisor_disabled_variants():
    for provider in ("disabled", "none", "", None):
        assert isinstance(get_advisor(provider), DisabledAdvisor)


def test_get_advisor_unknown_provider_raises():
    try:
        get_advisor("nope")
    except ValueError as exc:
        assert "nope" in str(exc)
    else:
        raise AssertionError("unknown provider must raise")


def test_openrouter_advisor_config(monkeypatch):
    monkeypatch.delenv("ADVISOR_MODEL", raising=False)
    adv = get_advisor("openrouter")
    assert isinstance(adv, OpenRouterAdvisor)
    assert adv.name == "openrouter"
    assert adv._base_url == "https://openrouter.ai/api/v1"
    assert adv._api_key_env == "OPENROUTER_API_KEY"
    assert get_advisor("openrouter", model="google/gemini-2.5-pro").model == (
        "google/gemini-2.5-pro"
    )


def test_advisor_model_env_override(monkeypatch):
    monkeypatch.setenv("ADVISOR_MODEL", "vendor/custom-model")
    assert OpenAIAdvisor().model == "vendor/custom-model"


def test_call_record_captures_openai_shape_response(monkeypatch):
    class _Message:
        content = "{}"

    class _Choice:
        message = _Message()
        finish_reason = "stop"

    class _Resp:
        choices = [_Choice()]
        id = "gen-test"
        model = "resolved-model"
        usage = {"prompt_tokens": 7, "completion_tokens": 3}

    calls = {}

    class _Completions:
        @staticmethod
        def create(**kwargs):
            calls.update(kwargs)
            return _Resp()

    class _Chat:
        completions = _Completions()

    class _Client:
        chat = _Chat()

    adv = OpenAIAdvisor(model="test-model", temperature=0.2)
    monkeypatch.setattr(adv, "_client", lambda: _Client())

    raw = adv.chat(system="s", user="u", max_tokens=12)

    assert raw == "{}"
    assert calls["model"] == "test-model"
    assert calls["max_tokens"] == 12
    assert calls["temperature"] == 0.2
    record = adv.call_records[0]
    assert record["response_id"] == "gen-test"
    assert record["response_model"] == "resolved-model"
    assert record["system_prompt"] == "s"
    assert record["user_prompt"] == "u"
    assert record["raw_response"] == "{}"
    assert record["usage"]["prompt_tokens"] == 7
    assert record["finish_reason"] == "stop"


def test_call_record_captures_error(monkeypatch):
    adv = OpenAIAdvisor(model="test-model")

    def _boom():
        raise RuntimeError("no key")

    monkeypatch.setattr(adv, "_client", _boom)
    try:
        adv.chat(system="s", user="u")
    except RuntimeError:
        pass
    else:
        raise AssertionError("chat must re-raise")
    assert adv.call_records[0]["error"].startswith("RuntimeError: no key")
    assert adv.call_records[0]["finished_at"]


def test_start_call_record_creates_records_list_lazily():
    class _Bare:
        name = "x"
        model = "m"

    advisor = _Bare()
    record = start_call_record(advisor, system="s", user="u", max_tokens=5)
    assert advisor.call_records == [record]
    finish_call_record(record, response=None, raw_response="r")
    assert record["raw_response"] == "r"


def test_serializable_mapping_variants():
    class _Dump:
        def model_dump(self):
            return {"a": 1}

    class _Attr:
        def __init__(self):
            self.b = 2

    assert serializable_mapping(None) == {}
    assert serializable_mapping({"k": "v"}) == {"k": "v"}
    assert serializable_mapping(_Dump()) == {"a": 1}
    assert serializable_mapping(_Attr()) == {"b": 2}
