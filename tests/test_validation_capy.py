from __future__ import annotations

import io

import numpy as np
import pytest

from megaton_lib.validation.capy import (
    CapyDragTargetError,
    CapySolveResult,
    _largest_component,
    _read_png_bytes,
    is_capy_answered,
    is_capy_puzzle_present,
    solve_capy_puzzle,
    wait_for_capy_answer,
)


class _FakeLocator:
    def __init__(self, *, visible: bool, count: int = 1) -> None:
        self.first = self
        self._visible = visible
        self._count = count
        self.value = ""

    def count(self) -> int:
        return self._count

    def is_visible(self) -> bool:
        return self._visible

    def get_attribute(self, name: str):
        if name == "value":
            return self.value
        return None


class _FakePage:
    def __init__(self, locator: _FakeLocator | Exception) -> None:
        self._locator = locator

    def locator(self, _selector: str):
        if isinstance(self._locator, Exception):
            raise self._locator
        return self._locator


class _FakeAnswerPage:
    def __init__(self, values: list[str | None]) -> None:
        self.values = values
        self.wait_calls: list[int] = []

    def query_selector(self, _selector: str):
        value = self.values[0] if len(self.values) == 1 else self.values.pop(0)
        if value is None:
            return None
        locator = _FakeLocator(visible=True)
        locator.value = value
        return locator

    def wait_for_timeout(self, timeout: int) -> None:
        self.wait_calls.append(timeout)


class _FakeMouse:
    def __init__(self) -> None:
        self.events: list[tuple[str, float | None, float | None]] = []

    def move(self, x: float, y: float, **_kwargs) -> None:
        self.events.append(("move", x, y))

    def down(self) -> None:
        self.events.append(("down", None, None))

    def up(self) -> None:
        self.events.append(("up", None, None))


class _FakeSolveLocator:
    def __init__(self, box: dict[str, float] | None) -> None:
        self.first = self
        self.box = box
        self.waited = False

    def wait_for(self, timeout: int) -> None:
        _ = timeout
        self.waited = True

    def bounding_box(self) -> dict[str, float] | None:
        return self.box

    def screenshot(self) -> bytes:
        return b"png"


class _FakeSequenceSolveLocator(_FakeSolveLocator):
    def __init__(self, boxes: list[dict[str, float] | None]) -> None:
        super().__init__(boxes[-1])
        self.boxes = boxes

    def bounding_box(self) -> dict[str, float] | None:
        if len(self.boxes) > 1:
            return self.boxes.pop(0)
        return self.boxes[0]


class _FakeSolvePage:
    def __init__(self) -> None:
        self.mouse = _FakeMouse()
        self.wait_calls: list[int] = []
        self.evaluate_calls: list[tuple[str, object]] = []
        self.drag_source_valid = True
        self.locators = {
            ".capy-captcha": _FakeSolveLocator({"x": 100, "y": 200, "width": 400, "height": 200}),
            '[id$="image-area"]': _FakeSolveLocator(
                {"x": 100, "y": 200, "width": 300, "height": 200},
            ),
            '[id$="pieces"] > div': _FakeSolveLocator(
                {"x": 420, "y": 230, "width": 80, "height": 80},
            ),
        }

    def locator(self, selector: str):
        return self.locators[selector]

    def evaluate(self, script: str, arg: object) -> object:
        self.evaluate_calls.append((script, arg))
        if isinstance(arg, dict):
            return {
                "ok": self.drag_source_valid,
                "hitTag": "DIV",
                "hitId": "capy-piece",
                "hitClass": "",
                "pieceRect": {"x": 420, "y": 230, "width": 80, "height": 80},
                "scroll": {"x": 0, "y": 180},
                "viewport": {"width": 1280, "height": 720},
            }
        return None

    def wait_for_timeout(self, timeout: int) -> None:
        self.wait_calls.append(timeout)


def test_capy_solve_result_bool_uses_solved_field() -> None:
    assert bool(
        CapySolveResult(
            solved=True,
            component_size=1,
            source_x=1,
            source_y=2,
            target_x=3,
            target_y=4,
        ),
    )
    assert CapySolveResult(
        solved=True,
        component_size=1,
        source_x=1,
        source_y=2,
        target_x=3,
        target_y=4,
        answered=True,
        answer_value_present=True,
    ).drag_performed
    assert not bool(
        CapySolveResult(
            solved=False,
            component_size=0,
            source_x=0,
            source_y=0,
            target_x=0,
            target_y=0,
        ),
    )


def test_is_capy_puzzle_present_handles_visible_missing_and_errors() -> None:
    assert is_capy_puzzle_present(_FakePage(_FakeLocator(visible=True))) is True
    assert is_capy_puzzle_present(_FakePage(_FakeLocator(visible=False))) is False
    assert is_capy_puzzle_present(_FakePage(_FakeLocator(visible=True, count=0))) is False
    assert is_capy_puzzle_present(_FakePage(RuntimeError("detached"))) is False


def test_is_capy_answered_checks_non_empty_non_null_value() -> None:
    assert is_capy_answered(_FakeAnswerPage(["answered-token"])) is True
    assert is_capy_answered(_FakeAnswerPage(["null"])) is False
    assert is_capy_answered(_FakeAnswerPage([""])) is False
    assert is_capy_answered(_FakeAnswerPage([None])) is False


def test_wait_for_capy_answer_polls_until_answer_value() -> None:
    page = _FakeAnswerPage(["null", "", "answered-token"])

    assert wait_for_capy_answer(page, timeout_ms=1000, poll_ms=25) is True
    assert page.wait_calls == [25, 25]


def test_read_png_bytes_decodes_rgba_png() -> None:
    Image = pytest.importorskip("PIL.Image")
    image = Image.new("RGBA", (2, 2), (10, 20, 30, 255))
    image.putpixel((1, 1), (200, 210, 220, 128))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")

    decoded = _read_png_bytes(buffer.getvalue())

    assert decoded.shape == (2, 2, 4)
    np.testing.assert_array_equal(decoded[0, 0], np.array([10, 20, 30, 255]))
    np.testing.assert_array_equal(decoded[1, 1], np.array([200, 210, 220, 128]))


def test_largest_component_returns_largest_bbox() -> None:
    mask = np.zeros((6, 6), dtype=bool)
    mask[0, 0] = True
    mask[2:5, 1:4] = True
    mask[5, 5] = True

    assert _largest_component(mask) == (9, 1, 2, 3, 4)


def test_solve_capy_centers_puzzle_before_calculating_drag(monkeypatch) -> None:
    import megaton_lib.validation.capy as mod

    page = _FakeSolvePage()
    monkeypatch.setattr(mod, "_read_png_bytes", lambda _data: np.zeros((200, 400, 3), dtype=int))
    monkeypatch.setattr(mod, "_largest_component", lambda _mask: (1200, 80, 40, 120, 80))

    result = solve_capy_puzzle(page, screenshot_settle_ms=0, settle_ms=0, drag_steps=3)

    assert page.evaluate_calls[0][1] == ".capy-captcha"
    assert page.wait_calls == [100]
    assert result.source_x == 460
    assert result.source_y == 270
    assert result.target_x == 200
    assert result.target_y == 260
    assert result.answered is False
    assert result.answer_value_present is False
    assert page.mouse.events == [
        ("move", 460, 270),
        ("down", None, None),
        ("move", 200, 260),
        ("up", None, None),
    ]


def test_solve_capy_waits_until_puzzle_geometry_is_measurable(monkeypatch) -> None:
    import megaton_lib.validation.capy as mod

    page = _FakeSolvePage()
    page.locators['[id$="image-area"]'] = _FakeSequenceSolveLocator(
        [
            None,
            {"x": 100, "y": 200, "width": 300, "height": 200},
        ],
    )
    page.locators['[id$="pieces"] > div'] = _FakeSequenceSolveLocator(
        [
            {"x": 0, "y": 0, "width": 0, "height": 0},
            {"x": 420, "y": 230, "width": 80, "height": 80},
        ],
    )
    monkeypatch.setattr(mod, "_read_png_bytes", lambda _data: np.zeros((200, 400, 3), dtype=int))
    monkeypatch.setattr(mod, "_largest_component", lambda _mask: (1200, 80, 40, 120, 80))

    result = solve_capy_puzzle(
        page,
        timeout_ms=1000,
        screenshot_settle_ms=0,
        settle_ms=0,
        drag_steps=3,
    )

    assert page.wait_calls == [100, 100]
    assert result.solved is True
    assert page.mouse.events[-2:] == [("move", 200, 260), ("up", None, None)]


def test_solve_capy_raises_with_diagnostics_when_drag_source_misses_piece(monkeypatch) -> None:
    import megaton_lib.validation.capy as mod

    page = _FakeSolvePage()
    page.drag_source_valid = False
    monkeypatch.setattr(mod, "_read_png_bytes", lambda _data: np.zeros((200, 400, 3), dtype=int))
    monkeypatch.setattr(mod, "_largest_component", lambda _mask: (1200, 80, 40, 120, 80))

    with pytest.raises(CapyDragTargetError) as exc_info:
        solve_capy_puzzle(page, screenshot_settle_ms=0, settle_ms=0, drag_steps=3)

    assert exc_info.value.diagnostics["source"] == {"x": 460.0, "y": 270.0}
    assert exc_info.value.diagnostics["target"] == {"x": 200.0, "y": 260.0}
    assert exc_info.value.diagnostics["scroll"] == {"x": 0, "y": 180}
    assert page.mouse.events == []
