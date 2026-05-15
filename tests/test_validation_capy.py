from __future__ import annotations

import io

import numpy as np
import pytest

from megaton_lib.validation.capy import (
    CapySolveResult,
    _largest_component,
    _read_png_bytes,
    is_capy_puzzle_present,
)


class _FakeLocator:
    def __init__(self, *, visible: bool, count: int = 1) -> None:
        self.first = self
        self._visible = visible
        self._count = count

    def count(self) -> int:
        return self._count

    def is_visible(self) -> bool:
        return self._visible


class _FakePage:
    def __init__(self, locator: _FakeLocator | Exception) -> None:
        self._locator = locator

    def locator(self, _selector: str):
        if isinstance(self._locator, Exception):
            raise self._locator
        return self._locator


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
