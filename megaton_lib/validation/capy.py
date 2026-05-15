"""CAPY puzzle helpers for Playwright-based validation."""

from __future__ import annotations

from dataclasses import dataclass
import io
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from playwright.sync_api import Page
else:
    Page = Any


@dataclass(slots=True)
class CapySolveResult:
    """Result details from a CAPY puzzle solve attempt."""

    solved: bool
    component_size: int
    source_x: float
    source_y: float
    target_x: float
    target_y: float

    def __bool__(self) -> bool:
        return self.solved


def is_capy_puzzle_present(page: Page, *, selector: str = ".capy-captcha") -> bool:
    """Return True when a visible CAPY puzzle is present on the page."""
    try:
        locator = page.locator(selector).first
        return bool(locator.count() and locator.is_visible())
    except Exception:
        return False


def solve_capy_puzzle(
    page: Page,
    *,
    captcha_selector: str = ".capy-captcha",
    image_area_selector: str = '[id$="image-area"]',
    piece_selector: str = '[id$="pieces"] > div',
    timeout_ms: int = 10000,
    min_component_size: int = 500,
    drag_steps: int = 25,
    screenshot_settle_ms: int = 500,
    settle_ms: int = 1500,
    hole_predicate: Callable[[Any], Any] | None = None,
) -> CapySolveResult:
    """Solve the visible CAPY drag puzzle by locating the puzzle hole.

    By default, the hole is detected as a cream-colored region. Pass
    ``hole_predicate`` to supply a custom image mask for another CAPY theme.

    The function only manipulates the current Playwright page. It does not
    submit the surrounding form.
    """
    capy = page.locator(captcha_selector).first
    capy.wait_for(timeout=timeout_ms)
    if screenshot_settle_ms > 0:
        page.wait_for_timeout(screenshot_settle_ms)
    image_box = page.locator(image_area_selector).first.bounding_box()
    capy_box = capy.bounding_box()
    piece_box = page.locator(piece_selector).first.bounding_box()
    if not image_box or not capy_box or not piece_box:
        raise RuntimeError("CAPY puzzle elements were not measurable")

    image = _read_png_bytes(capy.screenshot())[:, :, :3].astype(int)
    main = image[:, : int(round(image_box["width"])), :]
    cream_hole = hole_predicate(main) if hole_predicate is not None else _default_hole_mask(main)
    component = _largest_component(cream_hole)
    if component is None or component[0] < min_component_size:
        raise RuntimeError(f"CAPY puzzle hole was not detected: {component}")

    component_size, x_min, y_min, x_max, y_max = component
    source_x = piece_box["x"] + piece_box["width"] / 2
    source_y = piece_box["y"] + piece_box["height"] / 2
    target_x = capy_box["x"] + (x_min + x_max) / 2
    target_y = capy_box["y"] + (y_min + y_max) / 2

    page.mouse.move(source_x, source_y)
    page.mouse.down()
    page.mouse.move(target_x, target_y, steps=drag_steps)
    page.mouse.up()
    if settle_ms > 0:
        page.wait_for_timeout(settle_ms)

    return CapySolveResult(
        solved=True,
        component_size=int(component_size),
        source_x=float(source_x),
        source_y=float(source_y),
        target_x=float(target_x),
        target_y=float(target_y),
    )


def _read_png_bytes(data: bytes) -> Any:
    try:
        import numpy as np
        from PIL import Image
    except ImportError as exc:
        raise ImportError(
            "CAPY puzzle solving requires numpy and Pillow. "
            "Install megaton-app[validation] or install numpy and Pillow.",
        ) from exc

    try:
        with Image.open(io.BytesIO(data)) as image:
            return np.array(image.convert("RGBA"))
    except Exception as exc:
        raise ValueError("CAPY screenshot is not readable PNG data") from exc


def _default_hole_mask(image: Any) -> Any:
    red = image[:, :, 0]
    green = image[:, :, 1]
    blue = image[:, :, 2]
    return (red > 220) & (green > 215) & (blue > 170) & ((red - blue) > 20) & (
        (green - blue) > 10
    )


def _largest_component(mask: Any) -> tuple[int, int, int, int, int] | None:
    import numpy as np

    height, width = mask.shape
    seen = np.zeros_like(mask, bool)
    best: tuple[int, int, int, int, int] | None = None

    for y_pos in range(height):
        for x_pos in range(width):
            if not mask[y_pos, x_pos] or seen[y_pos, x_pos]:
                continue

            stack = [(x_pos, y_pos)]
            seen[y_pos, x_pos] = True
            xs: list[int] = []
            ys: list[int] = []
            while stack:
                current_x, current_y = stack.pop()
                xs.append(current_x)
                ys.append(current_y)
                for next_x in (current_x - 1, current_x, current_x + 1):
                    for next_y in (current_y - 1, current_y, current_y + 1):
                        if next_x == current_x and next_y == current_y:
                            continue
                        if (
                            0 <= next_x < width
                            and 0 <= next_y < height
                            and mask[next_y, next_x]
                            and not seen[next_y, next_x]
                        ):
                            seen[next_y, next_x] = True
                            stack.append((next_x, next_y))

            component = (len(xs), min(xs), min(ys), max(xs), max(ys))
            if best is None or component[0] > best[0]:
                best = component

    return best
