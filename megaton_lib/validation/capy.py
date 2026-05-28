"""CAPY puzzle helpers for Playwright-based validation."""

from __future__ import annotations

from dataclasses import dataclass
import io
import time
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
    answered: bool = False
    answer_value_present: bool = False

    @property
    def drag_performed(self) -> bool:
        """Return whether the drag action itself was completed."""
        return self.solved

    def __bool__(self) -> bool:
        return self.solved


class CapyDragTargetError(RuntimeError):
    """Raised when the calculated CAPY drag start does not hit the puzzle piece."""

    def __init__(self, message: str, diagnostics: dict[str, Any]) -> None:
        super().__init__(f"{message}: {diagnostics}")
        self.diagnostics = diagnostics


def is_capy_puzzle_present(page: Page, *, selector: str = ".capy-captcha") -> bool:
    """Return True when a visible CAPY puzzle is present on the page."""
    try:
        locator = page.locator(selector).first
        return bool(locator.count() and locator.is_visible())
    except Exception:
        return False


def is_capy_answered(page: Page, *, answer_selector: str = "input[name='capy_answer']") -> bool:
    """Return True when CAPY has written a non-empty answer token."""
    try:
        answer = page.query_selector(answer_selector)
        if answer is None:
            return False
        value = (answer.get_attribute("value") or "").strip().lower()
        return bool(value and value != "null")
    except RuntimeError:
        raise
    except Exception:
        return False


def wait_for_capy_answer(
    page: Page,
    *,
    timeout_ms: int = 5000,
    poll_ms: int = 100,
    answer_selector: str = "input[name='capy_answer']",
) -> bool:
    """Wait until CAPY writes an answer token and return whether it appeared."""
    if is_capy_answered(page, answer_selector=answer_selector):
        return True

    deadline = time.monotonic() + (timeout_ms / 1000)
    while time.monotonic() < deadline:
        sleep_ms = min(poll_ms, max(0, int((deadline - time.monotonic()) * 1000)))
        if sleep_ms > 0:
            try:
                page.wait_for_timeout(sleep_ms)
            except RuntimeError:
                raise
            except Exception:
                time.sleep(sleep_ms / 1000)
        if is_capy_answered(page, answer_selector=answer_selector):
            return True
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
    _scroll_capy_to_viewport_center(page, captcha_selector, timeout_ms=timeout_ms)
    if screenshot_settle_ms > 0:
        page.wait_for_timeout(screenshot_settle_ms)
    capy_box, image_box, piece_box = _wait_for_capy_puzzle_boxes(
        page,
        capy=capy,
        image_area_selector=image_area_selector,
        piece_selector=piece_selector,
        timeout_ms=timeout_ms,
    )

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

    _validate_drag_source(
        page,
        piece_selector=piece_selector,
        source_x=source_x,
        source_y=source_y,
        target_x=target_x,
        target_y=target_y,
        capy_box=capy_box,
        image_box=image_box,
        piece_box=piece_box,
    )

    page.mouse.move(source_x, source_y)
    page.mouse.down()
    page.mouse.move(target_x, target_y, steps=drag_steps)
    page.mouse.up()
    if settle_ms > 0:
        page.wait_for_timeout(settle_ms)
    answer_value_present = is_capy_answered(page)

    return CapySolveResult(
        solved=True,
        component_size=int(component_size),
        source_x=float(source_x),
        source_y=float(source_y),
        target_x=float(target_x),
        target_y=float(target_y),
        answered=answer_value_present,
        answer_value_present=answer_value_present,
    )


def _wait_for_capy_puzzle_boxes(
    page: Page,
    *,
    capy: Any,
    image_area_selector: str,
    piece_selector: str,
    timeout_ms: int,
    poll_ms: int = 100,
) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    """Wait until CAPY has finished loading and all drag geometry is measurable."""
    deadline = time.monotonic() + (timeout_ms / 1000)
    last: dict[str, Any] = {}

    while True:
        try:
            capy_box = capy.bounding_box()
            image_box = page.locator(image_area_selector).first.bounding_box()
            piece_box = page.locator(piece_selector).first.bounding_box()
        except RuntimeError:
            raise
        except Exception as exc:
            capy_box = image_box = piece_box = None
            last = {"error": str(exc)}

        if _is_measurable_box(capy_box) and _is_measurable_box(image_box) and _is_measurable_box(
            piece_box,
        ):
            return capy_box, image_box, piece_box

        last.update(
            {
                "capy_box": _box_diagnostics(capy_box),
                "image_box": _box_diagnostics(image_box),
                "piece_box": _box_diagnostics(piece_box),
            },
        )
        remaining_ms = int((deadline - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            raise RuntimeError(f"CAPY puzzle elements were not measurable: {last}")
        wait_ms = min(poll_ms, remaining_ms)
        try:
            page.wait_for_timeout(wait_ms)
        except RuntimeError:
            raise
        except Exception:
            time.sleep(wait_ms / 1000)


def _is_measurable_box(box: Any) -> bool:
    return bool(
        isinstance(box, dict)
        and float(box.get("width") or 0) > 0
        and float(box.get("height") or 0) > 0
    )


def _box_diagnostics(box: Any) -> dict[str, float] | None:
    if not isinstance(box, dict):
        return None
    return {
        "x": float(box.get("x") or 0),
        "y": float(box.get("y") or 0),
        "width": float(box.get("width") or 0),
        "height": float(box.get("height") or 0),
    }


def _validate_drag_source(
    page: Page,
    *,
    piece_selector: str,
    source_x: float,
    source_y: float,
    target_x: float,
    target_y: float,
    capy_box: dict[str, float],
    image_box: dict[str, float],
    piece_box: dict[str, float],
) -> None:
    diagnostics = {
        "source": {"x": source_x, "y": source_y},
        "target": {"x": target_x, "y": target_y},
        "capy_box": capy_box,
        "image_box": image_box,
        "piece_box": piece_box,
    }
    try:
        hit = page.evaluate(
            """
            ({ pieceSelector, sourceX, sourceY }) => {
              const piece = document.querySelector(pieceSelector);
              const element = document.elementFromPoint(sourceX, sourceY);
              const rect = piece ? piece.getBoundingClientRect() : null;
              return {
                ok: Boolean(piece && element && (element === piece || piece.contains(element))),
                hitTag: element ? element.tagName : null,
                hitId: element ? element.id : null,
                hitClass: element ? String(element.className || "") : null,
                pieceRect: rect ? {
                  x: rect.x,
                  y: rect.y,
                  width: rect.width,
                  height: rect.height
                } : null,
                scroll: { x: window.scrollX, y: window.scrollY },
                viewport: { width: window.innerWidth, height: window.innerHeight }
              };
            }
            """,
            {"pieceSelector": piece_selector, "sourceX": source_x, "sourceY": source_y},
        )
    except RuntimeError:
        raise
    except Exception as exc:
        diagnostics["error"] = str(exc)
        raise CapyDragTargetError("CAPY drag source could not be validated", diagnostics) from exc

    diagnostics.update(hit or {})
    if not isinstance(hit, dict) or not hit.get("ok"):
        raise CapyDragTargetError("CAPY drag source is not on the puzzle piece", diagnostics)


def _scroll_capy_to_viewport_center(page: Page, selector: str, *, timeout_ms: int) -> None:
    try:
        page.evaluate(
            """
            (captchaSelector) => {
              const element = document.querySelector(captchaSelector);
              if (element) {
                element.scrollIntoView({ block: "center", inline: "center" });
              }
            }
            """,
            selector,
        )
        page.wait_for_timeout(100)
        return
    except RuntimeError:
        raise
    except Exception:
        pass

    try:
        page.locator(selector).first.scroll_into_view_if_needed(timeout=timeout_ms)
    except RuntimeError:
        raise
    except Exception:
        return


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
