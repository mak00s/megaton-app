"""日付テンプレート解決

params.json の date_range.start / end に相対日付式を書けるようにする。

対応する式:
  today           → 実行日
  today-Nd        → N日前
  today+Nd        → N日後
  month-start     → 当月1日
  month-end       → 当月末日
  prev-month-start → 前月1日
  prev-month-end   → 前月末日
  week-start      → 今週月曜日（ISO: 月=0）
  YYYY-MM-DD      → そのまま（絶対日付はパススルー）
"""

from __future__ import annotations

import calendar
import os
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


# today±Nd パターン
_RELATIVE_RE = re.compile(r"^today([+-])(\d+)d$")
_DEFAULT_TZ = "Asia/Tokyo"


def _resolve_timezone() -> ZoneInfo:
    """DATE_TEMPLATE_TZ を解決（不正値は Asia/Tokyo にフォールバック）。"""
    tz_name = os.getenv("DATE_TEMPLATE_TZ", _DEFAULT_TZ).strip() or _DEFAULT_TZ
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo(_DEFAULT_TZ)


def _current_date_in_configured_tz() -> date:
    """現在日付を設定済みタイムゾーンで返す。"""
    return datetime.now(_resolve_timezone()).date()


def resolve_date(expr: str, *, reference: date | None = None) -> str:
    """日付テンプレート式を YYYY-MM-DD に解決する。

    Args:
        expr: 日付式（"today-7d", "prev-month-start", "2026-01-01" など）
        reference: 基準日（デフォルト: 実行日）

    Returns:
        "YYYY-MM-DD" 形式の文字列

    Raises:
        ValueError: 不明な日付式
    """
    ref = reference or _current_date_in_configured_tz()
    expr = expr.strip()

    # 絶対日付（YYYY-MM-DD）は実在チェックして返す
    if re.match(r"^\d{4}-\d{2}-\d{2}$", expr):
        try:
            datetime.strptime(expr, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid absolute date: '{expr}'") from e
        return expr

    if expr == "today":
        return ref.isoformat()

    m = _RELATIVE_RE.match(expr)
    if m:
        sign, days = m.group(1), int(m.group(2))
        delta = timedelta(days=days if sign == "+" else -days)
        return (ref + delta).isoformat()

    if expr == "month-start":
        return ref.replace(day=1).isoformat()

    if expr == "month-end":
        last_day = calendar.monthrange(ref.year, ref.month)[1]
        return ref.replace(day=last_day).isoformat()

    if expr == "prev-month-start":
        first = ref.replace(day=1)
        prev = first - timedelta(days=1)
        return prev.replace(day=1).isoformat()

    if expr == "prev-month-end":
        first = ref.replace(day=1)
        return (first - timedelta(days=1)).isoformat()

    if expr == "week-start":
        # ISO: Monday = 0
        return (ref - timedelta(days=ref.weekday())).isoformat()

    raise ValueError(
        f"Unknown date template: '{expr}'. "
        "Use today, today±Nd, month-start, month-end, "
        "prev-month-start, prev-month-end, week-start, or YYYY-MM-DD."
    )


def resolve_dates_in_params(params: dict) -> dict:
    """params dict 内の date_range.start / end をテンプレート解決する。

    元の dict は変更せず、新しい dict を返す。
    date_range がない場合（bigquery等）はそのまま返す。
    """
    date_range = params.get("date_range")
    if not date_range:
        return params

    start = date_range.get("start", "")
    end = date_range.get("end", "")

    resolved_start = resolve_date(start)
    resolved_end = resolve_date(end)

    if resolved_start == start and resolved_end == end:
        return params  # 変更なし

    new_params = dict(params)
    new_params["date_range"] = {
        "start": resolved_start,
        "end": resolved_end,
    }
    return new_params
