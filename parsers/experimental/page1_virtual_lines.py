# -*- coding: utf-8 -*-
"""
제1쪽 표 추출용 좌·우 가상 세로선(explicit_vertical_lines).

요구사항: X=27, X=567 (일부 문서는 오른쪽을 560으로 재시도).
"""

from __future__ import annotations

from typing import Any

from parsers.table_settings import LINE_TABLE_SETTINGS

PAGE1_VIRTUAL_LEFT_X: float = 27.0
PAGE1_VIRTUAL_RIGHT_X: float = 567.0
PAGE1_VIRTUAL_RIGHT_X_ALT: float = 560.0


def page1_explicit_vertical_lines() -> list[float]:
    return [PAGE1_VIRTUAL_LEFT_X, PAGE1_VIRTUAL_RIGHT_X]


def page1_explicit_vertical_lines_alt() -> list[float]:
    return [PAGE1_VIRTUAL_LEFT_X, PAGE1_VIRTUAL_RIGHT_X_ALT]


def page1_line_table_settings() -> dict[str, Any]:
    return {**LINE_TABLE_SETTINGS, "explicit_vertical_lines": page1_explicit_vertical_lines()}


def page1_line_table_settings_alt() -> dict[str, Any]:
    return {**LINE_TABLE_SETTINGS, "explicit_vertical_lines": page1_explicit_vertical_lines_alt()}
