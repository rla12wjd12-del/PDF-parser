#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pdfplumber 표 추출 공통 설정.

가상 좌우 세로선(explicit_vertical_lines)과 lines 전략을 한곳에서 정의하고,
extract_tables 다중 시도(가상선+lines → text → 기본값) 결과를 병합한다.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Sequence, TypeVar

# 건설기술인 경력증명서 표준 폼 외곽 가상선(필요 시 오버라이드)
# - 황규철(2025.07.24) 원시 표에서 '참여기간/비고' 컬럼 분리 기준으로 스윕 검증
VIRTUAL_LEFT_X: float = 27.0
VIRTUAL_RIGHT_X: float = 560.0

# DocumentContext 캐시 무효화용(설정 변경 시 버전만 올리면 됨)
TABLE_SETTINGS_VERSION: str = "2026-04-explicit-vlines-v2"

_SNAP_TOLERANCE: int = 2
_JOIN_TOLERANCE: int = 3
_EDGE_MIN_LENGTH: int = 10

LINE_TABLE_SETTINGS: dict[str, Any] = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
    "snap_tolerance": _SNAP_TOLERANCE,
    "join_tolerance": _JOIN_TOLERANCE,
    "edge_min_length": _EDGE_MIN_LENGTH,
    "explicit_vertical_lines": [VIRTUAL_LEFT_X, VIRTUAL_RIGHT_X],
}

TEXT_TABLE_SETTINGS: dict[str, Any] = {
    "vertical_strategy": "text",
    "horizontal_strategy": "text",
    "snap_tolerance": _SNAP_TOLERANCE,
    "join_tolerance": _JOIN_TOLERANCE,
    "edge_min_length": _EDGE_MIN_LENGTH,
}


def safe_extract_tables(page: Any, settings: dict[str, Any] | None) -> List[List[Any]]:
    """pdfplumber page.extract_tables 예외·None 안전 래퍼."""
    try:
        if settings is None:
            return list(page.extract_tables() or [])
        return list(page.extract_tables(settings) or [])
    except Exception:
        return []


def extract_tables_merged(page: Any) -> List[List[Any]]:
    """
    가상 세로선+lines 우선 → text → 기본값 순으로 추출한 모든 테이블 행렬을 이어붙인다.

    상위 파서는 `for table in extract_tables_merged(page)`로 스캔하면 후보 테이블 풀을 얻는다.
    """
    a = safe_extract_tables(page, LINE_TABLE_SETTINGS)
    b = safe_extract_tables(page, TEXT_TABLE_SETTINGS)
    c = safe_extract_tables(page, None)
    return a + b + c


T = TypeVar("T")


def pick_best_table(
    tables: Sequence[List[Any]],
    score_fn: Callable[[List[Any]], tuple],
) -> Optional[List[Any]]:
    """점수가 가장 높은 단일 테이블을 고른다. 후보가 없으면 None."""
    best: Optional[List[Any]] = None
    best_key: tuple | None = None
    for tbl in tables or []:
        if not tbl:
            continue
        try:
            k = score_fn(tbl)
        except Exception:
            continue
        if best_key is None or k > best_key:
            best_key = k
            best = tbl
    return best


def table_set_has_header_signals(
    tables: Sequence[List[Any]] | None,
    keywords: Sequence[str],
    *,
    max_scan_rows: int = 45,
) -> bool:
    """
    병합된 테이블 목록 중 하나라도 키워드가 포함된 '헤더/제목' 행이 있으면 True.
    표 품질이 너무 낮을 때 텍스트 전용 분기로 넘기기 위한 게이트에 사용한다.
    """
    if not tables or not keywords:
        return False
    for table in tables:
        for row in (table or [])[:max_scan_rows]:
            row_text = " ".join(str(c) for c in (row or []) if c)
            if not row_text.strip():
                continue
            if any(kw in row_text for kw in keywords):
                return True
    return False
