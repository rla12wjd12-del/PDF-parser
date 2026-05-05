# -*- coding: utf-8 -*-
"""
제1쪽(단일 페이지) 표 추출 미리보기: 가상 세로선별 시도·원시 표 그리드·선택 결과.

CLI/외부 도구에서 JSON·Excel로 내려보내기 위한 순수 데이터(dict) 생성.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from parsers.table_settings import extract_tables_merged, pick_best_table, safe_extract_tables

from parsers.experimental.page1_virtual_lines import (
    PAGE1_VIRTUAL_LEFT_X,
    PAGE1_VIRTUAL_RIGHT_X,
    PAGE1_VIRTUAL_RIGHT_X_ALT,
    page1_line_table_settings,
    page1_line_table_settings_alt,
)
from parsers.experimental.page1_table_extract import (
    merge_broken_rows,
    normalize_cell_text,
    normalize_table_rows,
    score_page1_table,
)


def _table_as_text_grid(table: List[Any] | None) -> List[List[str]]:
    """셀 단위 문자열 배열(JSON 직렬화용)."""
    if not table:
        return []
    out: List[List[str]] = []
    for row in table:
        if row is None:
            out.append([])
            continue
        out.append([normalize_cell_text(c) for c in row])
    return out


def _tables_as_text_grids(tables: List[Any]) -> List[List[List[str]]]:
    return [_table_as_text_grid(t) for t in (tables or [])]


def _collect_attempts(page: Any) -> List[Tuple[str, List[Any]]]:
    """표 추출 순서(primary 가상선 → alt 가상선 → 병합)와 동일하게 각 단계 결과를 적재."""
    steps: List[Tuple[str, List[Any]]] = []
    pri = safe_extract_tables(page, page1_line_table_settings()) or []
    steps.append(("virtual_primary", pri))
    alt = safe_extract_tables(page, page1_line_table_settings_alt()) or []
    steps.append(("virtual_alt", alt))
    merged = extract_tables_merged(page) or []
    steps.append(("merged", merged))
    return steps


def _chosen_strategy_and_tables(attempts: List[Tuple[str, List[Any]]]) -> Tuple[str, List[Any]]:
    for name, tbls in attempts:
        if tbls:
            return name, tbls
    return attempts[-1][0], attempts[-1][1]


def build_page1_table_preview_payload(
    *,
    pdf_path: str,
    page_index_zero_based: int,
    page: Any,
) -> Dict[str, Any]:
    """단일 페이지에 대해 가상선·표 그리드·best pick 등을 포함한 미리보기 dict."""
    width = getattr(page, "width", None)
    height = getattr(page, "height", None)
    attempts = _collect_attempts(page)
    chosen_name, chosen_tables = _chosen_strategy_and_tables(attempts)

    best_pick: List[List[str]] | None = None
    best_pick_index: int | None = None
    score_selected: Tuple[int, int] | None = None
    if chosen_tables:
        best_raw = pick_best_table(chosen_tables, score_page1_table)
        if best_raw:
            best_pick_index = next(
                (i for i, t in enumerate(chosen_tables) if t is best_raw),
                None,
            )
            best_pick = merge_broken_rows(normalize_table_rows(best_raw))
            score_selected = score_page1_table(best_raw)

    serialized_attempts: List[Dict[str, Any]] = []
    for name, tbls in attempts:
        grids = _tables_as_text_grids(tbls)
        serialized_attempts.append(
            {
                "strategy": name,
                "table_count": len(tbls),
                "tables_text_grid": grids,
                **(
                    {"per_table_shape": [[len(r) for r in g] for g in grids]}
                    if grids
                    else {}
                ),
            }
        )

    return {
        "meta": {
            "pdf_path": pdf_path,
            "page_index_zero_based": page_index_zero_based,
            "page_number": page_index_zero_based + 1,
            "page_size": {"width": width, "height": height},
            "virtual_vertical_lines_primary": [PAGE1_VIRTUAL_LEFT_X, PAGE1_VIRTUAL_RIGHT_X],
            "virtual_vertical_lines_alt": [PAGE1_VIRTUAL_LEFT_X, PAGE1_VIRTUAL_RIGHT_X_ALT],
            "chosen_extraction_strategy": chosen_name,
            "best_pick_table_index": best_pick_index,
            "best_pick_score": (
                {"header_hits": score_selected[0], "table_row_count_score": score_selected[1]}
                if score_selected
                else None
            ),
        },
        "extraction_attempts": serialized_attempts,
        "chosen_tables_text_grid": _tables_as_text_grids(chosen_tables),
        "best_table_normalized_rows": best_pick or [],
    }
