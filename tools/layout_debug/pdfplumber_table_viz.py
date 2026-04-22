from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _as_rect(x0: float, top: float, x1: float, bottom: float) -> Dict[str, float]:
    return {"x0": float(x0), "top": float(top), "x1": float(x1), "bottom": float(bottom)}


def find_tables_and_cells(page: Any, *, table_settings: Dict[str, Any] | None = None) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
    """
    pdfplumber page에서 테이블(선 기반/텍스트 기반) 감지 결과를 bbox 리스트로 반환한다.

    Returns:
        table_rects: 테이블 단위 bbox
        cell_rects: 셀 단위 bbox (가능한 경우)
    """
    settings = dict(table_settings or {})
    tables = []
    try:
        # pdfplumber 0.11+: page.find_tables(table_settings=...)
        tables = page.find_tables(table_settings=settings) or []
    except Exception:
        tables = []

    table_rects: List[Dict[str, float]] = []
    cell_rects: List[Dict[str, float]] = []

    for t in tables:
        bbox = getattr(t, "bbox", None)
        if bbox and isinstance(bbox, (list, tuple)) and len(bbox) == 4:
            x0, top, x1, bottom = bbox
            table_rects.append(_as_rect(x0, top, x1, bottom))

        cells = getattr(t, "cells", None)
        if not cells:
            continue
        for c in cells:
            if isinstance(c, dict):
                try:
                    cell_rects.append(_as_rect(c["x0"], c["top"], c["x1"], c["bottom"]))
                except Exception:
                    pass
            elif isinstance(c, (list, tuple)) and len(c) == 4:
                try:
                    x0, top, x1, bottom = c
                    cell_rects.append(_as_rect(x0, top, x1, bottom))
                except Exception:
                    pass

    return table_rects, cell_rects

