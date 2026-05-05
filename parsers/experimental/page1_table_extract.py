# -*- coding: utf-8 -*-
"""
가상 세로선이 포함된 설정으로 pdfplumber 표를 추출하고, 행 단위 텍스트로 정규화한다.

core `page_1_parser`의 `_raw_tables_from_page` ~ `_collect_page1_flat_table_rows` 흐름과 동등.
"""

from __future__ import annotations

import re
from typing import Any, List, Tuple

from parsers.table_settings import extract_tables_merged, pick_best_table, safe_extract_tables
from parsers.document_context import DocumentContext

from parsers.experimental.page1_virtual_lines import (
    page1_line_table_settings,
    page1_line_table_settings_alt,
)


def _is_hangul_char(ch: str) -> bool:
    if not ch:
        return False
    code = ord(ch)
    return 0xAC00 <= code <= 0xD7A3


def _smart_concat(a: str, b: str) -> str:
    a = (a or "").strip()
    b = (b or "").strip()
    if not a:
        return b
    if not b:
        return a
    if _is_hangul_char(a[-1]) and _is_hangul_char(b[0]) and not a.endswith((")", " ")):
        return a + b
    return a + " " + b


def normalize_cell_text(cell: Any) -> str:
    if cell is None:
        return ""
    s = str(cell).replace("\u00a0", " ").replace("\u200b", "").replace("\ufeff", "")
    s = re.sub(r"[\t\v\f\r]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def normalize_table_rows(table: List[Any]) -> List[List[str]]:
    out: List[List[str]] = []
    for row in table or []:
        if row is None:
            continue
        nr = [normalize_cell_text(c) for c in row]
        if any(x for x in nr):
            out.append(nr)
    return out


def merge_broken_rows(rows: List[List[str]]) -> List[List[str]]:
    """앞열이 비고 마지막 셀만 채워진 행을 이전 행에 이어붙인다."""
    if not rows:
        return []
    out: List[List[str]] = []
    for row in rows:
        nonempty = [i for i, c in enumerate(row) if c and c.strip()]
        if (
            len(row) >= 2
            and nonempty == [len(row) - 1]
            and out
            and any(out[-1][i].strip() for i in range(len(out[-1])) if i < len(row))
        ):
            prev = out[-1]
            tail = row[-1].strip()
            if prev:
                j = len(prev) - 1
                while j >= 0 and not (prev[j] or "").strip():
                    j -= 1
                if j >= 0:
                    prev[j] = _smart_concat(prev[j], tail)
                else:
                    prev[-1] = tail
            continue
        out.append(list(row))
    return out


def _row_join_for_detection(row: List[str]) -> str:
    return " ".join((c or "").replace("\n", " ").strip() for c in row if c is not None)


def score_page1_table(table: List[Any]) -> Tuple[int, int]:
    if not table:
        return (0, 0)
    keys = (
        "인적사항",
        "성명",
        "등급",
        "국가기술자격",
        "학력",
        "졸업일",
        "교육훈련",
        "교육기간",
        "상훈",
        "수여일",
        "벌점",
        "제재",
        "근무처",
        "근무기간",
        "상호",
    )
    hits = 0
    for row in table[:120]:
        rt = _row_join_for_detection([normalize_cell_text(c) for c in (row or [])])
        hits += sum(1 for k in keys if k in rt)
    return (hits, len(table))


def raw_tables_from_page(page: Any) -> List[List[Any]]:
    settings = page1_line_table_settings()
    tables = safe_extract_tables(page, settings) or []
    if not tables:
        tables = safe_extract_tables(page, page1_line_table_settings_alt()) or []
    if not tables:
        tables = extract_tables_merged(page) or []
    return tables


def extract_page1_normalized_rows_for_page(page: Any) -> List[List[str]]:
    raw = raw_tables_from_page(page)
    if not raw:
        return []
    best = pick_best_table(raw, score_page1_table)
    if best:
        return merge_broken_rows(normalize_table_rows(best))
    acc: List[List[str]] = []
    for tbl in raw:
        acc.extend(merge_broken_rows(normalize_table_rows(tbl)))
    return acc


def collect_page1_flat_table_rows(ctx: DocumentContext, page_indices: List[int]) -> List[List[str]]:
    acc: List[List[str]] = []
    for idx in page_indices:
        page = ctx.get_page(idx)
        if page is None:
            continue
        acc.extend(extract_page1_normalized_rows_for_page(page))
    return acc
