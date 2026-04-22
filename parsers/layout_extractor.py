#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
좌표(레이아웃) 기반 텍스트 추출 유틸.

목표
- `extract_text()` 줄바꿈 품질에 덜 의존하도록, word bbox를 이용해 라인 스트림/간이 테이블을 재구성한다.
- PyMuPDF(fitz)가 있으면 우선 사용하고, 없으면 pdfplumber.extract_words()로 폴백한다.

주의
- 파서 출력 스키마(JSON/Excel)는 여기서 다루지 않는다. (추출 레이어만 제공)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence

import re


@dataclass(frozen=True)
class Word:
    text: str
    x0: float
    x1: float
    top: float
    bottom: float


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _fitz_page_to_words(fitz_page: Any) -> List[Word]:
    """fitz page 객체에서 Word 리스트를 추출하는 공통 변환 로직."""
    ws = fitz_page.get_text("words") or []
    out: list[Word] = []
    for w in ws:
        if not w or len(w) < 5:
            continue
        x0, y0, x1, y1, txt = w[:5]
        t = _norm_space(str(txt or ""))
        if not t:
            continue
        out.append(
            Word(
                text=t,
                x0=_safe_float(x0),
                x1=_safe_float(x1),
                top=_safe_float(y0),
                bottom=_safe_float(y1),
            )
        )
    return out


def extract_words_from_fitz_doc(fitz_doc: Any, page_num: int) -> List[Word]:
    """
    이미 열린 fitz.Document에서 word 추출. open/close를 수행하지 않는다.
    DocumentContext가 핸들을 소유할 때 사용해 open/close 반복을 방지한다.
    """
    if page_num < 0 or page_num >= fitz_doc.page_count:
        return []
    page = fitz_doc.load_page(page_num)
    return _fitz_page_to_words(page)


def extract_words_from_pymupdf(pdf_path: str, page_num: int) -> List[Word]:
    """
    PyMuPDF 기반 word 추출 (경로 기반, 핸들 없을 때 사용).
    - page.get_text("words")는 (x0, y0, x1, y1, "word", block, line, word_no) 튜플을 반환한다.
    - y0/y1는 상단 기준(0=위) 좌표계라서, pdfplumber의 top/bottom 의미와 동일하게 취급할 수 있다.
    """
    import fitz  # type: ignore

    doc = fitz.open(pdf_path)
    try:
        if page_num < 0 or page_num >= doc.page_count:
            return []
        page = doc.load_page(page_num)
        return _fitz_page_to_words(page)
    finally:
        doc.close()


def extract_words_from_pdfplumber_page(page: Any) -> List[Word]:
    """
    pdfplumber page 객체에서 extract_words 결과를 Word 리스트로 정규화한다.
    """
    try:
        raw = page.extract_words(
            keep_blank_chars=False,
            use_text_flow=True,
            extra_attrs=["top", "bottom", "x0", "x1"],
        ) or []
    except Exception:
        raw = []
    out: list[Word] = []
    for w in raw:
        t = _norm_space(str((w or {}).get("text") or ""))
        if not t:
            continue
        out.append(
            Word(
                text=t,
                x0=_safe_float((w or {}).get("x0")),
                x1=_safe_float((w or {}).get("x1")),
                top=_safe_float((w or {}).get("top")),
                bottom=_safe_float((w or {}).get("bottom")),
            )
        )
    return out


def extract_words(
    *,
    pdf_path: Optional[str] = None,
    page_num: Optional[int] = None,
    pdfplumber_page: Any | None = None,
    engine: str = "auto",
    fitz_doc: Any | None = None,
) -> List[Word]:
    """
    word 추출 통합 엔트리.

    Args:
        pdf_path/page_num: PyMuPDF 경로 기반 추출에 필요
        pdfplumber_page: 이미 열린 pdfplumber page가 있으면 폴백으로 사용 가능
        engine: "auto" | "pymupdf" | "pdfplumber"
        fitz_doc: 이미 열린 fitz.Document (있으면 재사용해 open/close 반복 방지)
    """
    eng = (engine or "auto").lower().strip()
    if eng not in {"auto", "pymupdf", "pdfplumber"}:
        eng = "auto"

    if eng in {"auto", "pymupdf"} and page_num is not None:
        # 열린 핸들 우선 재사용
        if fitz_doc is not None:
            try:
                return extract_words_from_fitz_doc(fitz_doc, page_num)
            except Exception:
                if eng == "pymupdf":
                    return []
        elif pdf_path is not None:
            try:
                return extract_words_from_pymupdf(pdf_path, page_num)
            except Exception:
                if eng == "pymupdf":
                    return []

    if pdfplumber_page is not None:
        return extract_words_from_pdfplumber_page(pdfplumber_page)
    return []


def words_to_lines(words: Sequence[Word], *, y_tolerance: float = 2.0, join_gap: float = 1.0) -> List[str]:
    """
    word bbox를 이용해 라인 스트림을 재구성한다.
    - 같은 라인 판정: |top - cur_top| <= y_tolerance
    - 같은 라인 내 좌→우 정렬 후, x0-prev_x1이 join_gap 이하이면 공백 없이 붙인다(글자 중간 분절 보정)
    """
    if not words:
        return []

    ws = sorted(words, key=lambda w: (w.top, w.x0))
    grouped: list[list[Word]] = []
    cur: list[Word] = []
    cur_top: float | None = None

    for w in ws:
        t = _norm_space(w.text)
        if not t:
            continue
        if cur_top is None:
            cur_top = w.top
            cur = [w]
            continue
        if abs(w.top - cur_top) <= y_tolerance:
            cur.append(w)
        else:
            grouped.append(cur)
            cur = [w]
            cur_top = w.top
    if cur:
        grouped.append(cur)

    out: list[str] = []
    for g in grouped:
        g2 = sorted(g, key=lambda w: w.x0)
        parts: list[str] = []
        prev_x1: float | None = None
        for w in g2:
            t = _norm_space(w.text)
            if not t:
                continue
            if parts and prev_x1 is not None:
                if (w.x0 - prev_x1) <= join_gap:
                    parts[-1] = parts[-1] + t
                else:
                    parts.append(t)
            else:
                parts.append(t)
            prev_x1 = w.x1
        line = " ".join(parts).strip()
        if line:
            out.append(line)
    return out


def extract_lines(
    *,
    pdf_path: Optional[str] = None,
    page_num: Optional[int] = None,
    pdfplumber_page: Any | None = None,
    engine: str = "auto",
    y_tolerance: float = 2.0,
    join_gap: float = 1.0,
    fitz_doc: Any | None = None,
) -> List[str]:
    """
    통합 라인 스트림 추출.
    fitz_doc이 있으면 재사용해 open/close 반복을 방지한다.
    """
    words = extract_words(
        pdf_path=pdf_path,
        page_num=page_num,
        pdfplumber_page=pdfplumber_page,
        engine=engine,
        fitz_doc=fitz_doc,
    )
    return words_to_lines(words, y_tolerance=y_tolerance, join_gap=join_gap)

