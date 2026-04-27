#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF 단어(word) bbox를 덤프해 템플릿(열 x-구간/앵커)을 잡기 위한 검사 스크립트.

사용:
  python scripts/verify/inspect_words_bbox.py "testpdf/(2026.04.20)test.pdf"
"""

from __future__ import annotations

import sys
import re
from pathlib import Path
import pdfplumber


def find_first_page_fast_pymupdf(pdf_path: Path, keyword_sets: list[list[str]], max_pages: int = 500) -> int | None:
    """
    PyMuPDF로 텍스트를 빠르게 훑어 섹션 시작 페이지를 찾는다.
    (pdfplumber.extract_text()는 대용량 문서에서 매우 느릴 수 있음)
    """
    try:
        import fitz  # type: ignore
    except Exception:
        return None
    doc = fitz.open(str(pdf_path))
    try:
        n = min(max_pages, doc.page_count)
        for i in range(n):
            t = doc.load_page(i).get_text("text") or ""
            compact = re.sub(r"\s+", "", t)
            for ks in keyword_sets:
                ks_compact = [re.sub(r"\s+", "", k) for k in ks]
                if all(kc and (kc in compact) for kc in ks_compact):
                    return i
        return None
    finally:
        doc.close()


def find_first_page(pdf: pdfplumber.PDF, keyword_sets: list[list[str]], max_pages: int = 500) -> int | None:
    for i, page in enumerate(list(pdf.pages)[:max_pages]):
        t = page.extract_text() or ""
        for ks in keyword_sets:
            if all(k in t for k in ks):
                return i
    return None


def dump_header_hits(pdf: pdfplumber.PDF, page_idx: int, terms: list[str], limit: int = 140) -> None:
    page = pdf.pages[page_idx]
    words = page.extract_words(
        use_text_flow=True,
        keep_blank_chars=False,
        extra_attrs=["x0", "x1", "top", "bottom"],
    ) or []
    hits: list[tuple[str, str, float, float, float]] = []
    for w in words:
        tx = (w.get("text") or "").strip()
        if not tx:
            continue
        for term in terms:
            if term in tx:
                hits.append(
                    (
                        term,
                        tx,
                        float(w.get("x0") or 0.0),
                        float(w.get("x1") or 0.0),
                        float(w.get("top") or 0.0),
                    )
                )
    hits.sort(key=lambda x: (x[4], x[2]))
    print(f"\nPAGE {page_idx} header-like hits={len(hits)}")
    for h in hits[:limit]:
        print(h)


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/verify/inspect_words_bbox.py <pdf_path>")
        return 2
    pdf_path = Path(argv[1])
    if not pdf_path.exists():
        print(f"[ERROR] PDF not found: {pdf_path}")
        return 2

    header_terms = [
        "사업명",
        "발주자",
        "공사종류",
        "참여기간",
        "직무분야",
        "전문분야",
        "담당업무",
        "직위",
        "공사(용역)금액",
        "공사(용역)개요",
        "책임정도",
    ]

    tech_start = find_first_page_fast_pymupdf(pdf_path, [["1. 기술경력"], ["기술경력"]])
    cm_start = find_first_page_fast_pymupdf(
        pdf_path, [["2. 건설사업관리 및 감리경력"], ["건설사업관리", "감리경력"]]
    )
    print("tech_start", tech_start, "cm_start", cm_start)

    with pdfplumber.open(str(pdf_path)) as pdf:
        if tech_start is not None:
            dump_header_hits(pdf, tech_start, header_terms)
        if cm_start is not None:
            dump_header_hits(pdf, cm_start, header_terms)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

