#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

from parsers.table_settings import TABLE_SETTINGS_VERSION, extract_tables_merged


@dataclass
class DocumentContext:
    """
    열린 PDF 핸들과 페이지 단위 추출 결과를 캐시하는 컨텍스트.

    목적:
    - pdfplumber.open()을 상위에서 1회만 수행
    - PyMuPDF(fitz) 핸들도 문서당 1회만 열어 재사용 (open/close 반복 방지)
    - 하위 파서/검증 로직은 ctx를 통해 pages/text/tables/라인스트림을 재사용
    - 추출 단계 예외를 errors 리스트에 누적해 조용한 누락(Silent Failure)을 방지
    """

    pdf_path: str
    pdf: pdfplumber.PDF
    pages: List[Any]

    # 추출 단계 오류 누적 — 폴백 후 반드시 기록해 _파싱오류에 포함시킨다
    errors: List[dict] = field(default_factory=list)

    # PyMuPDF 문서 핸들 (문서당 1회 open, close()에서 해제)
    _fitz_doc: Optional[Any] = field(default=None, repr=False)

    _text_cache: Dict[int, str] = field(default_factory=dict)
    _tables_cache: Dict[Tuple[int, str], List[Any]] = field(default_factory=dict)
    _word_lines_cache: Dict[Tuple[int, str, float, float], List[str]] = field(default_factory=dict)
    _words_cache: Dict[Tuple[int, str], List[Any]] = field(default_factory=dict)

    @classmethod
    def open(cls, pdf_path: str) -> "DocumentContext":
        pdf = pdfplumber.open(pdf_path)
        ctx = cls(pdf_path=pdf_path, pdf=pdf, pages=list(pdf.pages))
        try:
            import fitz  # type: ignore  # PyMuPDF
            ctx._fitz_doc = fitz.open(pdf_path)
        except Exception as e:
            ctx.errors.append({"stage": "fitz.open", "page": -1, "error": repr(e)})
        return ctx

    def close(self) -> None:
        if self._fitz_doc is not None:
            try:
                self._fitz_doc.close()
            except Exception:
                pass
            self._fitz_doc = None
        try:
            self.pdf.close()
        except Exception:
            pass

    def __enter__(self) -> "DocumentContext":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @property
    def total_pages(self) -> int:
        return len(self.pages)

    def get_page(self, page_idx: int) -> Any | None:
        if page_idx < 0 or page_idx >= len(self.pages):
            return None
        return self.pages[page_idx]

    def get_text(self, page_idx: int) -> str:
        if page_idx in self._text_cache:
            return self._text_cache[page_idx]
        page = self.get_page(page_idx)
        if page is None:
            self._text_cache[page_idx] = ""
            return ""
        try:
            txt = page.extract_text() or ""
        except Exception as e:
            self.errors.append({
                "stage": "pdfplumber.extract_text",
                "page": page_idx + 1,
                "error": repr(e),
            })
            txt = ""
        self._text_cache[page_idx] = txt
        return txt

    def get_tables(self, page_idx: int) -> List[Any]:
        key = (page_idx, TABLE_SETTINGS_VERSION)
        if key in self._tables_cache:
            return self._tables_cache[key]
        page = self.get_page(page_idx)
        if page is None:
            self._tables_cache[key] = []
            return []
        try:
            tables = extract_tables_merged(page)
        except Exception as e:
            self.errors.append({
                "stage": "pdfplumber.extract_tables",
                "page": page_idx + 1,
                "error": repr(e),
            })
            tables = []
        self._tables_cache[key] = tables
        return tables

    def get_word_lines(
        self,
        page_idx: int,
        *,
        engine: str = "auto",
        y_tolerance: float = 2.0,
        join_gap: float = 1.0,
    ) -> List[str]:
        key = (page_idx, (engine or "auto"), float(y_tolerance), float(join_gap))
        if key in self._word_lines_cache:
            return self._word_lines_cache[key]
        page = self.get_page(page_idx)
        if page is None:
            self._word_lines_cache[key] = []
            return []

        try:
            from parsers.layout_extractor import extract_lines as _layout_extract_lines

            lines = _layout_extract_lines(
                pdf_path=self.pdf_path,
                page_num=page_idx,
                pdfplumber_page=page,
                engine=engine,
                y_tolerance=y_tolerance,
                join_gap=join_gap,
                fitz_doc=self._fitz_doc,
            )
        except Exception as e:
            self.errors.append({
                "stage": "layout_extractor.extract_lines",
                "page": page_idx + 1,
                "error": repr(e),
            })
            lines = []
        self._word_lines_cache[key] = lines
        return lines

    def get_words(self, page_idx: int, *, engine: str = "auto") -> List[Any]:
        """
        page_idx 페이지의 word(텍스트+좌표)를 반환한다.
        - engine="auto": PyMuPDF(fitz) 우선, 실패 시 pdfplumber extract_words 폴백
        - 반환 원소는 parsers.layout_extractor.Word(dataclass) 또는 이에 준하는 객체이다.
        - fitz_doc이 있으면 이미 열린 핸들을 재사용해 open/close 반복을 방지한다.
        """
        key = (page_idx, (engine or "auto"))
        if key in self._words_cache:
            return self._words_cache[key]
        page = self.get_page(page_idx)
        if page is None:
            self._words_cache[key] = []
            return []
        try:
            from parsers.layout_extractor import extract_words as _layout_extract_words

            ws = _layout_extract_words(
                pdf_path=self.pdf_path,
                page_num=page_idx,
                pdfplumber_page=page,
                engine=engine,
                fitz_doc=self._fitz_doc,
            )
        except Exception as e:
            self.errors.append({
                "stage": "layout_extractor.extract_words",
                "page": page_idx + 1,
                "error": repr(e),
            })
            ws = []
        self._words_cache[key] = ws
        return ws
