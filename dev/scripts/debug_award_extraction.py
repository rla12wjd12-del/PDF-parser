#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
디버그 전용: 특정 PDF에서 상훈(수여일/수여기관/종류및근거) 섹션이
텍스트/테이블로 어떻게 추출되는지 그대로 출력한다.

주의: 파서 로직을 변경하지 않는다. (원인 분석용 덤프)
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import pdfplumber

# repo root를 sys.path에 추가(스크립트를 어디서 실행하든 imports가 동작하도록)
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from parsers.table_settings import extract_tables_merged
import parsers.core.section_parsers as sp


def _flat_table_text(table: list[list[Any]]) -> str:
    return " ".join(str(c) for r in (table or []) for c in (r or []) if c)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf_path")
    ap.add_argument("--pages", type=int, default=4)
    ap.add_argument("--max-cand", type=int, default=8)
    ap.add_argument("--rows-after-header", type=int, default=10)
    args = ap.parse_args()

    pdf_path = args.pdf_path
    if not os.path.exists(pdf_path):
        raise SystemExit(f"PDF not found: {pdf_path!r}")

    with pdfplumber.open(pdf_path) as pdf:
        n = min(args.pages, len(pdf.pages))
        for pi in range(n):
            page = pdf.pages[pi]
            text = page.extract_text() or ""
            block = sp.extract_award_section_text(text)

            print("=" * 100)
            print(f"PAGE {pi + 1}/{len(pdf.pages)}")
            print(f"TEXT_LEN={len(text)} BLOCK_LEN={len(block)}")

            if block.strip():
                lines = [ln for ln in block.splitlines()]
                print("[AWARD_BLOCK_HEAD]")
                for ln in lines[:30]:
                    print(ln)
                if len(lines) > 45:
                    print("... (snip) ...")
                print("[AWARD_BLOCK_TAIL]")
                for ln in lines[-15:]:
                    print(ln)
            else:
                if ("상훈" in text) or ("수여일" in text):
                    k = text.find("상훈")
                    if k < 0:
                        k = text.find("수여일")
                    if k >= 0:
                        print("[CONTEXT_AROUND]")
                        print(text[max(0, k - 250) : k + 600])

            tables = extract_tables_merged(page) or []
            print(f"TABLES={len(tables)}")

            cand: list[tuple[int, list[list[Any]]]] = []
            for ti, t in enumerate(tables):
                flat = _flat_table_text(t)
                if ("수여일" in flat) or ("상훈" in flat):
                    cand.append((ti, t))

            print(f"CAND_TABLES={len(cand)}")
            for ti, t in cand[: args.max_cand]:
                cols_max = max((len(r or []) for r in (t or [])), default=0)
                print(f"- cand_idx={ti} rows={len(t)} cols_max={cols_max}")
                hi = sp._find_award_table_header_idx(t)
                print(f"  header_idx={hi}")
                if hi >= 0:
                    hr = t[hi]
                    dc = sp.find_column_index(hr, "수여일")
                    ic = sp.find_column_index(hr, "수여기관")
                    tc = sp.find_column_index(hr, "종류")
                    if tc < 0:
                        tc = sp.find_column_index(hr, "근거")
                    print(f"  header_row={hr}")
                    print(f"  cols(date,inst,type)={dc},{ic},{tc}")
                    for r in t[hi : hi + 1 + args.rows_after_header]:
                        print(f"  ROW {r}")
            print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

