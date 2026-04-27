#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_tables_merged 후보 테이블의 열 수/헤더 키워드 포함 여부를 빠르게 출력한다.

사용 예:
  python scripts/debug_table_candidates.py "testpdf/xxx.pdf" --page 8
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from parsers.table_settings import extract_tables_merged  # noqa: E402


KEYS = ["참여기간", "비고", "사업명", "발주자", "직무분야", "전문분야", "직위", "공사(용역)개요"]


def _cell_str(v: object) -> str:
    if v is None:
        return ""
    return str(v).replace("\r\n", "\n").replace("\r", "\n")


def _summarize(tbl: list[list[object]]) -> tuple[int, int, str]:
    max_cols = max((len(r) for r in tbl if r), default=0)
    lines: list[str] = []
    for r in (tbl or [])[:12]:
        if not r:
            continue
        row = " ".join(_cell_str(c) for c in r if _cell_str(c).strip())
        row = re.sub(r"\s+", " ", row).strip()
        if row:
            lines.append(row)
    head = " | ".join(lines)[:600]
    return max_cols, len(tbl), head


def _has_token_anywhere(tbl: list[list[object]], token: str) -> bool:
    if not token:
        return False
    for r in tbl or []:
        for c in (r or []):
            if token in _cell_str(c):
                return True
    return False


def _first_non_empty_row(tbl: list[list[object]]) -> list[str]:
    for r in tbl or []:
        if not r:
            continue
        if any(_cell_str(c).strip() for c in r):
            return [_cell_str(c) for c in r]
    return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", type=Path)
    ap.add_argument("--page", type=int, required=True, help="0-based page index")
    args = ap.parse_args()

    if not args.pdf.is_file():
        print(f"[ERROR] file not found: {args.pdf}", file=sys.stderr)
        return 2

    with pdfplumber.open(str(args.pdf)) as pdf:
        if args.page < 0 or args.page >= len(pdf.pages):
            print(f"[ERROR] page out of range: {args.page}", file=sys.stderr)
            return 2
        page = pdf.pages[args.page]
        tables = extract_tables_merged(page)

    print("pdf:", args.pdf)
    print("page_idx:", args.page, "candidates:", len(tables))
    for i, t in enumerate(tables):
        mc, nr, head = _summarize(t)
        flags = [k for k in KEYS if k in head]
        has_left = _has_token_anywhere(t, "참여기간")
        has_right = _has_token_anywhere(t, "비고")
        first = _first_non_empty_row(t)
        lens = sorted({len(r or []) for r in (t or [])[:15]})
        print(
            f"#{i:02d} cols={mc:02d} rows={nr:03d} "
            f"flags={flags} has_left={has_left} has_right={has_right} row_lens={lens}"
        )
        if first:
            preview = []
            for idx, cell in enumerate(first[: min(len(first), 12)]):
                s = re.sub(r"\s+", " ", cell).strip()
                if s:
                    preview.append(f"[{idx}]{s[:40]}")
            if preview:
                print(" first_row:", " ".join(preview))
        print(" head:", head)
        print("---")
        if i >= 14:
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

