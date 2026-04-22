#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
정제(clean) 없이 '순수 추출 표' 내용을 콘솔로 확인한다.

- `extract_tables_merged` 후보 테이블들을 그대로 출력(셀 텍스트)한다.
- 페이지/후보 인덱스를 좁혀 빠르게 확인할 수 있다.

사용 예:
  python scripts/export_raw_tables_console.py "originalPDF/황규철 경력증명서(2025.07.24).pdf" --page 8
  python scripts/export_raw_tables_console.py "testpdf/건설안전 김순식 경력증명서(2025.09.22).pdf" --page 8 --table 0
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from parsers.table_settings import extract_tables_merged  # noqa: E402


def _cell(v: object) -> str:
    if v is None:
        return ""
    return str(v).replace("\r\n", "\n").replace("\r", "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="순수 추출 표 콘솔 덤프(정제 없음)")
    ap.add_argument("pdf", type=Path, help="PDF 경로")
    ap.add_argument("--page", type=int, required=True, help="0-based page index")
    ap.add_argument("--table", type=int, default=None, help="후보 테이블 인덱스(미지정 시 전부)")
    ap.add_argument("--max-rows", type=int, default=60, help="테이블당 출력 최대 행 수")
    args = ap.parse_args()

    if not args.pdf.is_file():
        print(f"[ERROR] 파일 없음: {args.pdf}", file=sys.stderr)
        return 2

    with pdfplumber.open(str(args.pdf)) as pdf:
        if args.page < 0 or args.page >= len(pdf.pages):
            print(f"[ERROR] page out of range: {args.page}", file=sys.stderr)
            return 2
        page = pdf.pages[args.page]
        tables = extract_tables_merged(page)

    if not tables:
        print("[INFO] tables: 0")
        return 0

    targets = range(len(tables))
    if args.table is not None:
        if args.table < 0 or args.table >= len(tables):
            print(f"[ERROR] table out of range: {args.table}", file=sys.stderr)
            return 2
        targets = [args.table]

    print(f"[INFO] pdf={args.pdf} page_idx={args.page} candidates={len(tables)}")
    for ti in targets:
        t = tables[ti] or []
        max_cols = max((len(r) for r in t if r), default=0)
        print(f"\n=== table #{ti} ({len(t)} rows x {max_cols} cols) ===")
        for ri, row in enumerate(t[: max(1, args.max_rows)]):
            cells = [_cell(c) for c in (row or [])]
            # 탭 구분으로 표시(복붙 편의)
            print(f"{ri:03d}\t" + "\t".join(cells))
        if len(t) > args.max_rows:
            print(f"... truncated: {len(t) - args.max_rows} more rows")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

