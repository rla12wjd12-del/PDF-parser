#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xlsx 내부에 엑셀이 '수식'으로 인식한 셀(<f> 태그 / openpyxl data_type='f')이 있는지 점검한다.

사용:
  python scripts/scan_xlsx_formulas.py "excel_output/강완식_260413_raw_tables.xlsx"
"""

from __future__ import annotations

import argparse
from pathlib import Path

import openpyxl


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx", type=Path)
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()

    p = args.xlsx
    if not p.is_file():
        print("[ERROR] not found")
        return 2

    wb = openpyxl.load_workbook(p, data_only=False)
    formulas = 0
    samples: list[tuple[str, str, str]] = []
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for c in row:
                if c.data_type == "f":
                    formulas += 1
                    if len(samples) < args.limit:
                        samples.append((ws.title, c.coordinate, str(c.value)[:120]))
    # Windows 콘솔(cp949)에서 파일명 출력이 실패할 수 있어 경로 출력은 생략
    print("file_loaded: ok")
    print("sheets:", len(wb.worksheets))
    print("formula_cells:", formulas)
    if samples:
        print("samples:")
        for s in samples:
            print(" -", s)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

