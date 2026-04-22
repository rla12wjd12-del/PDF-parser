#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xlsx 내부에서 특정 토큰(예: '참여기간', '비고')이 실제로 존재하는지 검색한다.

사용:
  python scripts/search_tokens_in_xlsx.py "excel_output/손인호_20250922_raw_tables_no_formula.xlsx" --tokens 참여기간 비고
"""

from __future__ import annotations

import argparse
from pathlib import Path

import openpyxl


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx", type=Path)
    ap.add_argument("--tokens", nargs="+", default=["참여기간", "비고"])
    ap.add_argument("--limit", type=int, default=20, help="token별 출력 샘플 수")
    args = ap.parse_args()

    p = args.xlsx
    if not p.is_file():
        print("[ERROR] not found")
        return 2

    wb = openpyxl.load_workbook(p, data_only=True)
    tokens = [t for t in (args.tokens or []) if t]
    hits: dict[str, list[tuple[str, str, str]]] = {t: [] for t in tokens}
    hidden_cols = 0

    for ws in wb.worksheets:
        for _, dim in ws.column_dimensions.items():
            if getattr(dim, "hidden", False):
                hidden_cols += 1
        for row in ws.iter_rows(values_only=False):
            for c in row:
                v = c.value
                if not isinstance(v, str):
                    continue
                for t in tokens:
                    if t in v:
                        if len(hits[t]) < args.limit:
                            hits[t].append((ws.title, c.coordinate, v.replace("\n", " ")[:120]))

    print("file_loaded: ok")
    print("sheets:", len(wb.worksheets))
    print("hidden_cols:", hidden_cols)
    for t in tokens:
        print(f"token={t} samples={len(hits[t])}")
        for item in hits[t]:
            print(" -", item[0], item[1], item[2])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

