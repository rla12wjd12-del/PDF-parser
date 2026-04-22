#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
콘솔 인코딩(cp949) 문제를 피하기 위해,
xlsx 내부에 특정 토큰이 존재하는지만 True/False로 출력한다(한글 출력 최소화).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import openpyxl


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx", type=Path)
    ap.add_argument("--tokens", nargs="+", default=["참여기간", "비고"])
    args = ap.parse_args()

    if not args.xlsx.is_file():
        print("NOT_FOUND")
        return 2

    wb = openpyxl.load_workbook(args.xlsx, data_only=True)
    tokens = [t for t in (args.tokens or []) if t]
    found = {t: False for t in tokens}
    for ws in wb.worksheets:
        for row in ws.iter_rows(values_only=True):
            for v in row:
                if not isinstance(v, str):
                    continue
                for t in tokens:
                    if (not found[t]) and (t in v):
                        found[t] = True
            if all(found.values()):
                break
        if all(found.values()):
            break

    # 출력은 ASCII만
    for t in tokens:
        key = "TOK1" if t == tokens[0] else "TOK2"
        print(f"{key}={1 if found[t] else 0}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

