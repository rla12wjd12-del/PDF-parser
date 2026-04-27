#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_tables_with_custom_borders의 헤더 오버라이드(start/override)이 실제로 잡히는지 디버그한다.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from parsers.table_settings import LINE_TABLE_SETTINGS, safe_extract_tables
from scripts.export_tables_with_custom_borders import _header_override_rows, _infer_header_start_row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", type=Path)
    ap.add_argument("--page", type=int, required=True)
    ap.add_argument("--left", type=float, default=27.0)
    ap.add_argument("--right", type=float, default=560.0)
    args = ap.parse_args()

    with pdfplumber.open(str(args.pdf)) as pdf:
        page = pdf.pages[args.page]
        st = dict(LINE_TABLE_SETTINGS)
        st["explicit_vertical_lines"] = [args.left, args.right]
        tabs = safe_extract_tables(page, st)
    print("tabs:", len(tabs))
    if not tabs:
        return 0
    t = tabs[0]
    max_cols = max((len(r) for r in t if r), default=0)
    start = _infer_header_start_row(t)
    print("max_cols:", max_cols, "start:", start)
    print("row0 lens:", [len(r or []) for r in t[:5]])
    if start is None:
        return 0
    ov = _header_override_rows(max_cols)
    for rel in sorted(ov.keys()):
        tri = start + rel
        if tri >= len(t):
            continue
        print("before", rel, t[tri][: min(len(t[tri]), 10)])
        print("after ", rel, ov[rel][: min(len(ov[rel]), 10)])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

