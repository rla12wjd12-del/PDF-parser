#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
지정 PDF에 대해 1페이지 통합 파서를 실행하고 '근무처'만 추출해 출력한다.
"""
from __future__ import annotations

import sys
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from parsers.document_context import DocumentContext  # noqa: E402
from parsers.page_1_parser import parse_page_1  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: debug_parse_workplace.py <PDF_PATH>")
        return 2
    pdf_path = sys.argv[1]
    if not os.path.exists(pdf_path):
        print(f"[ERROR] missing: {pdf_path}")
        return 1

    with DocumentContext.open(pdf_path) as ctx:
        result = parse_page_1(ctx, page_num=0) or {}

    workplaces = result.get("근무처") or []
    print(f"[INFO] 근무처 레코드 수: {len(workplaces)}")
    for i, w in enumerate(workplaces, 1):
        print(f"  [{i}] {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
