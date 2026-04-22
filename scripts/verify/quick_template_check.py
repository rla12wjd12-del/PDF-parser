#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 문서(수백쪽) 파싱 없이, 특정 페이지에서 기술경력/CM 파서의 결과를 빠르게 확인한다.

사용:
  python scripts/verify/quick_template_check.py <pdf_path> --tech-page 15
  python scripts/verify/quick_template_check.py <pdf_path> --cm-page 200
"""

from __future__ import annotations

import os
import sys
import argparse
import json
import re
from pathlib import Path

# repo root on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from parsers.document_context import DocumentContext
from parsers.page_2_parser import parse_page_2
from parsers.page_3_parser import parse_page_3


def _compact(s: str) -> str:
    return re.sub(r"\s+", "", (s or "")).strip()


def _count_name_contamination(rows: list[dict]) -> int:
    bad = 0
    for r in rows:
        nm = _compact(str(r.get("사업명") or ""))
        jf = _compact(str(r.get("직무분야") or ""))
        dt = _compact(str(r.get("담당업무") or ""))
        if jf and dt and (jf + dt) and ((jf + dt) in nm):
            bad += 1
    return bad


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf_path")
    ap.add_argument("--tech-page", type=int, default=None, help="0-based page idx to run parse_page_2")
    ap.add_argument("--cm-page", type=int, default=None, help="0-based page idx to run parse_page_3")
    ap.add_argument("--limit", type=int, default=6)
    args = ap.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        raise SystemExit(f"[ERROR] PDF not found: {pdf_path}")

    with DocumentContext.open(str(pdf_path)) as ctx:
        if args.tech_page is not None:
            rows = parse_page_2(ctx, args.tech_page) or []
            print(f"[TECH] page={args.tech_page} rows={len(rows)} contaminated={_count_name_contamination(rows)}")
            print(json.dumps(rows[: args.limit], ensure_ascii=False, indent=2))

        if args.cm_page is not None:
            rows = parse_page_3(ctx, args.cm_page) or []
            print(f"[CM] page={args.cm_page} rows={len(rows)} contaminated={_count_name_contamination(rows)}")
            print(json.dumps(rows[: args.limit], ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

