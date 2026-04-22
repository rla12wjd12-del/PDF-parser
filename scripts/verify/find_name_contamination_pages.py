#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
지정 범위 페이지에서 '사업명 오염(직무분야+담당업무가 사업명에 포함)'이 발생하는 페이지를 찾는다.

예:
  # 템플릿 OFF(기존 로직)에서 오염이 있는 페이지 찾기
  powershell: $env:PDFPARSER_DISABLE_TEMPLATE='1'
  python scripts/verify/find_name_contamination_pages.py <pdf_path> --mode tech --start 0 --end 120

  # 템플릿 ON(기본)에서 동일 범위를 다시 체크
  (env var unset) python scripts/verify/find_name_contamination_pages.py ...
"""

from __future__ import annotations

import os
import sys
import argparse
import re
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from parsers.document_context import DocumentContext
from parsers.page_2_parser import parse_page_2
from parsers.page_3_parser import parse_page_3


def _compact(s: str) -> str:
    return re.sub(r"\s+", "", (s or "")).strip()


def contamination_count(rows: list[dict]) -> int:
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
    ap.add_argument("--mode", choices=["tech", "cm"], required=True)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--end", type=int, default=120)
    ap.add_argument("--min-bad", type=int, default=1)
    ap.add_argument("--max-report", type=int, default=40)
    args = ap.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        raise SystemExit(f"[ERROR] PDF not found: {pdf_path}")

    hits = []
    with DocumentContext.open(str(pdf_path)) as ctx:
        end = min(args.end, ctx.total_pages)
        for p in range(max(0, args.start), max(0, end)):
            try:
                if args.mode == "tech":
                    rows = parse_page_2(ctx, p) or []
                else:
                    rows = parse_page_3(ctx, p) or []
            except Exception:
                continue
            bad = contamination_count(rows)
            if bad >= args.min_bad:
                hits.append((bad, len(rows), p))
                if len(hits) >= args.max_report:
                    break

    hits.sort(reverse=True)
    print(f"mode={args.mode} disable_template={bool(os.environ.get('PDFPARSER_DISABLE_TEMPLATE'))} hits={len(hits)}")
    for bad, n, p in hits:
        print(f"page_idx={p} bad={bad} rows={n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

