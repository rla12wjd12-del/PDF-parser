#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
지정한 페이지에서 extract_words() 좌표를 덤프해
열 x-구간/행 y-band 템플릿을 잡는 보조 스크립트.

예:
  python scripts/verify/inspect_page_table_layout.py "testpdf/(2026.04.20)test.pdf" 4
  python scripts/verify/inspect_page_table_layout.py "testpdf/(2026.04.20)test.pdf" 4 --grep \"\\d{4}\\.\\d{2}\\.\\d{2}\"
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pdfplumber


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf_path")
    ap.add_argument("page_idx", type=int, help="0-based page index")
    ap.add_argument("--grep", default="", help="regex: text filter")
    ap.add_argument("--top-min", type=float, default=0.0, help="min top")
    ap.add_argument("--top-max", type=float, default=99999.0, help="max top")
    ap.add_argument("--limit", type=int, default=250, help="max rows printed")
    args = ap.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        raise SystemExit(f"[ERROR] PDF not found: {pdf_path}")

    pat = re.compile(args.grep) if args.grep else None

    with pdfplumber.open(str(pdf_path)) as pdf:
        if args.page_idx < 0 or args.page_idx >= len(pdf.pages):
            raise SystemExit(f"[ERROR] page out of range: {args.page_idx} / {len(pdf.pages)}")
        page = pdf.pages[args.page_idx]
        words = page.extract_words(
            use_text_flow=True,
            keep_blank_chars=False,
            extra_attrs=["x0", "x1", "top", "bottom"],
        ) or []

    rows = []
    for w in words:
        t = (w.get("text") or "").strip()
        if not t:
            continue
        top = float(w.get("top") or 0.0)
        if top < args.top_min or top > args.top_max:
            continue
        if pat and not pat.search(t):
            continue
        rows.append((top, float(w.get("x0") or 0.0), float(w.get("x1") or 0.0), t))

    rows.sort(key=lambda x: (x[0], x[1]))
    print(f"words={len(words)} filtered={len(rows)} page_idx={args.page_idx}")
    for r in rows[: args.limit]:
        print(r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

