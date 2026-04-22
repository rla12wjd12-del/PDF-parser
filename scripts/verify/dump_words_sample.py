#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_words()에서 뽑힌 텍스트가 실제로 한글을 포함하는지(콘솔 인코딩 vs 추출 품질) 확인용.

사용:
  python scripts/verify/dump_words_sample.py <pdf_path> <page_idx> <top_min> <top_max> <out_path>
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pdfplumber


def main(argv: list[str]) -> int:
    if len(argv) < 6:
        print("Usage: python scripts/verify/dump_words_sample.py <pdf_path> <page_idx> <top_min> <top_max> <out_path>")
        return 2
    pdf_path = Path(argv[1])
    page_idx = int(argv[2])
    top_min = float(argv[3])
    top_max = float(argv[4])
    out_path = Path(argv[5])
    if not pdf_path.exists():
        print(f"[ERROR] PDF not found: {pdf_path}")
        return 2

    with pdfplumber.open(str(pdf_path)) as pdf:
        page = pdf.pages[page_idx]
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
        if top < top_min or top > top_max:
            continue
        rows.append(
            {
                "top": top,
                "x0": float(w.get("x0") or 0.0),
                "x1": float(w.get("x1") or 0.0),
                "text": t,
            }
        )
    rows.sort(key=lambda r: (r["top"], r["x0"]))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(rows)} words to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

