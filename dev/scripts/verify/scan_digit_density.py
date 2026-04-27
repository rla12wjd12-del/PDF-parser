#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pdfplumber.extract_words() 기준으로 페이지별 '숫자(ASCII digit) 포함 단어' 밀도를 계산해
테이블 본문이 있을 법한 페이지를 찾는다.

사용:
  python scripts/verify/scan_digit_density.py <pdf_path> --start 0 --end 120 --topn 20
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pdfplumber


_HAS_ASCII_DIGIT = re.compile(r"[0-9]")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf_path")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--end", type=int, default=120)
    ap.add_argument("--topn", type=int, default=20)
    args = ap.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        raise SystemExit(f"[ERROR] PDF not found: {pdf_path}")

    scores: list[tuple[int, int, int]] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        n = len(pdf.pages)
        s = max(0, args.start)
        e = min(n, max(s, args.end))
        for i in range(s, e):
            page = pdf.pages[i]
            try:
                words = page.extract_words(
                    use_text_flow=True,
                    keep_blank_chars=False,
                ) or []
            except Exception:
                words = []
            total = 0
            digit = 0
            for w in words:
                t = (w.get("text") or "").strip()
                if not t:
                    continue
                total += 1
                if _HAS_ASCII_DIGIT.search(t):
                    digit += 1
            scores.append((digit, total, i))

    scores.sort(reverse=True)
    print(f"scanned={len(scores)} pages, show top {args.topn}")
    for digit, total, i in scores[: args.topn]:
        print(f"page_idx={i} digit_words={digit} total_words={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

