#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PyMuPDF로 특정 텍스트가 포함된 페이지 인덱스를 빠르게 찾는다.

사용:
  python scripts/verify/find_text_pages.py <pdf_path> <query>
"""

from __future__ import annotations

import sys
import re
from pathlib import Path


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print("Usage: python scripts/verify/find_text_pages.py <pdf_path> <query> [--max-pages N]")
        return 2
    pdf_path = Path(argv[1])
    q = argv[2]
    max_pages = None
    if "--max-pages" in argv:
        try:
            j = argv.index("--max-pages")
            max_pages = int(argv[j + 1])
        except Exception:
            max_pages = None
    if not pdf_path.exists():
        print(f"[ERROR] PDF not found: {pdf_path}")
        return 2
    import pdfplumber

    q_compact = re.sub(r"\s+", "", q)
    hits: list[int] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        pages = list(pdf.pages)
        if max_pages is not None:
            pages = pages[: max(0, max_pages)]
        for i, page in enumerate(pages):
            # extract_text는 비용이 크므로, 실패 시 빈 문자열로 빠르게 넘긴다.
            try:
                t = page.extract_text() or ""
            except Exception:
                t = ""
            if (q and (q in t)) or (q_compact and (q_compact in re.sub(r"\s+", "", t))):
                hits.append(i)

    print(f"query={q!r} hits={len(hits)} pages={hits[:200]}")
    if len(hits) > 200:
        print(f"... truncated, last={hits[-1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

