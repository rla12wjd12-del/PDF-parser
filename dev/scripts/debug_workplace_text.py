#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
근무처 섹션 텍스트 디버그 스크립트.
- 지정한 PDF에서 1~8쪽까지 텍스트를 페이지별로 출력해
  근무처 섹션이 몇 페이지에 걸쳐 있는지/페이지 경계의 헤더 모양을 확인한다.
"""
from __future__ import annotations

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pdfplumber  # type: ignore


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: debug_workplace_text.py <PDF_PATH>")
        return 2
    pdf_path = sys.argv[1]
    if not os.path.exists(pdf_path):
        print(f"[ERROR] missing: {pdf_path}")
        return 1

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"[INFO] total pages: {total}")
        for i in range(min(8, total)):
            text = pdf.pages[i].extract_text() or ""
            print("=" * 80)
            print(f"[PAGE {i + 1}] len={len(text)}")
            print("=" * 80)
            print(text)
            if "1. 기술경력" in text:
                print("[STOP] '1. 기술경력' 발견 - 이후 페이지는 생략")
                break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
