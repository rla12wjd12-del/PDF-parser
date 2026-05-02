#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""전체 PDF 파싱 후 주요 섹션 카운트만 요약 출력."""
from __future__ import annotations

import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from main import parse_full_document  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: debug_full_parse_summary.py <PDF_PATH>")
        return 2
    pdf_path = sys.argv[1]
    if not os.path.exists(pdf_path):
        print(f"[ERROR] missing: {pdf_path}")
        return 1

    result = parse_full_document(pdf_path) or {}

    info = result.get("인적사항") or {}
    print("[성명]", info.get("성명"))
    print("[관리번호]", info.get("관리번호"))
    print("[국가기술자격]", len(result.get("국가기술자격") or []))
    print("[학력]", len(result.get("학력") or []))
    print("[교육훈련]", len(result.get("교육훈련") or []))
    print("[상훈]", len(result.get("상훈") or []))
    print("[근무처]", len(result.get("근무처") or []))
    print("[기술경력]", len(result.get("기술경력") or []))
    print("[건설사업관리및감리경력]", len(result.get("건설사업관리및감리경력") or []))

    errors = result.get("_파싱오류") or []
    print(f"[_파싱오류 건수] {len(errors)}")
    for e in errors[:10]:
        print(" -", e)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
