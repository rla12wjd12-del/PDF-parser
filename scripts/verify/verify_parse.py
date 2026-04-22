#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF 파싱 간이 검증: 상훈(텍스트 블록 기대 건수 vs 파싱 건수), 기술경력 사업명 점검.

사용 예:
  python verify_parse.py "경로/증명서.pdf"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description="경력증명서 PDF 파싱 검증(상훈·기술경력 사업명)")
    ap.add_argument("pdf_path", help="검증할 PDF 경로")
    args = ap.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.is_file():
        print(f"[ERROR] 파일 없음: {pdf_path}")
        return 2

    root = Path(__file__).resolve().parent
    root = root.parents[2]
    sys.path.insert(0, str(root))

    from main import count_expected_awards_from_pdf, parse_full_document, log_technical_career_field_issues

    p = str(pdf_path)
    expected_awards = count_expected_awards_from_pdf(p)
    result = parse_full_document(p)
    parsed_awards = len(result.get("상훈") or [])
    award_ok = expected_awards == parsed_awards

    print(
        f"\n[VERIFY:상훈] 텍스트블록 기대 데이터행: {expected_awards}, "
        f"파싱 레코드: {parsed_awards} → {'PASS' if award_ok else 'FAIL'}"
    )

    print("[VERIFY:기술경력 사업명]")
    log_technical_career_field_issues(result, p)

    return 0 if award_ok else 1


if __name__ == "__main__":
    sys.exit(main())
