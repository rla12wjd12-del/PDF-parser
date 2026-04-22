#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
간단 검증 스크립트:
- 교육훈련에 상훈 텍스트(표창장/훈장/포장/...)가 섞였는지 확인
- 학력 학과(전공) 괄호 미닫힘 등 멀티라인 병합 실패 징후 확인
- 기술경력/CM경력의 사업명 타입/빈값 오류를 콘솔에 출력(main.py 로직 재사용)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from main import parse_full_document, log_technical_career_field_issues  # noqa: E402


_AWARD_HINT_PAT = re.compile(r"(표창장|훈장|포장|감사장|\[제\s*\d+호\]|제\s*\d+\s*호)")


def _paren_balance_ok(s: str) -> bool:
    if not isinstance(s, str):
        return True
    return s.count("(") <= s.count(")")


def verify_result(result: dict, label: str = "") -> int:
    errors = 0
    warns = 0

    trainings = result.get("교육훈련") or []
    if not isinstance(trainings, list):
        print(f"[ERROR] 교육훈련 타입 오류: {type(trainings).__name__} (PDF: {label})")
        errors += 1
        trainings = []

    for i, t in enumerate(trainings):
        if not isinstance(t, dict):
            print(f"[ERROR] 교육훈련[{i}] 레코드 타입 오류: {type(t).__name__} (PDF: {label})")
            errors += 1
            continue
        for k in ("과정명", "교육기관명", "교육인정여부"):
            v = str(t.get(k) or "")
            if _AWARD_HINT_PAT.search(v):
                print(
                    f"[ERROR] 교육훈련[{i}] 필드 `{k}`에 상훈 힌트 감지: {v!r} "
                    f"(기간: {t.get('교육기간_시작','')}~{t.get('교육기간_종료','')}, PDF: {label})"
                )
                errors += 1

    educations = result.get("학력") or []
    if not isinstance(educations, list):
        print(f"[ERROR] 학력 타입 오류: {type(educations).__name__} (PDF: {label})")
        errors += 1
        educations = []

    for i, e in enumerate(educations):
        if not isinstance(e, dict):
            print(f"[ERROR] 학력[{i}] 레코드 타입 오류: {type(e).__name__} (PDF: {label})")
            errors += 1
            continue
        major = str(e.get("학과") or "")
        if major and (not _paren_balance_ok(major)):
            print(
                f"[WARN] 학력[{i}] 학과 괄호 미닫힘 의심: {major!r} "
                f"(학교: {e.get('학교명','')}, 졸업일: {e.get('졸업일','')}, PDF: {label})"
            )
            warns += 1

    # 사업명 필드 검증(에러는 main.py 함수에서 출력; 여기서는 개수만 카운트)
    field_errs = log_technical_career_field_issues(result, label)
    errors += len(field_errs or [])

    print(
        f"[INFO] 검증 요약: errors={errors}, warns={warns}, "
        f"교육훈련={len(trainings)}, 상훈={len(result.get('상훈') or [])}, 학력={len(educations)}"
    )
    return errors


def main() -> int:
    default_pdf = BASE_DIR / "originalPDF" / "전주국토" / "경력증명서_소한섭.pdf"
    ap = argparse.ArgumentParser(description="PDF 파싱 결과 간단 검증")
    ap.add_argument(
        "pdf_path",
        nargs="?",
        default=str(default_pdf),
        help="검증할 PDF 경로 (기본: 소한섭 샘플)",
    )
    args = ap.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        print(f"[ERROR] PDF를 찾을 수 없습니다: {pdf_path}")
        return 2

    # 상대경로로 준 경우 프로젝트 루트 기준으로 보정
    if not pdf_path.is_absolute():
        pdf_path = (BASE_DIR / pdf_path).resolve()

    result = parse_full_document(str(pdf_path))
    errs = verify_result(result, str(pdf_path))
    return 1 if errs else 0


if __name__ == "__main__":
    raise SystemExit(main())

