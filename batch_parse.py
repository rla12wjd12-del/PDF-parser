#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
배치 파싱 스크립트: 여러 PDF를 순차 파싱하고 결과를 요약 출력한다.
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from main import parse_full_document, validate_output
import re

PDF_DIR = Path(__file__).parent / "originalPDF" / "경력증명서" / "대기자 경력증명서"
JSON_DIR = PDF_DIR / "JSON"

TARGET_FILES = [
    "민순기 경력증명서(2025.09.22).pdf",
    "박시범 경력증명서(2025.11.05).pdf",
    "박진식 경력증명서(2025.05.23).pdf",
    "박찬종 경력증명서(2025.09.22).pdf",
    "박현민 경력증명서(2025.10.16).pdf",
]


def _squash_newlines_in_obj(obj):
    if isinstance(obj, str):
        if ("\n" not in obj) and ("\r" not in obj):
            return obj
        s = re.sub(r"[ \t]*\r?\n[ \t]*", "", obj)
        s = re.sub(r"[ \t]+", " ", s).strip()
        return s
    if isinstance(obj, list):
        return [_squash_newlines_in_obj(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _squash_newlines_in_obj(v) for k, v in obj.items()}
    return obj


def main():
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    summary = []

    for fname in TARGET_FILES:
        pdf_path = PDF_DIR / fname
        if not pdf_path.exists():
            print(f"\n[SKIP] 파일 없음: {pdf_path}")
            summary.append({"파일": fname, "상태": "파일없음"})
            continue

        print(f"\n{'#'*70}")
        print(f"# 파싱: {fname}")
        print(f"{'#'*70}")

        result = parse_full_document(str(pdf_path))

        for _k in ["기술경력", "건설사업관리및감리경력"]:
            if isinstance(result.get(_k), list):
                result[_k] = _squash_newlines_in_obj(result[_k])

        stem = pdf_path.stem
        out_path = JSON_DIR / f"{stem}_final_result.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"[OK] JSON 저장: {out_path}")

        report = validate_output(result, str(pdf_path))
        verification = result.get("_검증", {})

        mismatches = []
        for sec, info in verification.items():
            if not info.get("일치", True):
                mismatches.append(f"  {sec}: PDF {info['PDF원본']} vs 파싱 {info['파싱결과']}")

        entry = {
            "파일": fname,
            "상태": "오류있음" if mismatches else "정상",
            "성명": (result.get("인적사항") or {}).get("성명", ""),
            "국가기술자격": len(result.get("국가기술자격", [])),
            "교육훈련": len(result.get("교육훈련", [])),
            "근무처": len(result.get("근무처", [])),
            "기술경력": len(result.get("기술경력", [])),
            "건설사업관리및감리경력": len(result.get("건설사업관리및감리경력", [])),
            "공사종류별인정일수": len(result.get("공사종류별인정일수", [])),
            "직무전문분야별인정일수": len(result.get("직무전문분야별인정일수", [])),
            "불일치": mismatches,
        }
        summary.append(entry)

    print(f"\n\n{'='*80}")
    print("배치 파싱 결과 요약")
    print(f"{'='*80}")
    all_ok = True
    for e in summary:
        status = e.get("상태", "")
        icon = "[OK]" if status == "정상" else "[!!]"
        if status != "정상":
            all_ok = False
        print(f"\n{icon} {e['파일']}")
        if status == "파일없음":
            continue
        print(f"    성명: {e.get('성명','')}")
        print(f"    국가기술자격: {e.get('국가기술자격',0)}, 교육훈련: {e.get('교육훈련',0)}, 근무처: {e.get('근무처',0)}")
        print(f"    기술경력: {e.get('기술경력',0)}, 건설사업관리및감리경력: {e.get('건설사업관리및감리경력',0)}")
        print(f"    공사종류별인정일수: {e.get('공사종류별인정일수',0)}, 직무전문분야별인정일수: {e.get('직무전문분야별인정일수',0)}")
        if e.get("불일치"):
            for m in e["불일치"]:
                print(f"    [MISMATCH] {m}")

    print(f"\n{'='*80}")
    if all_ok:
        print("전체 결과: 모든 파일 파싱 정상")
    else:
        print("전체 결과: 일부 파일에 불일치 또는 오류 존재")
    print(f"{'='*80}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
