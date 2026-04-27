#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
일괄 검증 스크립트: 여러 PDF에 대해 page_1 파싱 결과(등급/학력/상훈)를
원본(PDF 텍스트 추출)과 대조해 "명백한 누락/오염"을 자동 탐지한다.

주의:
- 완전한 의미 비교(레이아웃/표)까지는 자동화가 어렵기 때문에,
  1) 파싱 결과의 핵심 토큰이 원문 텍스트(fitz)에서 관측되는지,
  2) 상훈은 날짜 주변(윈도우)에서 기관/종류가 함께 관측되는지
  중심으로 검증한다.
- 실패 케이스는 콘솔 요약(JSON)으로 확인한다.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any

import fitz  # PyMuPDF

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from parsers.core.document_context import DocumentContext
from parsers.page_1_parser import parse_page_1

def _compact(s: str) -> str:
    return re.sub(r"\s+", "", (s or ""))


def _fitz_page0_text(pdf_path: str) -> str:
    doc = fitz.open(pdf_path)
    try:
        return doc[0].get_text("text") or ""
    finally:
        doc.close()


def _contains_token(text: str, token: str) -> bool:
    t = (token or "").strip()
    if not t:
        return True
    return t in (text or "")


def _window_around(text: str, anchor: str, window: int = 220) -> str:
    if not text or not anchor:
        return ""
    i = text.find(anchor)
    if i < 0:
        return ""
    a = max(0, i - window)
    b = min(len(text), i + window)
    return text[a:b]


def _validate_grade(grade: dict[str, Any], page_text: str) -> list[str]:
    """
    등급 검증:
    - 직무/전문 명칭과 등급(특/고/중/초급)이 원문 텍스트에 존재하는지 확인.
    """
    issues: list[str] = []
    if not isinstance(grade, dict):
        return ["grade_not_dict"]

    # 핵심 필드 목록(빈 값이면 skip)
    fields = [
        "설계시공_등_직무분야",
        "설계시공_등_직무분야_등급",
        "설계시공_등_전문분야",
        "설계시공_등_전문분야_등급",
        "건설사업관리_직무분야",
        "건설사업관리_직무분야_등급",
        "건설사업관리_전문분야",
        "건설사업관리_전문분야_등급",
        "품질관리_등급",
    ]
    for k in fields:
        v = str(grade.get(k) or "").strip()
        if not v:
            continue
        if not _contains_token(page_text, v):
            # 공백 삭제 비교로도 확인(예: '안전관리특급'처럼 붙는 경우)
            if _compact(v) not in _compact(page_text):
                issues.append(f"grade_token_missing:{k}={v}")
    return issues


def _validate_edu(edu: list[dict[str, Any]], page_text: str) -> list[str]:
    issues: list[str] = []
    if edu is None:
        return ["edu_none"]
    if not isinstance(edu, list):
        return ["edu_not_list"]
    for i, e in enumerate(edu):
        if not isinstance(e, dict):
            issues.append(f"edu_row_not_dict:{i}")
            continue
        dt = str(e.get("졸업일") or "").strip()
        if dt and re.match(r"^\d{4}-\d{2}-\d{2}$", dt):
            dot = dt.replace("-", ".")
            if dot not in page_text:
                issues.append(f"edu_date_missing:{i}={dt}")
        school = str(e.get("현재_학교명") or "").strip()
        major = str(e.get("학과") or "").strip()
        if school and _compact(school) not in _compact(page_text):
            issues.append(f"edu_school_missing:{i}={school}")
        if major and _compact(major) not in _compact(page_text):
            issues.append(f"edu_major_missing:{i}={major}")
    return issues


def _validate_awards(awards: list[dict[str, Any]], page_text: str) -> list[str]:
    issues: list[str] = []
    if awards is None:
        return ["awards_none"]
    if not isinstance(awards, list):
        return ["awards_not_list"]

    for i, a in enumerate(awards):
        if not isinstance(a, dict):
            issues.append(f"award_row_not_dict:{i}")
            continue
        dt = str(a.get("수여일") or "").strip()
        inst = str(a.get("수여기관") or "").strip()
        typ = str(a.get("종류및근거") or "").strip()
        if not dt or dt in {"해당없음"}:
            continue

        # 날짜가 페이지 원문에 존재하는지
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", dt):
            issues.append(f"award_date_bad:{i}={dt}")
            continue
        dot = dt.replace("-", ".")
        if dot not in page_text:
            issues.append(f"award_date_missing:{i}={dt}")
            continue

        win = _window_around(page_text, dot, window=280)
        # 기관/종류가 "날짜 주변"에서 함께 관측되는지(표는 줄 분리될 수 있어 compact 비교 사용)
        if inst and _compact(inst) not in _compact(win):
            issues.append(f"award_inst_missing_near_date:{i}={dt}|{inst}")
        # 종류는 토큰(표창장/표창패/훈장/포장/감사장 등)만이라도 들어 있어야 함
        if typ:
            core_ok = any(tok in typ for tok in ["표창장", "표창패", "훈장", "포장", "감사장", "상장", "유공표창", "우수상"])
            if not core_ok:
                issues.append(f"award_type_suspicious:{i}={dt}|{typ}")
            if _compact(typ) not in _compact(win):
                # 윈도우에 없다면 보수적으로 warning 처리
                issues.append(f"award_type_missing_near_date:{i}={dt}|{typ}")
        else:
            issues.append(f"award_type_empty:{i}={dt}")
    return issues


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", nargs="+", help="PDF paths to validate")
    args = ap.parse_args()

    # 가설(런타임 검증용)
    # H1: 텍스트 추출(인코딩/줄분리) 때문에 토큰 기반 검증이 실패한다.
    # H2: 등급/상훈이 표 셀 분리로 잘려 파싱 오염(기관/종류 혼입)이 발생한다.
    # H3: 학력 학위 토큰 다양성(대졸 등)으로 누락이 남아있다.
    # H4: 상훈 병합에서 "긴 문자열 우선" 잔재/파편이 남아 종류가 오염된다.
    # H5: 특정 PDF는 page_1 파서가 잘못된 텍스트 소스를 사용(페이지 0이 아닌 텍스트)한다.

    summary = {"total": 0, "ok": 0, "fail": 0, "by_pdf": []}

    for pdf_path in args.pdf:
        summary["total"] += 1
        pdf_abs = os.path.abspath(pdf_path)
        page_text = _fitz_page0_text(pdf_abs)

        with DocumentContext.open(pdf_abs) as ctx:
            parsed = parse_page_1(ctx, page_num=0)

        grade = parsed.get("등급")
        edu = parsed.get("학력")
        awards = parsed.get("상훈")

        issues: list[str] = []
        issues.extend(_validate_grade(grade if isinstance(grade, dict) else {}, page_text))
        issues.extend(_validate_edu(edu if isinstance(edu, list) else [], page_text))
        issues.extend(_validate_awards(awards if isinstance(awards, list) else [], page_text))

        if issues:
            summary["fail"] += 1
            summary["by_pdf"].append({"pdf": pdf_abs, "status": "FAIL", "issues": issues})
        else:
            summary["ok"] += 1
            summary["by_pdf"].append({"pdf": pdf_abs, "status": "OK", "issues": []})

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["fail"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

