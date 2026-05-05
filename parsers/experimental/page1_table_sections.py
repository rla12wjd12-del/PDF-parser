# -*- coding: utf-8 -*-
"""
정규화된 표 행을 섹션(인적/등급/자격/학력/교육훈련/상훈/벌점/근무처)으로 나눈다.

core `page_1_parser.detect_section_ranges` / `classify_unassigned_rows` 와 동등.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from parsers.experimental.page1_table_extract import normalize_cell_text


def _row_join_for_detection(row: List[str]) -> str:
    return " ".join((c or "").replace("\n", " ").strip() for c in row if c is not None)


def detect_section_ranges(rows: List[List[str]]) -> Dict[str, List[List[str]]]:
    buckets: Dict[str, List[List[str]]] = {
        "personal": [],
        "grade": [],
        "license": [],
        "education": [],
        "training": [],
        "award": [],
        "penalty": [],
        "workplace": [],
        "_unassigned": [],
    }
    current = "leading"
    leading_buf: List[List[str]] = []

    def _transition(line: str) -> Optional[str]:
        if "1. 기술경력" in line or "1.기술경력" in line:
            return "__stop__"
        if "근무기간" in line and "상호" in line:
            return "workplace"
        if ("벌점" in line or "제재일" in line) and ("제재" in line or "제재사항" in line or "종류" in line):
            return "penalty"
        if "상훈" in line and ("수여일" in line or "종류" in line or "근거" in line):
            return "award"
        if "교육기간" in line and "과정명" in line:
            return "training"
        if "졸업일" in line and "학교명" in line and ("학과" in line or "전공" in line):
            return "education"
        if "국가기술자격" in line and ("종목" in line or "합격" in line or "등록" in line):
            return "license"
        if "등급" in line and len(line) <= 40:
            return "grade"
        if "설계·시공" in line or "설계시공" in line or "건설사업관리" in line:
            if "기술경력" not in line:
                return "grade"
        if "인적사항" in line or "성명(한글)" in line or "관리번호" in line:
            return "personal"
        if "품질관리" in line and ("등급" in line or "특급" in line or "고급" in line):
            return "grade"
        return None

    for row in rows:
        line = _row_join_for_detection(row)
        new_sec = _transition(line)
        if new_sec == "__stop__":
            break
        if new_sec:
            current = new_sec
        if current == "leading":
            leading_buf.append(row)
            continue
        if current == "personal" and leading_buf:
            buckets["personal"].extend(leading_buf)
            leading_buf = []
        tgt = current if current != "leading" else "personal"
        if tgt in buckets:
            buckets[tgt].append(row)
        else:
            buckets["_unassigned"].append(row)

    if leading_buf:
        buckets["personal"].extend(leading_buf)
    return buckets


def classify_unassigned_rows(buckets: Dict[str, List[List[str]]]) -> None:
    raw = buckets.get("_unassigned") or []
    if not raw:
        return
    still: List[List[str]] = []
    for row in raw:
        line = _row_join_for_detection(row)
        if re.search(r"\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}\.\d{2}\.\d{2}", line):
            buckets["training"].append(row)
        elif re.fullmatch(r"\d{4}\.\d{2}\.\d{2}", line.strip()):
            buckets["award"].append(row)
        elif "기사" in line or "산업기사" in line or "기능사" in line or "기술사" in line:
            buckets["license"].append(row)
        else:
            still.append(row)
    buckets["_unassigned"] = still


def rows_to_multiline_text(rows: List[List[str]]) -> str:
    lines: List[str] = []
    for row in rows:
        parts = [normalize_cell_text(c) for c in row if normalize_cell_text(c)]
        if parts:
            lines.append(" ".join(parts))
    return "\n".join(lines)
