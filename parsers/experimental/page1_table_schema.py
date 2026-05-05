# -*- coding: utf-8 -*-
"""
섹션별 표 텍스트를 page1 JSON 부분 dict로 변환한다.

등급/학력/교육훈련/근무처 등은 core `page_1_parser`의 텍스트 파서를 그대로 재사용한다
(field_catalog·정규식 대량 중복 방지). 통합 시 이 의존성을 공통 모듈로 옮기면 된다.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List

from parsers.utils.logger import agent_debug_log as _page1_agent_log

from parsers.core.page_1_parser import (
    _extract_license_section_text,
    _parse_education_from_combined_text,
    _parse_grade_dict_from_normalized_text,
    _parse_training_row,
    _parse_workplace_body_lines,
    _parse_workplace_body_lines_single,
    _extract_training_rows_from_text,
    _yy_mm_dd_to_iso,
    _yyyy_mm_dd_to_iso,
)
from parsers.experimental.page1_table_extract import normalize_cell_text
from parsers.experimental.page1_table_sections import (
    classify_unassigned_rows,
    detect_section_ranges,
    rows_to_multiline_text,
)


def _row_join_for_detection(row: List[str]) -> str:
    return " ".join((c or "").replace("\n", " ").strip() for c in row if c is not None)


def parse_personal_info_from_table(rows: List[List[str]]) -> Dict[str, Any]:
    text_normalized = re.sub(r"\s+", " ", rows_to_multiline_text(rows))
    out: Dict[str, Any] = {
        "인적사항": {"성명": "", "생년월일": "", "주소": "", "관리번호": ""},
        "서류출력일자": "",
    }
    mgmt_num_match = re.search(
        r"관리번호\s*(?:[:：\s]*)(#\s*(?:\d\s*)+|\d(?:\s*\d)*)",
        text_normalized,
    )
    if mgmt_num_match:
        out["인적사항"]["관리번호"] = mgmt_num_match.group(1).replace(" ", "")
    else:
        mgmt_num_match_alt = re.search(r"(#\s*(?:\d\s*)+)", text_normalized[:500])
        if mgmt_num_match_alt:
            out["인적사항"]["관리번호"] = mgmt_num_match_alt.group(1).replace(" ", "")
    name_kor_match = re.search(r"성명\(한글\)\s+(\S+)", text_normalized)
    if name_kor_match:
        out["인적사항"]["성명"] = name_kor_match.group(1).strip()
    birth_match = re.search(r"생년월일\s+(\d{2}\.\d{2}\.\d{2})", text_normalized)
    if birth_match:
        out["인적사항"]["생년월일"] = _yy_mm_dd_to_iso(birth_match.group(1).strip())
    issue_match = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", text_normalized)
    if issue_match:
        yyyy, mm, dd = map(int, issue_match.groups())
        try:
            out["서류출력일자"] = datetime(yyyy, mm, dd).strftime("%Y-%m-%d")
        except ValueError:
            out["서류출력일자"] = ""
    addr_match = re.search(
        r"주소\s+(.+?)(?=\s+(?:"
        r"설계·시공|설계시공|"
        r"건설사업관리|"
        r"품질관리|"
        r"연락처|전화번호|전화|휴대전화|휴대|전자우편|이메일|"
        r"등급|국가기술자격|학력|교육훈련|상훈|벌점|제재|근무처"
        r")|$)",
        text_normalized,
    )
    if addr_match:
        out["인적사항"]["주소"] = addr_match.group(1).strip()
    return out


def parse_grade_from_table(rows: List[List[str]]) -> Dict[str, Any]:
    tn = re.sub(r"\s+", " ", rows_to_multiline_text(rows))
    return {"등급": _parse_grade_dict_from_normalized_text(tn)}


def parse_qualifications_from_table(rows: List[List[str]]) -> List[Dict[str, Any]]:
    raw = rows_to_multiline_text(rows)
    section = _extract_license_section_text(re.sub(r"[ \t]+", " ", raw))
    if not section:
        section = raw
    lic_row_pat = re.compile(
        r"(?P<name>[가-힣A-Za-z0-9·ㆍ\(\)\-/ ]+?(?:기사|산업기사|기능사|기술사|기능장))\s+"
        r"(?P<date>\d{4}\.\d{2}\.\d{2})"
        r"(?:\s+(?P<reg>[A-Z0-9\-]{4,}|\d{4,}|\S+))?",
        flags=re.MULTILINE,
    )
    out: List[Dict[str, Any]] = []
    for m in lic_row_pat.finditer(section):
        name = (m.group("name") or "").strip()
        date_raw = (m.group("date") or "").strip()
        reg = (m.group("reg") or "").strip()
        if not name or not re.fullmatch(r"\d{4}\.\d{2}\.\d{2}", date_raw):
            continue
        out.append(
            {
                "종목": name,
                "합격일": _yyyy_mm_dd_to_iso(date_raw),
                "등록번호": reg,
            }
        )
    return out


def parse_education_from_table(rows: List[List[str]]) -> List[Dict[str, Any]]:
    return _parse_education_from_combined_text(rows_to_multiline_text(rows))


def parse_training_from_table(rows: List[List[str]]) -> List[Dict[str, Any]]:
    chunk = rows_to_multiline_text(rows)
    out: List[Dict[str, Any]] = []
    for row in _extract_training_rows_from_text(chunk + "\n"):
        p = _parse_training_row(row)
        if p:
            out.append(p)
    return out


def parse_awards_from_table(rows: List[List[str]]) -> List[Dict[str, Any]]:
    if rows and any("수여일" in _row_join_for_detection(r) for r in rows):
        pass
    return []


def parse_penalties_from_table(rows: List[List[str]]) -> Dict[str, Any]:
    text = rows_to_multiline_text(rows)
    base: Dict[str, Any] = {"벌점": "해당없음", "제재사항": "해당없음"}
    if not text.strip():
        return base
    m_pts = re.search(r"벌점\s*[:：]?\s*([\d.]+)\s*점?", text.replace("\n", " "))
    if m_pts:
        base["벌점"] = m_pts.group(1).strip()
    if "해당없음" in text and "제재" in text:
        return base
    if re.search(r"제재일\s*\d{4}\.\d{2}\.\d{2}", text):
        base["제재사항"] = text.replace("\n", " ").strip()
    return base


def parse_workplaces_from_table(rows: List[List[str]]) -> List[Dict[str, Any]]:
    body_lines: List[str] = []
    for row in rows:
        ln = " ".join(normalize_cell_text(c) for c in row if normalize_cell_text(c)).strip()
        if ln:
            body_lines.append(ln)
    out: List[Dict[str, Any]] = []
    for w in _parse_workplace_body_lines_single(body_lines):
        out.append(w)
    for w in _parse_workplace_body_lines(body_lines):
        out.append(w)
    return out


def map_page1_table_rows_to_schema(flat_rows: List[List[str]]) -> Dict[str, Any]:
    """표 행 전체 → page1 JSON 부분 dict (core `map_rows_to_existing_schema` 동등)."""
    buckets = detect_section_ranges(flat_rows)
    classify_unassigned_rows(buckets)
    try:
        if buckets.get("_unassigned"):
            _page1_agent_log(
                run_id="page1-table-exp",
                hypothesis_id="U",
                location="experimental/page1_table_schema.py:map_page1_table_rows_to_schema",
                message="unassigned table rows after classify",
                data={"n": len(buckets["_unassigned"]), "sample": buckets["_unassigned"][:5]},
            )
    except Exception:
        pass

    personal = parse_personal_info_from_table(buckets.get("personal") or [])
    grade = parse_grade_from_table(buckets.get("grade") or [])
    lic = parse_qualifications_from_table(buckets.get("license") or [])
    edu = parse_education_from_table(buckets.get("education") or [])
    trn = parse_training_from_table(buckets.get("training") or [])
    aw = parse_awards_from_table(buckets.get("award") or [])
    pen = parse_penalties_from_table(buckets.get("penalty") or [])
    wp = parse_workplaces_from_table(buckets.get("workplace") or [])

    return {
        "인적사항": personal.get("인적사항") or {},
        "서류출력일자": personal.get("서류출력일자") or "",
        "등급": grade.get("등급") or {},
        "국가기술자격": lic,
        "학력": edu,
        "교육훈련": trn,
        "상훈": aw,
        "벌점및제재사항": pen,
        "근무처": wp,
    }


# core와 이름 호환용 별칭
map_rows_to_existing_schema = map_page1_table_rows_to_schema
