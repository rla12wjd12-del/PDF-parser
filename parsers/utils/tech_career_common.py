#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기술경력/CM(제2~3쪽) 공용 유틸 모음.

목표
- `page_2_parser.py`에 섞여 있던 "다른 모듈(main/page_3)이 실제로 import하는 공용 유틸"을 분리한다.
- 2쪽 본체 파서(텍스트/표 혼합)와 공용 유틸의 결합도를 낮춰, 유지보수/정리(삭제/격리) 판단을 쉽게 한다.
"""

from __future__ import annotations

from typing import Dict, List
import re

from parsers.document_context import DocumentContext
from parsers.tech_career_heuristics import load_tech_career_heuristics
from pathlib import Path
from field_catalog import best_match_specialty, get_field_catalog  # // [수정] 전문분야 정규화(카탈로그 기반)
import field_catalog as _fc  # // [수정] _compact_ws 등 내부 정규화와 동일 규칙 유지

# parsers/utils → PDF-parser 루트( field_catalog.py / data/ 와 동일 기준 )
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_TC_H = load_tech_career_heuristics(_PROJECT_ROOT)
_JOB_FIELD_HINTS = frozenset(_TC_H.job_field_hints or ())
_CATALOG = get_field_catalog(project_root=str(_PROJECT_ROOT))  # // [수정]

_DATE_RE = re.compile(r"^\d{4}\.\d{2}(?:\.\d{2})?$")

# // [수정] 병합셀·특허/신기술 꼬리가 붙은 전문분야는 잘라내지 않고 통째로 둔다.
_RE_KEEP_FULL_SPECIALTY = re.compile(
    r"특허|신기술|인\s*증서|인증서|\d{2,4}\s*-\s*\d+\s*-\s*\d+\s*호",
    re.I,
)
# // [수정] 공법/교량 등 설명이 전문분야 셀에 붙은 전형 패턴(인명 하드코딩 없음)
_RE_KEEP_FULL_SPECIALTY_EXTRA = re.compile(
    r"P\s*공법|교량\s*\d*\s*개|복층|저소음",
    re.I,
)

# `*` 뒤에 붙는 담당·직무 꼬리(전문분야+담당업무 병합 추출물)
_DUTY_TAIL_AFTER_STAR = (
    "공사감독",
    "공사관리",
    "시공관리",
    "설계감리",
    "감리",
    "감독",
    "분야책임기술인",
)


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\u00A0", " ").strip())


def _ocr_fix_specialty(s: str) -> str:
    # '입식 흙맑이'가 '립식'으로 깨지는 OCR
    return (s or "").replace("립식", "입식")


def _normalize_star_glue(s: str) -> str:
    """전문분야+직무가 `*`로 이어진 표기: 공백만 정리하고 `*`는 유지."""
    t = _norm_space(s)
    return re.sub(r"\s*\*\s*", "*", t)


def _dedupe_adjacent_repeat(s: str) -> str:
    """공백 제거 기준으로 동일 절반이 2회 반복되면 1회로."""
    s_n = re.sub(r"\s+", "", s)
    if len(s_n) >= 6 and (len(s_n) % 2 == 0):
        half = len(s_n) // 2
        if s_n[:half] == s_n[half:]:
            return s_n[:half]
    return s


def _should_keep_full_specialty_cell(s: str) -> bool:
    if _RE_KEEP_FULL_SPECIALTY.search(s):
        return True
    if _RE_KEEP_FULL_SPECIALTY_EXTRA.search(s) and len(s) >= 10:
        return True
    return False


def _try_rebuild_asterisk_compacted_specialties(left: str, right: str) -> str | None:
    """
    예: '도로및공항상하수도' + '공사감독' → 카탈로그 조각을 이어붙여
    '도로및공항상하수도*공사감독' 형태로 복원.
    """
    left_c = _fc._compact_ws(left)
    if not left_c:
        return None
    r = _norm_space(right)
    if not r:
        return None
    duty = ""
    for d in sorted(_DUTY_TAIL_AFTER_STAR, key=len, reverse=True):
        if r.startswith(d):
            duty = d
            break
    if not duty:
        return None

    specs = sorted(
        (_fc._compact_ws(sp) for sp in _CATALOG.all_specialties if _fc._compact_ws(sp)),
        key=len,
        reverse=True,
    )
    i = 0
    parts: List[str] = []
    while i < len(left_c):
        hit = ""
        for sp in specs:
            if left_c.startswith(sp, i):
                hit = sp
                break
        if not hit:
            return None
        parts.append(hit)
        i += len(hit)
    if i != len(left_c):
        return None
    return "".join(parts) + "*" + duty


def normalize_specialty_field(raw: str) -> str:
    """
    // [수정] '전문분야' 필드 정규화.
    JSON 오류 샘플 기반(하드코딩 금지, 일반 규칙):
    - 줄바꿈/과도 공백 제거, OCR(립식→입식), `*` 주변 공백 정리
    - 같은 문자열이 붙어서 2회 반복('토목구조토목구조' 등) → 1회로 축약
    - 특허·신기술·인증서·○○-○-○호 등 설명이 붙은 셀 → 잘라내지 않고 통째로 유지
    - `도로…상하수도*공사감독`처럼 전문분야+직무가 `*`로 붙은 경우 → 카탈로그 기반으로 좌측 압축 복원
    - 그 외 → 카탈로그 best-match, 실패 시 숫자/특허 앞까지 절단 폴백
    """
    s = _norm_space(_ocr_fix_specialty(str(raw or "")))
    if not s:
        return ""

    s = _normalize_star_glue(s)
    s = _dedupe_adjacent_repeat(s)

    if _should_keep_full_specialty_cell(s):
        return _norm_space(s)

    if "*" in s:
        a, b = s.split("*", 1)
        rebuilt = _try_rebuild_asterisk_compacted_specialties(a, b)
        if rebuilt:
            return rebuilt

    try:
        sp = best_match_specialty(s, _CATALOG)
        if sp:
            return sp
    except Exception:
        pass

    s2 = re.split(r"(?:\d{2,}|특허|인증서|신기술|호\)|호,|호\s)", s, maxsplit=1)[0].strip()
    s2 = _norm_space(s2)
    return s2


def normalize_duty_field(raw: str) -> str:
    """
    // [수정] '담당업무' 정규화.
    - 줄바꿈/공백 정리
    - 괄호가 열린 채로 끝나는 경우 ')' 보강(예: '(분야책임기술인')
    """
    s = _norm_space(str(raw or ""))
    if not s:
        return ""
    # 괄호 불균형 보정(최소 침습)
    if s.count("(") > s.count(")"):
        s = s + ")"
    return s


def normalize_worktype_field(raw: str) -> str:
    """// [수정] '공사종류' 공백 정규화(줄바꿈→공백, 중복 공백 축약)."""
    return _norm_space(str(raw or ""))


def _is_annotation_or_footnote_line(line: str) -> bool:
    """
    표 하단 각주·법령 설명 등(책임정도 별표 안내)이 사업명 라인으로 오인되는 것을 막는다.
    """
    t = re.sub(r"\s+", " ", (line or "").strip())
    if not t:
        return False
    if "책임정도의" in t and "*" in t:
        return True
    if re.match(r"^\(책임정도", t):
        return True
    if t.startswith("(*") and "는" in t and len(t) <= 120:
        return True
    if re.match(r"^\(\*", t) and ("별표" in t or "법" in t or "시행령" in t) and len(t) <= 160:
        return True
    if t in ("(인정일)", "(참여일)"):
        return True
    if re.fullmatch(r"\(?\s*인정일\s*\)?", t) or re.fullmatch(r"\(?\s*참여일\s*\)?", t):
        return True
    return False


def _is_footer_or_header_line(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return True
    if _is_annotation_or_footnote_line(s):
        return True
    if s.startswith("Page :"):
        return True
    if s.startswith("성명 :"):
        return True
    if "기술경력" in s and ("1." in s or s.startswith("1")):
        return True
    if s.startswith("(") and "쪽" in s:
        return True
    if "본 증명서는 인터넷으로 발급" in s:
        return True
    footer_keywords = [
        "발급증명서확인",
        "www.kocea.or.kr",
        "문서확인번호",
        "문서 하단",
        "문서하단",
        "바코드로",
        "위·변조",
        "위변조",
        "발급일로부터",
        "90일까지",
        "가능합니다",
        "확인해 주십시오",
        "확인해 주십시오.",
        "확인해 주십시오. 다만",
        "내용의 위",
        "변조 여부",
    ]
    if any(k in s for k in footer_keywords):
        return True
    header_keywords = [
        "사업명",
        "직무분야",
        "담당업무",
        "참여기간",
        "발주자",
        "공사종류",
        "전문분야",
        "직위",
        "공사(용역)개요",
        "책임정도",
        "공사(용역)금액",
        "적용 공법",
        "적용 융",
        "시설물 종류",
        "비고",
        "(인정일)",
        "(참여일)",
    ]
    if s.startswith("사업명") and ("직무분야" in s or "담당업무" in s):
        return True
    if s == "참여기간" or s == "비고":
        return True
    hit = sum(1 for k in header_keywords if k in s)
    if hit >= 2 and len(s) <= 80:
        return True
    return False


def _strip_tail_job_duty(name: str) -> tuple[str, str, str]:
    """
    사업명 문자열 끝에서 '직무분야(+담당업무)' 꼬리를 감지해 분리.
    Returns: (정제된_사업명, 직무분야, 담당업무)
    """
    s = re.sub(r"\s+", " ", (name or "")).strip()
    if not s:
        return s, "", ""
    parts = s.split(" ")
    if len(parts) >= 3 and parts[-2] in _JOB_FIELD_HINTS:
        project = " ".join(parts[:-2]).strip()
        if project:
            return project, parts[-2], parts[-1]
    if len(parts) >= 2 and parts[-1] in _JOB_FIELD_HINTS:
        candidate = " ".join(parts[:-1]).strip()
        if candidate:
            return candidate, parts[-1], ""
    return s, "", ""

def _line_starts_with_overview_continue_marker(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return False
    if t.startswith("┖→"):
        return True
    if t.startswith("\u2516\u2192"):
        return True
    return False


def _strip_overview_continue_marker(s: str) -> str:
    t = (s or "").strip()
    if t.startswith("┖→"):
        t = t[2:].strip()
    elif t.startswith("\u2516\u2192"):
        t = t[2:].strip()
    if t.startswith("→"):
        t = t[1:].strip()
    return t


def _is_overview_continuation_block_end_line(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return True
    if _is_footer_or_header_line(s):
        return True
    if s.startswith("「") and "건설기술" in s:
        return True
    if "건설기술 진흥법 시행규칙" in s:
        return True
    if "본 증명서는 인터넷으로 발급" in s:
        return True
    if re.match(r"^2\.\s*", s) and "건설사업관리" in s:
        return True
    return False


def _lookahead_has_date_followed_by_tilde(lines: List[str], after_j: int, max_scan: int = 16) -> bool:
    for k in range(after_j + 1, min(len(lines), after_j + max_scan + 1)):
        sk = (lines[k] or "").strip()
        if not sk:
            continue
        if _DATE_RE.match(sk):
            for nk in range(k + 1, min(len(lines), k + 8)):
                ns = (lines[nk] or "").strip()
                if not ns:
                    continue
                if ns.lstrip().startswith("~"):
                    return True
                break
            return False
    return False


def _is_probable_project_name_line(s: str) -> bool:
    """
    최소한의 보수적 판정.
    - 과도한 제거를 피하고, footer/header/각주/날짜/틸드 등은 caller에서 먼저 거른다.
    """
    t = re.sub(r"\s+", " ", (s or "")).strip()
    if not t:
        return False
    if len(t) <= 1:
        return False
    return True


def _line_begins_next_career_after_overview_continuation(lines: List[str], j: int, start_marker_index: int) -> bool:
    if j <= start_marker_index:
        return False
    ln = (lines[j] or "").strip()
    if not ln or _line_starts_with_overview_continue_marker(ln):
        return False
    if not _is_probable_project_name_line(ln):
        return False
    return _lookahead_has_date_followed_by_tilde(lines, j)


def extract_tech_overview_continuation_from_page_text(text: str) -> str:
    if not (text or "").strip():
        return ""
    raw_lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
    lines = [ln for ln in raw_lines if ln]
    start_i: int | None = None
    for i, ln in enumerate(lines):
        if _line_starts_with_overview_continue_marker(ln):
            start_i = i
            break
    if start_i is None:
        return ""
    chunks: List[str] = []
    for j in range(start_i, len(lines)):
        ln = lines[j]
        if _is_overview_continuation_block_end_line(ln):
            break
        if _DATE_RE.match(ln):
            is_next_career_start = False
            for nk in range(j + 1, min(len(lines), j + 6)):
                ns = (lines[nk] or "").strip()
                if not ns:
                    continue
                if ns.lstrip().startswith("~"):
                    is_next_career_start = True
                break
            if is_next_career_start:
                break
        if j > start_i and _line_begins_next_career_after_overview_continuation(lines, j, start_i):
            break
        if j == start_i:
            piece = _strip_overview_continue_marker(ln)
            if piece:
                chunks.append(piece)
        elif _line_starts_with_overview_continue_marker(ln):
            piece = _strip_overview_continue_marker(ln)
            if piece:
                chunks.append(piece)
        else:
            chunks.append(ln)
    return "\n".join(chunks).strip()


def merge_cross_page_tech_overviews(
    ctx: DocumentContext,
    careers: List[Dict],
    tech_start: int,
    tech_end: int,
    page_counts_after: List[tuple[int, int]],
) -> None:
    """
    기술경력 구간에서 i페이지 하단이 ┖→로 다음 쪽으로 넘어가는 경우,
    (i+1)페이지 상단 연장 텍스트를 해당 사업의 '공사(용역)개요'에 덧붙인다.
    """
    if not careers or tech_end <= tech_start + 1:
        return
    try:
        for si in range(len(page_counts_after) - 1):
            _, n_after = page_counts_after[si]
            next_page_idx, _ = page_counts_after[si + 1]
            if next_page_idx >= tech_end:
                continue
            if n_after <= 0:
                continue
            target = n_after - 1
            if target < 0 or target >= len(careers):
                continue
            if next_page_idx >= ctx.total_pages:
                continue
            tnext = ctx.get_text(next_page_idx) or ""
            cont = extract_tech_overview_continuation_from_page_text(tnext)
            if not cont:
                continue

            prev_ov = str(careers[target].get("공사(용역)개요") or "").strip()
            if prev_ov:
                careers[target]["공사(용역)개요"] = (prev_ov + "\n" + cont).strip()
            else:
                careers[target]["공사(용역)개요"] = cont

            try:
                pp = careers[target].get("_pdf_pages")
                if not isinstance(pp, list):
                    pp = []
                if (next_page_idx + 1) not in pp:
                    pp.append(next_page_idx + 1)
                careers[target]["_pdf_pages"] = sorted(
                    {int(x) for x in pp if str(x).strip().isdigit()}
                )
            except Exception:
                pass

            first_next_idx = n_after
            if first_next_idx < len(careers):
                next_name = str(careers[first_next_idx].get("사업명") or "").strip()
                cont_norm = re.sub(r"\s+", " ", cont).strip()
                name_norm = re.sub(r"\s+", " ", next_name).strip()
                # 연속 텍스트가 충분히 길 때만 사업명 정리 (짧으면 오판 위험)
                if cont_norm and len(cont_norm) >= 8 and name_norm.startswith(cont_norm):
                    cleaned = name_norm[len(cont_norm) :].strip()
                    careers[first_next_idx]["사업명"] = cleaned
    except Exception:
        return

