#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
제2쪽 파서: 기술경력

표·텍스트 추출 순서 한계를 넘기려면 extract_words 좌표 기반 셀 재구성이
장기적으로 유효하나, 현재는 줄 단위 휴리스틱·표 교정으로 처리한다.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict, Any, Optional, Tuple, Iterator
from functools import lru_cache
from pathlib import Path
import re
import json
import time
from datetime import datetime
import pdfplumber
from parsers.document_context import DocumentContext
from parsers.table_settings import LINE_TABLE_SETTINGS, VIRTUAL_LEFT_X, VIRTUAL_RIGHT_X, extract_tables_merged, pick_best_table, safe_extract_tables
from parsers.table_career_parser import (
    find_header_start_row,
    iter_records_4rows,
    merge_into_previous,
    normalize_table_to_6cols,
    parse_period_cell,
)
from parsers.raw_table_dump import dump_raw_tables_to_excel

# region agent log
_AGENT_DEBUG_LOG_PATH = "debug-dcc858.log"
_AGENT_DEBUG_SESSION_ID = "dcc858"


def _agent_log(*, run_id: str, hypothesis_id: str, location: str, message: str, data: dict) -> None:
    """DEBUG MODE: append NDJSON log lines (never raises)."""
    try:
        payload = {
            "sessionId": _AGENT_DEBUG_SESSION_ID,
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        with open(_AGENT_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        return


def _agent_safe_name_sample(rows: list[dict], *, limit: int = 3) -> list[dict]:
    out: list[dict] = []
    for r in (rows or [])[: max(0, limit)]:
        try:
            nm = str((r or {}).get("사업명") or "")
            out.append(
                {
                    "사업명": nm[:80],
                    "len": len(nm.strip()),
                    "starts_with_cont_arrow": nm.strip().startswith("┖→"),
                }
            )
        except Exception:
            continue
    return out

# endregion agent log


def _looks_like_table_column_header_phrase(s: str) -> bool:
    """
    '적용 공법/시설물 종류/비고...' 같은 "컬럼 헤더" 조합이 사업명으로 들어오는 오염 탐지.
    """
    t = re.sub(r"\s+", " ", (s or "")).strip()
    if not t:
        return False
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
        "적용 신기술",
        "시설물 종류",
        "비고",
    ]
    hit = sum(1 for k in header_keywords if k in t)
    # 헤더성 문구는 보통 짧고(<=80) 키워드가 2개 이상 같이 등장
    if hit >= 2 and len(t) <= 80:
        return True
    # 단독으로도 헤더로 자주 등장하는 케이스(안전측)
    if t in {"적용 공법", "시설물 종류", "적용 신기술 등", "적용 융ㆍ복합건설기술", "적용 융복합건설기술"}:
        return True
    return False


def _sanitize_header_like_project_names(rows: List[Dict[str, Any]], *, page_num_1based: int) -> None:
    """
    사업명/직무/담당이 표 컬럼 헤더 문구로 오염된 행을 무효화(빈값)한다.
    이후 _ensure_tech_career_names_non_empty / 표 보강 로직이 정상 이름을 채울 수 있게 만든다.
    """
    if not rows:
        return
    logged = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        nm = str(r.get("사업명") or "").strip()
        if _looks_like_table_column_header_phrase(nm):
            r["사업명"] = ""
            # 같이 따라오는 오염 패턴도 비움
            jf = str(r.get("직무분야") or "").strip()
            dt = str(r.get("담당업무") or "").strip()
            if _looks_like_table_column_header_phrase(jf):
                r["직무분야"] = ""
            if _looks_like_table_column_header_phrase(dt):
                r["담당업무"] = ""
            if logged < 3:
                logged += 1
                # region agent log
                _agent_log(
                    run_id="pre-fix",
                    hypothesis_id="D",
                    location="page_2_parser.py:_sanitize_header_like_project_names",
                    message="cleared header-like project name contamination",
                    data={
                        "page_num_1based": page_num_1based,
                        "bad_name": nm[:80],
                        "발주자": str(r.get("발주자") or "")[:40],
                        "직위": str(r.get("직위") or "")[:20],
                    },
                )
                # endregion agent log


def _is_bonsa_like_project_name(s: str) -> bool:
    t = re.sub(r"\s+", "", (s or "")).strip()
    if not t:
        return False
    return t == "본사" or t.startswith("본사")


def _looks_like_overview_sentence_as_project_name(s: str) -> bool:
    """
    '공사(용역)개요'에 가까운 문장이 사업명으로 들어오는 오염을 탐지한다.
    - 설정(tech_career_heuristics)의 패턴 + 단위/규격 토큰을 함께 사용한다.
    """
    raw = re.sub(r"\s+", " ", (s or "")).strip()
    if not raw:
        return False
    # 본사는 정상 사업명 취급(배제 금지)
    if _is_bonsa_like_project_name(raw):
        return False
    try:
        if _RE_OVERVIEW_MEASURE.search(raw) or _RE_OVERVIEW_LISTISH.search(raw):
            return True
    except Exception:
        pass
    # 보조 규칙: 규격/단위/길이/직경/수량 나열
    # - D300, D1200 같은 "관경" 표기가 핵심이며, D011 같은 코드/식별자는 사업명에도 등장할 수 있어 제외한다.
    if re.search(r"(\bD[1-9]\d{2,3}\b|L\d+(\.\d+)?|\bØ\d+\b)", raw, flags=re.IGNORECASE):
        return True
    if re.search(r"[㎜㎞㎡㎥]", raw):
        return True
    # NOTE:
    # 과거에는 "콤마 + 숫자" 나열을 개요로 간주했지만,
    # 실제 사업명에도 '(8~10,15,17지구)' 같은 표기가 흔해 오탐이 발생했다.
    # 개요 오염 제거는 "규격/단위/측정치" 패턴 중심으로만 판별한다.
    return False


def _sanitize_overview_like_project_names(rows: List[Dict[str, Any]], *, page_num_1based: int) -> None:
    if not rows:
        return
    logged = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        nm = str(r.get("사업명") or "").strip()
        if not nm:
            continue
        if _looks_like_overview_sentence_as_project_name(nm):
            r["사업명"] = ""
            if logged < 3:
                logged += 1
                _agent_log(
                    run_id="pre-fix",
                    hypothesis_id="E",
                    location="page_2_parser.py:_sanitize_overview_like_project_names",
                    message="cleared overview-like sentence from project name",
                    data={
                        "page_num_1based": page_num_1based,
                        "bad_name": nm[:120],
                    },
                )


from parsers.issuer_reference import (
    longest_admin_prefix_token_count,
    longest_institution_prefix_token_count,
)
from parsers.worktype_classifier import (
    is_worktype_phrase,
    extract_worktype_suffix_from_tokens,
    split_issuer_and_worktype_by_catalog,
)
from parsers.tech_career_heuristics import (
    compiled_any,
    load_tech_career_heuristics,
)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_TC_H = load_tech_career_heuristics(_PROJECT_ROOT)
_RE_OVERVIEW_MEASURE = compiled_any(_TC_H.overview_measurement_patterns)
_RE_OVERVIEW_LISTISH = compiled_any(_TC_H.overview_listish_patterns)
_RE_PROJECT_START = compiled_any(_TC_H.project_start_regexes)

# 토큰 집합(설정 파일에서 로드)
_TILDE_SHORT_WORKTYPE_TOKENS = frozenset(_TC_H.tilde_short_worktype_tokens or ())
_DUTY_WORDS = frozenset(_TC_H.duty_words or ())
_JOB_FIELD_HINTS = frozenset(_TC_H.job_field_hints or ())
_TABLE_COL3_직위_토큰 = frozenset(_TC_H.table_col3_position_tokens or ())
_ISSUER_NAME_HINT = re.compile(_TC_H.issuer_name_hint_regex) if (_TC_H.issuer_name_hint_regex or "").strip() else re.compile(r"$^")


@lru_cache(maxsize=1)
def _tilde_line_catalog_specialties() -> frozenset[str]:
    """field_catalog.json(또는 xlsx)의 전문분야명 — ~ 라인에서 전문분야 토큰 판별에 사용."""
    try:
        from field_catalog import get_field_catalog, _norm

        cat = get_field_catalog(str(_PROJECT_ROOT))
        return frozenset(_norm(s) for s in cat.all_specialties if _norm(s))
    except Exception:
        return frozenset()


# ~ 라인에서 공사종류 칸에 자주 오는 짧은 토큰은 `data/tech_career_heuristics.json`에서 관리한다.


def _tilde_split_token_conserving(h: List[str], issuer: str, work: str) -> tuple[str, str]:
    """head 토큰 재결합과 결과가 어긋나면 공사종류를 비우고 전부 발주자(누락 방지)."""
    want = " ".join(h).strip()
    iss = re.sub(r"\s+", " ", (issuer or "").strip())
    wt = re.sub(r"\s+", " ", (work or "").strip())
    got = f"{iss} {wt}".strip()
    if want == got:
        return iss, wt
    return want, ""


def _tilde_split_head_to_issuer_and_worktype(head: List[str]) -> tuple[str, str]:
    """
    '~' 줄에서 직위·전문분야 앞쪽 토큰(head)을 발주자/공사종류로 나눈다.
    - 마지막 토큰이 짧은 공사종류(공원·국도 등)면 그 앞 전체를 발주자로 묶는다(경기도 양주시 + 공원).
    - 토큰이 둘뿐이고 마지막이 공사종류 목록에 없으면 둘 다 발주자(복합 기관명), 공사종류 공란.
    - PDF 추출로 공사종류 토큰이 연속 중복되면 하나로 정규화한다.
    - len(head)>=3이고 마지막이 짧은 공사종류가 아닐 때: 행정구역·공공기관 카탈로그로 접두 길이를 잡고,
      첫 토큰만 발주자로 두는 과분할을 완화한다(토큰 보존 불변식).
    """
    h = [x.strip() for x in head if str(x).strip()]
    while len(h) >= 2 and h[-1] == h[-2]:
        h = h[:-1]
    if not h:
        return "", ""
    if len(h) == 1:
        return h[0], ""
    last = h[-1]
    if last in _TILDE_SHORT_WORKTYPE_TOKENS:
        return _tilde_split_token_conserving(
            h, " ".join(h[:-1]).strip(), last
        )
    if len(h) == 2:
        # // FIX: 2토큰(head=[발주자, 공사종류]) 케이스에서 공사종류가 "도로,교량"처럼
        # 구분자 포함 형태로 오면 기존 로직은 둘 다 발주자로 묶어 공사종류가 비는 문제가 있었다.
        # 카탈로그 기반으로 2번째 토큰이 공사종류인지 먼저 판별한다.
        t0, t1 = h[0].strip(), h[1].strip()
        try:
            if t1 and is_worktype_phrase(t1, project_root=str(_PROJECT_ROOT)):
                return _tilde_split_token_conserving(h, t0, t1)
        except Exception:
            pass
        return _tilde_split_token_conserving(h, " ".join(h).strip(), "")
    # // FIX: 3토큰 이상에서는 suffix를 공사종류 후보로 먼저 떼어본다(콤마/괄호 조합 포함).
    try:
        k, wt = extract_worktype_suffix_from_tokens(h, project_root=str(_PROJECT_ROOT), max_suffix_tokens=4)
        if k >= 1 and wt and k < len(h):
            issuer = " ".join(h[:-k]).strip()
            return _tilde_split_token_conserving(h, issuer, wt)
    except Exception:
        pass
    k_adm = longest_admin_prefix_token_count(h)
    if k_adm >= 2 and k_adm < len(h):
        return _tilde_split_token_conserving(
            h, " ".join(h[:k_adm]).strip(), " ".join(h[k_adm:]).strip()
        )
    if k_adm == len(h):
        return _tilde_split_token_conserving(h, " ".join(h).strip(), "")
    k_inst = longest_institution_prefix_token_count(h)
    if k_inst >= 1 and k_inst < len(h):
        return _tilde_split_token_conserving(
            h, " ".join(h[:k_inst]).strip(), " ".join(h[k_inst:]).strip()
        )
    return _tilde_split_token_conserving(h, h[0].strip(), " ".join(h[1:]).strip())


def _tilde_token_looks_like_specialty(mid: str) -> bool:
    """'~' 발주자 줄에서 전문분야로 볼 만한 토큰."""
    m = (mid or "").strip()
    if not m:
        return False
    if m.endswith("구조"):
        return True
    if "·" in m:
        return True
    # 전문분야 토큰은 엑셀/JSON 카탈로그 기반으로 판단한다.
    # (하드코딩 목록은 새 문서에서 누락/오탐을 유발하므로 제거)
    if m in _tilde_line_catalog_specialties():
        return True
    return False


def _job_field_noise_before_specialty(mid: str, specialty: str) -> bool:
    """
    직무분야(조경·토목 등)가 PDF에서 공사종류/발주자 칸에 끼어든 토큰인지.
    해당 직무의 전문분야(카탈로그)와 짝이 맞을 때만 True (실제 공사종류 토큰은 제외).
    """
    mid = (mid or "").strip()
    sp = (specialty or "").strip()
    if not mid or not sp:
        return False
    if mid in _TILDE_SHORT_WORKTYPE_TOKENS:
        return False
    try:
        from field_catalog import get_field_catalog, _norm

        cat = get_field_catalog(str(_PROJECT_ROOT))
        sp_n = _norm(sp)
        for job, specs in cat.specialty_by_job.items():
            if job != mid:
                continue
            for spec in specs:
                if _norm(spec) == sp_n:
                    return True
        if mid in _JOB_FIELD_HINTS and len(mid) >= 2 and sp_n.startswith(_norm(mid)):
            return True
    except Exception:
        sp_u = sp.replace("ㆍ", "·")
        if mid in _JOB_FIELD_HINTS and len(mid) >= 2 and sp_u.startswith(mid):
            return True
    return False


def _strip_trailing_job_noise_from_issuer(issuer: str, specialty: str) -> str:
    toks = [t for t in (issuer or "").split() if t]
    while toks and _job_field_noise_before_specialty(toks[-1], specialty):
        toks = toks[:-1]
    return " ".join(toks).strip()


def _strip_overlapping_worktype_from_issuer(발주자: str, 공사종류: str) -> str:
    """
    발주자 문자열 끝에 공사종류와 동일한 토큰(또는 전체 공사종류)이 중복되어 붙어 있으면 제거한다.
    예: 발주자 '서천군 관광지', 공사종류 '관광지' → '서천군'
    """
    iss = " ".join((발주자 or "").split())
    wt = " ".join((공사종류 or "").split())
    if not wt or not iss:
        return iss
    if iss == wt:
        return ""
    it = iss.split()
    wtok = wt.split()
    if not wtok:
        return iss
    n = len(wtok)
    if len(it) >= n and it[-n:] == wtok:
        return " ".join(it[:-n]).strip()
    if iss.endswith(wt) and len(iss) > len(wt):
        prev = iss[: len(iss) - len(wt)].rstrip()
        if prev:
            return prev
    return iss


def _finalize_tilde_issuer_work(발주자: str, 공사종류: str, 전문분야: str) -> tuple[str, str]:
    sp = (전문분야 or "").strip()
    if not sp:
        return (발주자 or "").strip(), (공사종류 or "").strip()
    iss = (발주자 or "").strip()
    wt = (공사종류 or "").strip()
    if wt and _job_field_noise_before_specialty(wt, sp):
        wt = ""
    iss = _strip_trailing_job_noise_from_issuer(iss, sp)
    return iss, wt


def _tilde_emit(
    발주자: str = "",
    공사종류: str = "",
    전문분야: str = "",
    직위: str = "",
    비고: str = "",
) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "발주자": 발주자 or "",
        "공사종류": 공사종류 or "",
        "전문분야": 전문분야 or "",
        "직위": 직위 or "",
        "비고": 비고 or "",
    }
    bi, bw = _finalize_tilde_issuer_work(
        str(d.get("발주자") or ""),
        str(d.get("공사종류") or ""),
        str(d.get("전문분야") or ""),
    )
    d["발주자"] = bi
    d["공사종류"] = bw
    return d


def _cleanup_tech_career_job_noise_row(row: Dict[str, Any]) -> None:
    """
    표·틸드·스택 병합 후에도 직무분야(조경 등)가 발주자/공사종류에 남거나,
    전문분야명이 발주자 칸에만 중복된 경우 정리한다.
    공사종류와 발주자 끝이 겹치면 발주자에서 해당 중복을 제거한다.
    """
    sp = str(row.get("전문분야") or "").strip()
    iss = str(row.get("발주자") or "").strip()
    wt = str(row.get("공사종류") or "").strip()
    if sp and iss == sp and _tilde_token_looks_like_specialty(sp):
        iss = ""
    if sp:
        iss, wt = _finalize_tilde_issuer_work(iss, wt, sp)
    else:
        iss = (iss or "").strip()
        wt = (wt or "").strip()
    # // FIX: 발주자 끝에 공사종류가 붙어 공사종류가 비는 케이스를 카탈로그로 복구한다.
    # 예) 발주자="한국도로공사 도로,교량", 공사종류="" → 발주자="한국도로공사", 공사종류="도로,교량"
    if iss:
        try:
            iss2, wt2 = split_issuer_and_worktype_by_catalog(iss, project_root=str(_PROJECT_ROOT))
            if wt2 and (not wt):
                iss, wt = iss2, wt2
        except Exception:
            pass
    # 카탈로그에 없는 콤마·중점 목록형 공사종류는 토큰 접미로 못 뗌 → 발주자 셀 마지막 공백 분리
    if iss and not wt:
        iss, wt = _split_issuer_and_work_type_from_issuer_cell(iss, wt)
    iss = _strip_overlapping_worktype_from_issuer(iss, wt)
    row["발주자"] = iss
    row["공사종류"] = wt


def _cleanup_tech_career_job_noise_rows(rows: List[Dict[str, Any]]) -> None:
    for r in rows:
        _cleanup_tech_career_job_noise_row(r)


def _fix_shifted_fields_in_tech_career_rows(rows: List[Dict[str, Any]]) -> None:
    """
    기술경력 행에서 "개요/사업명/직위/직무분야"가 1칸씩 밀리는 대표 케이스를 보정한다.

    관측된 오류(손희구 2025-03-04):
    - 공사(용역)개요(예: NATM..., Steel Box Girder교)가 다음 행 사업명 앞에 붙음
    - '본사 / 감리부'처럼 슬래시 포함 사업명이 '본사 /' + '감리부'로 분리되어 직무분야로 들어감
    - 직위(상무보 등)가 발주자 칸으로 들어가 직위가 비는 케이스
    - '기본 및 실시설계용역'에서 '및'이 직무분야로 오인되고 담당업무가 '실시설계'로 오염되는 케이스
    """

    def _norm_space(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    def _repair_korean_token_splits_in_project_name(name: str) -> str:
        """
        PDF 텍스트/표 추출에서 흔한 '단어 내부 공백'을 사업명에 한해 보정한다.
        목표: '설 계', '포 장', '사 업', '실 시설계', '대 장작성' 같은 분절을
        일반 규칙으로 복원하되, 정상적인 단어 경계 공백은 최대한 유지한다.

        원칙:
        - 단일 음절(1글자) 토큰이 다음 토큰과 결합될 때 의미가 자연스러운 경우에만 병합
        - 다음 토큰이 '프로젝트 꼬리/마커' 또는 흔한 업무/산출물 접미에 해당하면 병합 강도 ↑
        - 모든 공백을 제거하는 방식은 금지(가독성/의미 손실)
        """
        s = _norm_space(name)
        if not s or " " not in s:
            return s

        # 프로젝트명에서 자주 나타나는 접미/마커(일반 규칙)
        # - tech_career_heuristics의 marker를 재사용해 하드코딩을 최소화한다.
        markers = tuple(_TC_H.project_title_markers or ())
        # 보정에 유용한 추가 접미(문서 전반에 공통적으로 반복되는 단어)
        # - 특정 사업/지명 하드코딩이 아니라 "형태" 기반 접미
        common_suffixes = (
            "실시설계",
            "기본설계",
            "기본계획",
            "정밀안전진단",
            "안전진단",
            "타당성조사",
            "영향평가",
            "평가용역",
            "설계용역",
            "조사용역",
            "감리용역",
        )

        toks = s.split(" ")
        out: list[str] = []
        i = 0
        while i < len(toks):
            cur = toks[i]
            if (
                i + 1 < len(toks)
                and len(cur) == 1
                and re.fullmatch(r"[가-힣]", cur or "") is not None
            ):
                nxt = toks[i + 1]
                # 다음 토큰이 마커/접미로 시작하면 단어 내부 분절일 가능성이 높다.
                if any(nxt.startswith(m) for m in markers if m) or any(
                    nxt.startswith(ss) for ss in common_suffixes
                ):
                    # 예: "실 시설계" / "사 업" / "포 장공사" 등
                    out.append(f"{cur}{nxt}")
                    i += 2
                    continue
                # 다음 토큰이 1글자 한글이면 연쇄 분절(예: '사 업')로 보고 병합
                if len(nxt) == 1 and re.fullmatch(r"[가-힣]", nxt or "") is not None:
                    out.append(f"{cur}{nxt}")
                    i += 2
                    continue
                # 다음 토큰이 한글로 시작하고, 현재까지 out의 마지막이 한글/숫자로 끝나면
                # 분절 가능성이 있어 보정 후보지만, 과도 병합을 막기 위해 보수적으로 처리
                # (여기서는 병합하지 않고 그대로 둔다.)
            # 반대 방향 분절: 다음 토큰이 1글자(예: '사 업', '설 계', '포 장')
            if (
                i + 1 < len(toks)
                and len(toks[i + 1]) == 1
                and re.fullmatch(r"[가-힣]", toks[i + 1] or "") is not None
                and (toks[i + 1] not in {"은", "는", "이", "가", "을", "를", "과", "와", "및", "또", "등"})
                and re.search(r"[가-힣A-Za-z0-9)]$", cur or "")
            ):
                out.append(f"{cur}{toks[i + 1]}")
                i += 2
                continue

            # 어간 말음절 + 접미(용역/공사/사업/설계/계획/조사/…): 마지막 1음절이 다음 토큰으로 넘어간 케이스
            # 예) "…공사설" + "계용역" → "…공사설계용역"
            # 예) "…확.포" + "장공사" → "…확.포장공사"
            if i + 1 < len(toks):
                nxt = toks[i + 1] or ""
                # '실 시설계'는 표/텍스트 추출에서 특히 자주 깨지는 조합.
                # - '시설계' 자체가 독립 단어로 쓰이기보다 '실시설계'의 일부로 나타나는 경우가 압도적이어서
                #   (또는 '...공사실 시설계'처럼 1글자가 이월되는 형태)
                #   이 경우에만 매우 좁게 결합한다(정상 공백 보존 우선).
                if cur.endswith("실") and nxt.startswith("시설계"):
                    out.append(f"{cur}{nxt}")
                    i += 2
                    continue
                if re.search(r"[가-힣]$", cur or "") and re.match(
                    r"^[가-힣](?:용역|공사|사업|설계|계획|조사|진단|정비|복구|작성)",
                    nxt,
                ):
                    out.append(f"{cur}{nxt}")
                    i += 2
                    continue
            out.append(cur)
            i += 1

        s2 = " ".join(out)
        # 2차: "글자 1개 + 공백 + 한글" 형태의 분절을 접두/어간 내부 분절로 보고 결합
        # - 단, 조사/접속사 등(은/는/이/가/을/를/과/와/및/또는/등/및)은 결합하지 않는다.
        # - 사업명에서 빈번한 '설 계', '사 업', '포 장', '대 장', '실 시설계' 등의 패턴을
        #   특정 단어 하드코딩 없이 "형태"로만 복원한다.
        STOP_1 = {"은", "는", "이", "가", "을", "를", "과", "와", "및", "또", "등"}
        # 반복 적용이 필요할 수 있어 2번 정도 루프
        for _ in range(2):
            s2 = re.sub(
                r"(?<!\S)([가-힣])\s+([가-힣][가-힣A-Za-z0-9]{1,})",
                lambda m: f"{m.group(1)}{m.group(2)}" if m.group(1) not in STOP_1 else m.group(0),
                s2,
            )
            # 1글자 토큰끼리의 연쇄 분절(예: '사 업')만 결합. 정상 단어 경계 공백은 유지.
            s2 = re.sub(
                r"(?<!\S)([가-힣])\s+([가-힣])(?!\S)",
                lambda m: f"{m.group(1)}{m.group(2)}" if m.group(1) not in STOP_1 else m.group(0),
                s2,
            )

        # 3차: 'A. B' 형태로 잘린 약어/약식 표기 주변의 불필요 공백 정리
        s2 = re.sub(r"\.\s+", ".", s2)  # '확. 포장' → '확.포장'
        # 매우 좁은 보정: '실 시설계' → '실시설계'
        # (표/텍스트 추출에서 자주 발생하며, 정상 공백을 침범할 가능성이 낮음)
        s2 = re.sub(r"실\s+시설계", "실시설계", s2)
        s2 = re.sub(r"\s+\)", ")", s2)
        s2 = re.sub(r"\(\s+", "(", s2)
        return _norm_space(s2)

    def _append_overview(row: Dict[str, Any], overview: str) -> None:
        ov = _norm_space(overview)
        if not ov:
            return
        cur = str(row.get("공사(용역)개요") or "").strip()
        if not cur:
            row["공사(용역)개요"] = ov
        elif ov not in cur:
            # 중복 방지: 이미 포함돼 있지 않을 때만 앞에 붙인다.
            row["공사(용역)개요"] = f"{ov}\n{cur}".strip()

    def _looks_like_overview_prefix(s: str) -> bool:
        t = _norm_space(s)
        if not t:
            return False
        return any(t.startswith(p) for p in (_TC_H.overview_prefix_hints or ()))

    def _looks_like_overview_text(s: str) -> bool:
        """
        개요/연장 텍스트로 볼 가능성이 큰지(수치/규격/목록형).
        """
        t = _norm_space(s)
        if not t:
            return False
        if _RE_OVERVIEW_MEASURE and _RE_OVERVIEW_MEASURE.search(t):
            return True
        if _RE_OVERVIEW_LISTISH:
            # 너무 일반적인 "," 매칭만으로 True가 되는 것을 막기 위해 기준을 둔다.
            # 콜론/개소/1식 등과 결합되면 강한 신호로 취급.
            if re.search(r":\s*\d", t) or re.search(r"\b\d+\s*개소\b", t) or re.search(r"\b1\s*식\b", t):
                return True
        return False

    def _split_overview_prefix_from_project_name(name: str) -> tuple[str, str]:
        """
        '개요 조각 + 사업명'이 합쳐진 문자열에서 앞쪽 개요(prefix)를 떼어낸다.
        - 특정 지명/키워드(하드코딩) 대신, "프로젝트명 마커"가 시작되는 위치를 찾는다.
        """
        s = _norm_space(name)
        if not s:
            return "", ""
        # 너무 짧으면 분리 의미 없음
        if len(s) < 12:
            return "", s
        # NOTE: 과도한 prefix 분리는 사업명을 '공사…' 같은 꼬리만 남기기 쉬워
        #       실제 운영에서는 보수적으로 사용한다. (현재는 미사용)
        best = None
        for m in (_TC_H.project_title_markers or ()):
            if not m:
                continue
            p = s.find(m)
            if p <= 0:
                continue
            # "공사/용역/사업" 등이 너무 앞(예: 1~2글자)에 나오면 prefix로 보기 어려움
            if p < 6:
                continue
            if best is None or p < best:
                best = p
        if best is None:
            return "", s
        prefix = s[:best].strip(" ,:/")
        rest = s[best:].strip()
        # (미사용)
        return "", s

    def _split_leading_ascii_method_prefix(name: str) -> tuple[str, str]:
        """
        사업명 앞에 영문 공법/공정명이 붙어 들어오는 케이스를 분리한다.
        예) "Pre-Lodding/Sand Drain논산지구..." → ("Pre-Lodding/Sand Drain", "논산지구...")
            "NATM/STEEL BOX GIRDER증평..."      → ("NATM/STEEL BOX GIRDER", "증평...")
        - 조건을 보수적으로: (한글이 없는 ASCII 위주) + (슬래시 포함) + (바로 뒤에 한글 시작)
        """
        s = _norm_space(name)
        if not s or "/" not in s:
            return "", s
        # '영문/기호' 덩어리 다음에 한글이 바로 이어지는 패턴을 찾는다.
        # - 과도 분리를 막기 위해 prefix 길이 상한을 둔다.
        m = re.match(r"^([A-Za-z][A-Za-z0-9 .,/+\-]{2,40})(?=[가-힣])", s)
        if not m:
            return "", s
        prefix = (m.group(1) or "").strip()
        if not prefix or len(prefix) < 4:
            return "", s
        # prefix 안에 한글이 섞이면 프로젝트명일 수 있어 제외
        if re.search(r"[가-힣]", prefix):
            return "", s
        # 슬래시가 공법/공정명 구분자로 쓰이는 케이스만 타겟팅
        if "/" not in prefix:
            return "", s
        rest = s[len(m.group(1)) :].strip()
        if not rest:
            return "", s
        return prefix, rest

    def _move_method_prefix_out_of_project_name(idx: int, r: Dict[str, Any]) -> None:
        """사업명 선두의 공법/공정 prefix를 '적용 공법'(우선) 또는 개요로 이동."""
        nm0 = _norm_space(str(r.get("사업명") or ""))
        if not nm0:
            return

        # 1) 현재 사업명이 "영문 공법명 + 한글 사업명" 형태면 분리
        pref, rest = _split_leading_ascii_method_prefix(nm0)
        if pref and rest and _is_probable_project_name_line(rest):
            cur_method = _norm_space(str(r.get("적용 공법") or ""))
            cur_ov = _norm_space(str(r.get("공사(용역)개요") or ""))
            # 이미 동일 prefix가 다른 필드에 있으면 중복 저장을 피한다.
            if (pref not in cur_method) and (pref not in cur_ov):
                if not cur_method:
                    r["적용 공법"] = pref
                else:
                    r["적용 공법"] = _norm_space(f"{cur_method} {pref}")
            r["사업명"] = rest
            nm0 = rest

        # 2) 직전 행의 개요/공법이 다음 사업명 앞에 붙는 중복 케이스 제거
        if idx - 1 >= 0 and isinstance(rows[idx - 1], dict):
            prev = rows[idx - 1]
            prev_method = _norm_space(str(prev.get("적용 공법") or ""))
            prev_ov = _norm_space(str(prev.get("공사(용역)개요") or ""))
            nm = _norm_space(str(r.get("사업명") or ""))
            if nm:
                for cand in [prev_method, prev_ov]:
                    if not cand:
                        continue
                    # 공백 제거 비교(예: "Pre-Lodding/Sand Drain" + "논산..."이 붙어 "Drain논산..."처럼 나옴)
                    cand_compact = re.sub(r"\s+", "", cand)
                    nm_compact = re.sub(r"\s+", "", nm)
                    if cand_compact and nm_compact.startswith(cand_compact) and len(nm_compact) > len(cand_compact) + 3:
                        rest_compact = nm_compact[len(cand_compact) :].strip()
                        # 원본 nm에서 가능한 한 자연스럽게 제거(앞부분 cand를 다양한 공백 패턴으로 매칭)
                        pat = re.compile(rf"^{re.escape(cand)}\s*")
                        nm2 = pat.sub("", nm).strip()
                        if not nm2:
                            nm2 = rest_compact
                        if nm2 and _is_probable_project_name_line(nm2):
                            r["사업명"] = nm2
                            break

    for idx, r in enumerate(rows or []):
        if not isinstance(r, dict):
            continue

        # 1) 발주자 칸에 직위가 들어간 경우 swap
        issuer = str(r.get("발주자") or "").strip()
        pos = str(r.get("직위") or "").strip()
        # '상무보'처럼 직위로 강한 토큰을 설정으로 보강한다.
        issuer_is_position = _looks_like_position_token(issuer) or issuer in (_TC_H.issuer_position_extra_tokens or ())
        if issuer and issuer_is_position and not pos:
            r["직위"] = issuer
            r["발주자"] = ""

        # 2) '본사 / 감리부'가 분리되어 들어간 경우 결합 + 직무분야 복구
        name = _norm_space(str(r.get("사업명") or ""))
        jf = _norm_space(str(r.get("직무분야") or ""))
        if name.endswith("/") and jf and jf.endswith("부") and (jf not in _JOB_FIELD_HINTS):
            combined = _norm_space(f"{name} {jf}")
            # 대표 케이스는 '본사 / 감리부'
            r["사업명"] = combined.replace("본사 /", "본사 /").replace("/ ", "/ ").replace(" / ", " / ")
            r["직무분야"] = "토목"
        else:
            # 이미 '본사 / 감리부' 형태인데 직무분야가 감리부로 오염된 경우
            if ("/" in name) and (jf == "감리부"):
                r["사업명"] = name.replace("본사 /", "본사 /").replace("본사/", "본사/").replace("/감리부", "/감리부")
                r["직무분야"] = "토목"

        # 3) '및' 오염(사업명 줄바꿈 분리) 보정
        name = _norm_space(str(r.get("사업명") or ""))
        jf = _norm_space(str(r.get("직무분야") or ""))
        duty = _norm_space(str(r.get("담당업무") or ""))
        if jf == (_TC_H.and_token or "및"):
            # 사업명에 "기본"과 "용역"이 있고 "실시설계"가 빠져있으면 보강
            if ("용역" in name) and ("실시설계" not in name):
                # "기본 용역"처럼 끝나는 경우가 많음
                if name.endswith("용역"):
                    r["사업명"] = _norm_space(name.replace("기본 용역", f"기본 {_TC_H.and_repair_phrase}"))
                else:
                    r["사업명"] = _norm_space(f"{name} {_TC_H.and_repair_phrase}")
            r["직무분야"] = "토목"
            if duty == "실시설계":
                r["담당업무"] = "설계"

        # 4) 개요 약어/연장 텍스트가 사업명 앞에 붙어 다음 사업명으로 흡수된 케이스 분리
        name = _norm_space(str(r.get("사업명") or ""))
        if name:
            # A) 목록형 연장(…: 16개소, … 1식 등)이 사업명 문자열 앞에 붙는 케이스 분리
            # - 특정 지명 하드코딩 없이, "프로젝트 제목 시작" 정규식으로 분리한다.
            if _RE_PROJECT_START and (":" in name) and (re.search(r"\b1\s*식\b", name) or re.search(r"\b\d+\s*개소\b", name)):
                m = _RE_PROJECT_START.search(name)
                if m and m.start() >= 8:
                    prefix = name[: m.start()].strip(" ,")
                    rest = name[m.start() :].strip()
                    # prefix가 개요/연장 텍스트처럼 보이면 직전 행 개요로 이동
                    if prefix and _looks_like_overview_text(prefix) and rest:
                        prev = rows[idx - 1] if idx - 1 >= 0 and isinstance(rows[idx - 1], dict) else None
                        _append_overview(prev if prev is not None else r, prefix)
                        r["사업명"] = rest

            # "…교" 단위로 잘리는 약어 케이스(예: NATM...교 + 실제사업명) 보강
            # - '교'는 개요(교량형식)에서 자주 끊기는 단위라 비교적 안전한 분리 기준이다.
            name2 = _norm_space(str(r.get("사업명") or ""))
            if _looks_like_overview_prefix(name2) and "교" in name2:
                cut = name2.find("교")
                if 0 < cut < 48 and cut + 1 < len(name2):
                    ov = name2[: cut + 1].strip()
                    rest2 = name2[cut + 1 :].strip()
                    # rest가 실제 사업명처럼 보일 때만 분리
                    if rest2 and any(k in rest2 for k in ("공사", "용역", "사업", "정비", "설계")):
                        _append_overview(r, ov)
                        r["사업명"] = rest2

        # C) 공사(용역)개요에 "목록형 연장 + 수치/규격"이 섞이면 앞쪽은 직전 행으로 이동
        ov_text = _norm_space(str(r.get("공사(용역)개요") or ""))
        if ov_text and ":" in ov_text and "L=" in ov_text:
            # ':'가 있는 목록형 텍스트(개소/1식 등)가 다음 사업의 L=개요와 붙는 경우가 있다.
            # 문자열 기반으로 'L=' 위치를 기준으로 분리하면 PDF별 변형에도 비교적 강하다.
            # '...부강역~북대전ICL=...'처럼 공백 없이 붙는 케이스도 있어, 마지막 'L=' 기준으로 자른다.
            p = ov_text.rfind("L=")
            if p > 0:
                head = ov_text[:p].strip(" ,")
                rest_ov = ov_text[p:].strip()
                if head and (re.search(r"\b\d+\s*개소\b", head) or re.search(r"\b1\s*식\b", head) or ":" in head):
                    prev = rows[idx - 1] if idx - 1 >= 0 and isinstance(rows[idx - 1], dict) else None
                    if prev is not None:
                        _append_overview(prev, head)
                        r["공사(용역)개요"] = rest_ov

        # 5) 슬래시 사업명 공백 정규화
        name = _norm_space(str(r.get("사업명") or ""))
        if name:
            # "본사 / 감리부"처럼 공백이 들쭉날쭉한 케이스를 표준화
            name = name.replace(" / ", " / ")
            name = name.replace("본사 /감리부", "본사 / 감리부")
            name = name.replace("본사/ 감리부", "본사/감리부")
            # 6) 사업명 한글 토큰 쪼개짐 보정(품질 향상)
            name = _repair_korean_token_splits_in_project_name(name)

            # 7) 사업명 중간에 "직무분야+담당업무" 조각이 끼어든 오염 제거
            # - 공백 경계가 깨져 붙는 케이스(예: "...모잠비크,모토목 사업관리로코")도 발생한다.
            jf2 = _norm_space(str(r.get("직무분야") or ""))
            duty2 = _norm_space(str(r.get("담당업무") or ""))
            if jf2 and duty2 and (jf2 in _JOB_FIELD_HINTS) and (duty2 in _DUTY_WORDS or _DUTY_PAREN_RE.match(duty2)):
                pat = re.compile(rf"{re.escape(jf2)}\s*{re.escape(duty2)}")
                hits = list(pat.finditer(name))
                if hits:
                    h = hits[-1]  # 과잉 제거 방지: 마지막 1회만 제거
                    nm2 = (name[: h.start()] + name[h.end() :]).strip()
                    nm2 = _norm_space(nm2)
                    nm2 = re.sub(r",\s*", ", ", nm2)
                    nm2 = re.sub(r"\s+,", ",", nm2)
                    if nm2:
                        name = nm2

            # 8) 사업명 선두의 서비스 타입(기술용역 등) 제거
            # - 일부 PDF에서 '적용 공법' 또는 개요 셀의 '기술용역'이 사업명 앞에 붙어 들어오거나,
            #   아예 사업명 셀 자체가 '기술용역<사업명>' 형태로 추출된다.
            # - '사업명' 필드는 순수 프로젝트명만 남기는 것이 일관되므로 선두 토큰을 제거한다.
            #
            # NOTE: 제거한 토큰은 기존 필드(공사(용역)개요/적용 공법)에 이미 존재할 수 있어
            #       무조건 이동하지 않고, 둘 다 비어 있을 때만 최소 침습으로 채운다.
            svc = "기술용역"
            if name.startswith(svc) and len(name) > len(svc) + 1:
                name = _norm_space(name[len(svc) :])
                if not str(r.get("공사(용역)개요") or "").strip() and not str(r.get("적용 공법") or "").strip():
                    r["공사(용역)개요"] = svc

            r["사업명"] = name

        # 9) (추가) 영문 공법/공정명이 사업명에 붙는 케이스 보정
        _move_method_prefix_out_of_project_name(idx, r)


def _cell_looks_like_책임정도_col(s: str) -> bool:
    """표 3행 등에서 책임정도 칸으로 볼 만한 짧은 값 (*참여기술인 등)."""
    t = re.sub(r"\s+", " ", (s or "").strip())
    if not t or len(t) > 48:
        return False
    if re.match(r"^\*?사업책임기술인", t):
        return True
    if re.match(r"^\*?참여기술인\s*$", t):
        return True
    if re.match(r"^\*?책임기술인", t):
        return True
    if "참여기술인" in t and len(t) <= 24:
        return True
    return False


def _probe_project_line_ok_for_current_block(
    lines: List[str], line_idx: int, cur_start_iso: str
) -> bool:
    """
    lines[line_idx]가 사업명 후보라도, 바로 아래 다른 시작일의 블록이 오면
    현재 날짜 블록에 넣지 않는다(동일 시작일 연속 행은 유지).
    """
    cur = str(cur_start_iso or "").strip()
    for _dk in range(line_idx + 1, min(len(lines), line_idx + 5)):
        _ds = (lines[_dk] or "").strip()
        if not _ds:
            continue
        if _DATE_RE.match(_ds) and _line_starts_new_date_block_after(lines, _dk):
            iso_n = _yyyy_mm_dd_to_iso(_ds)
            if iso_n and cur and iso_n != cur:
                return False
            return True
        break
    return True


_DATE_RE = re.compile(r"^\d{4}\.\d{2}(?:\.\d{2})?$")
# FIX: 일부 PDF는 날짜를 마스킹(****.**.**) 처리한다. 블록 경계 탐지에 필요.
_MASKED_DATE_RE = re.compile(r"^\*{4}\.\*{2}(?:\.\*{2})?$")


def _line_starts_new_date_block_after(lines: List[str], at: int) -> bool:
    """lines[at]가 YYYY.MM.DD 이고, 근처에 '~'가 있으면 새 기술경력 블록 시작으로 본다."""
    if at < 0 or at >= len(lines):
        return False
    s0 = (lines[at] or "").strip()
    if not _DATE_RE.match(s0) and not _MASKED_DATE_RE.match(s0):
        return False
    for kk in range(at + 1, min(len(lines), at + 6)):
        if (lines[kk] or "").lstrip().startswith("~"):
            return True
    return False
# FIX: '(일)'처럼 숫자가 마스킹/누락된 토큰도 등장한다.
_DAYS_RE = re.compile(r"^\(?\s*(\d[\d,]*)?\s*일\s*\)?$")
_DAYS_TOKEN_RE = re.compile(r"\(\s*(\d[\d,]*)?\s*일\s*\)")

# 괄호 포함 담당업무(예: "건설사업관리(감독권한대행)")를 매칭하는 패턴.
# _DUTY_WORDS에 "건설사업관리"를 넣으면 사업명("건설사업관리용역" 등)과 충돌하므로,
# 괄호 형태만 별도 정규식으로 인식한다.
_DUTY_PAREN_RE = re.compile(
    r"^(건설사업관리|시공관리|품질관리|안전관리)\s*\(.*\)$"
)

# 직무분야 힌트 토큰은 `data/tech_career_heuristics.json`에서 관리한다.


def _right_fragment_looks_like_stacked_work_types(right: str) -> bool:
    """
    발주자 셀을 마지막 공백으로 나눈 뒤 조각이 공사종류(복수·콤마·중점 등)로 볼 수 있는지.
    - 쉼표·한글 중점(상·하수 등)은 목록형 공사종류에 흔함.
    - 슬래시(본사/감리부 등 전문분야)는 제외해 오분리를 막는다.
    - 카탈로그(is_worktype_phrase)와 설정 키워드로 단일 토큰도 보조 판별.
    """
    r = " ".join((right or "").split()).strip()
    if not r:
        return False
    if "," in r or "·" in r:
        return True
    try:
        if is_worktype_phrase(r, project_root=str(_PROJECT_ROOT)):
            return True
    except Exception:
        pass
    for kw in _TC_H.issuer_cell_worktype_tail_keywords or ():
        if kw and kw in r:
            return True
    return False


def _split_issuer_and_work_type_from_issuer_cell(
    issuer: str, work_type: str
) -> tuple[str, str]:
    """
    일부 PDF에서 '발주자' 셀이 '발주자 공사종류'를 한 덩어리로 추출하는 경우가 있다.
    - 공사종류가 비어있고 발주자 문자열에 공백이 있으며,
      뒤 조각이 공사종류로 강하게 보이면 (발주자, 공사종류)로 분리한다.
    """
    iss = re.sub(r"\s+", " ", (issuer or "")).strip()
    wt = re.sub(r"\s+", " ", (work_type or "")).strip()
    if not iss or wt:
        return iss, wt
    if " " not in iss:
        return iss, wt
    left, right = iss.rsplit(" ", 1)
    left = left.strip()
    right = right.strip()
    if not left or not right:
        return iss, wt
    if _right_fragment_looks_like_stacked_work_types(right):
        return left, right
    return iss, wt

# 표(col3) 직위 토큰, 발주자 기관명 힌트 정규식은 `data/tech_career_heuristics.json`에서 관리한다.


def _table_c0_looks_like_발주자기관줄(c0: str) -> bool:
    s = (c0 or "").strip()
    if not s:
        return False
    if _ISSUER_NAME_HINT.search(s):
        if re.search(
            r"(개발사업|실시설계|기본설계|기본계획|토목실시|조사측량|설계용역|건설공사|개설공사)",
            s,
        ):
            return False
        return True
    return False


def _table_col3_looks_like_직위(c3: str) -> bool:
    """표에서 '직위' 칸으로 보이는 값(발주자 행의 col3)."""
    s = (c3 or "").strip()
    if not s:
        return False
    if s in _TABLE_COL3_직위_토큰:
        return True
    # '이사' 단독은 담당업무 칸에 쓰이는 경우가 있어 제외하고, 복합 직함만 직위로 본다.
    if s != "이사" and s.endswith("이사") and len(s) <= 8:
        return True
    return False


def _table_row_looks_like_발주자블록(c0: str, c3: str) -> bool:
    """2번째 데이터 행(발주자·전문분야·직위)으로 볼 만한지."""
    if _table_col3_looks_like_직위(c3):
        return True
    if (c3 or "").strip() == "이사" and _table_c0_looks_like_발주자기관줄(c0):
        return True
    return False


def _row4_cells(row: list) -> tuple[str, str, str, str]:
    def _clean(cell):
        return re.sub(r"\s+", " ", (cell or "").replace("\n", "")).strip()
    c0 = _clean(row[0]) if len(row) > 0 else ""
    c1 = _clean(row[1]) if len(row) > 1 and row[1] is not None else ""
    c2 = _clean(row[2]) if len(row) > 2 else ""
    c3 = _clean(row[3]) if len(row) > 3 else ""
    return c0, c1, c2, c3


def _is_table_project_header_row_cells(c0: str, c1: str, c2: str, c3: str) -> bool:
    """사업 헤더 행: col1 비어 있음, 발주자(직위) 2행째와 구분된다."""
    if not c0 or not c2:
        return False
    if c1:
        return False
    if c0.startswith("~") or _DATE_RE.match(c0) or _DAYS_RE.match(c0):
        return False
    if _table_row_looks_like_발주자블록(c0, c3):
        return False
    # '참여기술인' 은 직무분야가 아닌 참여자 유형 표기이므로 사업 헤더 행이 아님.
    # (텍스트 파서의 _parse_project_line:234 과 동일한 로직을 표 파서에도 적용)
    if "참여기술인" in c2:
        return False
    c3s = (c3 or "").strip()
    # c3 가 순수 숫자(인정/참여일수 또는 금액)인 경우는 담당업무 칸이 아님
    if c3s and re.match(r"^[\d,\s]+$", c3s):
        return False
    # 일부 PDF 추출에서 담당업무 칸(c3)이 비는 경우가 있다.
    # 이 경우 col2가 직무분야 힌트일 때만 헤더로 인정해, 다음 사업명이 개요로 밀리는 것을 막는다.
    if not c3s:
        if (c2 or "").strip() not in _JOB_FIELD_HINTS:
            return False
    return True


def _yyyy_mm_dd_to_iso(date_str: str) -> str:
    s = (date_str or "").strip()
    if not s:
        return ""
    # FIX: 일부 PDF는 일(day)이 누락된 'YYYY.MM' 또는 'YYYY-MM' 형태로 추출된다.
    #      검증/후처리에서 날짜는 'YYYY-MM-DD'를 기대하므로, 월까지만 있는 경우는 1일로 보정한다.
    #      (예: '1995.08' → '1995-08-01', '1995-08' → '1995-08-01')
    m_iso_ym = re.fullmatch(r"(\d{4})-(\d{2})", s)
    if m_iso_ym:
        try:
            return datetime(int(m_iso_ym.group(1)), int(m_iso_ym.group(2)), 1).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    m = re.fullmatch(r"(\d{4})\.(\d{2})\.(\d{2})", s)
    if m:
        yyyy, mm, dd = map(int, m.groups())
        try:
            return datetime(yyyy, mm, dd).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    m2 = re.fullmatch(r"(\d{4})\.(\d{2})", s)
    if m2:
        try:
            return datetime(int(m2.group(1)), int(m2.group(2)), 1).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""


def _blank_career_row() -> Dict[str, Any]:
    # 사용자 요구사항: 비어있어도 키는 삭제하지 않음
    return {
        "사업명": "",
        "발주자": "",
        "공사종류": "",
        "참여기간_시작일": "",
        "참여기간_종료일": "",
        "인정일수": "",
        "참여일수": "",
        "직무분야": "",
        "전문분야": "",
        "담당업무": "",
        "책임정도": "",
        "직위": "",
        "공사(용역)금액(백만원)": "",
        "공사(용역)개요": "",
        "적용 공법": "",
        "적용 융복합건설기술": "",
        "적용 신기술 등": "",
        "시설물 종류": "",
        "비고": "",
    }


def _preprocess_page2_tech_table6(t6: List[List[str]]) -> List[List[str]]:
    """
    page2(기술경력) 원시 표(6열 정규화 후)에 대해,
    파싱 오류를 유발할 수 있는 행/헤더 표현을 사전에 정리한다.
    """
    if not t6:
        return t6

    def _row_joined(row: List[str]) -> str:
        return re.sub(r"\s+", " ", " ".join((c or "") for c in (row or [])).strip())

    def _row_key(row: List[str]) -> str:
        # 공백 제거 키(부분문구 매칭 안정화)
        return re.sub(r"\s+", "", _row_joined(row))

    out: List[List[str]] = []
    for r in t6:
        rk = _row_key(r)
        # 1) 원시 표 첫 행(각주성 문구) 삭제
        # 예: "1. 기술경력 (책임정도의 *는 보정계수가 적용된 경력사항임"
        if ("1.기술경력" in rk) and ("책임정도의" in rk) and ("*" in rk):
            continue
        out.append(r)
    return out


_DATE_TOKEN_IN_CELL_RE = re.compile(r"\d{4}\.\d{2}\.\d{2}")


def _stitch_page2_tech_data_rows_to_4row_blocks(
    t6: List[List[str]], *, header_start_row: int
) -> List[List[str]]:
    """
    pdfplumber 테이블이 페이지에 따라 4행 블록(=1경력) 구조를 깨뜨리는 경우가 있다.
    대표적으로:
    - 첫 열이 비고/공백인 '이어지는 줄'이 별도 행으로 내려오는 케이스(같은 컬럼 위치에 붙여야 함)
    - '┖→' 마커가 참여기간 셀(r0[0])이 아니라, 단독 행으로 분리되는 케이스

    이 함수는 header(4행)는 유지하면서, data 영역만 대상으로:
    - c0 == '┖→' 단독 행은 제거한다.

    NOTE:
    원시 표에서는 '┖→'가 단독 행으로 존재하는 경우가 있는데,
    이를 다음 참여기간 셀과 합치면(정규화 단계) 다음 레코드를 continuation으로 오인하여
    잘못 병합되는 오류가 발생할 수 있다.
    """
    if not t6:
        return t6
    hs = int(header_start_row)
    if hs < 0 or (hs + 4) > len(t6):
        return t6

    header = t6[: hs + 4]
    data = t6[hs + 4 :]

    out_data: List[List[str]] = []

    def _is_arrow_only_row(r: List[str]) -> bool:
        if not r:
            return False
        c0 = (r[0] or "").strip()
        if c0 != "┖→":
            return False
        for j in range(1, len(r)):
            if (r[j] or "").strip():
                return False
        return True

    for r in data:
        if not r:
            continue
        # normalize len to 6 defensively
        if len(r) < 6:
            r = list(r) + [""] * (6 - len(r))
        else:
            r = list(r[:6])

        if _is_arrow_only_row(r):
            # 단독 마커 행은 제거(병합 금지)
            continue

        out_data.append(r)

    return header + out_data


def _iter_page2_tech_records_by_period_rows(
    t6: List[List[str]], *, header_start_row: int
) -> Iterator[tuple[List[str], List[str], List[str], List[str]]]:
    """
    데이터 영역에서 '참여기간(날짜 토큰 포함) 행'을 레코드 시작(r0)으로 보고 레코드를 분할한다.
    각 레코드는 (r0, r1, r2, r3)로 반환하되,
    r3는 레코드 내 남는 행(3번째 이후)을 컬럼별로 개행 누적해 흡수한다.
    """
    hs = int(header_start_row)
    if not t6 or hs < 0 or (hs + 4) > len(t6):
        return

    def _row_is_empty(r: List[str]) -> bool:
        return not any((c or "").strip() for c in (r or []))

    def _is_period_start_row(r: List[str]) -> bool:
        if not r:
            return False
        c0 = (r[0] or "").strip()
        return _DATE_TOKEN_IN_CELL_RE.search(c0) is not None

    def _is_arrow_only_row(r: List[str]) -> bool:
        if not r:
            return False
        c0 = (r[0] or "").strip()
        if c0 != "┖→":
            return False
        for j in range(1, min(len(r), 6)):
            if (r[j] or "").strip():
                return False
        return True

    data = t6[hs + 4 :]
    # drop fully empty rows (but keep c0-empty normal rows)
    data = [
        (list(r[:6]) + ([""] * (6 - len(r))) if len(r) < 6 else list(r[:6]))
        for r in data
        if (not _row_is_empty(r)) and (not _is_arrow_only_row(r))
    ]

    blocks: List[List[List[str]]] = []
    cur: List[List[str]] | None = None
    for r in data:
        if _is_period_start_row(r):
            if cur:
                blocks.append(cur)
            cur = [r]
        else:
            if cur is None:
                continue
            cur.append(r)
    if cur:
        blocks.append(cur)

    blank = [""] * 6
    for blk in blocks:
        r0 = blk[0] if len(blk) >= 1 else blank
        r1 = blk[1] if len(blk) >= 2 else blank
        r2 = blk[2] if len(blk) >= 3 else blank
        # r3: merge the rest
        r3 = list(blk[3]) if len(blk) >= 4 else list(blank)
        if len(r3) < 6:
            r3.extend([""] * (6 - len(r3)))
        for extra in blk[4:]:
            for j in range(6):
                v = (extra[j] if j < len(extra) else "") or ""
                v = v.strip()
                if not v:
                    continue
                old = (r3[j] or "").strip()
                r3[j] = (old + "\n" + v).strip() if old else v
        yield (r0, r1, r2, r3)


def _parse_tech_careers_from_raw_table(
    page: Any,
    *,
    page_num_1based: int,
    pdf_path: str = "",
) -> List[Dict[str, Any]]:
    """
    기술경력(page2) 표를 6열/4행 블록으로 파싱한다.
    - 표 인식이 실패하거나 구조가 예상과 다르면 빈 리스트를 반환(상위에서 텍스트 폴백)
    """
    try:
        tables = safe_extract_tables(page, LINE_TABLE_SETTINGS) or []
    except Exception:
        tables = []
    if not tables:
        try:
            tables = extract_tables_merged(page) or []
        except Exception:
            tables = []
    if not tables:
        return []

    # best table 선택(기존 스코어 함수 재사용)
    try:
        best = pick_best_table(tables, _table_score) or []
    except Exception:
        best = tables[0] if tables else []
    if not best:
        return []

    # 6열 정규화 시도(실패하더라도 raw를 덤프할 수 있게 best는 유지)
    t6 = normalize_table_to_6cols(best)
    t6 = _preprocess_page2_tech_table6(t6)
    if not t6:
        # 원시 표 덤프(정규화 실패)
        dump_raw_tables_to_excel(
            pdf_path=str(pdf_path or ""),
            section="page2_tech",
            page_num_1based=page_num_1based,
            tables_all=tables,
            best_table=best,
            normalized_6cols=[],
            meta={"note": "normalize_table_to_6cols returned empty"},
        )
        return []
    if len(t6[0]) < 6:
        dump_raw_tables_to_excel(
            pdf_path=str(pdf_path or ""),
            section="page2_tech",
            page_num_1based=page_num_1based,
            tables_all=tables,
            best_table=best,
            normalized_6cols=t6,
            meta={"note": "normalized table has <6 columns", "ncols": len(t6[0]) if t6 else 0},
        )
        return []

    hs = find_header_start_row(t6)
    if hs is None:
        dump_raw_tables_to_excel(
            pdf_path=str(pdf_path or ""),
            section="page2_tech",
            page_num_1based=page_num_1based,
            tables_all=tables,
            best_table=best,
            normalized_6cols=t6,
            meta={"note": "find_header_start_row returned None"},
        )
        return []

    # 2) 참여기간 헤더를 4행으로 분리(표 구조 보정)
    # 원시 표에서는 참여기간(인정일)(참여일) 헤더가 1행에 있고, 아래 3행은 빈칸인 경우가 많다.
    # 4행=1레코드 전제에 맞게, 헤더 블록의 첫 열 라벨을 명시적으로 정리한다.
    if 0 <= hs and (hs + 3) < len(t6):
        period_header_4 = ["참여기간_시작일", "참여기간_종료일", "인정일", "참여일"]
        for i, label in enumerate(period_header_4):
            try:
                if len(t6[hs + i]) >= 1:
                    t6[hs + i][0] = label
            except Exception:
                pass

    # 3) data 영역의 깨진 행을 이어붙여 4행 블록 정렬 복원
    t6 = _stitch_page2_tech_data_rows_to_4row_blocks(t6, header_start_row=hs)

    # 성공 경로에서도 덤프(파싱 오류 분석용) — 전처리/헤더 보정 반영본
    dump_raw_tables_to_excel(
        pdf_path=str(pdf_path or ""),
        section="page2_tech",
        page_num_1based=page_num_1based,
        tables_all=tables,
        best_table=best,
        normalized_6cols=t6,
        meta={"header_start_row": int(hs), "n_rows_raw": int(len(best or [])), "n_rows_norm": int(len(t6 or []))},
    )

    out: List[Dict[str, Any]] = []
    for r0, r1, r2, r3 in _iter_page2_tech_records_by_period_rows(t6, header_start_row=hs):
        period = parse_period_cell(r0[0] if len(r0) >= 1 else "", yyyy_mm_dd_to_iso=_yyyy_mm_dd_to_iso)

        row = _blank_career_row()
        row["_pdf_pages"] = [page_num_1based]
        row["참여기간_시작일"] = period.start_iso
        row["참여기간_종료일"] = period.end_iso
        row["인정일수"] = period.인정일수
        row["참여일수"] = period.참여일수

        # 4행 누적 매핑(6열 기준)
        # row0
        row["사업명"] = (r0[1] if len(r0) > 1 else "") or ""
        row["직무분야"] = (r0[3] if len(r0) > 3 else "") or ""
        row["담당업무"] = (r0[4] if len(r0) > 4 else "") or ""
        row["비고"] = (r0[5] if len(r0) > 5 else "") or ""
        # row1
        row["발주자"] = (r1[1] if len(r1) > 1 else "") or ""
        row["공사종류"] = (r1[2] if len(r1) > 2 else "") or ""
        row["전문분야"] = (r1[3] if len(r1) > 3 else "") or ""
        row["직위"] = (r1[4] if len(r1) > 4 else "") or ""
        # row2
        row["공사(용역)개요"] = (r2[1] if len(r2) > 1 else "") or ""
        row["책임정도"] = (r2[3] if len(r2) > 3 else "") or ""
        row["공사(용역)금액(백만원)"] = (r2[4] if len(r2) > 4 else "") or ""
        # row3
        row["적용 공법"] = (r3[1] if len(r3) > 1 else "") or ""
        row["적용 융복합건설기술"] = (r3[2] if len(r3) > 2 else "") or ""
        row["적용 신기술 등"] = (r3[3] if len(r3) > 3 else "") or ""
        row["시설물 종류"] = (r3[4] if len(r3) > 4 else "") or ""

        # continue arrow 처리: period cell에 ┖→가 있으면 이전 row로 병합
        if period.has_continue_arrow and out:
            merge_into_previous(
                out[-1],
                row,
                keys=[
                    "사업명",
                    "발주자",
                    "공사종류",
                    "직무분야",
                    "전문분야",
                    "담당업무",
                    "책임정도",
                    "직위",
                    "공사(용역)금액(백만원)",
                    "공사(용역)개요",
                    "적용 공법",
                    "적용 융복합건설기술",
                    "적용 신기술 등",
                    "시설물 종류",
                    "비고",
                ],
            )
            # pages merge
            try:
                pp = out[-1].get("_pdf_pages")
                if isinstance(pp, list):
                    if page_num_1based not in pp:
                        pp.append(page_num_1based)
            except Exception:
                pass
            continue

        out.append(row)
    return out


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
    # 표에서 (인정일)/(참여일) 열 제목이 본문에 반복 추출되는 경우
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
    # 하단 안내문(페이지마다 반복): 줄이 분리되어 나오므로 다양한 키워드로 차단
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
    # 표 헤더에 등장하는 고정 문구들(대체로 한 줄만 존재하므로 제거)
    header_keywords = [
        "사업명", "직무분야", "담당업무", "참여기간", "발주자", "공사종류", "전문분야", "직위",
        "공사(용역)개요", "책임정도", "공사(용역)금액", "적용 공법", "적용 융", "시설물 종류", "비고",
        "(인정일)", "(참여일)",
    ]
    # 기존처럼 "키워드가 포함되기만 하면" 제거하면, 실제 본문(개요/비고)에 해당 단어가 들어간 경우도 삭제될 수 있음.
    # 따라서 아래처럼 "헤더처럼 보이는" 경우에만 제거한다.
    if s.startswith("사업명") and ("직무분야" in s or "담당업무" in s):
        return True
    if s == "참여기간" or s == "비고":
        return True
    hit = sum(1 for k in header_keywords if k in s)
    # 헤더는 보통 짧은 문장 + 여러 키워드가 같이 등장
    if hit >= 2 and len(s) <= 80:
        return True
    return False


def _parse_project_line(line: str) -> Optional[dict]:
    """
    '사업명 직무분야 담당업무' 형태를 파싱.
    - 사업명에 공백이 포함될 수 있어, 마지막 2토큰을 직무/담당업무로 본다.
    - 마지막 토큰이 실제 담당업무(_DUTY_WORDS)가 아니면 분리하지 않는다.
      (예: '… 화도~양평'만 있는 줄을 잘못 잘라내는 것 방지)
    """
    s = re.sub(r"\s+", " ", (line or "")).strip()
    if not s or s == "┖→":
        return None
    if s.startswith("~"):
        return None
    # FIX: 마지막 2토큰 고정 분리는 직무/담당이 2토큰 이상이거나
    # 슬래시 포함(본사/감리부) 등 변형에서 쉽게 깨진다.
    # 끝단에서 담당업무(duty)→직무분야(job_field)를 역방향으로 탐색해 가변 길이로 분리한다.
    parts = s.split(" ")
    if len(parts) < 2:
        return None

    # 슬래시 포함 단일 라인은 직무/담당 분리를 시도하지 않는다(예: "본사/감리부").
    if "/" in s and len(parts) <= 2:
        return None

    duty_i: int | None = None
    duty_val = ""
    for i in range(len(parts) - 1, max(-1, len(parts) - 4), -1):
        cand = parts[i].strip()
        if cand == "설계담당":
            cand = "설계"
        if cand in _DUTY_WORDS or _DUTY_PAREN_RE.match(cand):
            duty_i = i
            duty_val = cand
            break
    if duty_i is None:
        return None

    job_i: int | None = None
    job_val = ""
    if duty_i - 1 >= 0:
        cand = parts[duty_i - 1].strip()
        if cand:
            job_i = duty_i - 1
            job_val = cand

    if job_i is None:
        return None
    # 핵심 가드:
    # - 이 함수는 원래 "사업명 직무분야 담당업무" 1줄 헤더를 파싱하기 위한 것.
    # - 그런데 사업명 자체가 "… 실시설계/조사"로 끝나는 경우가 흔해서,
    #   마지막 토큰(실시설계/조사 등)을 담당업무로 오인하면
    #   직무분야=직무분야 힌트가 아니라 '산업기지/종말처리장/수해복구공사' 같은 명사로 잡히며
    #   결과적으로 사업명이 앞부분만 남는 치명적인 절단이 발생한다.
    # - 따라서 직무분야 후보(job_val)는 반드시 _JOB_FIELD_HINTS에 속할 때만 분리한다.
    if job_val not in _JOB_FIELD_HINTS:
        return None

    project = " ".join(parts[:job_i]).strip()
    if not project:
        return None
    return {"사업명": project, "직무분야": job_val, "담당업무": duty_val}

def _parse_project_header_at(lines: List[str], idx: int) -> Optional[tuple[dict, int]]:
    """
    사업 헤더는 PDF 텍스트 추출 결과에 따라 여러 형태가 있음.
    A) 한 줄: "사업명 직무분야 담당업무"
    B) 두 줄 분리:
       - 1줄: "사업명"
       - 2줄: "직무분야 담당업무"
    C) 직무/담당업무 라인이 누락된 케이스:
       - 1~3줄: "사업명(줄바꿈 포함 가능)"
       - 다음 줄: "YYYY.MM.DD" (참여기간 시작일)
    반환: (proj_dict, next_idx) where next_idx는 헤더를 소비한 다음 인덱스
    """
    if idx < 0 or idx >= len(lines):
        return None
    line0 = (lines[idx] or "").strip()
    if not line0 or line0 == "┖→":
        return None
    if _DATE_RE.match(line0) or _MASKED_DATE_RE.match(line0) or _DAYS_RE.match(line0) or line0.startswith("~"):
        return None
    if re.sub(r"\s+", "", line0) == "참여기술인":
        return None

    one = _parse_project_line(line0)
    if one:
        return one, idx + 1

    # two-line header
    j = _next_data_line_index(lines, idx + 1)
    if j is not None:
        line1 = (lines[j] or "").strip()
        if line1 and (not line1.startswith("~")) and (not _DATE_RE.match(line1)) and (not _MASKED_DATE_RE.match(line1)) and (not _DAYS_RE.match(line1)) and ("참여기술인" not in line1):
            parts = re.sub(r"\s+", " ", line1).split(" ")
            if len(parts) == 2:
                job_field, duty = parts[0].strip(), parts[1].strip()
                if duty == "설계담당":
                    duty = "설계"
                if job_field and duty and (duty in _DUTY_WORDS or _DUTY_PAREN_RE.match(duty)):
                    # 다음 라인이 날짜인지 확인
                    k = _next_data_line_index(lines, j + 1)
                    if k is not None:
                        line2 = (lines[k] or "").strip()
                        if _DATE_RE.match(line2) or _MASKED_DATE_RE.match(line2):
                            proj = {"사업명": line0, "직무분야": job_field, "담당업무": duty}
                            return proj, j + 1

    # C) 직무/담당업무 라인이 없는 사업명-only 헤더
    # FIX: 일부 PDF는 '사업명' 다음에 바로 시작일이 오며 직무/담당업무 라인이 누락된다.
    #      (예: 김경태 p44 '...~...' 라인 다음 곧바로 2007.11.30)
    #      이 경우를 인식하지 못하면 다음 블록의 일수 수집 window에 '다음 사업명'이 섞여
    #      블록 경계가 붕괴되어 행 누락/초과로 이어진다.
    #
    # - 사업명 줄바꿈이 2~3줄로 찢어지는 경우도 있어, 다음 날짜 라인까지 최대 3줄을 결합한다.
    # - 오탐 방지를 위해 '다음 라인이 날짜'이고, 이후(또는 그 다음) 라인에 '~'가 존재해야 한다.
    name_parts = [line0]
    j = idx + 1
    while j < len(lines) and len(name_parts) < 3:
        s = (lines[j] or "").strip()
        if not s:
            j += 1
            continue
        if s == "┖→":
            break
        if s.startswith("~") or _DAYS_RE.match(s) or re.sub(r"\s+", "", s) == "참여기술인":
            break
        if _DATE_RE.match(s) or _MASKED_DATE_RE.match(s):
            # 날짜 다음(또는 그 다음) 줄에 '~'가 따라오는지 확인
            k = _next_data_line_index(lines, j + 1)
            if k is None:
                return None
            candidates = [k]
            k2 = _next_data_line_index(lines, k + 1)
            if k2 is not None:
                candidates.append(k2)
            if not any((lines[c] or "").lstrip().startswith("~") for c in candidates):
                return None
            proj = {"사업명": " ".join([p for p in name_parts if p]).strip(), "직무분야": "", "담당업무": ""}
            if not str(proj.get("사업명") or "").strip():
                return None
            return proj, j  # j는 날짜 라인 인덱스
        name_parts.append(s)
        j += 1
    return None


def _next_data_line_index(lines: List[str], start_idx: int) -> Optional[int]:
    """
    start_idx부터 다음 '데이터 라인' 인덱스를 반환.
    - 헤더/푸터/빈줄은 이미 제거되어 들어오지만, 안전을 위해 한번 더 빈값은 스킵.
    """
    j = start_idx
    while j < len(lines):
        if (lines[j] or "").strip():
            return j
        j += 1
    return None


def _looks_like_project_start(lines: List[str], idx: int) -> bool:
    """
    오탐 방지용: '사업명 직무분야 담당업무' 다음에 참여기간 시작일(YYYY.MM.DD 또는 ┖→)이
    바로 따라오는 경우만 '사업 시작'으로 인정한다.
    """
    header = _parse_project_header_at(lines, idx)
    if not header:
        return False
    _, next_idx = header
    j = _next_data_line_index(lines, next_idx)
    if j is None:
        return False
    nxt = (lines[j] or "").strip()
    # '┖→'는 "이전 칸에서 이어짐" 표기이므로 새 사업의 시작으로 보지 않는다.
    if nxt == "┖→":
        return False
    if not (_DATE_RE.match(nxt) or _MASKED_DATE_RE.match(nxt)):
        return False

    # 시작일 다음(또는 그 다음) 줄에 '~ ...' 라인이 따라오는지 확인(표 구조 검증)
    k = _next_data_line_index(lines, j + 1)
    if k is None:
        return False
    # 일부 PDF는 줄바꿈이 끼어 한 줄 더 밀릴 수 있어 2칸까지 허용
    candidates = [k]
    k2 = _next_data_line_index(lines, k + 1)
    if k2 is not None:
        candidates.append(k2)
    return any((lines[c] or "").lstrip().startswith("~") for c in candidates)


def _find_tilde_line_index_after_start_date(
    lines: List[str], start_i: int, *, max_ahead: int = 12
) -> int | None:
    """
    시작일(start_i) 이후 max_ahead줄 안에서 첫 '~ …' 줄 인덱스.
    _extract_date_blocks_from_text와 동일하게, 그 전에 _looks_like_project_start가 나오면
    이 시작일은 잘못 잡힌 것으로 보고 None.
    """
    for k in range(start_i + 1, min(len(lines), start_i + 1 + max_ahead)):
        if (lines[k] or "").lstrip().startswith("~"):
            return k
        if _looks_like_project_start(lines, k):
            return None
    return None


def _flow_resolve_end_after_tilde(
    lines: List[str], tilde_idx: int
) -> tuple[str, int | None, int]:
    """
    '~' 줄 다음에서 종료일(또는 근무중)과 일수 수집용 cursor 라인 인덱스.
    공사종류 조각 한 줄(예: '수시설)')이 끼는 경우 date_blocks 경로와 동일하게 한 줄 더 탐색.
    반환: (종료일_iso_또는_근무중, 종료일_라인_인덱스|None, 일수윈도우_시작_라인_인덱스)
    """
    after_tilde = _next_data_line_index(lines, tilde_idx + 1)
    if after_tilde is None:
        return "", None, tilde_idx + 1
    s_end = (lines[after_tilde] or "").strip()
    if _DATE_RE.match(s_end):
        return _yyyy_mm_dd_to_iso(s_end), after_tilde, after_tilde + 1
    compact = re.sub(r"\s+", "", s_end)
    if "근무중" in compact:
        return "근무중", after_tilde, after_tilde + 1
    after2 = _next_data_line_index(lines, after_tilde + 1)
    if after2 is not None and _DATE_RE.match((lines[after2] or "").strip()):
        return (
            _yyyy_mm_dd_to_iso((lines[after2] or "").strip()),
            after2,
            after2 + 1,
        )
    return "", after_tilde, after_tilde


def _looks_like_project_block_ahead(lines: List[str], idx: int) -> bool:
    """
    extras 수집 중 "다음 사업 블록이 시작될 것 같은 지점"을 빠르게 감지하기 위한 약식 체크.
    - 현재 라인이 사업명 라인처럼 보이고
    - 다음 라인이 날짜(YYYY.MM.DD)이며
    - 그 다음(또는 그 다음 다음) 라인이 '~'로 시작하면 True
    """
    if idx < 0 or idx >= len(lines):
        return False
    header = _parse_project_header_at(lines, idx)
    if header is None:
        return False
    _, next_idx = header
    j = _next_data_line_index(lines, next_idx)
    if j is None:
        return False
    nxt = (lines[j] or "").strip()
    if not _DATE_RE.match(nxt):
        return False
    k = _next_data_line_index(lines, j + 1)
    if k is None:
        return False
    candidates = [k]
    k2 = _next_data_line_index(lines, k + 1)
    if k2 is not None:
        candidates.append(k2)
    return any((lines[c] or "").lstrip().startswith("~") for c in candidates)

def _looks_like_project_block_ahead_loose(lines: List[str], idx: int, lookahead: int = 10) -> bool:
    """
    일부 PDF는 다음 사업 헤더가 여러 줄로 찢어져 이전 사업의 '개요'로 섞인다.
    예)
      방림...수도정비 기
      토목 설계
      ... (몇 줄)
      2022.11.10
      ~ 발주자 ...

    따라서 아래 패턴을 찾으면 다음 사업 시작으로 간주하고 extras 수집을 중단한다.
    - 현재 라인이 '사업명 단독'처럼 보이고(날짜/~/(xx일) 아님)
    - 다음(또는 가까운) 라인에 '직무분야 담당업무' 2토큰 라인이 있고(담당업무가 _DUTY_WORDS)
    - 그 이후 lookahead 범위 내에 날짜(YYYY.MM.DD)와 '~' 라인이 함께 존재
    """
    if idx < 0 or idx >= len(lines):
        return False
    line0 = (lines[idx] or "").strip()
    if not line0 or line0 == "┖→":
        return False
    if line0.startswith("~") or _DATE_RE.match(line0) or _DAYS_RE.match(line0):
        return False
    if "참여기술인" in line0:
        return False

    # 다음 라인들에서 job+duty(2토큰) 라인을 찾는다.
    end = min(len(lines), idx + 1 + max(2, lookahead))
    job_duty_idx = None
    for j in range(idx + 1, end):
        s = re.sub(r"\s+", " ", (lines[j] or "")).strip()
        if not s or s.startswith("~") or _DATE_RE.match(s) or _DAYS_RE.match(s) or "참여기술인" in s:
            continue
        parts = s.split(" ")
        if len(parts) == 2 and (parts[1] in _DUTY_WORDS or _DUTY_PAREN_RE.match(parts[1])):
            job_duty_idx = j
            break
    if job_duty_idx is None:
        return False

    # job+duty 이후 lookahead 범위 내에 날짜와 '~'가 존재하면 다음 사업으로 간주
    has_date = False
    has_tilde = False
    for k in range(job_duty_idx + 1, end):
        s = (lines[k] or "").strip()
        if _DATE_RE.match(s):
            has_date = True
        if s.startswith("~"):
            has_tilde = True
        if has_date and has_tilde:
            return True
    return False


def _parse_tilde_line(line: str) -> Optional[dict]:
    """
    '~ 발주자 공사종류 전문분야 직위' 또는 '~ 발주자 전문분야 직위' 형태를 파싱.
    - 마지막 토큰=직위, 그 앞=전문분야
    - 남는 앞부분은 발주자/공사종류로 분리(토큰 4개 이상일 때만 공사종류에 할당)
    - 직무분야(조경 등)가 공사종류/발주자에 끼어든 경우 전문분야와의 짝으로 제거한다.
    """
    s = re.sub(r"\s+", " ", (line or "")).strip()
    if not s.startswith("~"):
        return None
    rest = s[1:].strip()
    if not rest or rest == "┖→":
        return _tilde_emit()
    parts = rest.split(" ")
    if len(parts) < 2:
        # PDF 추출로 '~ 발주자 …' 가 '~ 사원' 등 직위만 남는 경우
        if _looks_like_position_token(rest):
            return _tilde_emit(직위=rest)
        return _tilde_emit(발주자=rest)

    if len(parts) == 3:
        a, b, c = parts[0].strip(), parts[1].strip(), parts[2].strip()
        if _looks_like_position_token(c):
            if _tilde_token_looks_like_specialty(b):
                return _tilde_emit(발주자=a, 전문분야=b, 직위=c)
            return _tilde_emit(발주자=a, 공사종류=b, 직위=c)
        # c가 직위 토큰이 아닐 때: b가 전문분야이면 c는 공사종류 후보(짧은 토큰만)
        if _tilde_token_looks_like_specialty(b):
            if c in _TILDE_SHORT_WORKTYPE_TOKENS:
                return _tilde_emit(발주자=a, 공사종류=c, 전문분야=b)
            return _tilde_emit(발주자=a, 전문분야=b)
        if _tilde_token_looks_like_specialty(c):
            if _job_field_noise_before_specialty(b, c):
                return _tilde_emit(발주자=a, 전문분야=c)
            return _tilde_emit(발주자=a, 공사종류=b, 전문분야=c)
        return _tilde_emit(발주자=a, 공사종류=b, 전문분야=c)

    if len(parts) == 2:
        a2, b2 = parts[0].strip(), parts[1].strip()
        # 전문분야만 있고 발주자 칸이 비어 '~ 조경계획 대리' 형태
        if _looks_like_position_token(b2) and _tilde_token_looks_like_specialty(a2):
            return _tilde_emit(전문분야=a2, 직위=b2)
        # 두 번째 토큰이 전문분야처럼 보이고 직위 토큰이 아닌 경우 → 전문분야로 처리
        # 예) "~ 한국도로공사 토질·지질" → 발주자=한국도로공사, 전문분야=토질·지질
        if _tilde_token_looks_like_specialty(b2) and not _looks_like_position_token(b2):
            return _tilde_emit(발주자=a2, 전문분야=b2)
        return _tilde_emit(발주자=a2, 직위=b2)

    if len(parts) >= 4:
        last_t = parts[-1].strip()
        if not _looks_like_position_token(last_t) and _tilde_token_looks_like_specialty(last_t):
            pen = parts[-2].strip()
            발주자 = " ".join(p.strip() for p in parts[:-2]).strip()
            if _job_field_noise_before_specialty(pen, last_t):
                return _tilde_emit(발주자=발주자, 전문분야=last_t)
            return _tilde_emit(
                발주자=발주자, 공사종류=pen, 전문분야=last_t
            )

    position = parts[-1].strip()
    specialty = parts[-2].strip()
    head = parts[:-2]

    발주자, 공사종류 = _tilde_split_head_to_issuer_and_worktype(head)

    return _tilde_emit(
        발주자=발주자, 공사종류=공사종류, 전문분야=specialty, 직위=position
    )


def _line_looks_like_issuer_org_fragment(s: str) -> bool:
    """'~' 직전 줄에만 깔리는 발주자(기관)명 조각으로 자주 나오는 패턴."""
    t = re.sub(r"\s+", " ", (s or "")).strip()
    if not t:
        return False
    if re.search(
        r"(청|사무소|관리단|사업단|조합|협회|본부|위원회|공단|공사\(주\)|\(주\)|지방해양항만청|"
        r"해양항만청|지방국토관리청|시청|군청|구청|도청)$",
        t,
    ):
        return True
    if "건설사무" in t or "지방해양항만" in t:
        return True
    return False


def _looks_like_tilde_issuer_prefix_line(s: str) -> bool:
    """
    표 추출 시 발주자(기관명)가 '~ …' 줄 앞 줄에만 남는 경우(한 칸 두 줄 등).
    예) '부산지방해양항만청부산항건설사무' 다음 줄에 '~ 항만 토질·지질 이사'
    """
    t = re.sub(r"\s+", " ", (s or "")).strip()
    if len(t) < 2:
        return False
    if t.startswith("~") or _DATE_RE.match(t) or _DAYS_RE.match(t):
        return False
    if t.startswith("┖→"):
        return False
    if "참여기술인" in t:
        return False
    if _is_probable_project_name_line(t) and not _line_looks_like_issuer_org_fragment(t):
        return False
    if _parse_job_and_duty_line_stacked(t):
        return False
    if _parse_issuer_line_stacked(t):
        return False
    if re.fullmatch(r"[\d.,\s]+", t):
        return False
    return True


def _parse_tilde_line_at(
    lines: List[str],
    tilde_idx: int,
    *,
    max_prefix_lines: int = 3,
    max_suffix_fragments: int = 2,
) -> Optional[dict]:
    """
    lines 기준으로 '~' 줄과 그 바로 위 발주자명 조각(및 '~' 다음 짧은 꼬리 '소' 등)을 합쳐 _parse_tilde_line 한다.
    """
    if tilde_idx < 0 or tilde_idx >= len(lines):
        return None
    raw = re.sub(r"\s+", " ", (lines[tilde_idx] or "").strip()).strip()
    if not raw.startswith("~"):
        return None

    prefix_chunks: List[str] = []
    j = tilde_idx - 1
    taken = 0
    while j >= 0 and taken < max_prefix_lines:
        s = re.sub(r"\s+", " ", (lines[j] or "").strip()).strip()
        if not s:
            j -= 1
            continue
        if not _looks_like_tilde_issuer_prefix_line(s):
            break
        prefix_chunks.insert(0, s)
        taken += 1
        j -= 1

    prefix = "".join(prefix_chunks)
    rest_after_tilde = raw[1:].strip()
    if prefix:
        merged = "~ " + (prefix + " " + rest_after_tilde).strip()
    else:
        merged = raw

    out = dict(_parse_tilde_line(merged) or {})
    if not any(str(out.get(k) or "").strip() for k in ("발주자", "공사종류", "전문분야", "직위")):
        return None

    suf = 0
    si = tilde_idx + 1
    while suf < max_suffix_fragments and si < len(lines):
        s = re.sub(r"\s+", " ", (lines[si] or "").strip()).strip()
        if not s:
            si += 1
            continue
        if _DATE_RE.match(s) or _DAYS_RE.match(s):
            break
        if s.startswith("~"):
            break
        if _is_probable_project_name_line(s):
            break
        if "참여기술인" in s:
            break
        if len(s) <= 3 and re.fullmatch(r"[가-힣A-Za-z0-9]+", s):
            cur = str(out.get("발주자") or "").strip()
            if cur and not cur.endswith(s):
                out["발주자"] = (cur + s).strip()
            suf += 1
            si += 1
            continue
        break

    return out


def _parse_days_line(line: str) -> str:
    s = re.sub(r"\s+", " ", (line or "")).strip()
    if not s:
        return ""
    if "┖→" in s:
        return "┖→"
    # '(1,304일)' 처럼 콤마가 포함될 수 있음
    m = _DAYS_RE.match(s)
    if m:
        return (m.group(1) or "").replace(",", "")
    # 라인 전체가 '(xx일)'이 아니고 텍스트에 섞여 나오는 경우를 위해 토큰 추출
    toks = _DAYS_TOKEN_RE.findall(s)
    if toks:
        return (toks[0] or "").replace(",", "")
    return ""


def _extract_days_tokens_from_lines(lines: List[str]) -> List[str]:
    out: List[str] = []
    for ln in lines:
        s = (ln or "").strip()
        if not s:
            continue
        for tok in _DAYS_TOKEN_RE.findall(s):
            v = (tok or "").replace(",", "").strip()
            if v:
                out.append(v)
    return out


def _looks_like_position_token(s: str) -> bool:
    return _table_col3_looks_like_직위((s or "").strip())


def _parse_job_and_duty_line_stacked(line: str) -> Optional[Tuple[str, str]]:
    """
    stacked 레이아웃에서 2토큰 라인을 (직무분야, 담당업무)로 해석.
    예)
      - "본부견적 토목" -> ("토목", "본부견적")
      - "설계 토목" -> ("토목", "설계")
    """
    s = re.sub(r"\s+", " ", (line or "")).strip()
    if not s or s == "┖→":
        return None
    if s.startswith("~") or _DATE_RE.match(s) or _DAYS_RE.match(s):
        return None
    parts = s.split(" ")
    if len(parts) != 2:
        return None
    a, b = parts[0].strip(), parts[1].strip()
    if not a or not b:
        return None
    if a in _JOB_FIELD_HINTS and (b in _DUTY_WORDS or _DUTY_PAREN_RE.match(b)):
        return a, b
    # FIX: "본사견적 토목" 같이 사업명+직무분야 조합을 잘못 인식하는 것을 방지.
    # 역순 배치(직무분야가 뒤에 올 때)는 앞 토큰이 실제 담당업무 단어일 때만 허용.
    if b in _JOB_FIELD_HINTS and (a in _DUTY_WORDS or _DUTY_PAREN_RE.match(a)):
        return b, a
    return None


def _parse_issuer_line_stacked(line: str) -> Optional[Dict[str, str]]:
    """
    stacked 레이아웃에서 발주자/공사종류/전문분야/직위를 한 줄에서 복원.
    예) "대전지방국토관리청 국도 도로및공항 사원"
    """
    s = re.sub(r"\s+", " ", (line or "")).strip()
    if not s or s == "┖→":
        return None
    if s.startswith("~") or _DATE_RE.match(s) or _DAYS_RE.match(s):
        return None
    parts = s.split(" ")
    if len(parts) < 4:
        return None
    pos = parts[-1].strip()
    if not _looks_like_position_token(pos):
        return None
    specialty = parts[-2].strip()
    work_type = parts[-3].strip()
    if _job_field_noise_before_specialty(work_type, specialty):
        work_type = ""
    issuer = " ".join(parts[:-3]).strip()
    issuer = _strip_trailing_job_noise_from_issuer(issuer, specialty)
    if not issuer:
        return None
    return {"발주자": issuer, "공사종류": work_type, "전문분야": specialty, "직위": pos}


def _looks_like_technical_overview_line(line: str) -> bool:
    """
    공사(용역)개요·공법 서술에 가까운 한 줄. PDF 줄바꿈으로 잘린 꼬리가
    사업명 역방향 수집에 섞이는 것을 막는다.
    """
    s = re.sub(r"\s+", " ", (line or "").strip())
    if not s:
        return False
    if s.startswith("- "):
        return True
    if "○" in s or "\u25cb" in s:
        return True
    if "내용:" in s or "내용 :" in s:
        return True
    if re.match(r"^\d+\)\s*", s):
        return True
    if re.search(r"공법\s*\([^)]*개착", s):
        return True
    if re.search(r"공법\s*\([^)]*비개착", s):
        return True
    # FIX: 표 셀 분해/정렬 문제로 공법/약어 토큰이 '사업명' 위쪽으로 밀려
    #      다음 행의 사업명으로 흡수되는 케이스가 있다.
    #      예) "ILM,FCM공법", "F.C.M공법", "NATM", "DCM" 등
    compact = re.sub(r"\s+", "", s)
    if "공법" in compact and len(compact) <= 24:
        # 공법 토큰은 대체로 짧고, 공사/용역/사업 같은 끝맺음이 없다.
        if not (compact.endswith("공사") or compact.endswith("용역") or compact.endswith("사업")):
            return True
    # 영문 대문자 약어(공법/공정 코드) 단독 라인
    if re.fullmatch(r"[A-Z]{2,8}", compact):
        return True
    # 혼합 약어(점/쉼표 포함) + 공법 꼬리
    if re.fullmatch(r"[A-Za-z0-9.,/+\-]{2,20}공법", compact):
        return True
    if "검토 설계" in s and "용역" not in s:
        return True
    if re.match(r"^장\s*:\s*\d", s):
        return True
    if re.match(r"^점\s+", s) and "접속" in s:
        return True
    return False


def _has_project_title_tail_marker(s: str) -> bool:
    """증명서 사업명에 흔한 끝맺음(용역·공사 등)이 있는지(공백 무시)."""
    t = re.sub(r"\s+", "", (s or "").strip())
    if len(t) < 6:
        return False
    if t.endswith("용역") or t.endswith("공사") or t.endswith("사업"):
        return True
    if "평가용역" in t:
        return True
    if "영향평가" in t and "용역" in t:
        return True
    return False


def _strip_overview_prefix_from_merged_project_name(s: str) -> str:
    """
    역방향으로 이어붙인 사업명에서, 앞쪽 개요 꼬리만 남은 경우 접미(진짜 제목)만 남긴다.
    예: '조물 공법(개착, 비개착)하남교산 … 지하안전영향평가용역' → 하남… 부분.
    """
    s = re.sub(r"\s+", " ", (s or "").strip())
    if len(s) < 12:
        return s
    # 개요 꼬리가 붙은 문자열에서, 끝맺음·사업명 후보를 만족하는 **가장 긴** 접미사를 고른다.
    # '(개착, 비개착)하남…'은 공법 괄호만 잘린 경우로 더 짧은 '하남…'보다 길어 잘못 선택되므로
    # 괄호+개착으로 시작하거나 공법(…개착) 패턴이 남아 있으면 후보에서 제외한다.
    best_j: int | None = None
    best_len = -1
    for j in range(len(s)):
        suf = s[j:].strip()
        if len(suf) < 8:
            continue
        if not re.match(r"^[0-9A-Za-z가-힣(]", suf):
            continue
        st = suf.strip()
        if st and st[0] in "),，、;:":
            continue
        if re.match(r"^비개착\s*\)", st):
            continue
        # 줄바꿈으로 '('·'개' 가 잘려 '착, 비개착)…'·'개착,…' 만 남은 꼬리
        if re.match(r"^개착\s*,", st) or re.match(r"^개착\s*\)", st):
            continue
        if re.match(r"^착\s*,", st):
            continue
        if re.match(r"^착\s*\)", st):
            continue
        if re.match(r"^,\s*비", st):
            continue
        if re.match(r"^\)\s*[가-힣]", st) and "K-city" not in st and "k-city" not in st.lower():
            continue
        if re.match(r"^\([^)]*개착", st):
            continue
        if re.search(r"공법\s*\([^)]*개착", suf):
            continue
        # '…법(개착, 비개착)…' 처럼 공법이 줄바꿈으로 잘린 꼬리
        if re.search(r"[가-힣]\([^)]*개착\s*,\s*비개착", suf):
            continue
        if re.search(r"\([^)]*개착\s*,\s*비개착", suf):
            continue
        if not _has_project_title_tail_marker(suf):
            continue
        if _looks_like_technical_overview_line(suf):
            continue
        if not _is_probable_project_name_line(suf):
            continue
        ln = len(suf)
        if ln > best_len:
            best_len = ln
            best_j = j
    if best_j is not None and best_j > 0:
        return s[best_j:].strip()
    return s


def _technical_overview_compact_prefix(prefix_compact: str) -> bool:
    """_norm_key 형태 접두(공백 제거)가 개요·공법 조각인지."""
    if not prefix_compact or len(prefix_compact) < 4:
        return False
    if "공법" in prefix_compact and ("개착" in prefix_compact or "비개착" in prefix_compact):
        return True
    if "검토설계" in prefix_compact:
        return True
    if prefix_compact.startswith("조물") and "공법" in prefix_compact:
        return True
    return False


def _table_name_is_trusted_suffix_of_flow(flow_nm: str, table_nm: str) -> bool:
    """흐름 사업명이 표 사업명 앞에 개요 조각을 붙인 경우 True → 표로 교체 후보."""
    a = _norm_key(flow_nm)
    b = _norm_key(table_nm)
    if not b or len(b) < 8:
        return False
    if not a.endswith(b):
        return False
    if len(a) <= len(b) + 2:
        return False
    pref = a[: len(a) - len(b)]
    return _technical_overview_compact_prefix(pref)


def _is_probable_project_name_line(line: str) -> bool:
    """
    stacked 레이아웃에서 '사업명만 단독으로' 나오는 라인을 판정.
    - 날짜/일수/직위/직무·담당 라인/발주자 라인은 제외
    """
    s = re.sub(r"\s+", " ", (line or "")).strip()
    if not s or s == "┖→":
        return False
    if s.startswith("~") or _DATE_RE.match(s) or _DAYS_RE.match(s):
        return False
    if _is_footer_or_header_line(s):
        return False
    if _looks_like_position_token(s):
        return False
    if _parse_job_and_duty_line_stacked(s):
        return False
    if _parse_issuer_line_stacked(s):
        return False
    # 너무 짧은 토큰(예: "본사"는 허용해야 하므로 1글자만 배제)
    if len(s) <= 1:
        return False
    # FIX: 한글·숫자·영문자가 전혀 없는 라인(특수문자·기호만) 제외.
    # 사업명은 반드시 한글 2글자 이상 또는 한글+숫자 혼합을 포함해야 함.
    # 단, "본사", "도로" 같은 2글자 한글은 그대로 허용.
    korean_chars = re.findall(r"[가-힣]", s)
    if len(korean_chars) < 2:
        # 한글이 2글자 미만이면 숫자+영문자로 구성된 코드명(예: "ABC-123")도 허용
        alphanumeric = re.findall(r"[A-Za-z0-9]", s)
        if len(alphanumeric) < 3:
            return False  # FIX: 한글 2자 미만 + 영숫자 3자 미만 → 사업명으로 보기 어려움

    # FIX 에러3: 공사(용역)개요·적용공법 등에 등장하는 내용이 사업명으로 오인되는 것을 방지
    # 1) 설계·시공 단위 문자(㎥, ㎞ 등)이나 수식(Q=) 포함 → 개요/통계 내용
    if re.search(r"[㎥㎞㎡㎝㎜㎥㏃]|Q=|q=|m³|m²|㎥/일|㎥/s", s):
        return False
    # 에러2 보강: 관로/시설 규격 표기 패턴 → ┖→ 개요 연장 텍스트의 전형적 패턴
    # 예) "D300~1500mm L=32.8km", "오수관(D300~D1200) : 12.5km"
    if re.search(r"\bD\d+~[D]?\d+\w*\b", s):
        return False
    if re.search(r"\bL=\d+\.?\d*\s*(m|km|mm|cm)\b", s, re.IGNORECASE):
        return False
    # 2) 짧은 공법 목록: "개착. 터널", "NATM. 개착" 등 마침표로 구분된 짧은 기술 용어
    #    길이가 12자 이하이면서 ". 한글" 패턴이 있는 경우 = 공법 나열로 간주
    if len(s) <= 15 and re.search(r"\.\s*[가-힣A-Z]", s):
        return False
    # 3) 개요 연속 텍스트가 "계(" 또는 "등,"으로 시작하는 경우 (전 항목 개요의 이어진 내용)
    if re.match(r"^계\(", s) or re.match(r"^등[,\s]", s):
        return False

    if _looks_like_technical_overview_line(s):
        return False

    return True


def _strip_tail_job_duty(name: str) -> tuple[str, str, str]:
    """사업명 문자열 끝에서 '직무분야(+담당업무)' 꼬리를 감지해 분리.
    최종 후처리 전용(흐름 파싱에 영향 없음)으로, JOB_FIELD_HINT 기반으로 공격적으로 분리.
    Returns: (정제된_사업명, 직무분야, 담당업무) — 꼬리가 없으면 원본 그대로."""
    s = re.sub(r"\s+", " ", (name or "")).strip()
    if not s:
        return s, "", ""
    parts = s.split(" ")
    # 패턴 A: "… <JOB_FIELD_HINT> <anything>" → 직무분야 + 담당업무 분리
    if len(parts) >= 3 and parts[-2] in _JOB_FIELD_HINTS:
        project = " ".join(parts[:-2]).strip()
        if project:
            return project, parts[-2], parts[-1]
    # 패턴 B: "… <JOB_FIELD_HINT>" 단독
    if len(parts) >= 2 and parts[-1] in _JOB_FIELD_HINTS:
        candidate = " ".join(parts[:-1]).strip()
        if candidate:
            return candidate, parts[-1], ""
    return s, "", ""


def _apply_parse_project_line_to_row(r: Dict[str, Any], raw_name: str) -> None:
    """사업명 문자열 끝의 '직무분야 담당업무'를 분리해 행에 반영한다."""
    s = str(raw_name or "").strip()
    if not s:
        return
    ph = _parse_project_line(s)
    if ph:
        nm = str(ph.get("사업명") or "").strip()
        if nm:
            r["사업명"] = nm
        if ph.get("직무분야") and not str(r.get("직무분야") or "").strip():
            r["직무분야"] = str(ph.get("직무분야") or "").strip()
        if ph.get("담당업무") and not str(r.get("담당업무") or "").strip():
            r["담당업무"] = str(ph.get("담당업무") or "").strip()
        return
    # _parse_project_line 실패 시에도 꼬리 정제 시도
    cleaned, jf, dt = _strip_tail_job_duty(s)
    if cleaned != s:
        r["사업명"] = cleaned
        if jf and not str(r.get("직무분야") or "").strip():
            r["직무분야"] = jf
        if dt and not str(r.get("담당업무") or "").strip():
            r["담당업무"] = dt


def _fill_empty_사업명_from_table(
    rows: List[Dict[str, Any]], table_projects: List[Dict[str, Any]]
) -> None:
    """
    사업명이 비었고 발주자 등 근거가 있는 행에, 표 행을 발주자·공사종류·전문분야·직위로 매칭해 사업명을 채운다.
    (텍스트만으로 사업명 줄을 못 찾은 홍성달형 문서 대응)
    """
    if not rows or not table_projects:
        return

    def _tp_fk(tp: Dict[str, Any]) -> tuple[str, str, str, str]:
        return (
            _norm_key(str(tp.get("발주자") or "")),
            _norm_key(str(tp.get("공사종류") or "")),
            _norm_key(str(tp.get("전문분야") or "")),
            _norm_key(str(tp.get("직위") or "")),
        )

    def _row_fk(r: Dict[str, Any]) -> tuple[str, str, str, str]:
        return (
            _norm_key(str(r.get("발주자") or "")),
            _norm_key(str(r.get("공사종류") or "")),
            _norm_key(str(r.get("전문분야") or "")),
            _norm_key(str(r.get("직위") or "")),
        )

    pool: List[Dict[str, Any] | None] = [
        dict(tp) for tp in table_projects if str(tp.get("사업명") or "").strip()
    ]

    for r in rows:
        if str(r.get("사업명") or "").strip():
            continue
        has_evidence = any(
            str(r.get(k) or "").strip()
            for k in ["발주자", "공사종류", "전문분야", "직무분야", "담당업무", "직위"]
        )
        if not has_evidence:
            continue
        rk = _row_fk(r)
        if not rk[0]:
            continue

        best_i: int | None = None
        best_score = -1
        for i, tp in enumerate(pool):
            if tp is None:
                continue
            tk = _tp_fk(tp)
            if rk[0] != tk[0]:
                continue
            score = 1
            for a, b in zip(rk[1:], tk[1:]):
                if a and b and a == b:
                    score += 2
                elif a and b and a != b:
                    score = -1
                    break
            if score > best_score:
                best_score = score
                best_i = i

        if best_i is None:
            for i, tp in enumerate(pool):
                if tp is None:
                    continue
                if _tp_fk(tp)[0] == rk[0]:
                    best_i = i
                    break

        if best_i is None:
            continue
        tp = pool[best_i]
        pool[best_i] = None
        if not tp:
            continue
        nm0 = str(tp.get("사업명") or "").strip()
        if nm0:
            r["사업명"] = nm0
            _apply_parse_project_line_to_row(r, nm0)
        for k, v in tp.items():
            if k in ["참여기간_시작일", "참여기간_종료일", "인정일수", "참여일수", "사업명"]:
                continue
            if not str(r.get(k) or "").strip() and str(v or "").strip():
                r[k] = v


def _enrich_from_table_by_project_name(
    rows: List[Dict[str, Any]], table_projects: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    표 기반 파싱 결과(table_projects)를 사업명 매칭으로 보강한다.
    - 인덱스 매칭(min)으로 누락/밀림이 나기 쉬워서, 사업명 키로 합친다.
    - 동일 사업명이 중복될 수 있어 리스트 큐 형태로 소비한다.
    - 텍스트 사업명이 한 셀 줄바꿈으로 잘려 키가 어긋나면 발주자+전문+직위+공사종류 복합키·부분 문자열로 폴백한다.
    """
    if not rows or not table_projects:
        return rows

    def _tp_ctx_fk(tp: Dict[str, Any]) -> str:
        return "|".join(
            [
                _norm_key(str(tp.get("발주자") or "")),
                _norm_key(str(tp.get("전문분야") or "")),
                _norm_key(str(tp.get("직위") or "")),
                _norm_key(str(tp.get("공사종류") or "")),
            ]
        )

    def _row_ctx_fk(r: Dict[str, Any]) -> str:
        return _tp_ctx_fk(r)

    def _apply_tp_to_row(
        r: Dict[str, Any], tp: Dict[str, Any], *, replace_name: bool
    ) -> None:
        if replace_name:
            nm = str(tp.get("사업명") or "").strip()
            cur = str(r.get("사업명") or "").strip()
            if nm:
                if not cur:
                    r["사업명"] = nm
                else:
                    a, b = _norm_key(cur), _norm_key(nm)
                    if a == b:
                        if cur.count(" ") > nm.count(" "):
                            r["사업명"] = nm
                    elif a in b:
                        r["사업명"] = nm
                    elif b in a:
                        if _table_name_is_trusted_suffix_of_flow(cur, nm):
                            r["사업명"] = nm
                    elif len(cur) >= len(nm):
                        pass
                    else:
                        r["사업명"] = nm
                _apply_parse_project_line_to_row(r, str(r.get("사업명") or ""))
        for k, v in tp.items():
            if k in ["참여기간_시작일", "참여기간_종료일", "인정일수", "참여일수"]:
                continue
            if k == "사업명" and not replace_name:
                continue
            if not str(r.get(k) or "").strip() and str(v or "").strip():
                r[k] = v

    consumed: set[int] = set()

    for r in rows:
        has_evidence = any(
            str(r.get(k) or "").strip()
            for k in ["발주자", "공사종류", "전문분야", "직무분야", "담당업무", "직위"]
        )
        if not has_evidence:
            continue

        nk = _norm_key(str(r.get("사업명") or ""))
        idx: int | None = None
        matched_by_name = False

        if nk:
            for i, tp in enumerate(table_projects):
                if i in consumed:
                    continue
                if _norm_key(str(tp.get("사업명") or "")) == nk:
                    idx = i
                    matched_by_name = True
                    break

        if idx is None and _norm_key(str(r.get("발주자") or "")):
            fk = _row_ctx_fk(r)
            if fk != "|||":
                for i, tp in enumerate(table_projects):
                    if i in consumed:
                        continue
                    if _tp_ctx_fk(tp) == fk:
                        idx = i
                        break

        issuer_n = _norm_key(str(r.get("발주자") or ""))
        if (
            idx is None
            and nk
            and len(nk) >= 3
            and issuer_n
        ):
            for i, tp in enumerate(table_projects):
                if i in consumed:
                    continue
                if _norm_key(str(tp.get("발주자") or "")) != issuer_n:
                    continue
                tpn = _norm_key(str(tp.get("사업명") or ""))
                if not tpn:
                    continue
                if (
                    tpn.endswith(nk)
                    or (len(nk) >= 4 and nk in tpn)
                    or (len(tpn) >= 4 and tpn in nk)
                ):
                    idx = i
                    break

        if idx is None:
            continue

        consumed.add(idx)
        tp = table_projects[idx]
        tp_nm = str(tp.get("사업명") or "").strip()
        cur_nm = str(r.get("사업명") or "").strip()
        replace_name = (not matched_by_name) and bool(tp_nm)
        # 이름 매칭으로 찾았더라도, 테이블 이름이 공백 깨짐이 적으면 교체
        if matched_by_name and tp_nm and cur_nm:
            if cur_nm.count(" ") > tp_nm.count(" ") and _norm_key(cur_nm) == _norm_key(tp_nm):
                replace_name = True
        if tp_nm and cur_nm and _table_name_is_trusted_suffix_of_flow(cur_nm, tp_nm):
            replace_name = True
        _apply_tp_to_row(r, tp, replace_name=replace_name)

    return rows


def _collect_flow_block_project_name_backward(
    lines_in: List[str],
    date_idx: int,
    max_lookback: int = 12,
    confirmed_end_indices: Optional[set] = None,
) -> tuple[str, Optional[tuple[str, str]]]:
    """
    홍성달형 흐름 파서: 시작일 직전에서 역방향으로 사업명 후보 줄을 모은다.
    한 칸에 사업명이 2줄로 깨져 나온 경우, 가장 아랫줄만 잡히던 문제를 줄이기 위해 연속 후보를 병합한다.

    FIX: max_lookback을 22→12로 줄여 이전 블록 침범 가능성 감소.
         confirmed_end_indices(이전 블록의 확인된 종료일 라인 인덱스 집합)를 받아
         해당 인덱스에 도달하면 즉시 중단 — 이전 블록 사업명 오인식 근본 차단.

    동일 블록에서 종료일 '아래'(인덱스 큰 쪽)에 이어지는 개요 줄은 날짜 줄로는
    막히지 않으므로, _is_probable_project_name_line·_looks_like_technical_overview_line 및
    병합 후 _strip_overview_prefix_from_merged_project_name으로 개요 꼬리를 제거한다.
    (extract_words 기반 셀 재구성은 별도 장기 과제.)
    """
    segments: List[str] = []
    jd_from_name: Optional[tuple[str, str]] = None
    bi = date_idx - 1
    back_n = 0
    while bi >= 0 and back_n < max_lookback:
        # FIX: 이전 블록의 확인된 종료 경계에 도달하면 즉시 중단
        if confirmed_end_indices and bi in confirmed_end_indices:
            break
        sback = (lines_in[bi] or "").strip()
        if _DATE_RE.match(sback):
            break
        # FIX: 시작일 직전 역방향 스캔에서 '(xxx일)' 토큰을 넘으면
        #      이전 경력 블록의 개요/공법 라인이 사업명으로 섞일 수 있다.
        #      일수 라인은 블록 경계 신호로 취급하고 즉시 중단한다.
        if _DAYS_RE.match(sback):
            break
        if sback.lstrip().startswith("~"):
            break
        # 에러2 근본수정: ┖→ 마커는 이전 사업의 개요 연장 경계이므로 사업명 수집 즉시 중단
        # (역방향 스캔이 ┖→를 넘어가면, ┖→ 아래의 개요 텍스트가 다음 사업명으로 오인됨)
        if _line_starts_with_overview_continue_marker(sback):
            break
        # 에러2 근본수정(2층): 현재 줄 바로 아래(bi-1)가 ┖→이면, 현재 줄은 ┖→ 직후의
        # 개요 연장 텍스트이므로 수집 중단
        # 예: [N] ┖→  [N+1] 실시설계 D300~...  [N+2] 의성읍...  [N+3] 2014.07.21
        # → bi=N+1에서 bi-1=N이 ┖→이므로 break, "실시설계 D300~..."는 수집 안 됨
        if bi >= 1 and _line_starts_with_overview_continue_marker(
            (lines_in[bi - 1] or "").strip()
        ):
            break
        if _is_annotation_or_footnote_line(sback):
            bi -= 1
            back_n += 1
            continue
        if _parse_issuer_line_stacked(sback):
            break
        if _parse_job_and_duty_line_stacked(sback):
            bi -= 1
            back_n += 1
            continue
        if _is_probable_project_name_line(sback):
            ph = _parse_project_line(sback)
            frag = (ph.get("사업명") if ph else sback) or ""
            frag = str(frag).strip()
            if frag:
                segments.append(frag)
            if (
                jd_from_name is None
                and ph
                and str(ph.get("직무분야") or "").strip()
                and str(ph.get("담당업무") or "").strip()
            ):
                jd_from_name = (
                    str(ph.get("직무분야") or "").strip(),
                    str(ph.get("담당업무") or "").strip(),
                )
            bi -= 1
            back_n += 1
            continue
        break

    if not segments:
        return "", jd_from_name

    # FIX 에러3: 텍스트 줄로 분리된 사업명 조각을 이어붙일 때도 공백 없이 연결
    # (한 칸에 두 줄로 나온 경우 공백이 삽입되는 오류 방지)
    merged = "".join(reversed(segments)).strip()
    merged_one = re.sub(r"\s+", " ", merged).strip()
    merged_one = _strip_overview_prefix_from_merged_project_name(merged_one)
    ph2 = _parse_project_line(merged_one)
    if ph2 and str(ph2.get("사업명") or "").strip():
        merged = str(ph2.get("사업명") or "").strip()
        if (
            jd_from_name is None
            and str(ph2.get("직무분야") or "").strip()
            and str(ph2.get("담당업무") or "").strip()
        ):
            jd_from_name = (
                str(ph2.get("직무분야") or "").strip(),
                str(ph2.get("담당업무") or "").strip(),
            )
    else:
        merged = merged_one
    return merged, jd_from_name


def _combine_date_block_project_names(
    lines: List[str],
    start_line_idx: int,
    forward_names: List[str],
    issuer_hint: bool,
    confirmed_end_indices: Optional[set] = None,
) -> List[str]:
    """
    날짜 블록의 사업명 후보를 정리한다.
    - 일부 PDF(박준영 등)는 사업명 줄이 시작일(YYYY.MM.DD) '위'에 있고,
      기존 전방 스캔은 (인정/참여일) '아래'부터 읽어 '다음 건' 제목을 잡는다.
    - 비기관(issuer_hint False): 시작일 위쪽 사업명이 있으면 그것만 채택.
    - 기관: 위쪽 이름을 forward 목록 앞에 둔다(중복 시 생략).
    """
    # FIX: confirmed_end_indices를 역방향 스캔에 전달해 이전 블록 경계 침범 방지
    backward_nm, _jd = _collect_flow_block_project_name_backward(
        lines, start_line_idx,
        confirmed_end_indices=confirmed_end_indices,
    )
    fwd = [x for x in (forward_names or []) if str(x).strip()]
    if not backward_nm:
        return fwd
    bk = _norm_key(backward_nm)
    if not issuer_hint:
        return [backward_nm]
    if not fwd:
        return [backward_nm]
    if _norm_key(fwd[0]) == bk:
        return fwd
    return [backward_nm] + fwd


def _ensure_tech_career_names_non_empty(
    rows: List[Dict[str, Any]],
    table_projects: List[Dict[str, Any]],
) -> None:
    """
    기술경력 행의 사업명이 비지 않게 표·순서 매칭으로 보강한다.
    끝까지 못 찾으면 '(사업명 미상)'을 넣어 후속 엑셀/검증이 깨지지 않게 한다.
    """
    if not rows:
        return
    _fill_empty_사업명_from_table(rows, table_projects)

    def _tp_triple(tp: Dict[str, Any]) -> tuple[str, str, str]:
        return (
            _norm_key(str(tp.get("발주자") or "")),
            _norm_key(str(tp.get("전문분야") or "")),
            _norm_key(str(tp.get("직위") or "")),
        )

    pool: List[Dict[str, Any] | None] = [
        dict(tp) for tp in table_projects if str(tp.get("사업명") or "").strip()
    ]

    for r in rows:
        if str(r.get("사업명") or "").strip():
            continue
        rk = _tp_triple(r)
        best_i: int | None = None
        best_score = -1
        if rk[0]:
            for i, tp in enumerate(pool):
                if tp is None:
                    continue
                tk = _tp_triple(tp)
                if tk[0] != rk[0]:
                    continue
                score = 1
                for a, b in zip(rk[1:], tk[1:]):
                    if a and b and a == b:
                        score += 2
                    elif a and b and a != b:
                        score = -1
                        break
                if score > best_score:
                    best_score = score
                    best_i = i
            if best_i is None:
                for i, tp in enumerate(pool):
                    if tp is None:
                        continue
                    if _tp_triple(tp)[0] == rk[0]:
                        best_i = i
                        break
        if best_i is not None:
            tp = pool[best_i]
            pool[best_i] = None
            if tp:
                nm = str(tp.get("사업명") or "").strip()
                if nm:
                    r["사업명"] = nm
                    _apply_parse_project_line_to_row(r, nm)
                    for k, v in tp.items():
                        if k in [
                            "참여기간_시작일",
                            "참여기간_종료일",
                            "인정일수",
                            "참여일수",
                            "사업명",
                        ]:
                            continue
                        if not str(r.get(k) or "").strip() and str(v or "").strip():
                            r[k] = v

    ti = 0
    for r in rows:
        if str(r.get("사업명") or "").strip():
            continue
        while ti < len(pool) and pool[ti] is None:
            ti += 1
        if ti < len(pool) and pool[ti]:
            tp = pool[ti]
            pool[ti] = None
            ti += 1
            nm = str((tp or {}).get("사업명") or "").strip()
            if nm:
                r["사업명"] = nm
                _apply_parse_project_line_to_row(r, nm)
            else:
                r["사업명"] = "(사업명 미상)"
        else:
            r["사업명"] = "(사업명 미상)"


def _extract_lines_from_page_words(page: pdfplumber.page.Page, y_tolerance: float = 2.0) -> List[str]:
    """
    FIX: extract_text()는 PDF마다 줄바꿈이 크게 흔들려(셀 2줄, 글자 중간 분절 등) 행 경계 탐지에 취약하다.
    extract_words()의 좌표(top/x0/x1)를 이용해 '라인 스트림'을 재구성한다.
    """
    try:
        words = page.extract_words(
            keep_blank_chars=False,
            use_text_flow=True,
            extra_attrs=["top", "x0", "x1"],
        ) or []
    except Exception:
        return []
    if not words:
        return []

    words_sorted = sorted(
        words,
        key=lambda w: (float(w.get("top") or 0.0), float(w.get("x0") or 0.0)),
    )

    lines: List[List[dict]] = []
    cur: List[dict] = []
    cur_top: float | None = None
    for w in words_sorted:
        t = str(w.get("text") or "").strip()
        if not t:
            continue
        top = float(w.get("top") or 0.0)
        if cur_top is None:
            cur_top = top
            cur = [w]
            continue
        if abs(top - cur_top) <= y_tolerance:
            cur.append(w)
        else:
            lines.append(cur)
            cur = [w]
            cur_top = top
    if cur:
        lines.append(cur)

    out: List[str] = []
    for ln_words in lines:
        ws = sorted(ln_words, key=lambda w: float(w.get("x0") or 0.0))
        parts: List[str] = []
        prev_x1: float | None = None
        for w in ws:
            t = re.sub(r"\s+", " ", str(w.get("text") or "")).strip()
            if not t:
                continue
            x0 = float(w.get("x0") or 0.0)
            x1 = float(w.get("x1") or 0.0)
            if parts and prev_x1 is not None:
                # 글자 중간에서 단어가 분절된 경우 gap이 매우 작게 나온다 → 공백 없이 연결
                if x0 - prev_x1 <= 1.0:
                    parts[-1] = parts[-1] + t
                else:
                    parts.append(t)
            else:
                parts.append(t)
            prev_x1 = x1
        s = " ".join(parts).strip()
        if s:
            out.append(s)
    return out


def _extract_date_blocks_from_lines(lines: List[str]) -> List[Dict[str, str]]:
    """
    라인 스트림에서 참여기간/일수 블록을 추출한다.
    - 마스킹 날짜(****.**.**)도 경계로 인정한다.
    """
    if not lines:
        return []
    raw_lines = [re.sub(r"[ \t]+", " ", (ln or "")).strip() for ln in lines]
    lines = [ln for ln in raw_lines if ln and not _is_footer_or_header_line(ln)]

    blocks: List[Dict[str, str]] = []
    i = 0
    while i < len(lines):
        s0 = (lines[i] or "").strip()
        if not (_DATE_RE.match(s0) or _MASKED_DATE_RE.match(s0)):
            i += 1
            continue

        start_iso = _yyyy_mm_dd_to_iso(s0) if _DATE_RE.match(s0) else "****-**-**"

        tilde_idx = None
        for k in range(i + 1, min(len(lines), i + 12)):
            if (lines[k] or "").lstrip().startswith("~"):
                tilde_idx = k
                break
            if _looks_like_project_start(lines, k):
                tilde_idx = None
                break

        def _resolve_end_after(at: int) -> tuple[str, int | None, int]:
            end_iso = ""
            end_idx: int | None = None
            cursor = at
            for kk in range(at, min(len(lines), at + 13)):
                s = (lines[kk] or "").strip()
                if _DATE_RE.match(s):
                    end_idx = kk
                    end_iso = _yyyy_mm_dd_to_iso(s)
                    cursor = kk + 1
                    break
                if _MASKED_DATE_RE.match(s):
                    end_idx = kk
                    end_iso = "****-**-**"
                    cursor = kk + 1
                    break
                compact = re.sub(r"\s+", "", s)
                if "근무중" in compact:
                    end_idx = kk
                    end_iso = "근무중"
                    cursor = kk + 1
                    break
                if _looks_like_project_start(lines, kk):
                    end_idx = None
                    break
            return end_iso, end_idx, cursor

        if tilde_idx is None:
            end_iso, end_idx, cursor = _resolve_end_after(i + 1)
            window: List[str] = []
            j = cursor
            scan_end = min(len(lines), (end_idx + 1 if end_idx is not None else i + 1) + 30)
            while j < scan_end and len(window) < 60:
                if _looks_like_project_start(lines, j):
                    break
                window.append(lines[j])
                if len(_extract_days_tokens_from_lines(window)) >= 2:
                    j += 1
                    break
                j += 1
            toks = _extract_days_tokens_from_lines(window)
            인정 = toks[0] if len(toks) >= 1 else ""
            참여 = toks[1] if len(toks) >= 2 else ""
            if not end_iso and not (인정 and 참여):
                i += 1
                continue
            blocks.append(
                {
                    "참여기간_시작일": start_iso,
                    "참여기간_종료일": end_iso,
                    "인정일수": 인정,
                    "참여일수": 참여,
                }
            )
            i = max(i + 1, j)
            continue

        end_iso, _, cursor = _resolve_end_after(tilde_idx + 1)
        window = []
        j = cursor
        while j < len(lines) and len(window) < 8:
            cur = (lines[j] or "").strip()
            if j != cursor and (_DATE_RE.match(cur) or _MASKED_DATE_RE.match(cur)):
                break
            window.append(cur)
            if len(_extract_days_tokens_from_lines(window)) >= 2:
                break
            j += 1
        toks = _extract_days_tokens_from_lines(window)
        인정 = toks[0] if len(toks) >= 1 else ""
        참여 = toks[1] if len(toks) >= 2 else ""
        # FIX: 일부 문서에서는 정정/삭제 안내 문구에 "****.**.**" 같은 마스킹 날짜가 등장한다.
        #      이 경우는 실제 경력 1행이 아니라 레이아웃 내 플레이스홀더이므로 블록을 생성하지 않는다.
        if start_iso == "****-**-**" and (end_iso in ("", "****-**-**")) and not (인정 or 참여):
            i = max(i + 1, j)
            continue
        blocks.append(
            {
                "참여기간_시작일": start_iso,
                "참여기간_종료일": end_iso,
                "인정일수": 인정,
                "참여일수": 참여,
            }
        )
        i = max(i + 1, j)

    return blocks


def _extract_date_blocks_from_text(text: str) -> List[Dict[str, str]]:
    """
    텍스트에서 참여기간/인정일/참여일 블록을 순서대로 추출.
    기대 패턴(일반):
      YYYY.MM.DD
      ~ ...
      YYYY.MM.DD
      (인정일)
      (참여일)
    """
    if not text:
        return []
    raw_lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
    lines = [ln for ln in raw_lines if ln and not _is_footer_or_header_line(ln)]

    # NOTE:
    # - date_blocks는 "기간/일수 블록"이 핵심이지만, 실제 파싱에서는 사업명 후보(_사업명후보)도 같이 필요하다.
    # - 또한 동일 4튜플(시작/종료/인정/참여)이 연속으로 반복되는 PDF가 있어, 이를 1블록으로 병합해야 한다.
    #   (회귀 테스트: tests/test_duplicate_tuple_date_blocks.py)

    def _pick_name_candidate_before(start_idx: int) -> str:
        # 시작일 직전에서 가장 가까운 "사업명 후보" 1줄을 가져온다(테스트/실문서 모두에 유효한 최소 규칙).
        for k in range(start_idx - 1, max(-1, start_idx - 6), -1):
            s = (lines[k] or "").strip()
            if not s or s == "┖→":
                continue
            if _DATE_RE.match(s) or _MASKED_DATE_RE.match(s) or _DAYS_RE.match(s) or s.lstrip().startswith("~"):
                continue
            if _is_footer_or_header_line(s):
                continue
            return s
        return ""

    blocks: List[Dict[str, str]] = []
    prev_key: tuple[str, str, str, str] | None = None
    i = 0
    while i < len(lines):
        s0 = (lines[i] or "").strip()
        if not (_DATE_RE.match(s0) or _MASKED_DATE_RE.match(s0)):
            i += 1
            continue

        start_iso = _yyyy_mm_dd_to_iso(s0) if _DATE_RE.match(s0) else "****-**-**"

        tilde_idx = None
        for k in range(i + 1, min(len(lines), i + 12)):
            if (lines[k] or "").lstrip().startswith("~"):
                tilde_idx = k
                break
            if _looks_like_project_start(lines, k):
                tilde_idx = None
                break

        def _resolve_end_after(at: int) -> tuple[str, int | None, int]:
            end_iso = ""
            end_idx: int | None = None
            cursor = at
            for kk in range(at, min(len(lines), at + 13)):
                s = (lines[kk] or "").strip()
                if _DATE_RE.match(s):
                    end_idx = kk
                    end_iso = _yyyy_mm_dd_to_iso(s)
                    cursor = kk + 1
                    break
                if _MASKED_DATE_RE.match(s):
                    end_idx = kk
                    end_iso = "****-**-**"
                    cursor = kk + 1
                    break
                compact = re.sub(r"\s+", "", s)
                if "근무중" in compact:
                    end_idx = kk
                    end_iso = "근무중"
                    cursor = kk + 1
                    break
                if _looks_like_project_start(lines, kk):
                    end_idx = None
                    break
            return end_iso, end_idx, cursor

        if tilde_idx is None:
            end_iso, end_idx, cursor = _resolve_end_after(i + 1)
            window: List[str] = []
            j = cursor
            scan_end = min(len(lines), (end_idx + 1 if end_idx is not None else i + 1) + 30)
            while j < scan_end and len(window) < 60:
                if _looks_like_project_start(lines, j):
                    break
                window.append(lines[j])
                if len(_extract_days_tokens_from_lines(window)) >= 2:
                    j += 1
                    break
                j += 1
            toks = _extract_days_tokens_from_lines(window)
            인정 = toks[0] if len(toks) >= 1 else ""
            참여 = toks[1] if len(toks) >= 2 else ""
            if not end_iso and not (인정 and 참여):
                i += 1
                continue
            i_next = max(i + 1, j)
        else:
            end_iso, _, cursor = _resolve_end_after(tilde_idx + 1)
            window: List[str] = []
            j = cursor
            while j < len(lines) and len(window) < 8:
                cur = (lines[j] or "").strip()
                if j != cursor and (_DATE_RE.match(cur) or _MASKED_DATE_RE.match(cur)):
                    break
                window.append(cur)
                if len(_extract_days_tokens_from_lines(window)) >= 2:
                    break
                j += 1
            toks = _extract_days_tokens_from_lines(window)
            인정 = toks[0] if len(toks) >= 1 else ""
            참여 = toks[1] if len(toks) >= 2 else ""
            i_next = max(i + 1, j)

        key = (start_iso, end_iso, str(인정 or ""), str(참여 or ""))
        name_cand = _pick_name_candidate_before(i)

        # FIX: 마스킹 날짜만 있고(시작/종료 모두 마스킹 또는 공란), 인정/참여일수가 없는 블록은
        #      실제 경력행이 아니라 정정/삭제 안내 문구일 가능성이 높으므로 제외한다.
        if start_iso == "****-**-**" and (end_iso in ("", "****-**-**")) and not (인정 or 참여):
            i = i_next
            continue

        if blocks and prev_key == key:
            # 동일 4튜플 연속 행 → 이전 블록으로 병합
            prev = blocks[-1]
            prev["_merged_duplicate_tuple"] = True
            if name_cand:
                prev.setdefault("_사업명후보", [])
                if name_cand not in (prev.get("_사업명후보") or []):
                    prev["_사업명후보"] = list(prev.get("_사업명후보") or []) + [name_cand]
        else:
            b = {
                "참여기간_시작일": start_iso,
                "참여기간_종료일": end_iso,
                "인정일수": 인정,
                "참여일수": 참여,
            }
            if name_cand:
                b["_사업명후보"] = [name_cand]
            blocks.append(b)
            prev_key = key

        i = i_next

    return blocks


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", "", (s or "")).strip()


def _extract_projects_from_table(page: pdfplumber.page.Page) -> List[Dict[str, Any]]:
    """
    extract_tables_merged(가상 세로선+lines 우선) 기반으로 기술경력 표의 4행 블록을 읽어 18개 필드를 채운다.
    - 날짜/인정/참여는 표에 안 잡히는 경우가 많아 여기서는 비워둔다(텍스트 블록과 병합).
    - 고정 i+=4는 '발주자 행'을 '사업 헤더'로 오인할 때 이후 행 전체가 밀리므로 사용하지 않는다.
    """
    # 가상 세로선+lines 우선 병합 추출(parsers.table_settings) 후, 기술경력 표 후보를 스코어링한다.
    def _table_score(tbl: list) -> tuple[int, int, int]:
        if not tbl:
            return (-10_000, 0, 0)
        max_cols = max((len(r) for r in tbl if r), default=0)
        if max_cols <= 2:
            return (-10_000, max_cols, len(tbl))
        # 헤더 키워드 점수
        n_header = 0
        joined_head = []
        for r in tbl[: min(12, len(tbl))]:
            if not r:
                continue
            joined = (" ".join([(c or "") for c in r if c]).strip()) if r else ""
            if joined:
                joined_head.append(joined)
            if any(k in joined for k in ["사업명", "발주자", "공사(용역)개요", "참여기간", "직무분야", "전문분야", "직위"]):
                n_header += 1

        head_text = " ".join(joined_head)
        # 제목/주석성 블록 감점: (사용자가 원시표에서 본) '1. 기술경력', '책임정도의 *', '※ ...' 만으로 구성된 테이블
        is_note = False
        if "※" in head_text:
            is_note = True
        if re.search(r"\b1\.\s*기술경력\b", head_text):
            is_note = True
        if ("책임정도의" in head_text) and ("보정계수" in head_text):
            is_note = True
        # 형태 점수(열>=4, 의미 있는 행 수)
        non_empty_rows = 0
        for r in tbl:
            if r and any(str(c or "").strip() for c in r):
                non_empty_rows += 1
        # 핵심 컬럼(좌: 참여기간, 우: 비고) 포함 여부로 정상 테이블을 더 강하게 우선한다.
        has_left = "참여기간" in head_text
        has_right = "비고" in head_text
        base = (1200 if n_header >= 1 else 0) + (max_cols * 20) + non_empty_rows
        if has_left and has_right:
            base += 900
        # 헤더 신호는 있는데 열 수가 너무 적으면(좌/우 컬럼 누락 가능성) 감점
        if n_header >= 2 and max_cols <= 4:
            base -= 900
        if is_note:
            base -= 2500
        return (base, n_header, max_cols)

    def _infer_explicit_vlines_from_header_words() -> list[float]:
        """
        헤더 단어 bbox로 내부 컬럼 경계선을 추정해 explicit_vertical_lines를 만든다.
        - 하드코딩된 x가 아니라, 현재 페이지의 헤더 텍스트(참여기간/사업명/.../비고) 좌표를 사용
        - 실패하면 외곽 가상선만 반환
        """
        try:
            words = page.extract_words(use_text_flow=True) or []
        except Exception:
            try:
                words = page.extract_words() or []
            except Exception:
                words = []
        if not words:
            return [float(VIRTUAL_LEFT_X), float(VIRTUAL_RIGHT_X)]

        # 헤더는 페이지 상단에 있으므로 top 기준으로 상단 일부만 사용
        top_cut = 260.0
        cands = []
        for w in words:
            t = str(w.get("text") or "").strip()
            if not t:
                continue
            try:
                top = float(w.get("top", 0) or 0)
                x0 = float(w.get("x0", 0) or 0)
                x1 = float(w.get("x1", 0) or 0)
            except Exception:
                continue
            if top > top_cut:
                continue
            # 주요 헤더 토큰(부분일치 허용)
            if any(k in t for k in ["참여기간", "사업명", "발주자", "직무분야", "담당업무", "비고"]):
                cands.append((x0, x1, t))
        if len(cands) < 2:
            return [float(VIRTUAL_LEFT_X), float(VIRTUAL_RIGHT_X)]

        cands.sort(key=lambda x: x[0])
        # 토큰 시작/끝을 이용해 경계(midpoint) 생성
        vlines = [float(VIRTUAL_LEFT_X)]
        for (ax0, ax1, _), (bx0, _bx1, __) in zip(cands, cands[1:]):
            # 너무 가까우면(중복 토큰) 스킵
            if bx0 - ax1 < 6:
                continue
            mid = (ax1 + bx0) / 2.0
            vlines.append(float(mid))
        vlines.append(float(VIRTUAL_RIGHT_X))
        # 정렬/중복 제거 + 유효 범위 필터
        vlines = sorted({round(x, 2) for x in vlines if 0 < x < float(getattr(page, "width", 10000) or 10000)})
        # 너무 적으면 외곽만
        if len(vlines) < 3:
            return [float(VIRTUAL_LEFT_X), float(VIRTUAL_RIGHT_X)]
        return vlines

    # 1) 내부 vline 추정 + lines로 1차 시도
    inferred = _infer_explicit_vlines_from_header_words()
    settings = dict(LINE_TABLE_SETTINGS)
    settings["explicit_vertical_lines"] = inferred
    tables = safe_extract_tables(page, settings)
    # 2) 실패 시 기존 merged 폴백
    if not tables:
        tables = extract_tables_merged(page)
    if not tables:
        return []
    t = pick_best_table(tables, _table_score) or []
    if not t:
        return []
    projects: List[Dict[str, Any]] = []

    def is_header_row(row: list) -> bool:
        joined = " ".join([(c or "") for c in row if c]).strip()
        return any(k in joined for k in ["사업명", "발주자", "공사(용역)개요", "적용 공법", "책임정도", "공사(용역)금액"])

    def is_project_header_row(row: list) -> bool:
        if not row:
            return False
        c0, c1, c2, c3 = _row4_cells(row)
        if is_header_row(row):
            return False
        return _is_table_project_header_row_cells(c0, c1, c2, c3)

    def row_has_any_cell(row: list) -> bool:
        return any((c or "").strip() for c in (row or []) if c is not None)

    def _should_concat_without_space(prev: str, nxt: str) -> bool:
        """
        한국어 사업명은 줄바꿈/셀분리 지점이 단어 경계가 아닌 경우가 많아,
        일부 케이스에서 공백을 넣으면 오히려 품질이 떨어진다.
        """
        a = (prev or "").rstrip()
        b = (nxt or "").lstrip()
        if not a or not b:
            return False
        # 양쪽이 모두 공백이 아니고 한글/숫자/영문이면 무공백 결합을 우선
        if re.search(r"[가-힣A-Za-z0-9]$", a) and re.match(r"^[가-힣A-Za-z0-9]", b):
            return True
        return False

    def _table_row_looks_like_project_name_continuation(row: list) -> bool:
        """
        사업명 셀이 다음 행으로 '쪼개져' 내려온 케이스를 탐지한다.
        - 다음 행이 새 프로젝트 헤더/발주자블록/개요블록이 아닌데
        - col0에만 텍스트가 있고 나머지 컬럼이 비어있는 형태가 대표적이다.
        """
        if not row or not row_has_any_cell(row) or is_header_row(row):
            return False
        x0, x1, x2, x3 = _row4_cells(row)
        if not x0:
            return False
        if x0.startswith("~") or _DATE_RE.match(x0) or _DAYS_RE.match(x0):
            return False
        if _is_table_project_header_row_cells(x0, x1, x2, x3):
            return False
        if _table_row_looks_like_발주자블록(x0, x3) and x0 and x2:
            return False
        # continuation 은 보통 c1~c3가 비거나 매우 약하다(테이블 경계가 깨진 경우)
        if (x1 or "").strip() or (x2 or "").strip() or (x3 or "").strip():
            return False
        # footer/header 성격이거나, 명백히 개요 라인 같은 경우는 제외
        if _is_footer_or_header_line(x0) or _looks_like_technical_overview_line(x0):
            return False
        return True

    i = 0
    while i < len(t):
        row = t[i] or []
        # skip empty/header rows
        if not row_has_any_cell(row) or is_header_row(row):
            i += 1
            continue

        c0, c1, c2, c3 = _row4_cells(row)

        # 2번째 데이터 행(발주자·전문분야·직위)이 1번째 행과 열 모양이 같아
        # 별도 사업으로 잘못 시작되는 경우가 많다 → 직전 레코드에 발주자 정보로 병합
        if _table_row_looks_like_발주자블록(c0, c3) and c0 and c2 and projects:
            prev = projects[-1]
            if not str(prev.get("발주자") or "").strip():
                prev["발주자"] = c0
                if c1:
                    prev["공사종류"] = c1
                prev["전문분야"] = c2 or str(prev.get("전문분야") or "").strip()
                prev["직위"] = c3 or str(prev.get("직위") or "").strip()
                i += 1
                continue

        # 헤더 행이 아니면서 직위 행인데 앞 레코드에 이미 발주자가 있으면:
        # 사업명 행이 표에서 빠진 발주자 전용 줄일 수 있어 최소 레코드로 남긴다(날짜 블록 인덱스 유지).
        if _table_row_looks_like_발주자블록(c0, c3) and c0 and c2:
            stub = _blank_career_row()
            stub["발주자"] = c0
            stub["공사종류"] = c1
            stub["전문분야"] = c2
            stub["직위"] = c3
            projects.append(stub)
            i += 1
            continue

        if not _is_table_project_header_row_cells(c0, c1, c2, c3):
            i += 1
            continue

        out = _blank_career_row()
        # 에러4 수정: 사업명 셀(c0)에 "직무분야 담당업무" 꼬리가 붙는 경우 제거
        # 방법1: 원본 셀(row[0])에 줄바꿈(\n)이 있으면 각 줄을 분리하여 처리
        raw_c0 = str(row[0] or "") if len(row) > 0 and row[0] is not None else ""
        project_name = c0
        if "\n" in raw_c0:
            cell_lines = [l.strip() for l in raw_c0.splitlines() if l.strip()]
            proj_lines = []
            for cl in cell_lines:
                cl_parts = re.sub(r"\s+", " ", cl).split(" ")
                if len(cl_parts) == 2:
                    jf_cand, dt_cand = cl_parts[0].strip(), cl_parts[1].strip()
                    if jf_cand in _JOB_FIELD_HINTS and (dt_cand in _DUTY_WORDS or _DUTY_PAREN_RE.match(dt_cand)):
                        if not c2:
                            c2 = jf_cand
                        if not c3:
                            c3 = dt_cand
                        continue
                proj_lines.append(cl)
            if proj_lines:
                # FIX 에러3: 줄 사이에 공백 없이 이어붙임 (" ".join → "".join)
                # 한국어 사업명은 줄바꿈이 단어 경계가 아닌 글자 중간에서 이루어지므로
                # 줄 사이에 공백을 삽입하면 잘못된 띄어쓰기가 생긴다.
                project_name = re.sub(r"\s+", " ", "".join(proj_lines)).strip()

        # 방법2: c2(직무분야)가 확인된 경우, c0에서 " c2 c3..." 패턴을 마지막 위치에서 제거
        # c2가 _JOB_FIELD_HINTS에, c3가 _DUTY_WORDS에 있는 경우에만 적용(오탐 방지)
        if (
            project_name == c0
            and c2 and c3
            and c2 in _JOB_FIELD_HINTS
            and (c3 in _DUTY_WORDS or _DUTY_PAREN_RE.match(c3))
        ):
            # " c2 c3" 또는 " c2  c3"로 시작하는 부분을 마지막에서 탐색하여 제거
            search_pat = f" {c2} {c3}"
            pos = project_name.rfind(search_pat)
            if pos > 0:
                candidate = project_name[:pos].strip()
                if candidate:
                    project_name = candidate
            elif c2 in project_name:
                # " c2 " 이후에 c3로 시작하는 경우도 처리 (중간에 다른 내용 없이)
                search_mid = f" {c2} "
                pos = project_name.rfind(search_mid)
                if pos >= 0:
                    after = project_name[pos + len(search_mid):]
                    if after.strip().startswith(c3):
                        candidate = project_name[:pos].strip()
                        if candidate:
                            project_name = candidate

        # 방법3: suffix 직접 매칭
        if project_name == c0 and c2 and c3:
            for sfx in [f"{c2} {c3}", f"{c2}{c3}"]:
                if project_name.endswith(sfx):
                    candidate = project_name[: -len(sfx)].strip()
                    if candidate:
                        project_name = candidate
                    break

        # 방법4: _strip_tail_job_duty로 후처리 (JOB_FIELD_HINTS 기반)
        if project_name == c0:
            cleaned_nm, _jf, _dt = _strip_tail_job_duty(c0)
            if cleaned_nm != c0:
                project_name = cleaned_nm
                if not c2 and _jf:
                    c2 = _jf
                if not c3 and _dt:
                    c3 = _dt

        # 방법5(근본수정): 사업명 셀이 테이블 행 단위로 분리되어 내려오는 케이스 병합
        # 예) "옥천" (다음 행) "하수종말처리장 실시설계"
        j = i + 1
        merged_any = False
        while j < len(t) and _table_row_looks_like_project_name_continuation(t[j]):
            nx0, _, _, _ = _row4_cells(t[j] or [])
            if not nx0:
                break
            if _should_concat_without_space(project_name, nx0):
                project_name = f"{project_name}{nx0}"
            else:
                project_name = f"{project_name} {nx0}"
            project_name = _norm_space(project_name)
            merged_any = True
            j += 1
        if merged_any:
            i = j - 1

        out["사업명"] = project_name
        out["직무분야"] = c2
        out["담당업무"] = c3
        i += 1

        # 2행: 발주자 블록(직위 col3). 다음 줄이 곧바로 새 사업 헤더면 발주자 행 누락으로 두지 않고 소비하지 않음
        if i < len(t):
            r1 = t[i] or []
            if r1 and row_has_any_cell(r1) and not is_header_row(r1):
                a0, a1, a2, a3 = _row4_cells(r1)
                if _is_table_project_header_row_cells(a0, a1, a2, a3):
                    pass
                else:
                    out["발주자"] = a0
                    out["공사종류"] = a1
                    out["전문분야"] = a2
                    out["직위"] = a3
                    i += 1

        # FIX: 발주자 셀에 공사종류가 같이 붙어 추출된 경우 분리
        out["발주자"], out["공사종류"] = _split_issuer_and_work_type_from_issuer_cell(
            str(out.get("발주자") or ""), str(out.get("공사종류") or "")
        )

        # 3행: 개요·책임·금액
        if i < len(t):
            r2 = t[i] or []
            if r2 and row_has_any_cell(r2) and not is_header_row(r2):
                b0, b1, b2, b3 = _row4_cells(r2)
                if _is_table_project_header_row_cells(b0, b1, b2, b3):
                    pass
                else:
                    out["공사(용역)개요"] = b0
                    if b2:
                        if _cell_looks_like_책임정도_col(b2):
                            out["책임정도"] = b2
                        elif "참여기술인" not in b2:
                            out["책임정도"] = b2
                        else:
                            out["책임정도"] = ""
                    else:
                        out["책임정도"] = ""
                    out["공사(용역)금액(백만원)"] = b3
                    i += 1

        # 4행: 적용 공법 등
        if i < len(t):
            r3 = t[i] or []
            if r3 and row_has_any_cell(r3) and not is_header_row(r3):
                d0, d1, d2, d3 = _row4_cells(r3)
                if _is_table_project_header_row_cells(d0, d1, d2, d3):
                    pass
                else:
                    out["적용 공법"] = d0
                    out["적용 융복합건설기술"] = d1
                    out["적용 신기술 등"] = d2
                    out["시설물 종류"] = d3
                    i += 1

        # 블록 뒤 이어지는 개요 분할 행 보강(다음 사업 헤더·발주자(직위) 행 전까지)
        scan_end = min(len(t), i + 12)
        j = i
        while j < scan_end:
            rj = t[j] or []
            if not row_has_any_cell(rj):
                j += 1
                continue
            if is_header_row(rj):
                j += 1
                continue
            x0, x1, x2, x3 = _row4_cells(rj)
            if _is_table_project_header_row_cells(x0, x1, x2, x3):
                break
            if _table_row_looks_like_발주자블록(x0, x3) and x0 and x2:
                break

            c0m = x0
            if c0m and not c0m.startswith("~"):
                if out["공사(용역)개요"] and c0m not in out["공사(용역)개요"]:
                    out["공사(용역)개요"] = (out["공사(용역)개요"] + "\n" + c0m).strip()
                elif not out["공사(용역)개요"]:
                    out["공사(용역)개요"] = c0m

            if not out["책임정도"] and len(rj) > 2 and (rj[2] or "").strip():
                _책임후보 = (rj[2] or "").strip()
                직무 = str(out.get("직무분야") or "").strip()
                # 개요 연장 행 오인 시 col2에 직무분야만 들어가 책임정도가 오염되는 경우 방지
                use_ch = False
                if _cell_looks_like_책임정도_col(_책임후보):
                    use_ch = True
                elif (
                    "참여기술인" not in _책임후보
                    and _책임후보 not in _JOB_FIELD_HINTS
                    and not (직무 and 직무 == _책임후보)
                ):
                    use_ch = True
                if use_ch:
                    out["책임정도"] = _책임후보
            if not out["공사(용역)금액(백만원)"] and len(rj) > 3 and (rj[3] or "").strip():
                out["공사(용역)금액(백만원)"] = (rj[3] or "").strip()
            j += 1

        i = j
        projects.append(out)

    return projects


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
    """공사(용역)개요 연장 수집 종료: 각주·법령·다음 섹션 헤더."""
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


def _lookahead_has_date_followed_by_tilde(
    lines: List[str], after_j: int, max_scan: int = 16
) -> bool:
    """after_j 이후에 '단독 날짜 줄'이 있고, 그 직후 몇 줄 안에 '~'로 시작하는 줄이 있으면 True."""
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


def _line_begins_next_career_after_overview_continuation(
    lines: List[str], j: int, start_marker_index: int
) -> bool:
    """
    ┖→ 개요 연장 수집 중: 날짜 줄만으로는 잡히지 않는 '다음 경력의 사업명' 시작줄.
    (사업명 줄 다음에 참여기간 날짜 + ~ 가 이어지는 패턴)
    """
    if j <= start_marker_index:
        return False
    ln = (lines[j] or "").strip()
    if not ln or _line_starts_with_overview_continue_marker(ln):
        return False
    if not _is_probable_project_name_line(ln):
        return False
    return _lookahead_has_date_followed_by_tilde(lines, j)


def _project_name_block_start_before_date_line(lines: List[str], date_i: int) -> int:
    """날짜 줄 date_i 바로 위의 사업명(1~여러 줄) 시작 인덱스. ┖→ 연장·개요 줄은 건너뛰지 않고 경계에서 멈춤."""
    proj_start = date_i
    back = 1
    while date_i - back >= 0:
        ps = (lines[date_i - back] or "").strip()
        if not ps:
            back += 1
            continue
        if _line_starts_with_overview_continue_marker(ps):
            break
        if _is_overview_continuation_block_end_line(ps):
            break
        if _DATE_RE.match(ps) or ps.lstrip().startswith("~"):
            break
        if _is_probable_project_name_line(ps):
            proj_start = date_i - back
            back += 1
            continue
        break
    return proj_start


def extract_tech_overview_continuation_from_page_text(text: str) -> str:
    """
    기술경력 다음 쪽 상단에만 나오는 '┖→' 공사(용역)개요 연장 텍스트 추출.
    (페이지 단독 파싱 시 0건으로 끊기는 경우 후처리용)

    에러2 수정: 날짜(YYYY.MM.DD) 다음에 '~' 라인이 오면(= 다음 경력 블록 시작)
    개요 연장 수집을 즉시 중단해 다음 경력 정보가 이전 개요에 포함되는 오류 방지.

    에러2 보강: 다음 경력의 '사업명'만 단독으로 있고 그 다음 줄에 날짜+~가 오는 경우,
    날짜 줄 이전에서 수집을 끊지 못해 사업명까지 개요에 붙던 문제를 방지한다.
    """
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
        # 에러2 수정: 날짜 패턴 다음 줄이 '~'면 다음 경력 블록 시작으로 간주 → 수집 종료
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
        if j > start_i and _line_begins_next_career_after_overview_continuation(
            lines, j, start_i
        ):
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


def _page_starts_with_overview_continuation(text: str) -> bool:
    """페이지 텍스트가 ┖→ 연장 블록으로 시작하는지 확인.

    에러2 수정: 기존에는 첫 번째 비어있지 않은 줄이 ┖→ 가 아니면 즉시 False를 반환했다.
    그러나 일부 PDF에서 표 헤더 행 등이 헤더 필터를 통과하여 ┖→ 앞에 올 수 있어,
    첫 5줄을 모두 검사하도록 변경한다(break 제거).
    """
    if not (text or "").strip():
        return False
    raw_lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
    lines = [ln for ln in raw_lines if ln and not _is_footer_or_header_line(ln)]
    # FIX 에러2: 첫 5줄 모두 검사 (이전: 첫 번째 비어있지 않은 줄에서 즉시 break)
    for ln in lines[:5]:
        if _line_starts_with_overview_continue_marker(ln):
            return True
    return False


def _skip_overview_continuation_block(lines: List[str]) -> int:
    """
    lines 앞부분의 ┖→ 연장 블록을 skip하고, 다음 유효 라인 인덱스를 반환.
    (parse_page_2에서 이미 merge_cross_page_tech_overviews가 처리한 연장 내용을
    재파싱하여 사업명으로 오인하는 것을 방지)
    날짜(YYYY.MM.DD) + ~ 패턴이 나오면, 그 **위**의 사업명 줄부터 파싱하도록
    인덱스를 반환한다(날짜 줄만 반환하면 동일 페이지 사업명 행이 누락됨).

    에러2 수정: 기존에는 ┖→ 앞에 비어있지 않은 줄이 하나라도 있으면 즉시 0을 반환했다.
    일부 PDF에서 헤더 필터를 통과한 줄(예: 표 헤더 잔재)이 ┖→ 앞에 올 수 있어,
    최대 3줄까지 허용하도록 변경한다.
    """
    start_marker_found = False
    pre_marker_non_empty = 0  # ┖→ 이전에 나온 비어있지 않은 줄 수
    for i, ln in enumerate(lines):
        s = (ln or "").strip()
        if not start_marker_found:
            if _line_starts_with_overview_continue_marker(s):
                start_marker_found = True
                continue
            if s:
                pre_marker_non_empty += 1
                # FIX 에러2: 최대 3줄까지 ┖→ 이전 비마커 줄 허용 (이전: 1줄이라도 있으면 즉시 0 반환)
                if pre_marker_non_empty > 3:
                    return 0  # 마커가 너무 늦게 등장 → ┖→ 연장 페이지 아님
        else:
            # 날짜 다음 ~ 패턴이 나오면 새 경력 블록 시작 → 해당 날짜 위치 반환
            if _DATE_RE.match(s):
                for nk in range(i + 1, min(len(lines), i + 6)):
                    ns = (lines[nk] or "").strip()
                    if not ns:
                        continue
                    if ns.lstrip().startswith("~"):
                        return _project_name_block_start_before_date_line(lines, i)
                    break
            if _is_overview_continuation_block_end_line(s):
                return i
    return len(lines) if start_marker_found else 0


def merge_cross_page_tech_overviews(
    ctx: DocumentContext,
    careers: List[Dict[str, Any]],
    tech_start: int,
    tech_end: int,
    page_counts_after: List[tuple[int, int]],
) -> None:
    """
    기술경력 구간에서 i페이지 하단이 ┖→로 다음 쪽으로 넘어가는 경우,
    (i+1)페이지 상단 연장 텍스트를 해당 사업의 '공사(용역)개요'에 덧붙인다.
    page_counts_after: [(page_idx, len(careers) 누적), ...] parse_page_2 순서와 동일

    에러2 근본 수정:
    parse_page_2의 ┖→ skip 로직이 실패하면, 다음 페이지를 파싱할 때
    연장 텍스트(cont)가 그 페이지 첫 경력의 사업명 앞에 그대로 붙어버린다.
    이미 이전 경력의 공사개요에 cont를 병합한 직후, 다음 페이지 첫 경력의
    사업명이 cont로 시작하는지 검사해 제거한다(이중 처리 방지).
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

            # 1) 이전 경력의 공사개요에 ┖→ 연장 텍스트 병합 (기존 동작)
            prev_ov = str(careers[target].get("공사(용역)개요") or "").strip()
            if prev_ov:
                careers[target]["공사(용역)개요"] = (prev_ov + "\n" + cont).strip()
            else:
                careers[target]["공사(용역)개요"] = cont

            # 1.5) 연장 텍스트가 나온 '다음 페이지' 정보를 이전 경력에 기록
            try:
                pp = careers[target].get("_pdf_pages")
                if not isinstance(pp, list):
                    pp = []
                if (next_page_idx + 1) not in pp:
                    pp.append(next_page_idx + 1)
                careers[target]["_pdf_pages"] = sorted({int(x) for x in pp if str(x).strip().isdigit()})
            except Exception:
                pass

            # 2) 에러2 근본 수정:
            #    parse_page_2의 ┖→ skip이 실패했을 때, 같은 cont 텍스트가
            #    다음 페이지 첫 경력의 사업명 앞에 그대로 붙는 현상을 제거한다.
            #    n_after == 다음 페이지의 첫 번째 경력 인덱스
            first_next_idx = n_after
            if first_next_idx < len(careers):
                next_name = str(careers[first_next_idx].get("사업명") or "").strip()
                # 줄바꿈을 단일 공백으로 정규화한 뒤 비교
                cont_norm = re.sub(r"\s+", " ", cont).strip()
                name_norm = re.sub(r"\s+", " ", next_name).strip()
                if cont_norm and name_norm.startswith(cont_norm):
                    cleaned = name_norm[len(cont_norm) :].strip()
                    careers[first_next_idx]["사업명"] = cleaned
                    print(
                        f"    [에러2 근본수정] 경력[{first_next_idx}] 사업명 앞 "
                        f"┖→ 오염 텍스트 제거: '{cont_norm[:40]}...'"
                        f" → 사업명='{cleaned[:40]}'"
                    )
    except Exception:
        return


def _merge_continuation(prev: Dict[str, Any], cur: Dict[str, Any]) -> Dict[str, Any]:
    """
    '┖→'로 인해 다음 행으로 이어진 내용을 이전 사업(prev)에 병합한다.
    - 규칙: cur의 값이 비어있지 않고 '┖→'가 아니면 prev에 이어붙임(공백/개행 보존)
    """
    if not prev:
        return cur
    for k, v in cur.items():
        if k.startswith("_"):
            continue
        sv = str(v or "").strip()
        if not sv or sv == "┖→":
            continue
        pv = str(prev.get(k) or "").strip()
        if not pv:
            prev[k] = sv
        else:
            # 긴 서술 필드는 줄바꿈으로 연결, 짧은 필드는 공백으로 연결
            if k in ["공사(용역)개요", "적용 공법", "적용 융복합건설기술", "적용 신기술 등", "시설물 종류", "비고", "담당업무"]:
                prev[k] = pv + "\n" + sv
            else:
                prev[k] = pv + " " + sv
    return prev


def parse_page_2(ctx: DocumentContext, page_num: int) -> List[Dict[str, Any]]:
    """
    제2쪽 파싱: 기술경력
    
    Args:
        ctx: DocumentContext
        page_num: 페이지 번호 (0부터 시작)
    
    Returns:
        List[Dict]: 기술경력 리스트
    """
    careers = []
    page_num_1based = page_num + 1
    
    try:
        if page_num >= ctx.total_pages:
            print(f"⚠️ 페이지 번호 오류: {page_num + 1}페이지는 존재하지 않습니다.")
            _cleanup_tech_career_job_noise_rows(careers)
            return careers

        page = ctx.pages[page_num]
        text = ctx.get_text(page_num) or ""

        print(f"  - 기술경력 파싱 중... (페이지 {page_num_1based})")

        # region agent log
        _agent_log(
            run_id="pre-fix",
            hypothesis_id="A",
            location="page_2_parser.py:parse_page_2:entry",
            message="enter parse_page_2",
            data={
                "page_num_0based": page_num,
                "page_num_1based": page_num + 1,
                "text_len": len(text or ""),
            },
        )
        # endregion agent log

        if not text.strip():
            _cleanup_tech_career_job_noise_rows(careers)
            return careers

        # 0) 원시 표(6열) 기반 파싱 1차 시도(사용자 요구: page2는 표 기반이 주 경로)
        try:
            table_rows = (
                _parse_tech_careers_from_raw_table(
                    page,
                    page_num_1based=page_num_1based,
                    pdf_path=str(getattr(ctx, "pdf_path", "") or ""),
                )
                or []
            )
        except Exception:
            table_rows = []
        if table_rows:
            _sanitize_header_like_project_names(table_rows, page_num_1based=page_num_1based)
            _sanitize_overview_like_project_names(table_rows, page_num_1based=page_num_1based)
            _fix_shifted_fields_in_tech_career_rows(table_rows)
            _cleanup_tech_career_job_noise_rows(table_rows)
            for _r in (table_rows or []):
                if isinstance(_r, dict) and "_pdf_pages" not in _r:
                    _r["_pdf_pages"] = [page_num_1based]
            return table_rows

        # 0) 위치(템플릿) 기반 파싱 1차 시도
        # - word bbox로 열을 직접 분리해 사업명 오염(직무/담당업무/직위 혼입)을 줄인다.
        # - 결과가 의심스러우면 기존 하이브리드 로직으로 폴백한다.
        used_template = False
        tpl_meta: dict | None = None
        if (not os.environ.get("PDFPARSER_DISABLE_TEMPLATE")) and (not _page_starts_with_overview_continuation(text)):
            try:
                from parsers.template_table_parser import (
                    parse_tech_page_by_template,
                    is_tech_template_result_trustworthy,
                )

                words = ctx.get_words(page_num, engine="auto") or []
                tpl_rows, tpl_meta = parse_tech_page_by_template(words)
                if is_tech_template_result_trustworthy(tpl_rows, tpl_meta):
                    used_template = True
                    # 각 경력 row에 원본 PDF 페이지 정보 부여(후처리/엑셀에서도 추적 가능)
                    for _r in (tpl_rows or []):
                        if isinstance(_r, dict) and "_pdf_pages" not in _r:
                            _r["_pdf_pages"] = [page_num_1based]
                    # 표에서만 잘 잡히는 필드(개요/금액/적용공법 등)는 안전하게 보강(사업명 등은 override 금지)
                    try:
                        tbl = _extract_projects_from_table(page) or []
                    except Exception:
                        tbl = []

                    # 1) 우선 사업명 공란을 표에서 채워 매칭 기반 보강이 가능하게 만든다.
                    _ensure_tech_career_names_non_empty(tpl_rows, tbl)
                    _sanitize_header_like_project_names(tpl_rows, page_num_1based=page_num + 1)
                    _sanitize_overview_like_project_names(tpl_rows, page_num_1based=page_num + 1)
                    _ensure_tech_career_names_non_empty(tpl_rows, tbl)

                    # FIX: 템플릿 블록 누락 감지(페이지별 ~ 라인 수 > 템플릿 블록 수면 폴백)
                    # - 템플릿은 bbox 라인 스트림에서 (시작일-~-종료일-일수) 블록을 구성하는데,
                    #   줄바꿈/셀 병합으로 간격이 늘어나면 블록을 놓칠 수 있다.
                    # - 반면 text 기반 '~' 라인 수는 페이지 내 블록 수의 강한 하한이므로,
                    #   템플릿이 이 하한보다 작으면 신뢰 불가로 본다.
                    try:
                        raw_lines_full = [
                            re.sub(r"[ \t]+", " ", ln).strip() for ln in (text or "").splitlines()
                        ]
                        lines_full = [ln for ln in raw_lines_full if ln and not _is_footer_or_header_line(ln)]
                        n_tilde_lines_full = sum(
                            1 for ln in (lines_full or []) if (ln or "").lstrip().startswith("~")
                        )
                    except Exception:
                        n_tilde_lines_full = 0
                    if n_tilde_lines_full and (len(tpl_rows or []) < n_tilde_lines_full):
                        used_template = False
                        # region agent log
                        _agent_log(
                            run_id="pre-fix",
                            hypothesis_id="C",
                            location="page_2_parser.py:parse_page_2:template_reject_tilde_floor",
                            message="rejecting template due to tilde-line floor being higher than template rows",
                            data={
                                "page_num_1based": page_num + 1,
                                "n_tpl_rows": len(tpl_rows or []),
                                "n_tilde_lines_full": int(n_tilde_lines_full),
                                "n_tbl_projects": len(tbl or []),
                                "tpl_meta": tpl_meta or {},
                            },
                        )
                        # endregion agent log
                        raise RuntimeError("template_rows_below_tilde_floor")

                    # FIX: 템플릿 블록 추출이 누락되면(예: 6건 중 5건) 그대로 리턴되어
                    #      페이지별 항목 누락으로 이어진다. 표에서 확인된 "유효 블록 수"가
                    #      템플릿보다 명확히 많으면 템플릿을 신뢰하지 않고 폴백한다.
                    try:
                        tbl_valid = sum(
                            1
                            for tr in (tbl or [])
                            if str((tr or {}).get("참여기간_시작일") or "").strip()
                            and str((tr or {}).get("참여기간_종료일") or "").strip()
                        )
                    except Exception:
                        tbl_valid = 0
                    if tbl_valid >= (len(tpl_rows or []) + 1):
                        used_template = False
                        # region agent log
                        _agent_log(
                            run_id="pre-fix",
                            hypothesis_id="C",
                            location="page_2_parser.py:parse_page_2:template_reject_mismatch",
                            message="rejecting template due to table having more valid blocks",
                            data={
                                "page_num_1based": page_num + 1,
                                "n_tpl_rows": len(tpl_rows or []),
                                "tbl_valid": int(tbl_valid),
                                "n_tbl_projects": len(tbl or []),
                                "tpl_meta": tpl_meta or {},
                            },
                        )
                        # endregion agent log
                        raise RuntimeError("template_result_missing_blocks")

                    if tbl and tpl_rows:
                        def _nk(s: str) -> str:
                            return re.sub(r"\\s+", "", (s or "")).strip()

                        # table name -> row (동명이인/중복 대비: 가장 긴 개요를 선택)
                        tmap: dict[str, dict] = {}
                        for tr in tbl:
                            nm = str((tr or {}).get("사업명") or "").strip()
                            if not nm:
                                continue
                            key = _nk(nm)
                            cur = tmap.get(key)
                            if cur is None:
                                tmap[key] = tr
                            else:
                                if len(str(tr.get("공사(용역)개요") or "")) > len(str(cur.get("공사(용역)개요") or "")):
                                    tmap[key] = tr

                        # table period(start/end/days) -> row (이름이 잘못/부분 추출된 경우에도 보강 가능)
                        pmap: dict[tuple[str, str, str, str], dict] = {}
                        for tr in tbl:
                            s0 = str((tr or {}).get("참여기간_시작일") or "").strip()
                            e0 = str((tr or {}).get("참여기간_종료일") or "").strip()
                            d1 = str((tr or {}).get("인정일수") or "").strip()
                            d2 = str((tr or {}).get("참여일수") or "").strip()
                            if not (s0 and e0):
                                continue
                            keyp = (s0, e0, d1, d2)
                            cur = pmap.get(keyp)
                            if cur is None:
                                pmap[keyp] = tr
                            else:
                                if len(str(tr.get("공사(용역)개요") or "")) > len(str(cur.get("공사(용역)개요") or "")):
                                    pmap[keyp] = tr

                        # 인정/참여일수가 표/템플릿 중 한쪽에서 누락되는 경우를 위해 (start,end) 폴백 맵도 준비
                        pmap_se: dict[tuple[str, str], dict] = {}
                        for tr in tbl:
                            s0 = str((tr or {}).get("참여기간_시작일") or "").strip()
                            e0 = str((tr or {}).get("참여기간_종료일") or "").strip()
                            if not (s0 and e0):
                                continue
                            keyse = (s0, e0)
                            cur = pmap_se.get(keyse)
                            if cur is None:
                                pmap_se[keyse] = tr
                            else:
                                if len(str(tr.get("공사(용역)개요") or "")) > len(str(cur.get("공사(용역)개요") or "")):
                                    pmap_se[keyse] = tr

                        FILL_KEYS = [
                            # issuer/worktype/specialty: 템플릿이 x-구간을 못 잡아 빈값이 되기 쉬우므로
                            # "비어있을 때만" 표 값으로 채운다(override 금지).
                            "발주자",
                            "공사종류",
                            "전문분야",
                            "공사(용역)개요",
                            "책임정도",
                            "직위",
                            "공사(용역)금액(백만원)",
                            "담당업무",
                            "직무분야",
                            "적용 공법",
                            "적용 융복합건설기술",
                            "적용 신기술 등",
                            "시설물 종류",
                            "비고",
                        ]
                        for r in tpl_rows:
                            nm = str(r.get("사업명") or "").strip()
                            if not nm:
                                continue
                            is_bonsa = _is_bonsa_like_project_name(nm)
                            # 1) 기간(시작/종료/일수)로 우선 매칭(이름이 누락/부분인 경우도 보강 가능)
                            s0 = str(r.get("참여기간_시작일") or "").strip()
                            e0 = str(r.get("참여기간_종료일") or "").strip()
                            d1 = str(r.get("인정일수") or "").strip()
                            d2 = str(r.get("참여일수") or "").strip()
                            cand = pmap.get((s0, e0, d1, d2)) if (s0 and e0) else None
                            if cand is None and (s0 and e0):
                                cand = pmap_se.get((s0, e0))

                            # 2) 이름으로 매칭(기존 방식)
                            key = _nk(nm)
                            # 본사는 이름 기반 매칭을 막고(오염 방지), 기간 기반 매칭만 허용한다.
                            if cand is None and not is_bonsa:
                                cand = tmap.get(key)
                            if cand is None:
                                # suffix match (테이블이 더 짧게 잡히는 케이스)
                                if not is_bonsa:
                                    for tk, tv in tmap.items():
                                        if key.endswith(tk) or tk.endswith(key):
                                            cand = tv
                                            break
                            if cand is None:
                                continue

                            # 사업명은 원칙적으로 override 하지 않지만,
                            # 표의 사업명이 현재 사업명을 "포함(superstring)"하고 더 길면
                            # 줄바꿈/셀 래핑으로 누락된 꼬리(예: '용역')일 가능성이 높으므로 확장 허용.
                            try:
                                nm_tbl = str((cand or {}).get("사업명") or "").strip()
                                if nm_tbl:
                                    curk = _nk(nm)
                                    tbk = _nk(nm_tbl)
                                    if curk and tbk and (curk in tbk) and (len(nm_tbl) >= len(nm) + 2):
                                        r["사업명"] = nm_tbl
                            except Exception:
                                pass

                            for k in FILL_KEYS:
                                curv = str(r.get(k) or "").strip()
                                v = str((cand or {}).get(k) or "").strip()
                                if v:
                                    # 담당업무/직무분야는 템플릿이 접미 조각만 잡는 경우가 있어
                                    # (예: '기술인)') 짧은 값이면 표 값으로 교체한다.
                                    if not curv:
                                        r[k] = v
                                        # region agent log
                                        if k in ["발주자", "공사종류", "전문분야"]:
                                            _agent_log(
                                                run_id="pre-fix",
                                                hypothesis_id="D",
                                                location="page_2_parser.py:parse_page_2:template_fill_from_table",
                                                message="filled issuer/worktype/specialty from table (empty template)",
                                                data={
                                                    "page_num_1based": page_num + 1,
                                                    "field": k,
                                                    "value": v[:60],
                                                    "project_name": str(r.get("사업명") or "")[:60],
                                                },
                                            )
                                        # endregion agent log
                                    elif k in ["담당업무", "직무분야"]:
                                        # FIX: 직무분야에 '설계/계획/조사...' 같은 담당업무 토큰이 들어가는 경우,
                                        # 표 값이 직무분야(토목/건축/...)면 길이 비교 없이 교체한다.
                                        if (k == "직무분야") and (curv in _DUTY_WORDS):
                                            try:
                                                from field_catalog import get_field_catalog

                                                cat = get_field_catalog(str(_PROJECT_ROOT))
                                                if v in set(cat.job_fields):
                                                    r[k] = v
                                                    continue
                                            except Exception:
                                                # 카탈로그가 없으면 힌트 기반으로만 판단
                                                if v in _JOB_FIELD_HINTS:
                                                    r[k] = v
                                                    continue
                                        if (len(curv) <= 4) or (curv.endswith(")")):
                                            # 표 값이 더 길고 현재 값이 표 값에 포함/접미면 교체
                                            if (len(v) > len(curv)) and (curv in v or v.endswith(curv)):
                                                r[k] = v
                                    else:
                                        # 나머지 필드는 빈 값만 채움
                                        continue

                    _cleanup_tech_career_job_noise_rows(tpl_rows)
                    # region agent log
                    _agent_log(
                        run_id="pre-fix",
                        hypothesis_id="C",
                        location="page_2_parser.py:parse_page_2:template_return",
                        message="returning template rows",
                        data={
                            "page_num_1based": page_num + 1,
                            "tpl_meta": tpl_meta or {},
                            "n_tpl_rows": len(tpl_rows or []),
                            "n_tbl_projects": len(tbl or []),
                            "sample_names": _agent_safe_name_sample(tpl_rows, limit=3),
                        },
                    )
                    # endregion agent log
                    return tpl_rows
            except Exception:
                pass

        # 1) 표 기반 파싱(책임정도/금액/적용공법 등 포함) 시도
        table_projects: List[Dict[str, Any]] = []
        try:
            table_projects = _extract_projects_from_table(page)
        except Exception:
            table_projects = []

        # 2) 텍스트 기반 날짜/일수 블록 추출
        # 에러2 수정: 페이지가 ┖→ 연장 블록으로 시작하면 해당 부분을 제거 후 파싱
        # (merge_cross_page_tech_overviews가 이미 이전 페이지 경력에 연장 내용을 추가하므로 중복 파싱 방지)
        raw_lines_full = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
        lines_full = [ln for ln in raw_lines_full if ln and not _is_footer_or_header_line(ln)]

        parse_start_offset = 0
        parse_start_offset_words = 0
        if _page_starts_with_overview_continuation(text):
            skip_to = _skip_overview_continuation_block(lines_full)
            if skip_to > 0:
                parse_start_offset = skip_to
                print(
                    f"    [INFO] 페이지 {page_num + 1} 상단 ┖→ 연장 블록({skip_to}줄) skip (이전 페이지 경력에 이미 병합됨)"
                )

        text_for_parse = "\n".join(lines_full[parse_start_offset:])

        date_blocks: List[Dict[str, str]] = []
        word_lines: List[str] = []
        try:
            # FIX: 날짜/일수 블록은 extract_text() 줄바꿈에 민감하므로,
            # 가능한 경우 좌표 기반 라인 스트림을 사용한다.
            word_lines = ctx.get_word_lines(
                page_num, engine="auto", y_tolerance=2.0, join_gap=1.0
            )
            if not word_lines:
                word_lines = _extract_lines_from_page_words(page)

            if word_lines:
                # extract_text 기반 skip_to는 word_lines와 줄 구조가 다를 수 있어, word_lines에서도 재계산한다.
                try:
                    wl0 = [
                        ln
                        for ln in word_lines
                        if ln and not _is_footer_or_header_line(ln)
                    ]
                    skip_to_w = (
                        _skip_overview_continuation_block(wl0)
                        if _page_starts_with_overview_continuation(text)
                        else 0
                    )
                    if skip_to_w > 0:
                        parse_start_offset_words = skip_to_w
                except Exception:
                    parse_start_offset_words = 0
                date_blocks = _extract_date_blocks_from_lines(
                    word_lines[parse_start_offset_words:]
                )
            else:
                date_blocks = _extract_date_blocks_from_text(text_for_parse)
        except Exception:
            date_blocks = []

        # region agent log
        try:
            n_tilde_lines_full = sum(1 for ln in (lines_full or []) if (ln or "").lstrip().startswith("~"))
        except Exception:
            n_tilde_lines_full = -1
        try:
            n_tilde_word_lines = sum(1 for ln in (word_lines or []) if (ln or "").lstrip().startswith("~"))
        except Exception:
            n_tilde_word_lines = -1
        _agent_log(
            run_id="pre-fix",
            hypothesis_id="A",
            location="page_2_parser.py:parse_page_2:after_date_blocks",
            message="computed date_blocks and skip offsets",
            data={
                "page_num_1based": page_num + 1,
                "page_starts_with_cont": bool(_page_starts_with_overview_continuation(text)),
                "parse_start_offset_text_lines": int(parse_start_offset),
                "parse_start_offset_word_lines": int(parse_start_offset_words),
                "n_lines_full": len(lines_full or []),
                "n_word_lines": len(word_lines or []),
                "n_tilde_lines_full": n_tilde_lines_full,
                "n_tilde_word_lines": n_tilde_word_lines,
                "n_date_blocks": len(date_blocks or []),
                "used_template": used_template,
                "tpl_meta": tpl_meta or {},
            },
        )
        # endregion agent log

        # 3) stacked 레이아웃 대응: date_blocks 기준으로 "절대 누락 없이" 행을 생성한다.
        # - 표는 종종 일부만 잡히거나 순서가 밀려 누락/오파싱이 생긴다.
        # - 따라서 date_blocks(기간/일수)는 본체로 보고, 사업명/직무/발주자 등은 텍스트 컨텍스트 + 표 보강으로 채운다.
        # 본문 라인 스트림: 가능한 경우 word_lines(좌표 기반)를 우선 사용한다.
        # NOTE: word_lines(좌표 기반 라인) 경로는 향후 리팩터링 시 재활성화 가능.
        # 현재는 extract_text 기반 라인으로 행을 구성한다(안정성 우선).
        if False and word_lines:
            lines = [
                ln
                for ln in word_lines[parse_start_offset_words:]
                if ln and not _is_footer_or_header_line(ln)
            ]
        else:
            raw_lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text_for_parse.splitlines()]
            lines = [ln for ln in raw_lines if ln and not _is_footer_or_header_line(ln)]

            # 2.5) "홍성달형" 흐름 파서: 발주자 블록이 '~' 없이 한 줄로 나오고,
            #      블록 구조가 (직무/직위/발주자블록) + 시작일 + '~' + 종료일 + (xx일)(xx일) + 사업명 으로 반복되는 문서.
            #      이 경우 date_blocks/table 매핑보다 직접 흐름 파싱이 더 안정적이다.
            def _parse_flow_blocks(lines_in: List[str]) -> List[Dict[str, Any]]:
                out: List[Dict[str, Any]] = []
                # FIX: 이전 블록의 확인된 종료일 라인 인덱스 집합.
                # 역방향 스캔이 이 인덱스에 도달하면 즉시 중단해 이전 블록 사업명 오인식을 방지.
                # 개요가 종료일보다 아래(텍스트 순서상 뒤)에 있는 구간은 줄 단위 fence로
                # 안전하게 덮기 어려워, 사업명 휴리스틱·접두 제거·표 교정으로 보완한다.
                confirmed_end_indices: set = set()
                i2 = 0
                while i2 < len(lines_in):
                    s0 = (lines_in[i2] or "").strip()
                    if not _DATE_RE.match(s0):
                        i2 += 1
                        continue
                    # 시작일 다음 '~'는 바로 다음 줄이 아닐 수 있음(공사종류 줄이 끼어 추출되는 PDF).
                    # _extract_date_blocks_from_text와 동일하게 12줄 내 탐색.
                    j = _find_tilde_line_index_after_start_date(lines_in, i2)
                    if j is None:
                        i2 += 1
                        continue
                    start_iso = _yyyy_mm_dd_to_iso(s0)

                    end_iso, end_idx, day_cursor = _flow_resolve_end_after_tilde(lines_in, j)
                    if end_idx is None and not end_iso:
                        i2 += 1
                        continue

                    # FIX: 종료일 라인을 confirmed_end_indices에 기록(실제 날짜/근무중 줄만)
                    if end_idx is not None and (
                        _DATE_RE.match((lines_in[end_idx] or "").strip())
                        or "근무중" in re.sub(r"\s+", "", (lines_in[end_idx] or "").strip())
                    ):
                        confirmed_end_indices.add(end_idx)

                    # 인정/참여일수: 종료일 이후 근접 6줄에서 2개 토큰 수집
                    window = []
                    k = day_cursor
                    hit_next_date = False
                    while k < len(lines_in) and len(window) < 8:
                        cur = (lines_in[k] or "").strip()
                        # FIX: 동일 페이지 내에 같은 시작일 블록이 연속으로 나오는 케이스(김경태 p44 등)에서,
                        #      인정/참여일 토큰을 못 잡으면 window가 "다음 시작일"까지 먹어버려
                        #      다음 블록 1건이 통째로 스킵되는 문제가 있다.
                        #      → 일수 수집 중 다음 날짜(YYYY.MM.DD)를 만나면 즉시 중단하고 다음 블록에 맡긴다.
                        if k != day_cursor and _DATE_RE.match(cur):
                            hit_next_date = True
                            break
                        window.append(cur)
                        if len(_extract_days_tokens_from_lines(window)) >= 2:
                            break
                        k += 1
                    toks = _extract_days_tokens_from_lines(window)
                    인정 = toks[0] if len(toks) >= 1 else ""
                    참여 = toks[1] if len(toks) >= 2 else ""

                    # 사업명: 홍성달형 PDF는 '사업명 …' 줄이 참여 시작일 바로 위에 온다(전방 스캔은 다음 건 이름을 오탐).
                    date_idx = i2
                    name = ""
                    name_fwd_idx: int | None = None
                    # FIX: confirmed_end_indices를 전달해 이전 블록 경계 침범 방지
                    name, jd_from_name = _collect_flow_block_project_name_backward(
                        lines_in, date_idx,
                        confirmed_end_indices=confirmed_end_indices,
                    )

                    if not name:
                        scan_idx = _next_data_line_index(lines_in, k + 1)
                        scan_n = 0
                        # FIX: forward scan 범위를 20→10으로 줄여 다음 블록 사업명 오인식 감소
                        while scan_idx is not None and scan_n < 10:
                            cand = (lines_in[scan_idx] or "").strip()
                            if _DATE_RE.match(cand):
                                nx = _next_data_line_index(lines_in, scan_idx + 1)
                                if nx is not None and (lines_in[nx] or "").lstrip().startswith("~"):
                                    break
                            if _is_probable_project_name_line(cand):
                                ph = _parse_project_line(cand)
                                name = (ph.get("사업명") if ph else cand) or ""
                                name = str(name).strip()
                                if ph and str(ph.get("직무분야") or "").strip() and str(ph.get("담당업무") or "").strip():
                                    jd_from_name = (
                                        str(ph.get("직무분야") or "").strip(),
                                        str(ph.get("담당업무") or "").strip(),
                                    )
                                name_fwd_idx = scan_idx
                                break
                            scan_idx = _next_data_line_index(lines_in, scan_idx + 1)
                            scan_n += 1

                    row = _blank_career_row()
                    row.update(
                        {
                            "사업명": name,
                            "참여기간_시작일": start_iso,
                            "참여기간_종료일": end_iso,
                            "인정일수": 인정,
                            "참여일수": 참여,
                        }
                    )
                    if jd_from_name:
                        row["직무분야"], row["담당업무"] = jd_from_name

                    # 로컬 컨텍스트: 시작일 직전 4줄에서 직무/직위/발주자블록을 찾는다.
                    for back in range(i2 - 1, max(-1, i2 - 6), -1):
                        if back < 0:
                            break
                        sback = (lines_in[back] or "").strip()
                        jd = _parse_job_and_duty_line_stacked(sback)
                        if jd and not row["직무분야"] and not row["담당업무"]:
                            row["직무분야"], row["담당업무"] = jd[0], jd[1]
                        if _looks_like_position_token(sback) and not row["직위"]:
                            row["직위"] = sback
                        issuer = _parse_issuer_line_stacked(sback)
                        if issuer and not row["발주자"]:
                            row["발주자"] = issuer.get("발주자", "") or ""
                            row["공사종류"] = issuer.get("공사종류", "") or ""
                            row["전문분야"] = issuer.get("전문분야", "") or ""
                            row["직위"] = row["직위"] or (issuer.get("직위", "") or "")
                            break

                    # 참여 시작일 바로 다음 '~ …' 줄이 이 블록의 발주자/공사종류/전문분야/직위(홍성달 표준)
                    tp = _parse_tilde_line_at(lines_in, j) or {}
                    for kk in ["발주자", "공사종류", "전문분야", "직위"]:
                        v = str(tp.get(kk) or "").strip()
                        if v:
                            row[kk] = v

                    out.append(row)
                    # FIX: 위에서 next-date로 중단했으면 해당 날짜 라인을 재처리해야 한다.
                    i2 = k if hit_next_date else (k + 1)
                    if name_fwd_idx is not None:
                        i2 = max(i2, name_fwd_idx + 1)
                return out

            try:
                has_inline_issuer = any(_parse_issuer_line_stacked(ln) for ln in lines[:120])
                has_date_tilde = any(
                    _DATE_RE.match(lines[idx])
                    and (
                        idx + 1 < len(lines)
                        and (lines[idx + 1] or "").lstrip().startswith("~")
                    )
                    for idx in range(min(len(lines), 200))
                )
                # 발주자가 '~ 한국수자원공사 …' 한 줄에만 있고 _parse_issuer_line_stacked 에 안 잡히는 경우(홍성달 원본)
                has_name_above_start = any(
                    di > 0
                    and bool(_DATE_RE.match((lines[di] or "").strip()))
                    and _is_probable_project_name_line((lines[di - 1] or "").strip())
                    for di in range(1, min(len(lines), 200))
                )
            except Exception:
                has_inline_issuer = False
                has_date_tilde = False
                has_name_above_start = False

            if has_date_tilde and (has_inline_issuer or has_name_above_start):
                flow_rows = _parse_flow_blocks(lines)

                # FIX: 기존 단순 개수 비교(len(flow_rows) < len(date_blocks))는
                # 개수가 같지만 사업명이 모두 공란/오인식인 경우에도 flow_rows를 채택해
                # "사업명 밀림" 오류를 일으킨다. 품질점수 비교로 교체.
                def _quality_score(rows: List[Dict[str, Any]]) -> int:
                    """
                    파싱 결과의 품질을 점수화.
                    - 비어있지 않은 사업명 × 2
                    - 발주자 있는 행 × 1
                    - 시작일·종료일 모두 있는 행 × 1
                    fallback 사업명("(사업명 미상)", "본사")은 점수 미부여.
                    """
                    score = 0
                    _FALLBACK_NAMES = {"(사업명 미상)", "본사"}
                    for r in rows:
                        nm = str(r.get("사업명") or "").strip()
                        if nm and nm not in _FALLBACK_NAMES:
                            score += 2
                        if str(r.get("발주자") or "").strip():
                            score += 1
                        if str(r.get("참여기간_시작일") or "").strip() and str(r.get("참여기간_종료일") or "").strip():
                            score += 1
                    return score

                flow_score = _quality_score(flow_rows)
                # date_blocks는 아직 사업명 등이 채워지지 않은 상태라
                # 개수(커버리지) 기준만 사용하되, flow_rows 품질이 현저히 낮으면 폴백.
                use_flow = True
                # 건수 불일치: 동일 시작일을 flow가 분할하거나 누락 복구 후 건수가 달라질 수 있음.
                # 이 경우 date_blocks 경로가 더 일관된 블록 경계를 갖는다.
                if date_blocks and len(flow_rows) != len(date_blocks):
                    use_flow = False
                elif date_blocks:
                    # 건수가 같을 때: 품질점수 비교
                    if flow_score == 0 and len(date_blocks) > 0:
                        use_flow = False

                if use_flow:
                    _fill_empty_사업명_from_table(flow_rows, table_projects)
                    # 표로 보강(근거 있는 행만)
                    flow_rows = _enrich_from_table_by_project_name(flow_rows, table_projects)
                    for r in flow_rows:
                        _apply_parse_project_line_to_row(r, str(r.get("사업명") or ""))
                    _ensure_tech_career_names_non_empty(flow_rows, table_projects)
                    _fix_shifted_fields_in_tech_career_rows(flow_rows)
                    _cleanup_tech_career_job_noise_rows(flow_rows)
                    # region agent log
                    _agent_log(
                        run_id="pre-fix",
                        hypothesis_id="D",
                        location="page_2_parser.py:parse_page_2:flow_return",
                        message="returning flow rows",
                        data={
                            "page_num_1based": page_num + 1,
                            "n_flow_rows": len(flow_rows or []),
                            "n_date_blocks": len(date_blocks or []),
                            "sample_names": _agent_safe_name_sample(flow_rows, limit=3),
                        },
                    )
                    # endregion agent log
                    return flow_rows

            if date_blocks:
                out_rows: List[Dict[str, Any]] = []
                i = 0
                b = 0  # date_blocks index

                # 표(table) 기반 데이터는 "필드 보강" 용도로만 사용한다.
                # - 홍성달처럼 각 블록이 1건씩 잘 구분되는 문서에서는, 기간 블록별로 표 프로젝트를 '몰아넣는' 전략이 오히려 오파싱을 만든다.
                # - 사업명은 텍스트(사업명 라인)에서 복원하는 것을 원칙으로 하고,
                #   표는 같은 사업명 매칭으로 발주자/공사종류/전문분야/직위/개요/공법 등을 채우는 용도로만 쓴다.

                def _infer_local_ctx_at(start_idx: int) -> Dict[str, str]:
                    """
                    날짜블록 시작일 라인 주변(직전 3줄 + tilde(~) 라인)에서만 컨텍스트를 추론한다.
                    - 전역 누적 컨텍스트는 stacked 레이아웃에서 다음 블록으로 오염되기 쉬움.
                    """
                    out: Dict[str, str] = {}

                    # 1) start date 이후 1~6줄 내에서 발주자 블록을 찾는다.
                    # - 케이스 A: '~ ...' 라인 (기존)
                    # - 케이스 B: '~'가 분리되어 있고, 그 다음 줄에 "발주자 공사종류 전문분야 직위"가 존재(홍성달)
                    # - 케이스 C: '~' 없이 한 줄에 "발주자 공사종류 전문분야 직위"가 존재
                    for k in range(start_idx + 1, min(len(lines), start_idx + 7)):
                        s2 = (lines[k] or "").strip()
                        if not s2:
                            continue
                        if s2.lstrip().startswith("~"):
                            parsed = _parse_tilde_line_at(lines, k) or {}
                            for kk in ["발주자", "공사종류", "전문분야", "직위"]:
                                v = str(parsed.get(kk) or "").strip()
                                if v:
                                    out[kk] = v
                            continue
                        issuer = _parse_issuer_line_stacked(s2)
                        if issuer:
                            for kk in ["발주자", "공사종류", "전문분야", "직위"]:
                                v = str(issuer.get(kk) or "").strip()
                                if v:
                                    out[kk] = v
                            break

                    # 2) start date 직전 3줄: 직무/담당, 직위 단독 라인을 탐지
                    # - 단, 직전 라인에 "사업명"이 있으면 이전 블록의 내용일 가능성이 높아 거기서 탐색을 멈춘다.
                    for k in range(start_idx - 1, max(-1, start_idx - 4), -1):
                        if k < 0:
                            break
                        s2 = (lines[k] or "").strip()
                        if _is_probable_project_name_line(s2):
                            break
                        jd = _parse_job_and_duty_line_stacked(s2)
                        if jd:
                            out.setdefault("직무분야", jd[0])
                            out.setdefault("담당업무", jd[1])
                        if _looks_like_position_token(s2):
                            out.setdefault("직위", s2)
                    return out

                # region agent log
                _agent_log(
                    run_id="pre-fix",
                    hypothesis_id="A",
                    location="page_2_parser.py:parse_page_2:date_blocks_path",
                    message="entering date_blocks assembly path",
                    data={
                        "page_num_1based": page_num + 1,
                        "n_date_blocks": len(date_blocks or []),
                        "n_table_projects": len(table_projects or []),
                        "n_lines": len(lines or []),
                        "n_tilde_lines": sum(
                            1 for ln in (lines or []) if (ln or "").lstrip().startswith("~")
                        ),
                    },
                )
                # endregion agent log

                while i < len(lines) and b < len(date_blocks):
                    s = (lines[i] or "").strip()
                    # date block의 start date 라인을 만나면 해당 블록 처리
                    if _DATE_RE.match(s):
                        iso = _yyyy_mm_dd_to_iso(s)
                        # 시작일이 아닌 날짜(종료일 등)는 블록 소비를 유발하면 안 된다.
                        expected_start = str(date_blocks[b].get("참여기간_시작일") or "").strip()
                        if expected_start and iso and iso != expected_start:
                            i += 1
                            continue
                        # date_blocks는 "start date -> (~ 라인 근처) -> end date -> (xx일)(xx일)"을 기준으로 생성되므로
                        # 여기서 b번째 블록을 하나 소비한다고 가정한다(누락 방지를 위해 i와 무관하게 진행).
                        base = _blank_career_row()
                        base.update(date_blocks[b])
                        # 내부 처리용 키 제거(엑셀 변환/외부 출력에 포함되면 안 됨)
                        if "_사업명후보" in base:
                            base.pop("_사업명후보", None)
                        issuer_hint = bool(base.pop("_issuer_hint", False))
                        base.pop("_issuer_sig", None)

                        # 로컬 컨텍스트만 반영
                        local_ctx = _infer_local_ctx_at(i)
                        for kk, vv in local_ctx.items():
                            if not str(base.get(kk) or "").strip() and str(vv or "").strip():
                                base[kk] = vv
                        has_local_evidence = any(
                            str(base.get(k) or "").strip()
                            for k in ["발주자", "공사종류", "전문분야", "직무분야", "담당업무", "직위"]
                        )
                        # 규칙: PDF 상에 근거가 없는 블록(사업명+기간/일수만 존재)은
                        # 다른 필드를 절대 채우지 않는다(컨텍스트 오염 방지).
                        # - 기관발주(issuer_hint) 블록은 예외(발주자/전문분야 등이 존재할 수 있음).
                        if (not issuer_hint) and (not has_local_evidence):
                            for k in [
                                "발주자",
                                "공사종류",
                                "직무분야",
                                "전문분야",
                                "담당업무",
                                "책임정도",
                                "직위",
                                "공사(용역)금액(백만원)",
                                "공사(용역)개요",
                                "적용 공법",
                                "적용 융복합건설기술",
                                "적용 신기술 등",
                                "시설물 종류",
                                "비고",
                            ]:
                                base[k] = ""

                        # 사업명 결정 우선순위:
                        # 1) 텍스트 후보(_사업명후보) (원칙)
                        # 2) 그래도 없으면: 비기관 단기 블록은 '본사' 폴백
                        # 3) 기관발주 블록인데도 후보가 없으면: 표에서 다음 1건만 차용(최후의 폴백)
                        names: List[str] = []
                        fallback = list(date_blocks[b].get("_사업명후보") or [])
                        normalized: List[str] = []
                        for nm0 in fallback:
                            # 사업명 줄 끝의 '직무분야 담당업무'는 issuer_hint 여부와 관계없이 분리한다.
                            parsed_header = _parse_project_line(str(nm0))
                            if parsed_header:
                                if not str(base.get("직무분야") or "").strip():
                                    base["직무분야"] = parsed_header.get("직무분야", "") or base.get("직무분야", "")
                                if not str(base.get("담당업무") or "").strip():
                                    base["담당업무"] = parsed_header.get("담당업무", "") or base.get("담당업무", "")
                                nm = str(parsed_header.get("사업명") or "").strip()
                                if nm:
                                    normalized.append(nm)
                            else:
                                nm = str(nm0 or "").strip()
                                if nm:
                                    normalized.append(nm)
                        for nm in normalized:
                            if not names or _norm_key(names[-1]) != _norm_key(nm):
                                names.append(nm)

                        # ── 에러1 2차 보강: row 생성 단계에서도 다음 date_block의 주 후보와 겹치는 names 제거 ──
                        # _extract_date_blocks_from_text에서 이미 처리했지만,
                        # _parse_project_line으로 직무/담당을 분리한 뒤 normalized된 이름은
                        # 다시 비교해야 하므로 여기서도 한 번 더 필터링한다.
                        # 동일 4튜플 병합으로 둔 부수 사업명은 실제 별도 행이므로 여기서 제거하지 않는다.
                        if (not date_blocks[b].get("_merged_duplicate_tuple")) and len(names) > 1:
                            next_primary_norms: set[str] = set()
                            for future_b in range(b + 1, len(date_blocks)):
                                f_cands = date_blocks[future_b].get("_사업명후보") or []
                                if f_cands:
                                    nk = _norm_key(str(f_cands[0]))
                                    if nk:
                                        next_primary_norms.add(nk)
                            if next_primary_norms:
                                first_nm = names[0]
                                filtered = [first_nm] + [
                                    nm for nm in names[1:]
                                    if _norm_key(nm) not in next_primary_norms
                                ]
                                if len(filtered) != len(names):
                                    print(
                                        f"    [에러1 2차] b={b} names 추가 정리: "
                                        f"{[n[:25] for n in names]} → {[n[:25] for n in filtered]}"
                                    )
                                names = filtered
                        # ──────────────────────────────────────────────────────────────────────

                        # // FIX: 원칙적으로 "date_blocks 1개 = 기술경력 1행"이다.
                        # `_extract_date_blocks_from_text`가 동일 4튜플(기간/일수)이 중복 감지될 때만
                        # `_merged_duplicate_tuple`로 표시하고, 그때만 추가 사업명(부수 후보)을 허용한다.
                        # 그 외 케이스에서 names가 2개 이상이면 phantom row(초과 파싱)로 이어지므로 1개만 유지한다.
                        if (not date_blocks[b].get("_merged_duplicate_tuple")) and len(names) > 1:
                            names = names[:1]

                        # FIX: 과거에는 (비기관 + 근거 없음 + 단기) 블록을 '본사'로 강제 폴백했는데,
                        #      이 로직은 "본사만 있고 나머지 필드가 비어있는" 행을 대량 생성할 수 있다.
                        #      지금은 사업명을 강제하지 않고 공란으로 두어, 표/후단 보강 로직이
                        #      가능한 경우 실제 사업명으로 채우게 한다(근거 없는 본사 오탐 감소).
                        if (not issuer_hint) and (not names) and (not has_local_evidence):
                            # region agent log
                            _agent_log(
                                run_id="pre-fix",
                                hypothesis_id="D",
                                location="page_2_parser.py:parse_page_2:skip_bonsa_fallback",
                                message="skipped bonsa fallback; leaving project name empty for later enrichment",
                                data={
                                    "page_num_1based": page_num + 1,
                                    "start": str(base.get("참여기간_시작일") or ""),
                                    "end": str(base.get("참여기간_종료일") or ""),
                                    "인정일수": str(base.get("인정일수") or ""),
                                    "참여일수": str(base.get("참여일수") or ""),
                                },
                            )
                            # endregion agent log

                        # 기관발주 블록인데도 사업명을 못 잡으면: 표에서 1건만 차용
                        if issuer_hint and (not names) and table_projects:
                            nm = str((table_projects[0] or {}).get("사업명") or "").strip()
                            if nm:
                                names = [nm]

                        # 인덱스 전진: 다음 start date 위치까지(있으면) 이동해 중복 처리 방지
                        j = i + 1
                        scan_end = min(len(lines), i + 90)
                        while j < scan_end:
                            sj = (lines[j] or "").strip()
                            if b + 1 < len(date_blocks) and _DATE_RE.match(sj):
                                iso_j = _yyyy_mm_dd_to_iso(sj)
                                next_start = str(date_blocks[b + 1].get("참여기간_시작일") or "").strip()
                                if iso_j and next_start and iso_j == next_start:
                                    break
                            j += 1

                        if names:
                            for nm in names:
                                r = dict(base)
                                r["사업명"] = nm
                                out_rows.append(r)
                        else:
                            out_rows.append(base)

                        b += 1
                        i = j
                        continue

                    i += 1

                # 남은 date_blocks가 있으면(스캔이 끊겼거나 텍스트가 비정상)라도 절대 누락 없이 추가
                while b < len(date_blocks):
                    r = _blank_career_row()
                    r.update(date_blocks[b])
                    out_rows.append(r)
                    b += 1

                # 표 기반 결과를 사업명 매칭으로 보강(가능한 필드만)
                # 후처리(스택/추출 품질 보정):
                # - 일부 PDF는 '본사' 블록의 사업명이 텍스트 추출에서 '본사/공사부 ...'로 섞여,
                #   다음 블록(연말~익년)의 '본사/공사부'와 중복되는 현상이 있다.
                # - 같은 사업명이 연속으로 반복되고, 뒤 블록이 연도 걸침이면 앞 블록은 '본사'로 보정한다.
                def _year(iso_s: str) -> int:
                    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", (iso_s or "").strip())
                    return int(m.group(1)) if m else 0

                # 0) 발주자 칸에 직위 토큰이 들어가는 오탐 정정(예: "발주자": "사원")
                for r in out_rows:
                    issuer = str(r.get("발주자") or "").strip()
                    if issuer and _looks_like_position_token(issuer) and not str(r.get("직위") or "").strip():
                        r["직위"] = issuer
                        r["발주자"] = ""

                for idx in range(len(out_rows) - 1):
                    a = out_rows[idx]
                    b2 = out_rows[idx + 1]
                    an = str(a.get("사업명") or "").strip()
                    bn = str(b2.get("사업명") or "").strip()
                    if not an or an != bn:
                        continue
                    if "/" not in an:
                        continue
                    if str(a.get("발주자") or "").strip() or str(b2.get("발주자") or "").strip():
                        continue
                    by0 = _year(str(b2.get("참여기간_시작일") or ""))
                    by1 = _year(str(b2.get("참여기간_종료일") or ""))
                    if by0 and by1 and by0 != by1:
                        # 뒤 블록이 연도 걸침이면, 앞 블록은 '본사'로 보정
                        a["사업명"] = "본사"

                    # 단기(예: 37일) 블록에서 '단양~...' 같은 사업명이 다음 장기 블록과 중복되는 경우:
                    # - 단기 블록은 "본사"로 보정하고, 기관/전문분야 등도 비운다(오매핑 방지)
                    try:
                        인정_a = int(str(a.get("인정일수") or "0").strip() or "0")
                        인정_b = int(str(b2.get("인정일수") or "0").strip() or "0")
                    except Exception:
                        인정_a, 인정_b = 0, 0
                    if (
                        인정_a
                        and 인정_a <= 90
                        and 인정_b
                        and 인정_b >= 180
                        and "~" in an
                        and an == bn
                    ):
                        a["사업명"] = "본사"
                        for k in ["발주자", "공사종류", "전문분야", "직위"]:
                            a[k] = ""

                _fill_empty_사업명_from_table(out_rows, table_projects)
                out_rows = _enrich_from_table_by_project_name(out_rows, table_projects)

                # 보정(최종): 단기 블록에 '단양~...' 등이 잘못 붙는 케이스를 한 번 더 제거
                for idx in range(len(out_rows) - 1):
                    a = out_rows[idx]
                    b2 = out_rows[idx + 1]
                    an = str(a.get("사업명") or "").strip()
                    bn = str(b2.get("사업명") or "").strip()
                    if not an or an != bn or "~" not in an:
                        continue
                    try:
                        인정_a = int(str(a.get("인정일수") or "0").strip() or "0")
                        인정_b = int(str(b2.get("인정일수") or "0").strip() or "0")
                    except Exception:
                        continue
                    if 인정_a and 인정_b and 인정_a <= 90 and 인정_b >= 180:
                        a["사업명"] = "본사"
                        for k in ["발주자", "공사종류", "전문분야", "직위"]:
                            a[k] = ""

                # 1) "본사/공사부"가 같은 연도 내 블록에 붙는 오탐 보정(해당 PDF에서 1999.04.25~1999.11.30은 '본사')
                for r in out_rows:
                    name = str(r.get("사업명") or "").strip()
                    if "공사부" in name:
                        y0 = _year(str(r.get("참여기간_시작일") or ""))
                        y1 = _year(str(r.get("참여기간_종료일") or ""))
                        if y0 and y1 and y0 == y1:
                            r["사업명"] = "본사"
                            for k in ["발주자", "공사종류", "전문분야", "직무분야", "담당업무", "직위"]:
                                r[k] = ""

                # 2) 단기(예: 37일) 본사 블록은 '사업명만' 존재하는 케이스가 많아 자동 채움 제거
                for r in out_rows:
                    name = str(r.get("사업명") or "").strip()
                    if name == "본사":
                        try:
                            인정 = int(str(r.get("인정일수") or "0").strip() or "0")
                        except Exception:
                            인정 = 0
                        if 인정 and 인정 <= 60:
                            for k in ["발주자", "공사종류", "전문분야", "직무분야", "담당업무", "직위"]:
                                # 본사대기/토목부는 예외(실제 직무/직위 존재)
                                if str(r.get("사업명") or "").strip() == "본사대기/토목부":
                                    continue
                                r[k] = ""

                for r in out_rows:
                    _apply_parse_project_line_to_row(r, str(r.get("사업명") or ""))

                _ensure_tech_career_names_non_empty(out_rows, table_projects)
                _sanitize_header_like_project_names(out_rows, page_num_1based=page_num + 1)
                _sanitize_overview_like_project_names(out_rows, page_num_1based=page_num + 1)
                _ensure_tech_career_names_non_empty(out_rows, table_projects)
                _fix_shifted_fields_in_tech_career_rows(out_rows)
                _cleanup_tech_career_job_noise_rows(out_rows)
                # region agent log
                _agent_log(
                    run_id="pre-fix",
                    hypothesis_id="A",
                    location="page_2_parser.py:parse_page_2:date_blocks_return",
                    message="returning date_blocks rows",
                    data={
                        "page_num_1based": page_num + 1,
                        "n_rows": len(out_rows or []),
                        "sample_names": _agent_safe_name_sample(out_rows, limit=3),
                    },
                )
                # endregion agent log
                # 각 경력 row에 원본 PDF 페이지 정보 부여
                for _r in (out_rows or []):
                    if isinstance(_r, dict) and "_pdf_pages" not in _r:
                        _r["_pdf_pages"] = [page_num_1based]
                return out_rows

            # --- 이하: 표 추출 실패 시 기존 텍스트 기반 폴백 ---
            lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
            lines = [ln for ln in lines if ln and not _is_footer_or_header_line(ln)]

            i = 0
            while i < len(lines):
                # 1) 사업명 라인 탐색
                if not _looks_like_project_start(lines, i):
                    i += 1
                    continue
                parsed = _parse_project_header_at(lines, i)
                if not parsed:
                    i += 1
                    continue
                proj, i = parsed

                row = _blank_career_row()
                row.update(proj)
                # i는 이미 헤더 소비된 위치로 이동됨

                # 2) 참여기간 시작일 (또는 ┖→)
                if i < len(lines):
                    start_raw = lines[i].strip()
                    if start_raw == "┖→":
                        row["참여기간_시작일"] = "┖→"
                        i += 1
                    elif _DATE_RE.match(start_raw):
                        row["참여기간_시작일"] = _yyyy_mm_dd_to_iso(start_raw)
                        i += 1

                # 3) '~ ...' 라인 (발주자/전문분야/직위)
                if i < len(lines) and lines[i].lstrip().startswith("~"):
                    tilde = _parse_tilde_line_at(lines, i) or {}
                    row.update({k: v for k, v in tilde.items() if k in row})
                    i += 1

                # 4) 참여기간 종료일 (또는 ┖→)
                if i < len(lines):
                    end_raw = lines[i].strip()
                    if end_raw == "┖→":
                        row["참여기간_종료일"] = "┖→"
                        i += 1
                    elif _DATE_RE.match(end_raw):
                        row["참여기간_종료일"] = _yyyy_mm_dd_to_iso(end_raw)
                        i += 1

                # 5) 인정일수 (인정일) - (xx일) 또는 ┖→
                if i < len(lines):
                    인정 = _parse_days_line(lines[i])
                    if 인정:
                        row["인정일수"] = 인정
                        i += 1

                # 6) 참여일수 (참여일) - (xx일) 또는 ┖→
                if i < len(lines):
                    참여 = _parse_days_line(lines[i])
                    if 참여:
                        row["참여일수"] = 참여
                        i += 1

                # 7) ┖→ 연장 처리: 참여기간/일수에 ┖→가 있으면 이전 사업에 병합 대상으로 간주
                is_cont = any(str(row.get(k) or "").strip() == "┖→" for k in ["참여기간_시작일", "참여기간_종료일", "인정일수", "참여일수"])

                # 8) 추가 텍스트(표가 길어져 아래 칸으로 이어지는 경우) 수집
                # - 다음 사업 시작으로 보이는 라인 전까지를 '공사(용역)개요'에 누적(안전한 폴백)
                extras: list[str] = []
                while i < len(lines) and not _looks_like_project_start(lines, i):
                    # 다음 사업 블록처럼 보이면 extras에 섞지 않고 멈춘다.
                    if _looks_like_project_block_ahead(lines, i) or _looks_like_project_block_ahead_loose(lines, i, lookahead=12):
                        break
                    extra = (lines[i] or "").strip()
                    if extra and not _is_footer_or_header_line(extra):
                        # '┖→'만 단독으로 나오면 다음 줄이 연장 본문일 확률이 높아 그대로 보관
                        extras.append(extra)
                    i += 1

                if extras:
                    # ┖→만 있는 라인은 제거하고, 나머지만 결합
                    cleaned = [e for e in extras if e != "┖→"]
                    if cleaned:
                        row["공사(용역)개요"] = "\n".join(cleaned).strip()

                if is_cont and careers:
                    careers[-1] = _merge_continuation(careers[-1], row)
                else:
                    careers.append(row)

            _ensure_tech_career_names_non_empty(careers, table_projects)
    
    except Exception as e:
        # FIX: Windows cp949 콘솔에서 이모지 출력 시 UnicodeEncodeError가 발생할 수 있다.
        print(f"[ERROR] 제2쪽 파싱 오류: {e}")
    
    def _drop_placeholder_masked_rows(rows: List[Dict[str, Any]]) -> None:
        """
        마스킹 날짜(****-**-**)만 남고 실데이터가 없는 플레이스홀더 행을 제거한다.
        - 실제 경력행인데 날짜만 마스킹된 경우(발주자/공사종류/일수 등 다른 값이 존재)는 유지한다.
        """
        if not rows:
            return

        def _has_real_payload(r: Dict[str, Any]) -> bool:
            # 사업명은 안내 문구에도 들어갈 수 있으므로, 그 외 핵심 필드에 값이 있는지로 판단한다.
            keys = [
                "발주자",
                "공사종류",
                "직무분야",
                "전문분야",
                "담당업무",
                "책임정도",
                "직위",
                "인정일수",
                "참여일수",
                "공사(용역)금액(백만원)",
                "공사(용역)개요",
            ]
            for k in keys:
                v = str(r.get(k) or "").strip()
                if v and v != "┖→":
                    return True
            return False

        kept: List[Dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            s = str(r.get("참여기간_시작일") or "").strip()
            e = str(r.get("참여기간_종료일") or "").strip()
            if s == "****-**-**" and e == "****-**-**" and (not _has_real_payload(r)):
                continue
            kept.append(r)
        rows[:] = kept

    _cleanup_tech_career_job_noise_rows(careers)
    _fix_shifted_fields_in_tech_career_rows(careers)
    _drop_placeholder_masked_rows(careers)
    # fallback/template 경로 모두에서 _pdf_pages 누락 방지
    for _r in (careers or []):
        if isinstance(_r, dict) and "_pdf_pages" not in _r:
            _r["_pdf_pages"] = [page_num_1based]
    # region agent log
    _agent_log(
        run_id="pre-fix",
        hypothesis_id="A",
        location="page_2_parser.py:parse_page_2:final_return",
        message="returning careers (fallback path)",
        data={
            "page_num_1based": page_num + 1,
            "n_rows": len(careers or []),
            "sample_names": _agent_safe_name_sample(careers, limit=3),
        },
    )
    # endregion agent log
    return careers

