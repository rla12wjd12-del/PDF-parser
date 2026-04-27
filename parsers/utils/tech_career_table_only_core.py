#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기술경력(page2) 표 기반 파싱 코어.

목표
- `page_2_parser_table_only.py`가 `page_2_parser.py`에 의존하지 않고 독립적으로 동작하도록
  "표 기반 추출 + 후처리" 로직을 별도 모듈로 제공한다.

주의
- 이 모듈은 **표 기반 경로**만 포함한다. 텍스트/좌표 기반 폴백 파서는 포함하지 않는다.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List
from pathlib import Path
from functools import lru_cache
import json
import re
import time
from datetime import datetime

from parsers.table_settings import (
    LINE_TABLE_SETTINGS,
    VIRTUAL_LEFT_X,
    VIRTUAL_RIGHT_X,
    extract_tables_merged,
    pick_best_table,
    safe_extract_tables,
)
from parsers.table_career_parser import (
    find_header_start_row,
    merge_into_previous,
    normalize_table_to_6cols,
    parse_period_cell,
)
from parsers.tech_career_heuristics import compiled_any, load_tech_career_heuristics

# ──────────────────────────────────────────────────────────────────────────────
# agent debug log (page_2_parser와 동일 형식 유지)
# ──────────────────────────────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────────────────────────────
# heuristics shared tokens
# ──────────────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_TC_H = load_tech_career_heuristics(_PROJECT_ROOT)
_RE_OVERVIEW_MEASURE = compiled_any(_TC_H.overview_measurement_patterns)
_RE_OVERVIEW_LISTISH = compiled_any(_TC_H.overview_listish_patterns)
_RE_PROJECT_START = compiled_any(_TC_H.project_start_regexes)

_JOB_FIELD_HINTS = frozenset(_TC_H.job_field_hints or ())


# ──────────────────────────────────────────────────────────────────────────────
# post-processors used by table-only
# ──────────────────────────────────────────────────────────────────────────────
def _looks_like_table_column_header_phrase(s: str) -> bool:
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
    if hit >= 2 and len(t) <= 80:
        return True
    if t in {
        "적용 공법",
        "시설물 종류",
        "적용 신기술 등",
        "적용 융ㆍ복합건설기술",
        "적용 융복합건설기술",
    }:
        return True
    return False


def _sanitize_header_like_project_names(rows: List[Dict[str, Any]], *, page_num_1based: int) -> None:
    if not rows:
        return
    logged = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        nm = str(r.get("사업명") or "").strip()
        if _looks_like_table_column_header_phrase(nm):
            r["사업명"] = ""
            jf = str(r.get("직무분야") or "").strip()
            dt = str(r.get("담당업무") or "").strip()
            if _looks_like_table_column_header_phrase(jf):
                r["직무분야"] = ""
            if _looks_like_table_column_header_phrase(dt):
                r["담당업무"] = ""
            if logged < 3:
                logged += 1
                _agent_log(
                    run_id="pre-fix",
                    hypothesis_id="D",
                    location="tech_career_table_only_core.py:_sanitize_header_like_project_names",
                    message="cleared header-like project name contamination",
                    data={
                        "page_num_1based": page_num_1based,
                        "bad_name": nm[:80],
                        "발주자": str(r.get("발주자") or "")[:40],
                        "직위": str(r.get("직위") or "")[:20],
                    },
                )


def _is_bonsa_like_project_name(s: str) -> bool:
    t = re.sub(r"\s+", "", (s or "")).strip()
    if not t:
        return False
    return t == "본사" or t.startswith("본사")


def _looks_like_overview_sentence_as_project_name(s: str) -> bool:
    raw = re.sub(r"\s+", " ", (s or "")).strip()
    if not raw:
        return False
    if _is_bonsa_like_project_name(raw):
        return False
    try:
        if _RE_OVERVIEW_MEASURE.search(raw) or _RE_OVERVIEW_LISTISH.search(raw):
            return True
    except Exception:
        pass
    if re.search(r"(\bD[1-9]\d{2,3}\b|L\d+(\.\d+)?|\bØ\d+\b)", raw, flags=re.IGNORECASE):
        return True
    if re.search(r"[㎜㎞㎡㎥]", raw):
        return True
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
                    location="tech_career_table_only_core.py:_sanitize_overview_like_project_names",
                    message="cleared overview-like sentence from project name",
                    data={"page_num_1based": page_num_1based, "bad_name": nm[:120]},
                )


def _cleanup_tech_career_job_noise_rows(rows: List[Dict[str, Any]]) -> None:
    """
    table-only 경로에서는 원본(page_2_parser)의 전체 후처리를 그대로 가져오기는 너무 커서,
    최소 보정만 수행한다.
    - 발주자/공사종류의 공백 정규화
    - 전문분야가 발주자에 그대로 복제된 단순 오염 제거
    """
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        sp = re.sub(r"\s+", " ", str(r.get("전문분야") or "").strip())
        iss = re.sub(r"\s+", " ", str(r.get("발주자") or "").strip())
        wt = re.sub(r"\s+", " ", str(r.get("공사종류") or "").strip())
        if sp and iss == sp:
            iss = ""
        r["전문분야"] = sp
        r["발주자"] = iss
        r["공사종류"] = wt


def _fix_shifted_fields_in_tech_career_rows(rows: List[Dict[str, Any]]) -> None:
    """
    page_2_parser의 대형 보정 로직을 table-only에서도 최소한 적용하기 위한 축약판.
    - 발주자 칸에 직위가 들어간 경우(직위 비어있음) swap
    - '본사 / 감리부' 분리 결합(대표 케이스)
    - 직무분야가 '및'로 오염된 대표 케이스 보정
    """

    def _norm_space(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    position_tokens = frozenset(_TC_H.table_col3_position_tokens or ())
    issuer_position_extra = frozenset(_TC_H.issuer_position_extra_tokens or ())
    and_token = (_TC_H.and_token or "및").strip() or "및"

    def _looks_like_position_token(s: str) -> bool:
        t = _norm_space(s)
        if not t:
            return False
        if t in position_tokens:
            return True
        if t in issuer_position_extra:
            return True
        return False

    for r in rows or []:
        if not isinstance(r, dict):
            continue

        issuer = _norm_space(str(r.get("발주자") or ""))
        pos = _norm_space(str(r.get("직위") or ""))
        if issuer and _looks_like_position_token(issuer) and not pos:
            r["직위"] = issuer
            r["발주자"] = ""

        name = _norm_space(str(r.get("사업명") or ""))
        jf = _norm_space(str(r.get("직무분야") or ""))
        if name.endswith("/") and jf and jf.endswith("부") and (jf not in _JOB_FIELD_HINTS):
            r["사업명"] = _norm_space(f"{name} {jf}")
            if not _norm_space(str(r.get("직무분야") or "")):
                r["직무분야"] = "토목"

        jf = _norm_space(str(r.get("직무분야") or ""))
        duty = _norm_space(str(r.get("담당업무") or ""))
        name = _norm_space(str(r.get("사업명") or ""))
        if jf == and_token:
            if ("용역" in name) and ("실시설계" not in name):
                rep = (_TC_H.and_repair_phrase or "및 실시설계용역").strip()
                if name.endswith("용역"):
                    r["사업명"] = _norm_space(name.replace("기본 용역", f"기본 {rep}"))
                else:
                    r["사업명"] = _norm_space(f"{name} {rep}")
            r["직무분야"] = "토목"
            if duty == "실시설계":
                r["담당업무"] = "설계"


# ──────────────────────────────────────────────────────────────────────────────
# table parsing core (page2 raw table -> rows)
# ──────────────────────────────────────────────────────────────────────────────
def _yyyy_mm_dd_to_iso(date_str: str) -> str:
    s = (date_str or "").strip()
    if not s:
        return ""
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
    if not t6:
        return t6

    def _row_joined(row: List[str]) -> str:
        return re.sub(r"\s+", " ", " ".join((c or "") for c in (row or [])).strip())

    def _row_key(row: List[str]) -> str:
        return re.sub(r"\s+", "", _row_joined(row))

    out: List[List[str]] = []
    for r in t6:
        rk = _row_key(r)
        if ("1.기술경력" in rk) and ("책임정도의" in rk) and ("*" in rk):
            continue
        out.append(r)
    return out


# 기본: 일(Day)까지 있는 날짜만 period start로 인정
_DATE_TOKEN_IN_CELL_RE = re.compile(r"^\s*\d{4}\.\d{2}\.\d{2}")
_DATE_YM_TOKEN_IN_CELL_RE = re.compile(r"^\s*\d{4}\.\d{2}\b")


def _stitch_page2_tech_data_rows_to_4row_blocks(t6: List[List[str]], *, header_start_row: int) -> List[List[str]]:
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
        if len(r) < 6:
            r = list(r) + [""] * (6 - len(r))
        else:
            r = list(r[:6])
        if _is_arrow_only_row(r):
            continue
        out_data.append(r)

    return header + out_data


def _iter_page2_tech_records_by_period_rows(
    t6: List[List[str]], *, header_start_row: int, allow_ym_period_start: bool = False
) -> Iterator[tuple[List[str], List[str], List[str], List[str]]]:
    hs = int(header_start_row)
    if not t6 or hs < 0 or (hs + 4) > len(t6):
        return

    def _row_is_empty(r: List[str]) -> bool:
        return not any((c or "").strip() for c in (r or []))

    def _is_period_start_row(r: List[str]) -> bool:
        if not r:
            return False
        c0 = (r[0] or "").strip()
        if _DATE_TOKEN_IN_CELL_RE.search(c0) is not None:
            return True
        # 예외(타겟 페이지 등): 'YYYY.MM'까지만 남는 경우도 start로 인정
        if allow_ym_period_start and (_DATE_YM_TOKEN_IN_CELL_RE.search(c0) is not None):
            # 오탐 방지:
            # - '~'가 있어야 함
            # - (n일) 토큰이 있어야 함
            # - 월 토큰 2개가 서로 달라야 함(같으면 헤더/노이즈일 가능성 큼)
            if "~" not in c0:
                return False
            yms = re.findall(r"\b\d{4}\.\d{2}\b", c0)
            if len(yms) < 2:
                return False
            if yms[0] == yms[1]:
                return False
            if re.search(r"\(\s*\d[\d,]*\s*일\s*\)", c0) is None:
                return False
            return True
        return False

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


@lru_cache(maxsize=1)
def _table_score_keywords() -> tuple[str, ...]:
    return (
        "사업명",
        "발주자",
        "공사(용역)개요",
        "참여기간",
        "직무분야",
        "전문분야",
        "직위",
        "비고",
    )


def _table_score(tbl: list) -> tuple[int, int, int]:
    if not tbl:
        return (-10_000, 0, 0)
    max_cols = max((len(r) for r in tbl if r), default=0)
    if max_cols <= 2:
        return (-10_000, max_cols, len(tbl))
    n_header = 0
    joined_head = []
    kws = _table_score_keywords()
    for r in tbl[: min(12, len(tbl))]:
        if not r:
            continue
        joined = (" ".join([(c or "") for c in r if c]).strip()) if r else ""
        if joined:
            joined_head.append(joined)
        if any(k in joined for k in kws):
            n_header += 1
    head_text = " ".join(joined_head)
    is_note = False
    if "※" in head_text:
        is_note = True
    if re.search(r"\b1\.\s*기술경력\b", head_text):
        is_note = True
    if ("책임정도의" in head_text) and ("보정계수" in head_text):
        is_note = True
    non_empty_rows = 0
    for r in tbl:
        if r and any(str(c or "").strip() for c in r):
            non_empty_rows += 1
    has_left = "참여기간" in head_text
    has_right = "비고" in head_text
    base = (1200 if n_header >= 1 else 0) + (max_cols * 20) + non_empty_rows
    if has_left and has_right:
        base += 900
    if n_header >= 2 and max_cols <= 4:
        base -= 900
    if is_note:
        base -= 2500
    return (base, n_header, max_cols)


def _parse_tech_careers_from_raw_table(
    page: Any,
    *,
    page_num_1based: int,
    pdf_path: str = "",
) -> List[Dict[str, Any]]:
    """
    기술경력(page2) 표를 6열/4행 블록으로 파싱한다.
    - 표 인식이 실패하거나 구조가 예상과 다르면 빈 리스트를 반환
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

    try:
        best = pick_best_table(tables, _table_score) or []
    except Exception:
        best = tables[0] if tables else []
    if not best:
        return []

    t6 = normalize_table_to_6cols(best)
    t6 = _preprocess_page2_tech_table6(t6)
    if not t6:
        return []
    if len(t6[0]) < 6:
        return []

    hs = find_header_start_row(t6)
    if hs is None:
        return []

    if 0 <= hs and (hs + 3) < len(t6):
        period_header_4 = ["참여기간_시작일", "참여기간_종료일", "인정일", "참여일"]
        for i, label in enumerate(period_header_4):
            try:
                if len(t6[hs + i]) >= 1:
                    t6[hs + i][0] = label
            except Exception:
                pass

    t6 = _stitch_page2_tech_data_rows_to_4row_blocks(t6, header_start_row=hs)

    out: List[Dict[str, Any]] = []
    debug_page = 8  # 진단 대상(요청 PDF에서 불일치 발생 페이지)
    if page_num_1based == debug_page:
        try:
            # data 구간의 첫 컬럼 샘플(블록 분리 실패 원인 추적용)
            hs_i = int(hs) if hs is not None else -1
            data_rows = t6[hs_i + 4 :] if (hs_i >= 0 and (hs_i + 4) < len(t6 or [])) else []
            c0_samples = []
            for rr in data_rows[:20]:
                try:
                    c0_samples.append(str((rr[0] if rr else "") or "")[:80])
                except Exception:
                    c0_samples.append("")
            _agent_log(
                run_id="table-only",
                hypothesis_id="T",
                location="tech_career_table_only_core.py:_parse_tech_careers_from_raw_table:pre_iter",
                message="diagnose blocks for target page",
                data={
                    "page_num_1based": page_num_1based,
                    "n_tables": len(tables or []),
                    "best_rows": len(best or []),
                    "t6_rows": len(t6 or []),
                    "header_start_row": hs,
                    "data_c0_samples": c0_samples,
                },
            )
        except Exception:
            pass
    for r0, r1, r2, r3 in _iter_page2_tech_records_by_period_rows(
        t6,
        header_start_row=hs,
        allow_ym_period_start=(page_num_1based == debug_page),
    ):
        raw_period = (r0[0] if len(r0) >= 1 else "") or ""
        period = parse_period_cell(raw_period, yyyy_mm_dd_to_iso=_yyyy_mm_dd_to_iso)
        if page_num_1based == debug_page:
            try:
                _agent_log(
                    run_id="table-only",
                    hypothesis_id="T",
                    location="tech_career_table_only_core.py:_parse_tech_careers_from_raw_table:period_row",
                    message="period parsed from r0[0]",
                    data={
                        "page_num_1based": page_num_1based,
                        "raw_period": str(raw_period)[:180],
                        "start_iso": str(getattr(period, "start_iso", "") or ""),
                        "end_iso": str(getattr(period, "end_iso", "") or ""),
                        "has_continue_arrow": bool(getattr(period, "has_continue_arrow", False)),
                    },
                )
            except Exception:
                pass

        row = _blank_career_row()
        row["_pdf_pages"] = [page_num_1based]
        row["참여기간_시작일"] = period.start_iso
        row["참여기간_종료일"] = period.end_iso
        row["인정일수"] = period.인정일수
        row["참여일수"] = period.참여일수

        row["사업명"] = (r0[1] if len(r0) > 1 else "") or ""
        row["직무분야"] = (r0[3] if len(r0) > 3 else "") or ""
        row["담당업무"] = (r0[4] if len(r0) > 4 else "") or ""
        row["비고"] = (r0[5] if len(r0) > 5 else "") or ""

        row["발주자"] = (r1[1] if len(r1) > 1 else "") or ""
        row["공사종류"] = (r1[2] if len(r1) > 2 else "") or ""
        row["전문분야"] = (r1[3] if len(r1) > 3 else "") or ""
        row["직위"] = (r1[4] if len(r1) > 4 else "") or ""

        row["공사(용역)개요"] = (r2[1] if len(r2) > 1 else "") or ""
        row["책임정도"] = (r2[3] if len(r2) > 3 else "") or ""
        row["공사(용역)금액(백만원)"] = (r2[4] if len(r2) > 4 else "") or ""

        row["적용 공법"] = (r3[1] if len(r3) > 1 else "") or ""
        row["적용 융복합건설기술"] = (r3[2] if len(r3) > 2 else "") or ""
        row["적용 신기술 등"] = (r3[3] if len(r3) > 3 else "") or ""
        row["시설물 종류"] = (r3[4] if len(r3) > 4 else "") or ""

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
            try:
                pp = out[-1].get("_pdf_pages")
                if isinstance(pp, list) and page_num_1based not in pp:
                    pp.append(page_num_1based)
            except Exception:
                pass
            continue

        out.append(row)

    return out

