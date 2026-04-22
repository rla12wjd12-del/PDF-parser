#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
제3쪽 파서: 건설사업관리 및 감리경력
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict, Any, Optional
import re
import json
import time
from datetime import datetime

from parsers.page_2_parser import (
    _collect_flow_block_project_name_backward,
    _enrich_from_table_by_project_name,
    _is_annotation_or_footnote_line,
)
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

# endregion agent log

_DATE_RE = re.compile(r"^\d{4}\.\d{2}(?:\.\d{2})?$")
_DAYS_RE = re.compile(r"^\(?\s*(\d[\d,]*)\s*일\s*\)?$")
_DAYS_TOKEN_RE = re.compile(r"\(\s*(\d[\d,]*)\s*일\s*\)")

_POSITION_TOKENS = frozenset(
    {
        "부장",
        "차장",
        "과장",
        "대리",
        "주임",
        "사원",
        "선임",
        "책임",
        "수석",
        "소장",
        "실장",
        "팀장",
        "본부장",
        "단장",
        "감리원",
        "관리원",
        "검사원",
        "반장",
        "조장",
    }
)

_ISSUER_HINT = re.compile(
    r"(시청|군청|구청|도청|관리청|도로공사|전력공사|수자원공사|농어촌공사|환경공단|시설공단|도시공사|"
    r"주택공사|토지공사|국토관리청|지방국토)"
)


def _yyyy_mm_dd_to_iso(date_str: str) -> str:
    s = (date_str or "").strip()
    if not s:
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
        return f"{m2.group(1)}-{m2.group(2)}"
    return ""


def _blank_cm_career_row() -> Dict[str, Any]:
    # 기술경력과 동일한 출력 스키마를 최대한 유지(사용자 요구: 키 삭제 금지 방향)
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


_CM_STOP_KEYWORDS_STRICT = (
    # CM 경력 테이블 "아래"의 요약 블록 라벨들(너무 범용 키워드는 오탐이 많아 제외)
    "업무수행기간",
    "용역완성비율",
    "인정일수현황",
)


def _row_has_stop_keyword(row: list[str], *, keywords: tuple[str, ...]) -> tuple[bool, str]:
    """
    stop 키워드는 데이터 셀 내 일반 텍스트(예: '...인정일수...')에서도 등장할 수 있어
    '행 전체 포함(any in joined)' 대신, '셀 시작(prefix)' 위주로 보수적으로 판정한다.
    """
    cells = [str(c or "").replace(" ", "").strip() for c in (row or [])]
    for k in keywords:
        for c in cells:
            if not c:
                continue
            if c.startswith(k):
                return True, k
    return False, ""


def _parse_cm_careers_from_raw_table(
    page: Any,
    *,
    page_num_1based: int,
    pdf_path: str = "",
) -> List[Dict[str, Any]]:
    """
    CM(page3) 표를 6열/4행 블록으로 파싱한다.
    - 표 아래 요약(업무수행기간/용역완성비율 등)이 섞이지 않도록 중단 키워드를 적용한다.
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
        best = pick_best_table(tables, _cm_table_score) or []
    except Exception:
        best = tables[0] if tables else []
    if not best:
        return []

    t6 = normalize_table_to_6cols(best)
    if not t6 or len(t6[0]) < 6:
        dump_raw_tables_to_excel(
            pdf_path=str(pdf_path or ""),
            section="page3_cm",
            page_num_1based=page_num_1based,
            tables_all=tables,
            best_table=best,
            normalized_6cols=t6 if isinstance(t6, list) else [],
            meta={"note": "normalize_table_to_6cols failed or <6 columns"},
        )
        return []

    hs = find_header_start_row(t6)
    if hs is None:
        dump_raw_tables_to_excel(
            pdf_path=str(pdf_path or ""),
            section="page3_cm",
            page_num_1based=page_num_1based,
            tables_all=tables,
            best_table=best,
            normalized_6cols=t6,
            meta={"note": "find_header_start_row returned None"},
        )
        return []

    # 성공 경로에서도 덤프(오류 분석용)
    dump_raw_tables_to_excel(
        pdf_path=str(pdf_path or ""),
        section="page3_cm",
        page_num_1based=page_num_1based,
        tables_all=tables,
        best_table=best,
        normalized_6cols=t6,
        meta={"header_start_row": int(hs), "n_rows_raw": int(len(best or [])), "n_rows_norm": int(len(t6 or []))},
    )

    def _is_period_start_row(row: list[str]) -> bool:
        if not row:
            return False
        # period row는 보통 어딘가에 'YYYY.MM.DD'를 포함(열이 밀리는 케이스 대비)
        joined = " ".join(str(c or "") for c in (row or []))
        return bool(re.search(r"\b\d{4}\.\d{2}\.\d{2}\b", joined))

    def _iter_cm_records(table6: list[list[str]], *, header_start: int) -> list[list[list[str]]]:
        """
        CM 테이블은 pdfplumber 추출 품질에 따라 4행 블록이 깨질 수 있어
        (행 누락/합쳐짐) '기간 시작 행'을 기준으로 레코드를 재구성한다.
        - 다음 기간 시작 행이 나오기 전까지의 행들을 1 레코드 후보로 묶고
        - 4행 미만이면 빈 행으로 패딩하여 4행을 맞춘다.
        """
        if not table6:
            return []

        # 헤더 크기가 문서마다 달라, header_start 이후 가까운 구간에서 첫 데이터(기간) 시작 행을 찾는다.
        data_start = None
        for i in range(header_start, min(len(table6), header_start + 12)):
            if _is_period_start_row(table6[i] or []):
                data_start = i
                break
        if data_start is None:
            data_start = header_start + 4

        rows = table6[data_start:]
        rows = [r for r in (rows or []) if r and any((c or "").strip() for c in r)]

        records: list[list[list[str]]] = []
        cur: list[list[str]] = []
        for r in rows:
            if _is_period_start_row(r):
                if cur:
                    records.append(cur)
                cur = [r]
            else:
                if not cur:
                    # stray row before first period-start; skip
                    continue
                cur.append(r)
        if cur:
            records.append(cur)

        # normalize to 4 rows
        out: list[list[list[str]]] = []
        blank = [""] * 6
        for rec in records:
            rec2 = (rec or [])[:4]
            if len(rec2) < 4:
                rec2 = rec2 + [blank[:] for _ in range(4 - len(rec2))]
            out.append(rec2)
        return out

    # 헤더 이후 데이터 영역에서 stop 키워드가 등장하면 그 이전까지만 남긴다.
    cut = None
    cut_kw = ""
    for i in range(hs + 4, len(t6)):
        rowi = t6[i] or []
        joined = "".join((c or "").replace(" ", "") for c in rowi)
        if not joined.strip():
            continue
        hit, kw = _row_has_stop_keyword(rowi, keywords=_CM_STOP_KEYWORDS_STRICT)
        if hit:
            cut = i
            cut_kw = kw
            break
    if cut is not None:
        _agent_log(
            run_id="cm-stop-cut",
            hypothesis_id="CM",
            location="page_3_parser.py:_parse_cm_careers_from_raw_table:stop_cut",
            message="cutting CM table at stop keyword",
            data={
                "page_num_1based": int(page_num_1based),
                "header_start": int(hs),
                "cut_row_index": int(cut),
                "cut_keyword": cut_kw,
                "cut_row_joined": "".join((c or "").strip() for c in (t6[cut] or []))[:240],
                "total_rows_before_cut": int(len(t6)),
            },
        )
        t6 = t6[:cut]

    # 4행 블록 파싱 전, 데이터 행 수/배수 여부 로깅(디버그)
    try:
        data_rows = [r for r in (t6[hs + 4 :] or []) if r and any((c or "").strip() for c in r)]
        _agent_log(
            run_id="cm-4rows-shape",
            hypothesis_id="CM",
            location="page_3_parser.py:_parse_cm_careers_from_raw_table:data_shape",
            message="CM table data shape before 4-row grouping",
            data={
                "page_num_1based": int(page_num_1based),
                "header_start": int(hs),
                "n_rows_total": int(len(t6)),
                "n_rows_data_non_empty": int(len(data_rows)),
                "n_rows_data_mod_4": int(len(data_rows) % 4),
                "first_data_row_joined": "".join((c or "").strip() for c in (data_rows[0] if data_rows else []))[:240],
            },
        )
    except Exception:
        pass

    out: List[Dict[str, Any]] = []
    blocks = _iter_cm_records(t6, header_start=hs)
    try:
        lens = [len([r for r in b if r and any((c or "").strip() for c in r)]) for b in (blocks or [])]
        _agent_log(
            run_id="cm-records",
            hypothesis_id="CM",
            location="page_3_parser.py:_parse_cm_careers_from_raw_table:records_built",
            message="built CM records from table rows",
            data={
                "page_num_1based": int(page_num_1based),
                "header_start": int(hs),
                "n_blocks": int(len(blocks or [])),
                "non_empty_rows_per_block": lens[:20],
            },
        )
    except Exception:
        pass

    for block in (blocks or []):
        if not block or len(block) != 4:
            continue
        r0, r1, r2, r3 = block
        period = parse_period_cell(r0[0] if len(r0) >= 1 else "", yyyy_mm_dd_to_iso=_yyyy_mm_dd_to_iso)

        row = _blank_cm_career_row()
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
                if isinstance(pp, list):
                    if page_num_1based not in pp:
                        pp.append(page_num_1based)
            except Exception:
                pass
            continue

        out.append(row)

    return out


def _blank_cm_work_periods() -> Dict[str, str]:
    """
    '업무수행기간'은 CM 경력 '각 행'의 필드가 아니라,
    CM 섹션 하단의 별도 요약 블록이므로 별도 객체로 분리한다.
    """
    return {
        "비고": "업무 수행 중복기간은 건수로 나누어 산정하여 기록함",
        # 1) 건설사업관리
        "건설사업관리_업무수행기간_일": "",
        "건설사업관리_상주_일": "",
        "건설사업관리_상주_감독권한대행등_일": "",
        "건설사업관리_상주_시공단계_일": "",
        "건설사업관리_기술지원_일": "",
        "건설사업관리_기술지원_감독권한대행등_일": "",
        "건설사업관리_기술지원_시공단계_일": "",
        # 2) 감리
        "감리_업무수행기간_일": "",
        "감리_상주_일": "",
        "감리_상주_공동주택_일": "",
        "감리_상주_다중이용시설_일": "",
        "감리_기술지원_일": "",
        "감리_기술지원_공동주택_일": "",
        "감리_기술지원_다중이용시설_일": "",
        # 3) 안전관리
        "안전관리_업무수행기간_일": "",
    }


def _parse_cm_work_periods(text: str) -> Dict[str, str]:
    """
    CM 섹션 하단의 '업무수행기간' 블록을 최대한 복원한다.

    예) (공백/줄바꿈은 PDF 추출에 따라 달라질 수 있음)
    - 건설사업관리 업무 수행기간 #,### 일
      상주 : #,### 일  (감독 권한대행 등 ... / 시공 단계 ...)
      기술지원 : #,### 일 (감독 권한대행 등 ... / 시공 단계 ...)
    - 감리 업무 수행기간 #,### 일
      상 주 : #,### 일 (공동주택/다중이용시설)
      기술지원 : #,### 일 (공동주택/다중이용시설)
    - 건설사업관리기술인으로서 안전관리 업무 수행기간 : #,### 일
    """
    out = _blank_cm_work_periods()
    if not text:
        return out

    compact = re.sub(r"\s+", "", text).replace("：", ":")

    def _num(s: str) -> str:
        return re.sub(r"[^\d,]", "", s or "").replace(",", "").strip()

    def _extract_days_after(label: str, hay: str, *, max_scan: int = 260) -> str:
        if not label:
            return ""
        p = hay.find(label)
        if p < 0:
            return ""
        window = hay[p : p + max_scan]
        m = re.search(re.escape(label) + r"[:：]?(?P<n>\d[\d,]*)일", window)
        if m:
            return _num(m.group("n"))
        return ""

    def _slice_between(start_label: str, end_labels: list[str]) -> str:
        sp = compact.find(start_label) if start_label else -1
        if sp < 0:
            return ""
        ep = len(compact)
        for el in end_labels or []:
            p = compact.find(el, sp + len(start_label))
            if p >= 0:
                ep = min(ep, p)
        return compact[sp:ep]

    # --- 건설사업관리 ---
    cm_block = _slice_between(
        "건설사업관리업무수행기간",
        ["감리업무수행기간", "건설사업관리기술인으로서안전관리업무수행기간", "용역완성비율"],
    )
    if cm_block:
        out["건설사업관리_업무수행기간_일"] = _extract_days_after("건설사업관리업무수행기간", cm_block) or out[
            "건설사업관리_업무수행기간_일"
        ]
        out["건설사업관리_상주_일"] = _extract_days_after("상주", cm_block) or out["건설사업관리_상주_일"]
        out["건설사업관리_기술지원_일"] = _extract_days_after("기술지원", cm_block) or out["건설사업관리_기술지원_일"]

        # 상주 상세
        out["건설사업관리_상주_감독권한대행등_일"] = (
            _extract_days_after("감독권한대행등건설사업관리", cm_block) or out["건설사업관리_상주_감독권한대행등_일"]
        )
        out["건설사업관리_상주_시공단계_일"] = (
            _extract_days_after("시공단계건설사업관리", cm_block) or out["건설사업관리_상주_시공단계_일"]
        )

        # 기술지원 상세(같은 라벨이 2번 나올 수 있어, 뒤쪽 1회 더 스캔)
        # - "감독권한대행등건설사업관리" / "시공단계건설사업관리"는 상주/기술지원 양쪽 모두에 등장하므로
        #   기술지원 구간을 대략 '기술지원' 이후로 잘라 재탐색한다.
        kpos = cm_block.find("기술지원")
        if kpos >= 0:
            cm_block_tech = cm_block[kpos:]
            out["건설사업관리_기술지원_감독권한대행등_일"] = (
                _extract_days_after("감독권한대행등건설사업관리", cm_block_tech) or out["건설사업관리_기술지원_감독권한대행등_일"]
            )
            out["건설사업관리_기술지원_시공단계_일"] = (
                _extract_days_after("시공단계건설사업관리", cm_block_tech) or out["건설사업관리_기술지원_시공단계_일"]
            )

    # --- 감리 ---
    sup_block = _slice_between(
        "감리업무수행기간",
        ["건설사업관리기술인으로서안전관리업무수행기간", "용역완성비율"],
    )
    if sup_block:
        out["감리_업무수행기간_일"] = _extract_days_after("감리업무수행기간", sup_block) or out["감리_업무수행기간_일"]

        # '상 주'도 공백 제거 시 '상주'로 동일해진다.
        out["감리_상주_일"] = _extract_days_after("상주", sup_block) or out["감리_상주_일"]
        out["감리_기술지원_일"] = _extract_days_after("기술지원", sup_block) or out["감리_기술지원_일"]

        # 상주 상세(공동주택/다중이용시설)
        # - 동일 라벨이 기술지원에도 반복되므로, 상주/기술지원 구간을 분리해 각각 추출
        spos = sup_block.find("상주")
        tpos = sup_block.find("기술지원")
        sup_block_stay = sup_block[spos : (tpos if (spos >= 0 and tpos > spos) else len(sup_block))] if spos >= 0 else sup_block
        sup_block_tech = sup_block[tpos:] if tpos >= 0 else ""

        out["감리_상주_공동주택_일"] = _extract_days_after("공동주택", sup_block_stay) or out["감리_상주_공동주택_일"]
        out["감리_상주_다중이용시설_일"] = _extract_days_after("다중이용시설", sup_block_stay) or out["감리_상주_다중이용시설_일"]
        if sup_block_tech:
            out["감리_기술지원_공동주택_일"] = _extract_days_after("공동주택", sup_block_tech) or out["감리_기술지원_공동주택_일"]
            out["감리_기술지원_다중이용시설_일"] = _extract_days_after("다중이용시설", sup_block_tech) or out["감리_기술지원_다중이용시설_일"]

    # --- 안전관리 ---
    out["안전관리_업무수행기간_일"] = (
        _extract_days_after("건설사업관리기술인으로서안전관리업무수행기간", compact)
        or _extract_days_after("안전관리업무수행기간", compact)
        or out["안전관리_업무수행기간_일"]
    )

    return out


def _is_footer_or_header_line(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return True
    if _is_annotation_or_footnote_line(s):
        return True
    if s.startswith("Page :") or s.startswith("성명 :"):
        return True
    if "건설사업관리" in s and ("2." in s or s.startswith("2")):
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
    ]
    if any(k in s for k in footer_keywords):
        return True
    # 표 헤더 라인(오탐 방지: 짧고 키워드가 여러 개 있을 때만)
    header_keywords = ["사업명", "참여기간", "발주자", "공사종류", "직무분야", "전문분야", "직위", "책임정도", "공사(용역)금액"]
    hit = sum(1 for k in header_keywords if k in s)
    if hit >= 2 and len(s) <= 80:
        return True
    return False


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


def _next_data_line_index(lines: List[str], start_idx: int) -> Optional[int]:
    j = start_idx
    while j < len(lines):
        if (lines[j] or "").strip():
            return j
        j += 1
    return None


def _looks_like_project_start_loose(lines: List[str], idx: int) -> bool:
    """
    건설사업관리/감리경력 표는 문서별로 헤더 분리가 달라 텍스트 폴백에서만 사용.
    - 현재 라인이 날짜/일수/~가 아니고
    - 다음 1~4줄 내에 날짜(YYYY.MM.DD)와 '~' 라인이 함께 나타나면 사업 블록 시작으로 간주
    """
    if idx < 0 or idx >= len(lines):
        return False
    s0 = (lines[idx] or "").strip()
    if not s0 or s0 == "┖→":
        return False
    if s0.startswith("~") or _DATE_RE.match(s0) or _DAYS_RE.match(s0):
        return False
    has_date = False
    has_tilde = False
    for j in range(idx + 1, min(len(lines), idx + 6)):
        sj = (lines[j] or "").strip()
        if _DATE_RE.match(sj):
            has_date = True
        if sj.lstrip().startswith("~"):
            has_tilde = True
    return has_date and has_tilde


def _extract_date_blocks_from_text(text: str) -> List[Dict[str, str]]:
    """
    텍스트에서 참여기간/인정일/참여일 블록을 순서대로 추출.
    (page_2_parser의 로직을 건설사업관리 섹션에도 그대로 적용)
    """
    if not text:
        return []
    raw_lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
    lines = [ln for ln in raw_lines if ln and not _is_footer_or_header_line(ln)]

    blocks: List[Dict[str, str]] = []
    i = 0
    while i < len(lines):
        if not _DATE_RE.match(lines[i]):
            i += 1
            continue
        start = _yyyy_mm_dd_to_iso(lines[i])

        # '~' 라인을 근접 구간에서 찾는다(표 구조 검증)
        tilde_idx = None
        # FIX: CM 섹션도 기술경력과 동일하게, start date 이후 줄바꿈/셀 분할로 인해
        #      '~' 라인이 더 멀리 떨어질 수 있다. (특정 페이지에서 누락 발생 케이스)
        for k in range(i + 1, min(len(lines), i + 12)):
            if (lines[k] or "").lstrip().startswith("~"):
                tilde_idx = k
                break
            if _looks_like_project_start_loose(lines, k):
                tilde_idx = None
                break
        if tilde_idx is None:
            # FIX: '~' 라인이 누락되거나 너무 멀리 있는 경우 폴백.
            #      start date 이후 근접 구간에서 종료일(또는 근무중) + (xx일)(xx일) 토큰 2개를 찾으면 블록으로 인정한다.
            end = ""
            cursor = i + 1
            end_idx = None
            for k in range(i + 1, min(len(lines), i + 13)):
                s = (lines[k] or "").strip()
                if _DATE_RE.match(s):
                    end_idx = k
                    end = _yyyy_mm_dd_to_iso(s)
                    cursor = k + 1
                    break
                compact = re.sub(r"\s+", "", s)
                if "근무중" in compact:
                    end_idx = k
                    end = "근무중"
                    cursor = k + 1
                    break
                if _looks_like_project_start_loose(lines, k):
                    end_idx = None
                    break

            window: List[str] = []
            j = cursor
            scan_end = min(len(lines), (end_idx + 1 if end_idx is not None else i + 1) + 30)
            while j < scan_end and len(window) < 60:
                if _looks_like_project_start_loose(lines, j):
                    break
                window.append(lines[j])
                if len(_extract_days_tokens_from_lines(window)) >= 2:
                    j += 1
                    break
                j += 1
            toks = _extract_days_tokens_from_lines(window)
            인정 = toks[0] if len(toks) >= 1 else ""
            참여 = toks[1] if len(toks) >= 2 else ""

            if not end and not (인정 and 참여):
                i += 1
                continue

            blocks.append(
                {
                    "참여기간_시작일": start,
                    "참여기간_종료일": end,
                    "인정일수": 인정,
                    "참여일수": 참여,
                }
            )
            i = j
            continue

        # 종료일
        end = ""
        after_tilde = _next_data_line_index(lines, tilde_idx + 1)
        cursor = tilde_idx + 1
        if after_tilde is not None:
            s_end = (lines[after_tilde] or "").strip()
            if _DATE_RE.match(s_end):
                end = _yyyy_mm_dd_to_iso(s_end)
                cursor = after_tilde + 1
            else:
                compact = re.sub(r"\s+", "", s_end)
                if "근무중" in compact:
                    end = "근무중"
                    cursor = after_tilde + 1
                else:
                    after2 = _next_data_line_index(lines, after_tilde + 1)
                    if after2 is not None and _DATE_RE.match((lines[after2] or "").strip()):
                        end = _yyyy_mm_dd_to_iso((lines[after2] or "").strip())
                        cursor = after2 + 1
                    else:
                        cursor = after_tilde

        # 인정/참여일: 다음 사업 시작 전까지 토큰 2개를 수집
        window: List[str] = []
        j = cursor
        max_lines = 60
        while j < len(lines) and len(window) < max_lines:
            if _looks_like_project_start_loose(lines, j):
                break
            cur = (lines[j] or "").strip()
            # FIX: 일수 수집 중 다음 날짜를 만나면 window가 다음 블록까지 먹는 것을 방지
            if j != cursor and _DATE_RE.match(cur):
                break
            window.append(lines[j])
            if len(_extract_days_tokens_from_lines(window)) >= 2:
                j += 1
                break
            j += 1

        toks = _extract_days_tokens_from_lines(window)
        인정 = toks[0] if len(toks) >= 1 else ""
        참여 = toks[1] if len(toks) >= 2 else ""

        blocks.append(
            {
                "참여기간_시작일": start,
                "참여기간_종료일": end,
                "인정일수": 인정,
                "참여일수": 참여,
            }
        )
        i = j
    return blocks


def _looks_like_position_token(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return False
    if t in _POSITION_TOKENS:
        return True
    if t != "이사" and t.endswith("이사") and len(t) <= 8:
        return True
    return False


def _is_probable_project_name_line(s: str) -> bool:
    t = re.sub(r"\s+", " ", (s or "")).strip()
    if not t or t == "┖→":
        return False
    if t.startswith("~") or _DATE_RE.match(t) or _DAYS_RE.match(t):
        return False
    if _is_footer_or_header_line(t):
        return False
    if _looks_like_position_token(t):
        return False
    # 발주자/기관 라인(대체로 공공기관 키워드 포함)은 사업명으로 보지 않는다.
    if _ISSUER_HINT.search(t) and len(t.split(" ")) >= 2:
        # "단양~..."처럼 기관 키워드가 들어가는 사업명은 흔치 않으므로 보수적으로 제외
        return False
    if len(t) <= 1:
        return False
    return True


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", "", (s or "")).strip()


def _extract_cm_projects_from_table(page: Any) -> List[Dict[str, Any]]:
    """
    건설사업관리 및 감리경력 표(대체로 3행 블록)를 테이블 기반으로 파싱.
    - 날짜/일수는 표에 누락되는 경우가 많아 여기서는 비워두고, 텍스트 블록과 병합한다.
    """

    def _cm_table_score(tbl: list) -> tuple[int, int, int]:
        if not tbl:
            return (-10_000, 0, 0)
        max_cols = max((len(r) for r in tbl if r), default=0)
        if max_cols <= 2:
            return (-10_000, max_cols, len(tbl))
        n_header = 0
        head_lines: list[str] = []
        for r in tbl[: min(14, len(tbl))]:
            j = " ".join([(c or "") for c in (r or []) if c]).strip()
            if j:
                head_lines.append(j)
            if any(k in j for k in ["사업명", "참여기간", "발주자", "직무분야", "전문분야", "직위", "공사(용역)개요"]):
                n_header += 1

        head_text = " ".join(head_lines)
        is_note = False
        if "※" in head_text:
            is_note = True
        if re.search(r"\b2\.\s*건설사업관리\s*및\s*감리경력\b", head_text):
            is_note = True
        if ("건설기술" in head_text) and ("시행령" in head_text) and ("제45조" in head_text):
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

    def _infer_explicit_vlines_from_header_words() -> list[float]:
        try:
            words = page.extract_words(use_text_flow=True) or []
        except Exception:
            try:
                words = page.extract_words() or []
            except Exception:
                words = []
        if not words:
            return [float(VIRTUAL_LEFT_X), float(VIRTUAL_RIGHT_X)]

        top_cut = 280.0
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
            if any(k in t for k in ["참여기간", "사업명", "발주자", "직무분야", "담당업무", "비고"]):
                cands.append((x0, x1, t))
        if len(cands) < 2:
            return [float(VIRTUAL_LEFT_X), float(VIRTUAL_RIGHT_X)]

        cands.sort(key=lambda x: x[0])
        vlines = [float(VIRTUAL_LEFT_X)]
        for (ax0, ax1, _), (bx0, _bx1, __) in zip(cands, cands[1:]):
            if bx0 - ax1 < 6:
                continue
            vlines.append(float((ax1 + bx0) / 2.0))
        vlines.append(float(VIRTUAL_RIGHT_X))
        vlines = sorted({round(x, 2) for x in vlines if 0 < x < float(getattr(page, "width", 10000) or 10000)})
        if len(vlines) < 3:
            return [float(VIRTUAL_LEFT_X), float(VIRTUAL_RIGHT_X)]
        return vlines

    inferred = _infer_explicit_vlines_from_header_words()
    settings = dict(LINE_TABLE_SETTINGS)
    settings["explicit_vertical_lines"] = inferred
    tables = safe_extract_tables(page, settings)
    if not tables:
        tables = extract_tables_merged(page)
    if not tables:
        return []
    t = pick_best_table(tables, _cm_table_score) or []
    if not t:
        return []

    def is_header_row(row: list) -> bool:
        joined = " ".join([(c or "") for c in row if c]).strip()
        return any(k in joined for k in ["사업명", "참여기간", "발주자", "공사(용역)금액", "책임정도", "직위", "직무분야", "전문분야"])

    def is_project_header_row(row: list) -> bool:
        if not row:
            return False
        c0 = (row[0] or "").strip() if len(row) > 0 else ""
        c1 = (row[1] or "").strip() if len(row) > 1 and row[1] is not None else ""
        c2 = (row[2] or "").strip() if len(row) > 2 else ""
        c3 = (row[3] or "").strip() if len(row) > 3 else ""
        if not c0 or not c2 or not c3:
            return False
        if is_header_row(row):
            return False
        # 프로젝트 헤더는 보통 col1이 비어 있음(None/빈 문자열)
        if c1:
            return False
        # 안내/라벨/날짜성 텍스트 배제
        if c0.startswith("~") or _DATE_RE.match(c0) or _DAYS_RE.match(c0):
            return False
        return True

    projects: List[Dict[str, Any]] = []
    i = 0
    while i < len(t):
        row = t[i] or []
        if not any((c or "").strip() for c in row) or is_header_row(row):
            i += 1
            continue

        if not is_project_header_row(row):
            i += 1
            continue

        out = _blank_cm_career_row()
        out["사업명"] = re.sub(r"\s+", " ", (row[0] or "").replace("\n", "")).strip()
        out["직무분야"] = (row[2] or "").strip()
        # 표 컬럼 4번째는 문서에 따라 '담당업무' 또는 '전문분야'로 쓰이는 경우가 있어,
        # 우선 '담당업무'에 넣고, 필요 시 후처리에서 조정 가능하게 한다.
        out["담당업무"] = (row[3] or "").strip()

        # 다음 행: 발주자/공사종류/전문분야/직위
        if i + 1 < len(t):
            r1 = t[i + 1] or []
            if r1 and not is_header_row(r1):
                out["발주자"] = (r1[0] or "").strip() if len(r1) > 0 else ""
                out["공사종류"] = (r1[1] or "").strip() if len(r1) > 1 else ""
                out["전문분야"] = (r1[2] or "").strip() if len(r1) > 2 else ""
                out["직위"] = (r1[3] or "").strip() if len(r1) > 3 else ""

        # 다음 행: 개요/책임/금액
        if i + 2 < len(t):
            r2 = t[i + 2] or []
            if r2 and not is_header_row(r2):
                out["공사(용역)개요"] = (r2[0] or "").strip() if len(r2) > 0 else ""
                out["책임정도"] = (r2[2] or "").strip() if len(r2) > 2 else ""
                out["공사(용역)금액(백만원)"] = (r2[3] or "").strip() if len(r2) > 3 else ""

        # 개요가 여러 행으로 찢어진 경우 보강(다음 프로젝트 헤더 전까지)
        scan_end = min(len(t), i + 10)
        for j in range(i + 3, scan_end):
            rj = t[j] or []
            if not rj or not any((c or "").strip() for c in rj):
                continue
            if is_header_row(rj):
                continue
            if is_project_header_row(rj):
                break
            c0 = (rj[0] or "").strip() if len(rj) > 0 else ""
            if c0:
                if out["공사(용역)개요"] and c0 not in out["공사(용역)개요"]:
                    out["공사(용역)개요"] = (out["공사(용역)개요"] + "\n" + c0).strip()
                elif not out["공사(용역)개요"]:
                    out["공사(용역)개요"] = c0

        projects.append(out)
        i += 3
    return projects


def _parse_recent_1y_service_stats(text: str) -> Dict[str, str]:
    """
    하단부: '건설사업관리 및 감리(최근 1년간) 용역 완성비율 :' 이후의
    - % 값
    - 참여건수(상주/기술지원)
    - 완료건수(상주/기술지원)
    를 최대한 복원.

    PDF 인코딩/추출 품질에 따라 라벨이 깨질 수 있어:
    1) %는 강하게 잡고
    2) '건' 숫자 토큰을 순서 기반으로 4개까지 매핑한다.
    """
    out = {
        "건설사업관리및감리_최근1년_용역완성비율(%)": "",
        "건설사업관리및감리_최근1년_참여건수_상주": "",
        "건설사업관리및감리_최근1년_참여건수_기술지원": "",
        "건설사업관리및감리_최근1년_완료건수_상주": "",
        "건설사업관리및감리_최근1년_완료건수_기술지원": "",
    }
    if not text:
        return out

    # 앵커 찾기: 공백/개행 무시
    compact = re.sub(r"\s+", "", text)
    anchor = "용역완성비율"
    pos = compact.find(anchor)
    if pos < 0:
        return out

    window = compact[pos : pos + 300]

    m = re.search(r"용역완성비율[:：]?(?P<pct>\d+(?:\.\d+)?)%", window)
    if m:
        out["건설사업관리및감리_최근1년_용역완성비율(%)"] = m.group("pct")

    # 건수 토큰 수집(상주/기술지원 라벨이 깨지는 케이스 대비)
    nums = [n for n in re.findall(r"(\d+)\s*건", window)]
    if len(nums) >= 4:
        out["건설사업관리및감리_최근1년_참여건수_상주"] = nums[0]
        out["건설사업관리및감리_최근1년_참여건수_기술지원"] = nums[1]
        out["건설사업관리및감리_최근1년_완료건수_상주"] = nums[2]
        out["건설사업관리및감리_최근1년_완료건수_기술지원"] = nums[3]
    elif len(nums) == 2:
        # 최소 정보만 있는 경우(총 참여/총 완료만 출력되는 양식)
        out["건설사업관리및감리_최근1년_참여건수_상주"] = nums[0]
        out["건설사업관리및감리_최근1년_완료건수_상주"] = nums[1]
    return out


def parse_page_3(ctx: DocumentContext, page_num: int) -> List[Dict[str, Any]]:
    """
    제3쪽 파싱: 건설사업관리 및 감리경력
    
    Args:
        ctx: DocumentContext
        page_num: 페이지 번호 (0부터 시작)
    
    Returns:
        List[Dict]: 건설사업관리 및 감리경력 리스트
    """
    careers = []
    page_num_1based = page_num + 1
    
    try:
        if page_num >= ctx.total_pages:
            print(f"⚠️ 페이지 번호 오류: {page_num + 1}페이지는 존재하지 않습니다.")
            return careers

        page = ctx.pages[page_num]
        text = ctx.get_text(page_num) or ""
            
        print(f"  - 건설사업관리 및 감리경력 파싱 중... (페이지 {page_num_1based})")

        # region agent log
        _agent_log(
            run_id="pre-fix",
            hypothesis_id="CM",
            location="page_3_parser.py:parse_page_3:entry",
            message="enter parse_page_3",
            data={"page_num_1based": page_num + 1, "text_len": len(text or "")},
        )
        # endregion agent log

        if not text.strip():
            return careers

        # 0) 원시 표(6열) 기반 파싱 1차 시도(사용자 요구: page3는 표 기반이 주 경로)
        try:
            table_rows = (
                _parse_cm_careers_from_raw_table(
                    page,
                    page_num_1based=page_num_1based,
                    pdf_path=str(getattr(ctx, "pdf_path", "") or ""),
                )
                or []
            )
        except Exception:
            table_rows = []
        if table_rows:
            for _r in (table_rows or []):
                if isinstance(_r, dict) and "_pdf_pages" not in _r:
                    _r["_pdf_pages"] = [page_num_1based]
            return table_rows

        # 0) 위치(템플릿) 기반 파싱 1차 시도
        # - 기술경력과 동일하게 word bbox로 열을 직접 분리해 '사업명 오염'을 줄인다.
        try:
            import os
            if os.environ.get("PDFPARSER_DISABLE_TEMPLATE"):
                raise RuntimeError("template disabled by env")
            from parsers.template_table_parser import (
                parse_cm_page_by_template,
                is_tech_template_result_trustworthy,
            )

            words = ctx.get_words(page_num, engine="auto") or []
            tpl_rows, tpl_meta = parse_cm_page_by_template(words)
            if is_tech_template_result_trustworthy(tpl_rows, tpl_meta):
                for _r in (tpl_rows or []):
                    if isinstance(_r, dict) and "_pdf_pages" not in _r:
                        _r["_pdf_pages"] = [page_num_1based]
                # FIX: CM 템플릿도 블록 누락 감지(페이지 내 '~' 라인 하한보다 작으면 폴백)
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
                    _agent_log(
                        run_id="pre-fix",
                        hypothesis_id="CM",
                        location="page_3_parser.py:parse_page_3:template_reject_tilde_floor",
                        message="rejecting CM template due to tilde-line floor",
                        data={
                            "page_num_1based": page_num + 1,
                            "n_tpl_rows": len(tpl_rows or []),
                            "n_tilde_lines_full": int(n_tilde_lines_full),
                            "tpl_meta": tpl_meta or {},
                        },
                    )
                    raise RuntimeError("cm_template_rows_below_tilde_floor")

                # 표 기반으로만 잘 잡히는 개요/금액 등은 안전 보강(사업명/직무/담당업무 override 금지)
                try:
                    table_projects = _extract_cm_projects_from_table(page) or []
                except Exception:
                    table_projects = []
                if table_projects and tpl_rows:
                    def _nk(s: str) -> str:
                        return re.sub(r"\\s+", "", (s or "")).strip()

                    tmap: dict[str, dict] = {}
                    for tr in table_projects:
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

                    FILL_KEYS = [
                        "공사(용역)개요",
                        "책임정도",
                        "직위",
                        "공사(용역)금액(백만원)",
                        "비고",
                        "전문분야",
                    ]
                    for r in tpl_rows:
                        nm = str(r.get("사업명") or "").strip()
                        if not nm:
                            continue
                        key = _nk(nm)
                        cand = tmap.get(key)
                        if cand is None:
                            for tk, tv in tmap.items():
                                if key.endswith(tk) or tk.endswith(key):
                                    cand = tv
                                    break
                        if cand is None:
                            continue
                        for k in FILL_KEYS:
                            if str(r.get(k) or "").strip():
                                continue
                            v = str((cand or {}).get(k) or "").strip()
                            if v:
                                r[k] = v

                _agent_log(
                    run_id="pre-fix",
                    hypothesis_id="CM",
                    location="page_3_parser.py:parse_page_3:template_return",
                    message="returning CM template rows",
                    data={
                        "page_num_1based": page_num + 1,
                        "n_tpl_rows": len(tpl_rows or []),
                        "tpl_meta": tpl_meta or {},
                    },
                )
                return tpl_rows
        except Exception:
            pass

        # 1) 표 기반 파싱(프로젝트 기본 필드)
        table_projects: List[Dict[str, Any]] = []
        try:
            table_projects = _extract_cm_projects_from_table(page)
        except Exception:
            table_projects = []

        # 2) 텍스트 기반 날짜/일수 블록 추출
        date_blocks: List[Dict[str, str]] = []
        word_lines: List[str] = []
        try:
            word_lines = ctx.get_word_lines(
                page_num, engine="auto", y_tolerance=2.0, join_gap=1.0
            )
            if word_lines:
                date_blocks = _extract_date_blocks_from_text("\n".join(word_lines))
            else:
                date_blocks = _extract_date_blocks_from_text(text)
        except Exception:
            date_blocks = []

        if table_projects:
            # 같은 페이지에서 표 행 수 = 텍스트 날짜 블록 수이면, 표가 사업명·직무 열을 더 정확히 갖고 있으므로 1:1로 합친다.
            if date_blocks and len(table_projects) == len(date_blocks):
                out_rows = []
                for i, tp in enumerate(table_projects):
                    r = _blank_cm_career_row()
                    r.update(tp)
                    r.update(date_blocks[i])
                    out_rows.append(r)
                for _r in (out_rows or []):
                    if isinstance(_r, dict) and "_pdf_pages" not in _r:
                        _r["_pdf_pages"] = [page_num_1based]
                return out_rows

            # date_blocks 기준으로 누락 없이 행 생성 후, 사업명은 시작일 **직전** 텍스트에서 역방향 수집(기술경력과 동일).
            if word_lines:
                lines = [
                    re.sub(r"[ \t]+", " ", ln).strip()
                    for ln in word_lines
                    if ln and not _is_footer_or_header_line(ln)
                ]
            else:
                raw_lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
                lines = [ln for ln in raw_lines if ln and not _is_footer_or_header_line(ln)]

            out_rows: List[Dict[str, Any]] = []
            if date_blocks:
                i = 0
                b = 0
                while i < len(lines) and b < len(date_blocks):
                    s = (lines[i] or "").strip()
                    if _DATE_RE.match(s):
                        iso = _yyyy_mm_dd_to_iso(s)
                        expected_start = str(date_blocks[b].get("참여기간_시작일") or "").strip()
                        if expected_start and iso and iso != expected_start:
                            i += 1
                            continue

                        base = _blank_cm_career_row()
                        base.update(date_blocks[b])
                        name, jd_from_name = _collect_flow_block_project_name_backward(lines, i)
                        if name:
                            base["사업명"] = name
                        if jd_from_name:
                            base["직무분야"] = jd_from_name[0]
                            base["담당업무"] = jd_from_name[1]
                        out_rows.append(base)

                        b += 1
                        j = i + 1
                        scan_end = min(len(lines), i + 90)
                        while j < scan_end:
                            sj = (lines[j] or "").strip()
                            if b < len(date_blocks) and _DATE_RE.match(sj):
                                iso_j = _yyyy_mm_dd_to_iso(sj)
                                next_start = str(date_blocks[b].get("참여기간_시작일") or "").strip()
                                if iso_j and next_start and iso_j == next_start:
                                    break
                            j += 1
                        i = j
                        continue
                    i += 1

                while b < len(date_blocks):
                    r = _blank_cm_career_row()
                    r.update(date_blocks[b])
                    out_rows.append(r)
                    b += 1
            else:
                out_rows = table_projects[:] if table_projects else []

            out_rows = _enrich_from_table_by_project_name(out_rows, table_projects)
            return out_rows

        if date_blocks:
            if word_lines:
                lines = [
                    re.sub(r"[ \t]+", " ", ln).strip()
                    for ln in word_lines
                    if ln and not _is_footer_or_header_line(ln)
                ]
            else:
                raw_lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
                lines = [ln for ln in raw_lines if ln and not _is_footer_or_header_line(ln)]
            out_rows: List[Dict[str, Any]] = []
            i = 0
            b = 0
            while i < len(lines) and b < len(date_blocks):
                s = (lines[i] or "").strip()
                if _DATE_RE.match(s):
                    iso = _yyyy_mm_dd_to_iso(s)
                    expected_start = str(date_blocks[b].get("참여기간_시작일") or "").strip()
                    if expected_start and iso and iso != expected_start:
                        i += 1
                        continue
                    base = _blank_cm_career_row()
                    base.update(date_blocks[b])
                    name, jd_from_name = _collect_flow_block_project_name_backward(lines, i)
                    if name:
                        base["사업명"] = name
                    if jd_from_name:
                        base["직무분야"] = jd_from_name[0]
                        base["담당업무"] = jd_from_name[1]
                    out_rows.append(base)
                    b += 1
                    j = i + 1
                    scan_end = min(len(lines), i + 90)
                    while j < scan_end:
                        sj = (lines[j] or "").strip()
                        if b < len(date_blocks) and _DATE_RE.match(sj):
                            iso_j = _yyyy_mm_dd_to_iso(sj)
                            next_start = str(date_blocks[b].get("참여기간_시작일") or "").strip()
                            if iso_j and next_start and iso_j == next_start:
                                break
                        j += 1
                    i = j
                    continue
                i += 1
            while b < len(date_blocks):
                r = _blank_cm_career_row()
                r.update(date_blocks[b])
                out_rows.append(r)
                b += 1
            for _r in (out_rows or []):
                if isinstance(_r, dict) and "_pdf_pages" not in _r:
                    _r["_pdf_pages"] = [page_num_1based]
            return out_rows

    except Exception as e:
        print(f"❌ 제3쪽 파싱 오류: {e}")
    
    for _r in (careers or []):
        if isinstance(_r, dict) and "_pdf_pages" not in _r:
            _r["_pdf_pages"] = [page_num_1based]
    return careers

