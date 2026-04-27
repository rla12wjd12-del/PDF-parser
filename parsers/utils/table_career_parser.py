#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
page2/page3 경력 표(원시 테이블)를 6열/4행 레코드로 파싱하기 위한 공통 유틸.

전제:
- pdfplumber의 table은 list[list[cell]] 형태
- 각 경력은 4행 블록으로 반복된다.
- 표 인식 오류로 "빈 컬럼이 끼어" 10열처럼 보이는 경우가 있어 6열로 정규화한다.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable, Iterator, Sequence


_DATE_FULL = re.compile(r"^\s*(\d{4})\.(\d{2})\.(\d{2})\s*$")
# NOTE:
# 참여기간 셀은 'YYYY.MM.DD ~ YYYY.MM.DD'처럼 한 줄로 추출되거나,
# 줄바꿈으로 'YYYY.MM.DD\n~\nYYYY.MM.DD'처럼 분해되어 들어올 수 있다.
# 전자는 셀 전체가 날짜 문자열이 아니므로 _DATE_FULL로는 탐지할 수 없다.
# 따라서 "문자열 어디서든" 날짜 토큰을 찾기 위한 패턴을 별도로 둔다.
_DATE_TOKEN = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})")
_DAYS_PAREN = re.compile(r"\(\s*(\d[\d,]*)\s*일\s*\)")


def _cell_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).replace("\r\n", "\n").replace("\r", "\n").strip()


def _row_is_empty(row: Sequence[Any] | None) -> bool:
    if not row:
        return True
    for c in row:
        if _cell_str(c):
            return False
    return True


def normalize_table_to_6cols(table: list[list[Any]], *, scan_rows: int = 50) -> list[list[str]]:
    """
    테이블에서 "모든 행에서 빈 컬럼"을 제거하여 6열로 정규화한다.
    - 10열처럼 보이지만 중간 빈 컬럼이 끼어 있는 케이스를 6열로 복원
    - 여전히 6열 초과이면, 빈도가 낮은 컬럼부터 제거(순서 유지)한다.
    """
    if not table:
        return []

    # 문자열화 + 행 길이 정규화
    rows = [[_cell_str(c) for c in (r or [])] for r in table]
    max_cols = max((len(r) for r in rows), default=0)
    if max_cols == 0:
        return []
    for r in rows:
        if len(r) < max_cols:
            r.extend([""] * (max_cols - len(r)))

    # 컬럼별 non-empty count (상단 일부만 스캔: 성능 + 헤더/데이터 구간 중심)
    lim = min(len(rows), max(1, scan_rows))
    counts = [0] * max_cols
    for i in range(lim):
        r = rows[i]
        for j in range(max_cols):
            if r[j].strip():
                counts[j] += 1

    # 1) 완전 빈 컬럼 제거
    keep = [j for j, c in enumerate(counts) if c > 0]
    if not keep:
        return []
    rows2 = [[r[j] for j in keep] for r in rows]

    # 2) 6열보다 많으면, 최소 빈도 컬럼부터 제거(단, 순서 유지)
    if len(keep) > 6:
        # 기존 방식(빈도 낮은 컬럼부터 드롭)은, "의미 있는 컬럼이지만 값이 드문 컬럼"을 떨어뜨려
        # 이후 고정 인덱스 매핑(사업명/발주자 등)이 1칸씩 밀리는 치명적인 오염을 유발할 수 있다.
        # 따라서 헤더 키워드가 감지되는 컬럼은 우선 보존한다.

        # 필수: 첫 컬럼(참여기간)과 마지막 컬럼(비고 블록)은 유지
        first_col = keep[0]
        last_col = keep[-1]
        mandatory = {first_col, last_col}

        # 헤더/라벨 키워드(상단 스캔 구간에서만 탐지해 비용 제한)
        header_keywords = (
            "참여기간",
            "사업명",
            "발주자",
            "공사종류",
            "직무분야",
            "전문분야",
            "담당업무",
            "책임정도",
            "직위",
            "공사(용역)개요",
            "공사(용역)금액",
            "적용 공법",
            "적용 신기술",
            "적용 융",
            "시설물 종류",
            "비고",
        )

        def _cell_has_header_kw(txt: str) -> bool:
            t = (txt or "").replace(" ", "").strip()
            if not t:
                return False
            return any(k.replace(" ", "") in t for k in header_keywords)

        lim2 = min(len(rows), max(1, min(scan_rows, 60)))
        must_keep: set[int] = set()
        # 상단에서 키워드가 한 번이라도 등장한 컬럼은 보존 후보
        for i in range(min(lim2, 24)):  # 헤더는 대개 상단 20여행 안에 존재
            r = rows[i]
            for j in keep:
                if _cell_has_header_kw(r[j]):
                    must_keep.add(j)

        # 빈도 기반 드롭 후보: mandatory/must_keep 제외
        kept_counts = [(j, counts[j]) for j in keep]
        candidates = [kc for kc in kept_counts if kc[0] not in (mandatory | must_keep)]
        candidates.sort(key=lambda x: (x[1], x[0]))  # count asc

        to_drop: set[int] = set()
        # 드롭하면서도 mandatory/must_keep는 절대 드롭하지 않는다.
        while (len(keep) - len(to_drop)) > 6 and candidates:
            col, _ = candidates.pop(0)
            to_drop.add(col)

        # 그래도 6열을 초과하면(키워드 보존 컬럼이 너무 많음),
        # 마지막 수단으로 must_keep 중에서도 빈도 낮은 것부터 드롭하되, mandatory는 유지한다.
        if (len(keep) - len(to_drop)) > 6:
            mk = [(j, counts[j]) for j in keep if (j in must_keep) and (j not in mandatory)]
            mk.sort(key=lambda x: (x[1], x[0]))  # count asc
            while (len(keep) - len(to_drop)) > 6 and mk:
                col, _ = mk.pop(0)
                to_drop.add(col)

        keep_final = [j for j in keep if j not in to_drop]
        rows2 = [[rows[i][j] for j in keep_final] for i in range(len(rows))]

    # 3) 정확히 6열이 아니면(예: 5열), 그대로 반환(상위에서 폴백/에러 처리)
    return rows2


def find_header_start_row(table6: list[list[str]], *, max_scan: int = 18) -> int | None:
    """
    헤더 블록의 시작 행을 찾는다.
    1) 명시적 키워드(사업명/비고/참여기간) 탐지
    2) 폴백: 날짜로 시작하지 않고 non-empty 3개 이상인 첫 행
    """
    if not table6:
        return None
    lim = min(len(table6), max_scan)
    for i in range(lim):
        t = " ".join(c for c in table6[i] if c).replace(" ", "")
        if ("사업명" in t) and ("비고" in t or "참여기간" in t):
            return i
        if ("참여기간" in t) and ("사업명" in t):
            return i
    # fallback heuristic
    for i in range(1, lim):
        r = table6[i]
        non_empty = sum(1 for c in r if (c or "").strip())
        if non_empty < 3:
            continue
        c0 = (r[0] or "").strip()
        if c0 and _DATE_FULL.match(c0):
            continue
        return i
    return None


@dataclass
class PeriodParse:
    start_iso: str
    end_iso: str
    인정일수: str
    참여일수: str
    has_continue_arrow: bool


def parse_period_cell(cell_text: str, *, yyyy_mm_dd_to_iso) -> PeriodParse:
    """
    참여기간 셀 텍스트에서 시작/종료/일수를 분리한다.
    예:
      1996.03.14\n~\n1996.04.23\n(31일)\n(41일)
    """
    raw = (cell_text or "").strip()
    # ┖→ 처리 원칙:
    # - 참여기간 셀에 '┖→'만 단독으로 존재하는 경우: 이전 경력에 이어붙임(continuation) 마커로 본다.
    # - 날짜 토큰(YYYY.MM.DD)이 함께 존재하는 경우: 표 추출 과정에서 '┖→' 단독 행이
    #   다음 셀과 합쳐진 아티팩트일 수 있으므로 continuation으로 보지 않는다.
    has_arrow = ("┖→" in raw) and (_DATE_TOKEN.search(raw) is None)
    # remove arrow marker for parsing
    s = raw.replace("┖→", " ").strip()

    # 날짜 토큰 추출:
    # - 셀 전체가 날짜인 케이스(라인 단위 분해)뿐 아니라
    # - 'YYYY.MM.DD ~ YYYY.MM.DD'처럼 한 줄에 붙는 케이스도 처리해야 한다.
    # 우선 토큰 패턴으로 전부 찾고, 없으면 라인 단위로 _DATE_FULL을 재시도한다.
    dates = [m.group(0).strip() for m in _DATE_TOKEN.finditer(s)]
    if not dates:
        # 폴백: 줄 단위가 정확히 날짜로 분리돼 있는 케이스(공백/개행만 있는 경우)
        dates = [m.group(0).strip() for m in _DATE_FULL.finditer(s)]

    start_iso = yyyy_mm_dd_to_iso(dates[0]) if len(dates) >= 1 else ""
    s_compact = s.replace(" ", "")
    if len(dates) >= 2:
        end_iso = yyyy_mm_dd_to_iso(dates[1])
    elif "근무중" in s_compact or (len(dates) == 1 and "근무" in s and "중" in s):
        end_iso = "근무중"
    else:
        end_iso = ""

    days = [m.group(1).replace(",", "").strip() for m in _DAYS_PAREN.finditer(s)]
    인정 = days[0] if len(days) >= 1 else ""
    참여 = days[1] if len(days) >= 2 else ""
    return PeriodParse(start_iso=start_iso, end_iso=end_iso, 인정일수=인정, 참여일수=참여, has_continue_arrow=has_arrow)


def iter_records_4rows(table6: list[list[str]], *, header_start: int) -> Iterator[list[list[str]]]:
    """
    헤더 시작 행(header_start) 이후의 데이터 영역을 4행 블록으로 반환한다.
    - 헤더 블록은 4행으로 간주하고 skip
    - 완전 공백 행은 제거
    - 남은 행을 4개씩 묶는다.
    """
    if not table6:
        return
    data = table6[header_start + 4 :]
    data = [r for r in data if not _row_is_empty(r)]
    # 4의 배수가 아니면 tail은 버린다(상위에서 오류 수집 가능)
    n = (len(data) // 4) * 4
    for i in range(0, n, 4):
        yield data[i : i + 4]


def merge_into_previous(prev: dict[str, Any], cur: dict[str, Any], *, keys: Sequence[str]) -> None:
    """
    cur의 값이 있으면 prev에 이어붙인다.
    """
    for k in keys:
        v = str(cur.get(k) or "").strip()
        if not v:
            continue
        # leading arrow tokens 제거(사업명/비고 등에서 나오는 케이스)
        v = re.sub(r"^\s*┖→\s*", "", v).strip()
        if not v:
            continue
        old = str(prev.get(k) or "")
        if not old.strip():
            prev[k] = v
        else:
            prev[k] = (old + v).strip()

