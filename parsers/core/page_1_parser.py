#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
제1쪽 파서: 인적사항, 등급, 국가기술자격, 학력, 교육훈련, 상훈, 벌점 및 제재사항, 근무처

표 기반(우선): 각 스캔 페이지에 대해 pdfplumber `explicit_vertical_lines` (X=27, X=567)로
표를 추출한 뒤 행을 섹션별로 분류하고, `parse_page_1_from_text` 결과와 병합한다.

수동 점검(가정: 로컬에 테스트 PDF가 있음):
  - `external/PDF-parser`에서 `python main.py <PDF경로>` (프로젝트 CLI에 맞게 인자 조정)
  - 출력 JSON에서 인적사항/등급/자격/학력/교육훈련/근무처 필드가 누락 없이 채워졌는지 확인
  - CI/샌드박스에 샘플 PDF가 없으면 자동 회귀 테스트는 생략될 수 있음(TODO)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Any, Dict, List, Optional, Tuple
import csv
import re
from datetime import datetime

from parsers.table_settings import (
    LINE_TABLE_SETTINGS,
    extract_tables_merged,
    pick_best_table,
    safe_extract_tables,
)
from parsers.utils.logger import agent_debug_log as _page1_agent_log
from parsers.section_parsers import (
    parse_award_info,
    AWARD_NOT_APPLICABLE_TEMPLATE,
    parse_penalty_and_sanction_info,
    parse_grade_info,
    parse_workplace_info,
    parse_license_info,
    license_registration_quality_key,
    is_standalone_license_grade_label,
)
from parsers.document_context import DocumentContext
from parsers.utils.company_change_markers import get_company_change_markers


_NATIONAL_TECH_LICENSE_NAMES: set[str] | None = None


def _load_national_tech_license_names() -> set[str]:
    """
    프로젝트 내 `data/국가기술자격.csv`(종목명 목록)를 로드한다.
    - 파일이 없거나 로드 실패 시 빈 set 반환(파서 동작은 유지)
    """
    global _NATIONAL_TECH_LICENSE_NAMES
    if _NATIONAL_TECH_LICENSE_NAMES is not None:
        return _NATIONAL_TECH_LICENSE_NAMES
    names: set[str] = set()
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(base_dir, "data", "국가기술자격.csv")
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                # 헤더가 '종목명'으로 고정되어 있다고 가정하되, 없으면 첫 컬럼을 사용
                field = "종목명" if "종목명" in (reader.fieldnames or []) else ((reader.fieldnames or [None])[0])
                for row in reader:
                    if not row:
                        continue
                    v = (row.get(field) if field else None) or ""
                    v = str(v).strip()
                    if v:
                        names.add(v)
    except Exception:
        names = set()
    _NATIONAL_TECH_LICENSE_NAMES = names
    return names

def _squash_spaces_inside_parentheses(s: str) -> str:
    """
    괄호 내부에서만 공백/탭/줄바꿈을 제거해 '(원격 교육)' -> '(원격교육)'처럼 복원한다.
    """
    if not s:
        return ""
    out = []
    depth = 0
    for ch in s:
        if ch == "(":
            depth += 1
            out.append(ch)
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            out.append(ch)
            continue
        if depth > 0 and ch.isspace():
            continue
        out.append(ch)
    return "".join(out)


def _extract_training_rows_from_text(combined_text: str) -> list[str]:
    """
    교육훈련 표의 '한 행'을 복원한다.
    - 행 시작은 'YYYY.MM.DD ~ YYYY.MM.DD'가 같은 줄에 있는 케이스로 가정
    - 과정명이 2줄(또는 페이지 상단)로 찢어진 경우, 다음 행이 시작되기 전까지 이어붙인다.
    - 근무처처럼 'YYYY.MM.DD ~' 만 단독으로 오는 형식이 나오면 교육훈련 섹션이 끝났다고 보고 중단한다.
    """
    if not combined_text:
        return []

    lines = [(ln or "").strip() for ln in combined_text.splitlines()]
    lines = [ln for ln in lines if ln]

    row_start_pat = re.compile(r"^\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}\.\d{2}\.\d{2}\b")
    workplace_like_pat = re.compile(r"^\d{4}\.\d{2}\.\d{2}\s*~\s*$")  # 근무처에서 자주 발생

    rows: list[str] = []
    buf: list[str] = []
    in_table = False

    embedded_row_pat = re.compile(r"\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}\.\d{2}\.\d{2}\b")
    award_line_pat = re.compile(r"^\d{4}\.\d{2}\.\d{2}\b")  # 상훈 데이터는 보통 단일 날짜로 시작

    def _split_embedded_rows(line: str) -> list[str]:
        """
        pdf 텍스트 추출이 한 줄에 여러 행을 붙여놓는 경우가 있어,
        'YYYY.MM.DD ~ YYYY.MM.DD' 패턴을 기준으로 행 단위로 분리한다.
        """
        s = (line or "").strip()
        if not s:
            return []
        hits = list(embedded_row_pat.finditer(s))
        if len(hits) == 1:
            # 라인 앞에 '교육)' 같은 잔여 조각이 붙는 경우가 있어, 행 패턴이 라인 중간에서 시작하면 앞부분은 버린다.
            h = hits[0]
            return [s[h.start():].strip()] if h.start() > 0 else [s]
        if len(hits) == 0:
            return [s]
        parts: list[str] = []
        for idx, h in enumerate(hits):
            start = h.start()
            end = hits[idx + 1].start() if idx + 1 < len(hits) else len(s)
            parts.append(s[start:end].strip())
        return [p for p in parts if p]

    stop_all = False
    for ln0 in lines:
        for ln in _split_embedded_rows(ln0):
            # 페이지마다 헤더가 반복되므로, 헤더를 만나면 그 이후부터 다시 테이블 파싱을 시작한다.
            if "교육기간" in ln and "과정명" in ln and "교육기관명" in ln:
                # 진행 중이던 행이 있으면 flush
                if buf:
                    rows.append(_join_training_row_buf(buf))
                    buf = []
                in_table = True
                continue

            # 섹션 종료(기술경력 시작)면 전체 종료
            if "1. 기술경력" in ln:
                if buf:
                    rows.append(_join_training_row_buf(buf))
                    buf = []
                stop_all = True
                break

            if not in_table:
                continue

            # 교육훈련 섹션 내에서 'YYYY.MM.DD'로 시작하지만 '~'가 없는 라인은
            # 상훈 데이터가 바로 이어진 케이스가 많다(원본 PDF 재현).
            # 이 경우 현재 교육훈련 행 버퍼를 flush하고, 교육훈련 표 파싱을 종료한다.
            if award_line_pat.match(ln) and ("~" not in ln):
                if buf:
                    rows.append(_join_training_row_buf(buf))
                    buf = []
                in_table = False
                continue

            # 근무처 영역으로 넘어가면, 이번 페이지의 교육훈련 표는 끝난 것으로 보고 대기 상태로 전환.
            if workplace_like_pat.match(ln) or "근무기간" in ln or ln == "근무처":
                if buf:
                    rows.append(_join_training_row_buf(buf))
                    buf = []
                in_table = False
                continue

            if row_start_pat.match(ln):
                if buf:
                    rows.append(_join_training_row_buf(buf))
                    buf = []
                buf.append(ln)
                continue

            if buf:
                # 페이지 헤더/설명 문구는 continuation에서 제외(과정명 오염 방지)
                skip_snippets = [
                    "본 증명서는",
                    "문서확인번호",
                    "발급번호",
                    "관리번호",
                    "■ 건설기술",
                    "Page :",
                    "의무교육",
                    "계속교육",
                    "홈페이지",
                    "위·변조",
                ]
                if any(snip in ln for snip in skip_snippets):
                    continue
                buf.append(ln)

        if stop_all:
            break

    if buf:
        rows.append(_join_training_row_buf(buf))
    return rows


def _is_hangul_char(ch: str) -> bool:
    if not ch:
        return False
    code = ord(ch)
    return 0xAC00 <= code <= 0xD7A3


def _smart_concat(a: str, b: str) -> str:
    a = (a or "").strip()
    b = (b or "").strip()
    if not a:
        return b
    if not b:
        return a
    if _is_hangul_char(a[-1]) and _is_hangul_char(b[0]) and not a.endswith((")", " ")):
        return a + b
    return a + " " + b


def _join_training_row_buf(buf: list[str]) -> str:
    # [수정] 동일 표 행이 PDF 줄바꿈으로 쪼개질 때 " ".join을 쓰면 셀 내부 한글이 단어 한가운데에서
    # 끊긴 것처럼 보이는 불필요 공백이 생긴다. 줄 단위 trim 후 경계마다 _smart_concat으로 이어붙인다.
    if not buf:
        return ""
    acc = (buf[0] or "").strip()
    for part in buf[1:]:
        acc = _smart_concat(acc, (part or "").strip())
    return acc.strip()


def _parse_training_row(row_text: str) -> dict | None:
    """
    복원된 교육훈련 한 행을 dict로 변환.
    포맷(대략):
      YYYY.MM.DD ~ YYYY.MM.DD <과정명...> <교육기관명> [교육인정여부]
    """
    if not row_text:
        return None

    # 공백 정리(괄호 내부 공백 제거는 course 확정 후에만 적용)
    row = re.sub(r"[ \t]+", " ", row_text).strip()
    row = row.replace("** 해당없음 **", "").strip()

    m = re.match(r"^(?P<s>\d{4}\.\d{2}\.\d{2})\s*~\s*(?P<e>\d{4}\.\d{2}\.\d{2})\s+(?P<body>.+)$", row)
    if not m:
        return None

    start_raw = m.group("s")
    end_raw = m.group("e")
    body_raw = (m.group("body") or "").strip()
    body_raw = re.sub(r"\s+", " ", body_raw).strip()
    if not body_raw:
        return None

    # 상훈 힌트가 섞이면 교육훈련 행이 오염된 것으로 보고, 힌트 전까지만 잘라 최소 침습 보정
    # (섹션 경계 감지 실패/레이아웃 변형 대비)
    award_hints = ["표창장", "훈장", "포장", "감사장", "[제", "제"]
    for h in award_hints:
        if h in body_raw:
            body_raw = body_raw.split(h, 1)[0].strip()
            break
    if not body_raw:
        return None

    recognition_keywords = ["건설사업관리", "설계·시공", "품질관리"]
    org_suffixes = (
        "교육원",
        "협회",
        "공단",
        "공사",
        "연구원",
        "센터",
        "연수원",
        "재단",
        "학회",
        "진흥원",
        "대학교",
        "대학",
        "전문학교",
        "캠퍼스",
    )

    tokens = [t for t in body_raw.split(" ") if t]
    if not tokens:
        return None

    # recognition은 보통 기관명 뒤에 위치하지만, 레이아웃 깨짐으로 중간에 끼는 경우도 있어 "마지막 등장"을 사용
    recognition = ""
    rec_idx = None
    for i in range(len(tokens) - 1, -1, -1):
        if tokens[i] in recognition_keywords:
            recognition = tokens[i]
            rec_idx = i
            break

    tokens_wo_rec = tokens if rec_idx is None else (tokens[:rec_idx] + tokens[rec_idx + 1 :])

    # 기관명은 suffix로 탐색(끝에서부터)
    org_end_idx = None
    for i in range(len(tokens_wo_rec) - 1, -1, -1):
        if tokens_wo_rec[i].endswith(org_suffixes):
            org_end_idx = i
            break
    if org_end_idx is None:
        return None

    org_tok = tokens_wo_rec[org_end_idx]
    prev_tok = tokens_wo_rec[org_end_idx - 1] if org_end_idx - 1 >= 0 else ""

    # '(재)'가 분리돼 있거나 '('가 드롭된 경우 보정
    if org_tok.startswith("재)") and not org_tok.startswith("(재)"):
        org_tok = "(" + org_tok
    if prev_tok in ("(재)", "( 재 )", "(재"):
        if not org_tok.startswith("(재)"):
            org_tok = "(재)" + org_tok.lstrip()
        # course 토큰에서 '(재)'는 제거되도록 org 토큰에 흡수
        course_tokens = tokens_wo_rec[: org_end_idx - 1]
    else:
        course_tokens = tokens_wo_rec[:org_end_idx]

    org = re.sub(r"\s+", "", org_tok).replace("( 재 )", "(재)")

    # 기관명 뒤에 붙은 조각(페이지 잔여/줄바꿈 과정명)을 과정명으로 흡수
    trailing_tokens = tokens_wo_rec[org_end_idx + 1 :]
    course = " ".join(course_tokens).strip()
    if trailing_tokens:
        course = _smart_concat(course, " ".join(trailing_tokens).strip())

    # recognition은 tokens 탐색 결과를 그대로 사용한다.

    # '(원격'처럼 괄호가 반쯤만 남는 케이스(페이지 상단/하단) 보정
    if "(" in course and course.count("(") > course.count(")"):
        # 특히 '(원격' 패턴은 거의 항상 '(원격교육)' 의미
        if course.endswith("(원격"):
            course = course + "교육)"
        else:
            # 일반 케이스: 열린 괄호 이후 공백/줄바꿈만 누락된 수준이면 닫는 괄호를 붙여본다(최소 침습)
            course = course + ")"

    # 과정명 확정 후 괄호 내부 공백 제거
    course = _squash_spaces_inside_parentheses(course)

    # 다른 섹션 헤더/꼬리 텍스트가 섞이면 그 앞까지만 유지
    stop_tokens = [
        "수여일",
        "수여기관",
        "종류 및 근거",
        "종류및근거",
        "상훈",
        "벌점",
        "제재일",
        "제재기간",
        "제재기관",
        "제재사항",
        "근무기간",
        "상호",
    ]
    for tok in stop_tokens:
        if tok in course:
            course = course.split(tok, 1)[0].strip()

    # '(원격'만 남는 케이스 보정(페이지/줄바꿈으로 '교육)'가 누락됨)
    if course.endswith("(원격"):
        course = course + "교육)"
    if course.endswith("(원격교육"):
        course = course + ")"
    if not course:
        return None

    # 과정명 끝에 이상한 잔여 조각이 붙는 케이스 정리(페이지 헤더 잔여 등)
    course = course.replace("교육훈련", "").strip()
    course = re.sub(r"\s+", " ", course).strip()

    if len(course) > 200:
        return None

    return {
        "교육기간_시작": _yyyy_mm_dd_to_iso(start_raw),
        "교육기간_종료": _yyyy_mm_dd_to_iso(end_raw),
        "과정명": course,
        "교육기관명": org,
        "교육인정여부": recognition,
    }

def _workplace_body_lines_from_text(raw: str) -> list[str]:
    """근무처 섹션의 모든 페이지 본문 라인을 모은다.

    배경: 건설기술인 경력증명서는 근무처 섹션이 2페이지 이상으로 넘어갈 수 있고,
    각 페이지 상단에 다음 형태가 반복된다.

        근무처
        근무기간   상호   근무기간   상호    <- 컬럼 헤더
        ... 데이터 행 ...
        본 증명서는 인터넷으로 ...           <- 페이지 푸터(2줄)
        문서하단의 바코드로 ...

    그리고 다음 페이지 상단에는 인적사항/등급/교육훈련 등의 헤더가 다시 등장한 뒤,
    '근무기간/상호' 컬럼 헤더가 다시 나오고 그 아래에 추가 데이터 행이 이어진다.

    구현:
    - "in_block" 플래그를 사용해, '근무기간'+'상호' 라인을 만나면 블록 진입(헤더는 스킵).
    - 블록 안에서 데이터 라인을 본문에 누적.
    - '본 증명서는...' 또는 'kocea.or.kr' 라인을 만나면 블록 종료(다음 페이지 헤더까지 무시).
    - 단독 '근무처' 라벨/페이지 헤더 잡음(인적사항·등급 등)은 in_block=False 상태이므로 자연 스킵.
    - 진짜 종료 앵커는 '1. 기술경력' 한 가지뿐.
    """
    lines = [(ln or "").strip() for ln in (raw or "").splitlines()]
    body: list[str] = []
    in_block = False

    for ln in lines:
        if "1. 기술경력" in ln:
            break
        if not ln:
            continue
        # 페이지마다 등장하는 컬럼 헤더 → 블록 진입
        if ("근무기간" in ln) and ("상호" in ln):
            in_block = True
            continue
        if not in_block:
            continue
        # 페이지 푸터에서 블록 종료(다음 페이지 헤더까지 본문 수집 일시 중단)
        if ln.startswith("본 증명서는") or ("kocea.or.kr" in ln):
            in_block = False
            continue
        # 페이지 상단에서 '근무처' 섹션 라벨이 단독 라인으로 다시 들어오는 케이스 보강.
        # (블록 진입 직전·직후에 끼는 잡음)
        if ln.strip() == "근무처":
            continue
        body.append(re.sub(r"\s+", " ", ln).strip())
    return body


def _normalize_company_markers(s: str) -> str:
    """
    회사명 문자열 내 '現:' 표기를 추출 편차(전각 콜론/공백/개행)까지 흡수해 표준화한다.
    """
    s = (s or "")
    # 전각 콜론 → 반각 콜론
    s = s.replace("：", ":")
    # 개행/다중 공백 정리
    s = re.sub(r"\s+", " ", s)
    # 변경 사유 마커 표준화: "키워드 : " → "키워드:"
    # NOTE: 근무처 상호명 변경 표식은 문서/발급기관/추출기에 따라 다양하게 등장할 수 있어
    #       여기서는 보수적으로 "키워드 + 콜론" 형태만 정규화한다.
    for mk in get_company_change_markers():
        s = re.sub(rf"{re.escape(mk)}\s*:\s*", f"{mk}:", s)
    return s.strip()


def _normalize_company(s: str) -> tuple[str, str]:
    s = _normalize_company_markers(s)
    prev = s
    curr = ""
    # 변경 사유 마커(現/흡수합병/분할설립/상호변경/법인전환/합병/양수도 등) 이후를 "현재 상호"로 취급
    for mk in [m + ":" for m in get_company_change_markers()]:
        if mk in s:
            left, right = s.split(mk, 1)
            prev = (left or "").strip()
            curr = (right or "").strip()
            break
    else:
        # pdfplumber가 '現' 문자를 드롭한 경우: "구상호 :신상호" 패턴 감지
        if ":" in s:
            left, right = s.split(":", 1)
            left = left.strip()
            right = right.strip()
            if left and right and (
                "(주)" in right
                or re.search(r"[가-힣A-Za-z0-9]", right)
            ):
                prev = left
                curr = right

    # 추가 규칙: "YYYY.MM.DD :(주)..." 형태의 상호변경 표기 분리
    # 예) "(주)A 2016.07.24 :(주)B" → 이전=(주)A, 현재=(주)B
    def _split_date_colon(blob: str) -> tuple[str, str] | None:
        m = re.search(r"\b(\d{4}\.\d{2}\.\d{2})\s*[:：]\s*(.+)$", blob or "")
        if not m:
            return None
        right = (m.group(2) or "").strip()
        left = ((blob[: m.start()] or "")).strip()
        left = re.sub(r"\b\d{4}\.\d{2}\.\d{2}\b", " ", left)
        left = re.sub(r"\s+", " ", left).strip()
        if left and right and re.search(r"[가-힣A-Za-z0-9]", right):
            return left, right
        return None

    if prev and (not curr):
        split = _split_date_colon(prev)
        if split:
            prev, curr = split
    elif curr:
        # 일부 케이스에서는 이전/현재 분리가 실패해 '현재_상호명'에 구상호+날짜+콜론이 그대로 섞인다.
        split = _split_date_colon(curr)
        if split:
            left, right = split
            if not prev:
                prev = left
            curr = right

    # 일부 추출 경로에서는 '현/現' 폴백(콜론)과 실제 변경 마커가 중첩되어
    # curr 값이 "흡수합병:(주)..."처럼 마커를 포함한 채로 남는다.
    # 이 경우 마커 접두어를 제거해 현재 상호만 남기고, 이전 상호(prev)는 유지한다.
    if curr:
        for mk in get_company_change_markers():
            mk = str(mk or "").strip()
            if not mk:
                continue
            prefix = mk.rstrip(":") + ":"
            if curr.startswith(prefix):
                curr = curr[len(prefix):].strip()
                break

    # 규칙: 현재 상호명이 비어 있으면, 현재에 넣고 이전은 빈 값
    if prev and not curr:
        curr = prev
        prev = ""
    return prev, curr


def _end_value(end_raw: str) -> str:
    end_compact = (end_raw or "").replace(" ", "")
    if not end_raw:
        return ""
    if "근무중" in end_compact or ("근" in end_raw and "무" in end_raw):
        return "근무중"
    return _workplace_date_to_iso(end_raw)


def _parse_workplace_body_lines(body_lines: list[str]) -> list[dict]:
    """
    근무처 텍스트 블록을 단순 라인 기반으로 파싱한다.

    처리:
    - 2열(좌/우) 서식: '시작행'과 '종료행'을 pending으로 짝지어 2개 레코드를 생성
    - 특수행: 흡수합병/근무중
    """
    out: list[dict] = []
    pending: dict | None = None  # {"l_start","l_co","r_start","r_co"}

    _DATE = r"\d{4}\.\d{2}(?:\.\d{2})?"
    start_row_pat = re.compile(
        rf"^({_DATE})\s*~\s*(.+?)\s+({_DATE})\s*~\s*(.+)$"
    )
    end_row_pat = re.compile(
        rf"^({_DATE})\s+"
        rf"(?:現?:(.+?)\s+)?"
        rf"({_DATE}|근\s*무\s*중)"
        # 우측 현재상호는 ':현재상호'로 오기도 하고('2016.07.24 :(주)A'),
        # 그냥 '흡수합병:(주)B'처럼 바로 이어지기도 한다.
        rf"\s*(?:(?::\s*)?(.+))?$"
    )

    # 표/텍스트 추출 편차로 인해 "회사명"과 "사유:신상호"가 서로 다른 줄로 내려오는 경우가 있다.
    # 예) "(주)A" 다음 줄에 "분할설립:(주)B" → 직전 레코드의 회사명에 결합해 이전/현재를 재분리한다.
    reasons = [m for m in get_company_change_markers() if m not in {"現", "현"}]
    reason_alt = "|".join(re.escape(x) for x in reasons) if reasons else "흡수합병"
    standalone_reason_pat = re.compile(rf"^({reason_alt})\s*[:：]\s*(.+)$")

    i = 0
    while i < len(body_lines):
        ln = body_lines[i]

        # 일부 추출에서는 라인 앞에 '근무처' 같은 라벨이 붙는다.
        # NOTE: 근무처는 'YYYY.MM'만 있는 구간도 있어, 가장 앞의 날짜 토큰(YYYY.MM(.DD)?)부터 잘라 정규식을 안정화한다.
        dm = re.search(_DATE, ln)
        if dm and dm.start() > 0:
            ln = ln[dm.start():].strip()

        # 사유만 단독 라인으로 내려오는 케이스(직전 레코드에 결합)
        sr = standalone_reason_pat.match(ln)
        if sr and out:
            last = out[-1]
            last_prev = str(last.get("이전_상호명") or "").strip()
            last_curr = str(last.get("현재_상호명") or "").strip()
            # 직전 레코드가 "현재만" 채워진 상태라면(회사명 단독 라인으로 해석된 상태)만 보정한다.
            right = sr.group(2).strip()
            looks_like_company = bool(re.search(r"(주\)|\(|[가-힣A-Za-z0-9])", right))
            if (not last_prev) and last_curr and looks_like_company:
                combined = f"{last_curr} {sr.group(1).strip()}:{sr.group(2).strip()}"
                p, c = _normalize_company(combined)
                last["이전_상호명"] = p
                last["현재_상호명"] = c
                i += 1
                continue

        # 표 셀 병합 등으로 "사유:구상호 종료일:신상호"가 한 줄로 내려오는 케이스(흡수합병/분할설립/상호변경 등)
        change_line = re.search(
            rf"^({_DATE})\s+({reason_alt})\s*[:：]\s*(.+?)\s+"
            rf"({_DATE})\s*[:：]\s*(.+)$",
            ln,
        )
        if change_line:
            # 2열 서식에서 우측 컬럼의 종료행이 "흡수합병:" 형식으로 내려오는 경우가 있다.
            # 이때는 pending(좌/우 시작행)을 먼저 종료시킨 후, 별도 merger 레코드는 추가하지 않는다.
            if pending and (pending.get("r_start") == change_line.group(1)):
                l_end = change_line.group(1)  # 우측 시작일이 좌측 종료일로 내려오는 케이스
                r_end = change_line.group(4)

                l_prev, l_curr = _normalize_company(str(pending.get("l_co") or ""))
                # 우측은 "사유:현재상호"로 반영
                reason = change_line.group(2).strip()
                r_prev, r_curr = _normalize_company(
                    f"{change_line.group(3).strip()} {reason}:{change_line.group(5).strip()}"
                )

                out.append(
                    {
                        "근무기간_시작": _workplace_date_to_iso(str(pending.get("l_start") or "")),
                        "근무기간_종료": _end_value(l_end),
                        "이전_상호명": l_prev,
                        "현재_상호명": l_curr,
                    }
                )
                out.append(
                    {
                        "근무기간_시작": _workplace_date_to_iso(str(pending.get("r_start") or "")),
                        "근무기간_종료": _end_value(r_end),
                        "이전_상호명": r_prev,
                        "현재_상호명": r_curr,
                    }
                )
                pending = None
            else:
                out.append({
                    "근무기간_시작": _workplace_date_to_iso(change_line.group(1)),
                    "근무기간_종료": _workplace_date_to_iso(change_line.group(4)),
                    "이전_상호명": change_line.group(3).strip(),
                    "현재_상호명": change_line.group(5).strip(),
                })
            i += 1
            continue

        m = start_row_pat.search(ln)
        if m:
            pending = {
                "l_start": m.group(1),
                "l_co": m.group(2).strip(),
                "r_start": m.group(3),
                "r_co": m.group(4).strip(),
            }
            i += 1
            continue

        m = end_row_pat.search(ln)
        if pending:
            l_prev_override: str | None = None
            l_curr_override: str | None = None
            if m:
                l_end = m.group(1)
                l_curr_inline = (m.group(2) or "").strip()
                r_end = m.group(3)
                r_curr = (m.group(4) or "").strip()
            else:
                # 2열 레이아웃에서 좌측 종료일 뒤에 '사유:신상호'가 끼고, 이어서 우측 종료일이 나오는 케이스
                # 예) "2007.08.01 분할설립:(주)B 2016.07.24 :(주)C"
                end_row_with_reason_pat = re.compile(
                    rf"^({_DATE})\s+({reason_alt})\s*[:：]\s*(.+?)\s+({_DATE}|근\s*무\s*중)"
                    rf"\s*(?:(?::\s*)?(.+))?$"
                )
                mr = end_row_with_reason_pat.search(ln)
                if mr:
                    l_end = mr.group(1)
                    # 좌측 회사명에 변경 사유를 결합해 이전/현재 분리(_normalize_company가 처리)
                    l_curr_inline = f"{mr.group(2).strip()}:{mr.group(3).strip()}"
                    r_end = mr.group(4)
                    r_curr = (mr.group(5) or "").strip()
                else:
                    # PDF 텍스트 추출 인코딩이 깨져 사유(분할설립 등) 키워드가 mojibake로 들어오는 경우가 있다.
                    # 이때는 마커 단어 자체를 신뢰하지 않고, "어떤 토큰:" 뒤에 회사명이 오는 형태를
                    # 좌측 상호 변경으로 간주해 (이전=기존 좌측 회사명, 현재=콜론 뒤 회사명)으로 직접 설정한다.
                    generic_reason_pat = re.compile(
                        rf"^({_DATE})\s+([^\s:：]{{1,30}})\s*[:：]\s*(.+?)\s+({_DATE}|근\s*무\s*중)"
                        rf"\s*(?:(?::\s*)?(.+))?$"
                    )
                    gm = generic_reason_pat.search(ln)
                    if gm:
                        l_end = gm.group(1)
                        l_prev_override = str(pending.get('l_co') or '').strip()
                        l_curr_override = (gm.group(3) or '').strip()
                        r_end = gm.group(4)
                        r_curr = (gm.group(5) or "").strip()
                        l_curr_inline = ""
                    else:
                        # 또 다른 변형: "사유:신상호"가 공백 없이 한 토큰으로 붙어오는 케이스
                        # 예) "2007.08.01 분할설립:(주)B 2016.07.24 :(주)C"
                        glued_token_pat = re.compile(
                            rf"^({_DATE})\s+(\S+)\s+({_DATE}|근\s*무\s*중)"
                            rf"\s*(?:(?::\s*)?(.+))?$"
                        )
                        gt = glued_token_pat.search(ln)
                        if gt:
                            tok = (gt.group(2) or "").strip()
                            tok = tok.replace("：", ":")
                            if ":" in tok:
                                _reason, _new = tok.split(":", 1)
                                _new = (_new or "").strip()
                                if _new:
                                    l_end = gt.group(1)
                                    l_prev_override = str(pending.get("l_co") or "").strip()
                                    l_curr_override = _new
                                    r_end = gt.group(3)
                                    r_curr = (gt.group(4) or "").strip()
                                    l_curr_inline = ""
                        if l_prev_override is None:
                            # end row가 '퇴사사유:...' 같은 잡문구를 끼고 나오면 정규식이 실패할 수 있다.
                            # 이때는 라인 내 날짜 2개를 사용해 (좌종료, 우종료)로 해석한다.
                            dates = re.findall(r"\d{4}\.\d{2}\.\d{2}", ln)
                            if len(dates) >= 2:
                                l_end, r_end = dates[0], dates[1]
                                l_curr_inline = ""
                                # 우측 현재상호는 보통 ':' 뒤에 온다(있으면 사용)
                                r_curr = ""
                                if ":" in ln:
                                    r_curr = (ln.split(":", 1)[1] or "").strip()
                            else:
                                i += 1
                                continue

            if l_prev_override is not None and l_curr_override is not None:
                l_prev, l_curr = l_prev_override, l_curr_override
            else:
                l_co = pending["l_co"]
                if l_curr_inline:
                    # l_curr_inline이 "현상호"인 경우도 있고("現:..."),
                    # "사유:신상호"로 들어오는 경우도 있어 그냥 결합만 해준다.
                    if re.match(rf"^({reason_alt}):", l_curr_inline):
                        l_co = l_co + " " + l_curr_inline
                    else:
                        l_co = l_co + " 現:" + l_curr_inline
                l_prev, l_curr = _normalize_company(l_co)
            r_prev, r_curr2 = _normalize_company(pending["r_co"] + (f" 現:{r_curr}" if r_curr else ""))

            out.append(
                {
                    "근무기간_시작": _workplace_date_to_iso(pending["l_start"]),
                    "근무기간_종료": _end_value(l_end),
                    "이전_상호명": l_prev,
                    "현재_상호명": l_curr,
                }
            )
            out.append(
                {
                    "근무기간_시작": _workplace_date_to_iso(pending["r_start"]),
                    "근무기간_종료": _end_value(r_end),
                    "이전_상호명": r_prev,
                    "현재_상호명": r_curr2,
                }
            )
            pending = None
            i += 1
            continue

        ongoing = re.match(rf"^({_DATE})\s*~\s*(.+)$", ln)
        if ongoing:
            # 다음 줄에 '근무중'이 내려오는 일반 케이스
            if i + 1 < len(body_lines):
                nxt_compact = re.sub(r"\s+", "", body_lines[i + 1])
                if "근무중" in nxt_compact:
                    out.append({
                        "근무기간_시작": _workplace_date_to_iso(ongoing.group(1)),
                        "근무기간_종료": "근무중",
                        "이전_상호명": "",
                        "현재_상호명": ongoing.group(2).strip(),
                    })
                    i += 2
                    continue
            # 폴백: 페이지 텍스트 추출이 '근무중'을 드롭하고 종료일이 끝까지 비는 케이스가 있다.
            # 다음 라인들이 섹션 종료(각주/공백/라벨)로만 이어지면 근무중으로 간주한다.
            tail = " ".join(body_lines[i + 1 : i + 6]) if i + 1 < len(body_lines) else ""
            if not re.search(r"\d{4}\.\d{2}\.\d{2}", tail) and ("퇴" in tail or "사" in tail or "유" in tail or "본 증명서는" in tail):
                out.append({
                    "근무기간_시작": _workplace_date_to_iso(ongoing.group(1)),
                    "근무기간_종료": "근무중",
                    "이전_상호명": "",
                    "현재_상호명": ongoing.group(2).strip(),
                })
                i += 1
                continue

        i += 1

    # 회사명이 줄바꿈으로 찢어진 경우 보강: pending이 남아있다면 보수적으로 버린다(오탐 방지)

    out.sort(key=lambda x: (x.get("근무기간_시작") or ""))
    return out


def _parse_workplaces_from_text(page_text: str) -> list[dict]:
    """
    pdfplumber 테이블이 오른쪽 컬럼(2세트)을 누락하는 경우를 위한 텍스트 기반 근무처 파싱.
    """
    if not page_text:
        return []

    body = _workplace_body_lines_from_text(page_text)
    if body:
        parsed = _parse_workplace_body_lines(body)
        if parsed:
            return parsed

    t = re.sub(r'\s+', ' ', page_text)
    m = re.search(r'(근무처\s+.*?)(?=\s+(?:기술경력|교육훈련|상훈|벌점|제재사항|제재일|국가기술자격|학력)\s+|$)', t)
    section = (m.group(1) if m else t)

    section = re.sub(r'\b근무처\b', ' ', section)
    section = re.sub(r'근무기간\s*상호', ' ', section)

    lines = [ln for ln in (page_text or "").splitlines() if ln and ln.strip()]
    table_lines = [re.sub(r'\s+', ' ', ln).strip() for ln in lines]
    out: list[dict] = []

    # NOTE: 근무처 회사명 마커/이전-현재 분리는 파일 상단의 공용 함수(_normalize_company*)를 사용한다.

    def _end_value(end_raw: str) -> str:
        end_compact = (end_raw or "").replace(" ", "")
        if not end_raw:
            return ""
        if "근무중" in end_compact or ("근" in end_raw and "무" in end_raw):
            return "근무중"
        return _workplace_date_to_iso(end_raw)

    # 패턴: "좌시작 ~ 좌상호  우시작 ~ 우상호" (시작행)
    _DATE = r"\d{4}\.\d{2}(?:\.\d{2})?"
    start_row_pat = re.compile(rf'({_DATE})\s*~\s*(.+?)\s+({_DATE})\s*~\s*(.+)$')
    # 패턴: "좌종료 [現:좌현재상호] 우종료 [:우현재상호]" (종료행)
    # pdfplumber가 '現' 문자를 드롭해 " :(주)창설 2021.12.31" 형태가 되는 케이스도 처리
    end_row_pat = re.compile(
        rf'^({_DATE})\s+'                        # 좌종료일
        rf'(?:現?:(.+?)\s+)?'                    # [옵션] 좌현재상호 (現: 또는 :로 시작)
        rf'({_DATE}|근\s*무\s*중)'               # 우종료일
        rf'\s*(?:(?::\s*)?(.+))?$'               # [옵션] 우현재상호(콜론 유무 모두 허용)
    )

    pending = None  # {"l_start","l_co","r_start","r_co"}

    for ln in table_lines:
        # 일부 추출에서는 라인 앞에 '근무처' 같은 라벨이 붙어 start_row_pat가 실패한다.
        # 첫 날짜 패턴부터 잘라내어 정규식을 안정화한다.
        dm = re.search(_DATE, ln)
        if dm and dm.start() > 0:
            ln = ln[dm.start():].strip()

        m = start_row_pat.search(ln)
        if m:
            pending = {
                "l_start": m.group(1),
                "l_co": m.group(2).strip(),
                "r_start": m.group(3),
                "r_co": m.group(4).strip(),
            }
            continue

        parsed_end = False
        m = end_row_pat.search(ln)
        if pending:
            if m:
                l_end = m.group(1)
                l_curr_inline = (m.group(2) or "").strip()  # end 행에서 추출된 좌측 현재상호
                r_end = m.group(3)
                r_curr = (m.group(4) or "").strip()
                parsed_end = True
            else:
                dates = re.findall(r"\d{4}\.\d{2}\.\d{2}", ln)
                if len(dates) >= 2:
                    l_end, r_end = dates[0], dates[1]
                    l_curr_inline = ""
                    r_curr = ""
                    if ":" in ln:
                        r_curr = (ln.split(":", 1)[1] or "").strip()
                    parsed_end = True
                else:
                    # 회사명이 줄바꿈으로 찢어진 경우 보강
                    pending["r_co"] = (pending["r_co"] + " " + ln).strip()
                    continue

        if pending and parsed_end:

            # end 행에 좌측 현재상호가 포함된 경우(現이 drop된 ":상호" 케이스) l_co에 보강
            l_co = pending["l_co"]
            if l_curr_inline:
                l_co = l_co + " 現:" + l_curr_inline

            l_prev, l_curr = _normalize_company(l_co)
            r_prev, r_curr2 = _normalize_company(pending["r_co"] + (f" 現:{r_curr}" if r_curr else ""))

            out.append({
                "근무기간_시작": _workplace_date_to_iso(pending["l_start"]),
                "근무기간_종료": _end_value(l_end),
                "이전_상호명": l_prev,
                "현재_상호명": l_curr
            })
            out.append({
                "근무기간_시작": _workplace_date_to_iso(pending["r_start"]),
                "근무기간_종료": _end_value(r_end),
                "이전_상호명": r_prev,
                "현재_상호명": r_curr2
            })
            pending = None
            continue

    if not out:
        return []

    out.sort(key=lambda x: (x.get("근무기간_시작") or ""))
    return out


def _yy_mm_dd_to_iso(date_str: str) -> str:
    """
    '74.09.03' 같은 2자리 연도 날짜를 '1974-09-03' 형태로 변환.
    00~(현재연도%100) 는 2000년대, 그 외는 1900년대로 해석.
    """
    m = re.fullmatch(r'(\d{2})\.(\d{2})\.(\d{2})', (date_str or '').strip())
    if not m:
        return ""
    yy, mm, dd = map(int, m.groups())
    current_yy = int(datetime.now().strftime("%y"))
    year = 2000 + yy if yy <= current_yy else 1900 + yy
    try:
        dt = datetime(year, mm, dd)
    except ValueError:
        return ""
    return dt.strftime("%Y-%m-%d")


def _yyyy_mm_dd_to_iso(date_str: str) -> str:
    """
    '2001.12.26' 또는 '2001-12-26'을 '2001-12-26'으로 통일.
    """
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
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        yyyy, mm, dd = map(int, m.groups())
        try:
            return datetime(yyyy, mm, dd).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return s


def _yyyy_mm_to_iso(date_str: str) -> str:
    """
    'YYYY.MM'를 'YYYY-MM-01'로 변환.
    근무처 등에서 월 단위로만 표기되는 구간을 지원한다.
    """
    s = (date_str or "").strip()
    if not s:
        return ""
    m = re.fullmatch(r"(\d{4})\.(\d{2})", s)
    if not m:
        return ""
    yyyy, mm = int(m.group(1)), int(m.group(2))
    try:
        return datetime(yyyy, mm, 1).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _workplace_date_to_iso(token: str) -> str:
    s = (token or "").strip()
    if not s:
        return ""
    iso = _yyyy_mm_dd_to_iso(s)
    if iso and re.fullmatch(r"\d{4}-\d{2}-\d{2}", iso):
        return iso
    iso2 = _yyyy_mm_to_iso(s)
    return iso2 or iso


def _parse_workplace_body_lines_single(body_lines: list[str]) -> list[dict]:
    """
    근무처 단일 컬럼(라인 기반) 파서.
    지원 형식(예):
      1990.01 ~
      1991.01
      신화건설(주)
      1991.01.14 ~
      1999.08.01
      한진건설(주)
      흡수합병:(주)한진중공업
    """
    if not body_lines:
        return []

    out: list[dict] = []

    start_pat = re.compile(r"^(?P<s>\d{4}\.\d{2}(?:\.\d{2})?)\s*~\s*$")
    end_pat = re.compile(r"^(?P<e>\d{4}\.\d{2}(?:\.\d{2})?)\s*$")
    ongoing_pat = re.compile(r"^근\s*무\s*중\s*$")

    markers = [str(m or "").strip() for m in get_company_change_markers()]
    markers = [m for m in markers if m]

    def _is_marker_line(s: str) -> bool:
        t = (s or "").strip()
        if not t:
            return False
        for mk in markers:
            if t.startswith(mk):
                return True
            if t.startswith(mk.rstrip(":") + ":"):
                return True
        return False

    cur_start_raw = ""
    cur_end_raw = ""
    company_buf: list[str] = []

    def _flush_record() -> None:
        nonlocal cur_start_raw, cur_end_raw, company_buf
        if not cur_start_raw:
            return
        start_iso = _workplace_date_to_iso(cur_start_raw)
        if ongoing_pat.match(cur_end_raw or ""):
            end_iso = "근무중"
        else:
            end_iso = _workplace_date_to_iso(cur_end_raw)
        company_raw = " ".join(x for x in company_buf if x).strip()
        if not company_raw:
            cur_start_raw, cur_end_raw, company_buf = "", "", []
            return
        prev, curr = _normalize_company(company_raw)
        out.append(
            {
                "근무기간_시작": start_iso,
                "근무기간_종료": end_iso,
                "이전_상호명": prev,
                "현재_상호명": curr,
            }
        )
        cur_start_raw, cur_end_raw, company_buf = "", "", []

    i = 0
    while i < len(body_lines):
        ln = (body_lines[i] or "").strip()
        if not ln:
            i += 1
            continue

        m_s = start_pat.match(ln)
        if m_s:
            _flush_record()
            cur_start_raw = m_s.group("s")
            cur_end_raw = ""
            company_buf = []
            i += 1
            continue

        if cur_start_raw and not cur_end_raw:
            if ongoing_pat.match(ln):
                cur_end_raw = "근무중"
                i += 1
                continue
            m_e = end_pat.match(ln)
            if m_e:
                cur_end_raw = m_e.group("e")
                i += 1
                continue

        if cur_start_raw:
            if any(k in ln for k in ["근무처", "근무기간", "상호"]):
                i += 1
                continue
            # 사유 라인이 별도 줄이면 그대로 붙인다(이전/현재 분리는 _normalize_company가 담당)
            if _is_marker_line(ln):
                company_buf.append(ln)
            else:
                company_buf.append(ln)
        i += 1

    _flush_record()
    return out


# 학력: 한 줄에 "졸업일 학교 … 학위[상태]"가 오고, 다음 줄이 "학력 YYYY.MM.DD …"처럼
# 섹션 라벨이 날짜 앞에 붙는 양식이 있다. 라벨이 있으면 새 레코드 시작으로 인식해야 하며,
# 이미 한 줄로 병합된 문자열은 날짜+학위 패턴으로 분리한다.
# // [수정] 한국건설엔지니어링협회 경력증명서 등: 학사·석사 외 '대졸[졸업]', '전문[졸업]' 등 학력구분+[상태] 표기를 동일하게 허용한다.
_EDU_KIND = r"(?:학사|석사|박사과정|박사수료|박사|없음|대졸|전문졸업|전문|고졸|중졸|초졸|석·박|본\s*석사|본석사)"
_EDU_BRACKET_FULL = re.compile(rf"(?P<deg>{_EDU_KIND})\[\s*(?P<st>[^\]]+)\s*\]")
_EDU_ONE_LINE = re.compile(
    rf"^(?:학력\s+)?(?P<date>\d{{4}}\.\d{{2}}\.\d{{2}})\s+"
    rf"(?P<body>.+{_EDU_KIND}\[[^\]]+\])\s*$",
)
_EDU_SEG_FIND = re.compile(
    rf"(?:^|\s)(?:학력\s+)?(?P<date>\d{{4}}\.\d{{2}}\.\d{{2}})\s+"
    rf"(?P<rest>.+?{_EDU_KIND}\[[^\]]+\])",
)


def _strip_leading_hakryeok_label(line: str) -> str:
    """줄 시작의 '학력' 섹션 라벨(날짜 앞에만 붙는 경우) 제거."""
    s = (line or "").strip()
    return re.sub(r"^학력\s+", "", s)


def _split_merged_education_line(line: str) -> list[str]:
    """
    한 문자열에 복수 학력이 붙어 있는 경우(예: 첫 줄+라벨 있는 둘째 줄이 병합) 분리.
    각 원소는 'YYYY.MM.DD 나머지…학위[상태]' 형태.
    """
    s = (line or "").strip()
    if not s:
        return []
    matches = list(_EDU_SEG_FIND.finditer(s))
    if not matches:
        return []
    out: list[str] = []
    for m in matches:
        d = (m.group("date") or "").strip()
        rest = (m.group("rest") or "").strip()
        if d and rest:
            out.append(f"{d} {rest}".strip())
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Page 1 표 기반 파싱: 가상 세로선(요구사항 X=27, X=567) + pdfplumber 표 추출
# ──────────────────────────────────────────────────────────────────────────────
PAGE1_VIRTUAL_LEFT_X: float = 27.0
PAGE1_VIRTUAL_RIGHT_X: float = 567.0

PAGE1_LINE_TABLE_SETTINGS: dict[str, Any] = {
    **LINE_TABLE_SETTINGS,
    "explicit_vertical_lines": [PAGE1_VIRTUAL_LEFT_X, PAGE1_VIRTUAL_RIGHT_X],
}

PAGE1_LINE_TABLE_SETTINGS_ALT: dict[str, Any] = {
    **LINE_TABLE_SETTINGS,
    "explicit_vertical_lines": [27.0, 560.0],
}


def _extract_license_section_text(raw: str) -> str:
    """국가기술자격 섹션 텍스트만 잘라낸다 (parse_page_1_from_text와 동일 규칙)."""
    if not raw:
        return ""
    t = re.sub(r"[ \t]+", " ", raw)
    m = re.search(
        r"(국가기술자격[\s\S]*?)(?=\n\s*(?:학력|교육훈련|상훈|벌점|제재사항|근무처)\b|\Z)",
        t,
        flags=0,
    )
    return (m.group(1) if m else "")


def _parse_grade_dict_from_normalized_text(text_normalized: str) -> Dict[str, str]:
    """등급 9필드 dict (빈 문자열 기본)."""
    grade: Dict[str, str] = {
        "설계시공_등_직무분야": "",
        "설계시공_등_직무분야_등급": "",
        "설계시공_등_전문분야": "",
        "설계시공_등_전문분야_등급": "",
        "건설사업관리_직무분야": "",
        "건설사업관리_직무분야_등급": "",
        "건설사업관리_전문분야": "",
        "건설사업관리_전문분야_등급": "",
        "품질관리_등급": "",
    }
    simple_pattern = (
        r"([\w가-힣]+)\s+(고급|중급|초급|특급)\s+\*\*\s*해당없음\s*\*\*\s+([\w가-힣]+)\s+(고급|중급|초급|특급)"
    )
    match = re.search(simple_pattern, text_normalized)
    if match:
        grade["설계시공_등_직무분야"] = match.group(1)
        grade["설계시공_등_직무분야_등급"] = match.group(2)
        grade["건설사업관리_직무분야"] = match.group(3)
        grade["건설사업관리_직무분야_등급"] = match.group(4)
    try:
        if not match:
            from field_catalog import get_field_catalog, best_match_specialty  # lazy import

            catalog = get_field_catalog(
                project_root=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
            grade_tokens_pat = r"(특급|고급|중급|초급)"

            def _cut(src: str, start_kw: str, end_kws: list[str]) -> str:
                if not src:
                    return ""
                sidx = src.find(start_kw)
                if sidx < 0:
                    return ""
                sub = src[sidx + len(start_kw) :]
                epos = None
                for ek in end_kws:
                    j = sub.find(ek)
                    if j >= 0:
                        epos = j if epos is None else min(epos, j)
                return sub[:epos] if epos is not None else sub

            design_block = _cut(text_normalized, "설계·시공 등", ["건설사업관리", "품질관리"])
            cm_block = _cut(
                text_normalized,
                "건설사업관리",
                ["품질관리", "국가기술자격", "학력", "교육훈련", "상훈", "벌점", "근무처", "1. 기술경력"],
            )

            def _pick_job_and_grade(block: str) -> tuple[str, str]:
                for jf in catalog.job_fields:
                    m = re.search(rf"{re.escape(jf)}\s*{grade_tokens_pat}", block or "")
                    if m:
                        return jf, m.group(1)
                return "", ""

            def _pick_specialty_and_grade(block: str) -> tuple[str, str]:
                for sp in sorted(catalog.all_specialties, key=len, reverse=True):
                    if not sp:
                        continue
                    m = re.search(rf"{re.escape(sp)}\s*{grade_tokens_pat}", block or "")
                    if m:
                        return sp, m.group(1)
                sp2 = best_match_specialty(block or "", catalog)
                if sp2:
                    m2 = re.search(rf"{re.escape(sp2)}\s*{grade_tokens_pat}", block or "")
                    if m2:
                        return sp2, m2.group(1)
                return "", ""

            jf, jg = _pick_job_and_grade(design_block)
            sp, sg = _pick_specialty_and_grade(design_block)
            if jf and jg:
                grade["설계시공_등_직무분야"] = jf
                grade["설계시공_등_직무분야_등급"] = jg
            if sp and sg:
                grade["설계시공_등_전문분야"] = sp
                grade["설계시공_등_전문분야_등급"] = sg

            jf, jg = _pick_job_and_grade(cm_block)
            sp, sg = _pick_specialty_and_grade(cm_block)
            if jf and jg:
                grade["건설사업관리_직무분야"] = jf
                grade["건설사업관리_직무분야_등급"] = jg
            if sp and sg:
                grade["건설사업관리_전문분야"] = sp
                grade["건설사업관리_전문분야_등급"] = sg

            m_q = re.search(rf"품질관리\s*{grade_tokens_pat}", text_normalized)
            if m_q:
                grade["품질관리_등급"] = m_q.group(1)
    except Exception:
        pass
    return grade


def _parse_education_from_combined_text(combined_text: str) -> List[Dict[str, Any]]:
    """학력 섹션과 동일한 규칙으로 combined_text에서 학력 리스트만 추출한다."""
    edu_rows: list[dict] = []
    edu_start_pat = re.compile(
        rf"^(?:학력\s+)?(?P<date>\d{{4}}\.\d{{2}}\.\d{{2}})\s+"
        rf"(?P<body>.+{_EDU_KIND}\[[^\]]+\])\s*$"
    )
    section_start_like = re.compile(
        r"^(?:\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}\.\d{2}\.\d{2}\b|"
        r"근무기간|수여일|교육기간|1\.\s*기술경력|2\.\s*건설사업관리|근무처|상훈|교육훈련)"
    )

    merged_lines: list[str] = []
    buf_line = ""
    for raw_line in (combined_text or "").splitlines():
        line = re.sub(r"[ \t]+", " ", (raw_line or "")).strip()
        if not line:
            continue
        if "졸업일" in line and "학교명" in line and ("학과" in line or "전공" in line) and "학위" in line:
            continue

        if edu_start_pat.match(line):
            if buf_line:
                merged_lines.append(buf_line.strip())
            buf_line = line
            continue

        if buf_line:
            if (not section_start_like.match(line)) and (
                not re.match(r"^(?:학력\s+)?\d{4}\.\d{2}\.\d{2}\b", line)
            ):
                buf_line = (buf_line + " " + line).strip()
                continue
            merged_lines.append(buf_line.strip())
            buf_line = ""
            if edu_start_pat.match(line):
                buf_line = line
            elif re.match(r"^(?:학력\s+)?\d{4}\.\d{2}\.\d{2}\b", line):
                buf_line = line
            continue

    if buf_line:
        merged_lines.append(buf_line.strip())

    edu_line_segments: list[str] = []
    for line in merged_lines:
        segs = _split_merged_education_line(line)
        if len(segs) > 1:
            edu_line_segments.extend(segs)
        elif len(segs) == 1:
            edu_line_segments.append(segs[0])
        else:
            t = _strip_leading_hakryeok_label(line)
            if t != line.strip():
                t2 = _split_merged_education_line(t)
                edu_line_segments.extend(t2 if t2 else [t])
            else:
                edu_line_segments.append(line.strip())

    for line in edu_line_segments:
        if not _EDU_BRACKET_FULL.search(line):
            continue
        line = _strip_leading_hakryeok_label(line)
        m_date = re.match(r"^(?P<date>\d{4}\.\d{2}\.\d{2})\s+(?P<rest>.+)$", line)
        if not m_date:
            continue
        date_raw = (m_date.group("date") or "").strip()
        rest = (m_date.group("rest") or "").strip()

        deg_hits = list(_EDU_BRACKET_FULL.finditer(rest))
        if not deg_hits:
            continue
        last = deg_hits[-1]
        degree = (last.group("deg") or "").strip()
        status = (last.group("st") or "").strip()
        before = rest[: last.start()].strip()
        after = rest[last.end() :].strip()

        words = [p for p in before.split(" ") if p]
        if words and words[-1] == "학력":
            words = words[:-1]
        if len(words) < 2:
            continue
        major_idx = None
        for _wi, w in enumerate(words):
            w = w.strip()
            if any(w.endswith(suf) for suf in ["학과", "전공", "학부", "과"]):
                if w in {"과정"}:
                    continue
                major_idx = _wi
                break

        if major_idx is None and len(words) >= 3:
            tail = words[-1].strip()
            prev = words[-2].strip()
            if tail in {"전문", "일반", "야간", "주간"} and any(
                prev.endswith(suf) for suf in ["학과", "전공", "학부", "과"]
            ):
                major_idx = len(words) - 2

        if major_idx is None:
            major = words[-1].strip()
            school = " ".join(words[:-1]).strip()
        else:
            end_i = len(words)
            while end_i > major_idx:
                cand = words[end_i - 1].strip()
                if cand in {"전문", "일반", "야간", "주간"}:
                    end_i -= 1
                    continue
                break
            major = " ".join(words[major_idx:end_i]).strip()
            school = " ".join(words[:major_idx]).strip()
        if after:
            major = (major + " " + after).strip()
        major = re.sub(r"\s+학력\s*$", "", (major or "").strip()).strip()

        if not school or not major:
            continue

        school_full = re.sub(r"\s+", " ", school.replace("：", ":")).strip()
        prev_school = ""
        curr_school = school_full

        def _looks_like_school_name(s: str) -> bool:
            t = re.sub(r"\s+", "", (s or "")).strip()
            if not t:
                return False
            return any(k in t for k in ["대학교", "대학", "전문대학", "고등학교", "중학교", "초등학교", "학교"])

        m_sc = re.search(r"\(\s*(?:現|현)\s*:\s*([^)]+)\)", school_full)
        if m_sc:
            curr_school = re.sub(r"\s+", " ", (m_sc.group(1) or "")).strip()
            prev_school = re.sub(
                r"\s+",
                " ",
                (school_full[: m_sc.start()] + school_full[m_sc.end() :]).strip(),
            ).strip()
        else:
            m_sc2 = re.search(r"\(\s*:\s*([^)]+)\)", school_full)
            if m_sc2:
                cand_curr = re.sub(r"\s+", " ", (m_sc2.group(1) or "")).strip()
                cand_prev = re.sub(
                    r"\s+",
                    " ",
                    (school_full[: m_sc2.start()] + school_full[m_sc2.end() :]).strip(),
                ).strip()
                if (
                    _looks_like_school_name(cand_prev)
                    and _looks_like_school_name(cand_curr)
                    and not any(cand_curr.endswith(suf) for suf in ["학과", "전공", "학부", "과"])
                ):
                    prev_school, curr_school = cand_prev, cand_curr

        prev_school = re.sub(r"\s+", " ", (prev_school or "")).strip()
        curr_school = re.sub(r"\s+", " ", (curr_school or "")).strip()
        if prev_school and not curr_school:
            curr_school = prev_school
        if not prev_school and not curr_school:
            curr_school = school_full

        edu_rows.append(
            {
                "졸업일": _yyyy_mm_dd_to_iso(date_raw),
                "이전_학교명": prev_school,
                "현재_학교명": curr_school,
                "학과": major,
                "학위": degree,
                "상태": status,
            }
        )

    if not edu_rows:
        return []
    seen = set()
    dedup = []
    for e in edu_rows:
        key = (
            e.get("졸업일", ""),
            e.get("이전_학교명", ""),
            e.get("현재_학교명", ""),
            e.get("학과", ""),
            e.get("학위", ""),
            e.get("상태", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        dedup.append(e)
    dedup.sort(key=lambda x: (x.get("졸업일") or ""))
    return dedup


def normalize_cell_text(cell: Any) -> str:
    if cell is None:
        return ""
    s = str(cell).replace("\u00a0", " ").replace("\u200b", "").replace("\ufeff", "")
    s = re.sub(r"[\t\v\f\r]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def split_multiline_cell(cell: str) -> List[str]:
    t = normalize_cell_text(cell)
    if not t:
        return []
    return [ln.strip() for ln in t.split("\n") if ln.strip()]


def normalize_table_rows(table: List[Any]) -> List[List[str]]:
    out: List[List[str]] = []
    for row in table or []:
        if row is None:
            continue
        nr = [normalize_cell_text(c) for c in row]
        if any(x for x in nr):
            out.append(nr)
    return out


def merge_broken_rows(rows: List[List[str]]) -> List[List[str]]:
    """앞열이 비고 마지막 셀만 채워진 행을 이전 행에 이어붙인다."""
    if not rows:
        return []
    out: List[List[str]] = []
    for row in rows:
        nonempty = [i for i, c in enumerate(row) if c and c.strip()]
        if (
            len(row) >= 2
            and nonempty == [len(row) - 1]
            and out
            and any(out[-1][i].strip() for i in range(len(out[-1])) if i < len(row))
        ):
            prev = out[-1]
            tail = row[-1].strip()
            if prev:
                j = len(prev) - 1
                while j >= 0 and not (prev[j] or "").strip():
                    j -= 1
                if j >= 0:
                    prev[j] = _smart_concat(prev[j], tail)
                else:
                    prev[-1] = tail
            continue
        out.append(list(row))
    return out


def _row_join_for_detection(row: List[str]) -> str:
    return " ".join((c or "").replace("\n", " ").strip() for c in row if c is not None)


def _score_page1_table(table: List[Any]) -> Tuple[int, int]:
    if not table:
        return (0, 0)
    keys = (
        "인적사항",
        "성명",
        "등급",
        "국가기술자격",
        "학력",
        "졸업일",
        "교육훈련",
        "교육기간",
        "상훈",
        "수여일",
        "벌점",
        "제재",
        "근무처",
        "근무기간",
        "상호",
    )
    hits = 0
    for row in table[:120]:
        rt = _row_join_for_detection([normalize_cell_text(c) for c in (row or [])])
        hits += sum(1 for k in keys if k in rt)
    return (hits, len(table))


def _raw_tables_from_page(page: Any) -> List[List[Any]]:
    tables = safe_extract_tables(page, PAGE1_LINE_TABLE_SETTINGS) or []
    if not tables:
        tables = safe_extract_tables(page, PAGE1_LINE_TABLE_SETTINGS_ALT) or []
    if not tables:
        tables = extract_tables_merged(page) or []
    return tables


def _extract_page1_normalized_rows_for_page(page: Any) -> List[List[str]]:
    raw = _raw_tables_from_page(page)
    if not raw:
        return []
    best = pick_best_table(raw, _score_page1_table)
    if best:
        return merge_broken_rows(normalize_table_rows(best))
    acc: List[List[str]] = []
    for tbl in raw:
        acc.extend(merge_broken_rows(normalize_table_rows(tbl)))
    return acc


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


def classify_unassigned_rows(
    buckets: Dict[str, List[List[str]]],
) -> None:
    """미분류 행을 휴리스틱으로 인접 섹션에 붙인다(보수적으로)."""
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


def _rows_to_multiline_text(rows: List[List[str]]) -> str:
    lines: List[str] = []
    for row in rows:
        parts = [normalize_cell_text(c) for c in row if normalize_cell_text(c)]
        if parts:
            lines.append(" ".join(parts))
    return "\n".join(lines)


def parse_personal_info_from_table(rows: List[List[str]]) -> Dict[str, Any]:
    text_normalized = re.sub(r"\s+", " ", _rows_to_multiline_text(rows))
    out = {"인적사항": {"성명": "", "생년월일": "", "주소": "", "관리번호": ""}, "서류출력일자": ""}
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
    tn = re.sub(r"\s+", " ", _rows_to_multiline_text(rows))
    return {"등급": _parse_grade_dict_from_normalized_text(tn)}


def parse_qualifications_from_table(rows: List[List[str]]) -> List[Dict[str, Any]]:
    raw = _rows_to_multiline_text(rows)
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
    return _parse_education_from_combined_text(_rows_to_multiline_text(rows))


def parse_training_from_table(rows: List[List[str]]) -> List[Dict[str, Any]]:
    chunk = _rows_to_multiline_text(rows)
    out: List[Dict[str, Any]] = []
    for row in _extract_training_rows_from_text(chunk + "\n"):
        p = _parse_training_row(row)
        if p:
            out.append(p)
    return out


def parse_awards_from_table(rows: List[List[str]]) -> List[Dict[str, Any]]:
    """표만으로 상훈이 애매하면 빈 리스트 — 상위에서 parse_award_info 폴백."""
    if rows and any("수여일" in _row_join_for_detection(r) for r in rows):
        # TODO: 표 전용 상훈 레코드 매핑(수여일/종류/근거) — 현재는 폴백에 맡김
        pass
    return []


def parse_penalties_from_table(rows: List[List[str]]) -> Dict[str, Any]:
    """표 텍스트에서 벌점/제재 토큰을 보수적으로 수집(상세는 section_parsers 폴백)."""
    text = _rows_to_multiline_text(rows)
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


def map_rows_to_existing_schema(flat_rows: List[List[str]]) -> Dict[str, Any]:
    """표 행 전체 → page1 JSON 부분 dict."""
    buckets = detect_section_ranges(flat_rows)
    classify_unassigned_rows(buckets)
    try:
        if buckets.get("_unassigned"):
            _page1_agent_log(
                run_id="page1-table",
                hypothesis_id="U",
                location="page_1_parser.py:map_rows_to_existing_schema",
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

    merged: Dict[str, Any] = {
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
    return merged


def _fresh_page1_result() -> Dict[str, Any]:
    return {
        "인적사항": {"성명": "", "생년월일": "", "주소": "", "관리번호": ""},
        "서류출력일자": "",
        "등급": {
            "설계시공_등_직무분야": "",
            "설계시공_등_직무분야_등급": "",
            "설계시공_등_전문분야": "",
            "설계시공_등_전문분야_등급": "",
            "건설사업관리_직무분야": "",
            "건설사업관리_직무분야_등급": "",
            "건설사업관리_전문분야": "",
            "건설사업관리_전문분야_등급": "",
            "품질관리_등급": "",
        },
        "국가기술자격": [],
        "학력": [],
        "교육훈련": [],
        "상훈": [],
        "벌점및제재사항": {"벌점": "해당없음", "제재사항": "해당없음"},
        "근무처": [],
    }


def _personal_is_empty(d: Dict[str, Any]) -> bool:
    p = d.get("인적사항") or {}
    return not any(str(p.get(k) or "").strip() for k in ("성명", "생년월일", "주소", "관리번호"))


def _grade_is_empty(d: Dict[str, Any]) -> bool:
    g = d.get("등급") or {}
    if not isinstance(g, dict):
        return True
    return not any(str(v or "").strip() for v in g.values())


def _merge_page1_table_first_then_text(table_part: Dict[str, Any], text_part: Dict[str, Any]) -> Dict[str, Any]:
    out = _fresh_page1_result()

    tp = table_part.get("인적사항") or {}
    ep = text_part.get("인적사항") or {}
    out["인적사항"] = {
        "성명": (tp.get("성명") or ep.get("성명") or "").strip(),
        "생년월일": (tp.get("생년월일") or ep.get("생년월일") or "").strip(),
        "주소": (tp.get("주소") or ep.get("주소") or "").strip(),
        "관리번호": (tp.get("관리번호") or ep.get("관리번호") or "").strip(),
    }
    if _personal_is_empty({"인적사항": out["인적사항"]}) and not _personal_is_empty(text_part):
        out["인적사항"] = dict(ep)

    out["서류출력일자"] = (table_part.get("서류출력일자") or text_part.get("서류출력일자") or "").strip()

    tg = table_part.get("등급") or {}
    eg = text_part.get("등급") or {}
    if isinstance(tg, dict) and not _grade_is_empty({"등급": tg}):
        merged_g = dict(tg)
        if isinstance(eg, dict):
            for k, v in eg.items():
                if not str(merged_g.get(k) or "").strip() and str(v or "").strip():
                    merged_g[k] = v
        out["등급"] = merged_g
    else:
        out["등급"] = dict(eg) if isinstance(eg, dict) else out["등급"]

    def _uniq_license(rows: List[Any]) -> List[Dict[str, Any]]:
        seen: set[tuple[str, str, str]] = set()
        acc: List[Dict[str, Any]] = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            k = (
                str(r.get("종목") or ""),
                str(r.get("합격일") or ""),
                str(r.get("등록번호") or ""),
            )
            if k in seen:
                continue
            if any(k):
                seen.add(k)
                acc.append(r)
        return acc

    out["국가기술자격"] = _uniq_license((table_part.get("국가기술자격") or []) + (text_part.get("국가기술자격") or []))

    def _edu_key(e: Any) -> tuple:
        if not isinstance(e, dict):
            return tuple()
        return (
            str(e.get("졸업일") or ""),
            str(e.get("이전_학교명") or ""),
            str(e.get("현재_학교명") or ""),
            str(e.get("학과") or ""),
            str(e.get("학위") or ""),
            str(e.get("상태") or ""),
        )

    edus: Dict[tuple, Dict[str, Any]] = {}
    for e in (table_part.get("학력") or []) + (text_part.get("학력") or []):
        k = _edu_key(e)
        if k and k not in edus:
            edus[k] = e if isinstance(e, dict) else {}
    out["학력"] = sorted(edus.values(), key=lambda x: (x.get("졸업일") or ""))

    def _tr_key(t: Any) -> tuple:
        if not isinstance(t, dict):
            return tuple()
        return (
            str(t.get("교육기간_시작") or ""),
            str(t.get("교육기간_종료") or ""),
            str(t.get("과정명") or ""),
            str(t.get("교육기관명") or ""),
        )

    trs: Dict[tuple, Dict[str, Any]] = {}
    for t in (table_part.get("교육훈련") or []) + (text_part.get("교육훈련") or []):
        k = _tr_key(t)
        if k and k not in trs:
            trs[k] = t if isinstance(t, dict) else {}
    out["교육훈련"] = list(trs.values())

    out["상훈"] = (table_part.get("상훈") or text_part.get("상훈") or [])[:]
    if not out["상훈"]:
        out["상훈"] = text_part.get("상훈") or []

    tpen = table_part.get("벌점및제재사항") or {}
    epen = text_part.get("벌점및제재사항") or {}
    out["벌점및제재사항"] = {"벌점": "해당없음", "제재사항": "해당없음"}
    if isinstance(tpen, dict):
        tb = str(tpen.get("벌점") or "").strip()
        if tb and tb != "해당없음":
            out["벌점및제재사항"]["벌점"] = tpen.get("벌점")
        tj = tpen.get("제재사항")
        if isinstance(tj, list) and tj:
            out["벌점및제재사항"]["제재사항"] = tj
        elif str(tj or "").strip() and str(tj).strip() != "해당없음":
            out["벌점및제재사항"]["제재사항"] = tj
    if isinstance(epen, dict):
        if str(out["벌점및제재사항"].get("벌점") or "") in ("", "해당없음"):
            eb = str(epen.get("벌점") or "").strip()
            if eb:
                out["벌점및제재사항"]["벌점"] = epen.get("벌점")
        ej = epen.get("제재사항")
        if out["벌점및제재사항"].get("제재사항") in ("", "해당없음", None):
            if isinstance(ej, list) and ej:
                out["벌점및제재사항"]["제재사항"] = ej
            elif str(ej or "").strip() and str(ej).strip() != "해당없음":
                out["벌점및제재사항"]["제재사항"] = ej

    def _wp_key(w: Any) -> tuple:
        if not isinstance(w, dict):
            return tuple()
        return (
            str(w.get("근무기간_시작") or ""),
            str(w.get("근무기간_종료") or ""),
            str(w.get("이전_상호명") or ""),
            str(w.get("현재_상호명") or ""),
        )

    wps: Dict[tuple, Dict[str, Any]] = {}
    for w in (table_part.get("근무처") or []) + (text_part.get("근무처") or []):
        k = _wp_key(w)
        if not any(k):
            continue
        if k not in wps:
            wps[k] = w if isinstance(w, dict) else {}
    out["근무처"] = sorted(wps.values(), key=lambda x: (x.get("근무기간_시작") or ""))
    return out


def _collect_page1_flat_table_rows(ctx: DocumentContext, page_indices: List[int]) -> List[List[str]]:
    acc: List[List[str]] = []
    for idx in page_indices:
        page = ctx.get_page(idx)
        if page is None:
            continue
        acc.extend(_extract_page1_normalized_rows_for_page(page))
    return acc


def _finalize_workplace_list(wp_in: List[Any]) -> List[Dict[str, Any]]:
    """근무처 중복 제거·상호 보정·경계 병합(parse_page_1_from_text와 동일)."""
    wp_seen: set[tuple[str, str, str, str]] = set()
    wp_dedup: list[dict] = []
    for w in wp_in or []:
        if not isinstance(w, dict):
            continue
        k = (
            str(w.get("근무기간_시작") or ""),
            str(w.get("근무기간_종료") or ""),
            str(w.get("이전_상호명") or ""),
            str(w.get("현재_상호명") or ""),
        )
        if k in wp_seen:
            continue
        wp_seen.add(k)
        wp_dedup.append(w)

    for w in wp_dedup:
        cur = str(w.get("현재_상호명") or "").strip()
        if not cur:
            continue
        if re.search(r"\b\d{4}\.\d{2}\.\d{2}\s*[:：]\s*", cur):
            p, c = _normalize_company(cur)
            if c and c != cur:
                if not str(w.get("이전_상호명") or "").strip():
                    w["이전_상호명"] = p
                w["현재_상호명"] = c

    for i in range(1, len(wp_dedup)):
        prev = wp_dedup[i - 1]
        nxt = wp_dedup[i]
        if not isinstance(prev, dict) or not isinstance(nxt, dict):
            continue
        prev_end = str(prev.get("근무기간_종료") or "").strip()
        nxt_start = str(nxt.get("근무기간_시작") or "").strip()
        if not prev_end or not nxt_start or prev_end != nxt_start:
            continue
        prev_prev = str(prev.get("이전_상호명") or "").strip()
        prev_curr = str(prev.get("현재_상호명") or "").strip()
        nxt_prev = str(nxt.get("이전_상호명") or "").strip()
        nxt_curr = str(nxt.get("현재_상호명") or "").strip()
        if prev_prev:
            continue
        if not prev_curr or not nxt_prev or not nxt_curr:
            continue
        if (":" in nxt_prev) or ("：" in nxt_prev):
            continue
        prev["이전_상호명"] = prev_curr
        prev["현재_상호명"] = nxt_prev
    wp_dedup.sort(key=lambda x: (x.get("근무기간_시작") or ""))
    return wp_dedup


def parse_page_1_from_text(combined_text: str) -> Dict[str, Any]:
    """
    제1-3쪽 텍스트 통합 파싱: 인적사항, 등급, 국가기술자격, 학력, 교육훈련, 상훈, 벌점 및 제재사항, 근무처
    
    Args:
        combined_text: 1-3페이지 통합 텍스트 (교육훈련이 여러 페이지에 걸쳐 있음)
    
    Returns:
        Dict: 추출된 데이터
    """
    result = {
        '인적사항': {
            '성명': '',
            '생년월일': '',
            '주소': '',
            '관리번호': ''
        },
        '서류출력일자': '',
        '등급': {
            '설계시공_등_직무분야': '',
            '설계시공_등_직무분야_등급': '',
            '설계시공_등_전문분야': '',
            '설계시공_등_전문분야_등급': '',
            '건설사업관리_직무분야': '',
            '건설사업관리_직무분야_등급': '',
            '건설사업관리_전문분야': '',
            '건설사업관리_전문분야_등급': '',
            '품질관리_등급': ''
        },
        '국가기술자격': [],
        '학력': [],
        '교육훈련': [],
        '상훈': [],
        '벌점및제재사항': {"벌점": "해당없음", "제재사항": "해당없음"},
        '근무처': []
    }
    
    try:
        # 줄바꿈을 공백으로 치환 (정규식 매칭 개선)
        text_normalized = re.sub(r'\s+', ' ', combined_text)

        # 관리번호 파싱 (보통 1페이지 상단 좌측)
        # 패턴: "관리번호" 키워드 뒤에 숫자, 또는 #숫자 (숫자 사이 공백 허용)
        # - #이 붙은 경우: "# 4 1 0 0 0 4 4 8" → "#41000448"
        # - #이 없는 경우: "4 1 0 0 0 4 4 8" → "41000448"
        #   (PDF 추출기가 자릿수 사이를 공백으로 분리해 내보내는 케이스 대응)
        mgmt_num_match = re.search(
            r'관리번호\s*(?:[:：\s]*)(#\s*(?:\d\s*)+|\d(?:\s*\d)*)',
            text_normalized,
        )
        if mgmt_num_match:
            # 공백 제외 요청에 따라 모든 공백 제거
            result['인적사항']['관리번호'] = mgmt_num_match.group(1).replace(" ", "")
        else:
            # 관리번호 키워드가 매칭되지 않은 경우의 백업: 페이지 최상단에서
            # #숫자 또는 공백 분리된 숫자 블록을 찾는다.
            mgmt_num_match_alt = re.search(
                r'(#\s*(?:\d\s*)+)', text_normalized[:500]
            )
            if mgmt_num_match_alt:
                result['인적사항']['관리번호'] = mgmt_num_match_alt.group(1).replace(" ", "")
        
        # 인적사항 파싱 (항목 키는 항상 유지)
        name_kor_match = re.search(r'성명\(한글\)\s+(\S+)', text_normalized)
        if name_kor_match:
            result['인적사항']['성명'] = name_kor_match.group(1).strip()

        birth_match = re.search(r'생년월일\s+(\d{2}\.\d{2}\.\d{2})', text_normalized)
        if birth_match:
            result['인적사항']['생년월일'] = _yy_mm_dd_to_iso(birth_match.group(1).strip())

        # 서류 출력일자(발급/출력일) 파싱: 보통 1페이지 상단의 'YYYY년 M월 D일'
        issue_match = re.search(r'(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일', text_normalized)
        if issue_match:
            yyyy, mm, dd = map(int, issue_match.groups())
            try:
                result['서류출력일자'] = datetime(yyyy, mm, dd).strftime("%Y-%m-%d")
            except ValueError:
                result['서류출력일자'] = ""

        # 주소 파싱: "주소" 라벨 이후 다음 섹션 키워드 전까지
        addr_match = re.search(
            r'주소\s+(.+?)(?=\s+(?:'
            r'설계·시공|설계시공|'
            r'건설사업관리|'
            r'품질관리|'
            r'연락처|전화번호|전화|휴대전화|휴대|전자우편|이메일|'
            r'등급|국가기술자격|학력|교육훈련|상훈|벌점|제재|근무처'
            r')|$)',
            text_normalized
        )
        if addr_match:
            result['인적사항']['주소'] = addr_match.group(1).strip()

        result["등급"] = _parse_grade_dict_from_normalized_text(text_normalized)
        
        # 국가기술자격 파싱 (텍스트 폴백: 표 추출 실패 케이스 대응)
        # - 기존 코드는 '토목기사' 하드코딩이라 대부분 누락됨
        # - "국가기술자격" 섹션을 잘라서 (종목, 합격일, 등록번호) 패턴을 반복 추출
        license_section = _extract_license_section_text(combined_text)
        if license_section:
            # 등록번호는 문서/기관에 따라 형식이 다양해서 공백 전까지 토큰으로 수집(없을 수도 있음)
            # 종목명은 '...기사/산업기사/기능사/기술사'로 끝나는 문자열로 잡는다.
            lic_row_pat = re.compile(
                r"(?P<name>[가-힣A-Za-z0-9·ㆍ\(\)\-/ ]+?(?:기사|산업기사|기능사|기술사|기능장))\s+"
                r"(?P<date>\d{4}\.\d{2}\.\d{2})"
                r"(?:\s+(?P<reg>[A-Z0-9\-]{4,}|\d{4,}|\S+))?",
                flags=re.MULTILINE,
            )
            for m in lic_row_pat.finditer(license_section):
                name = (m.group("name") or "").strip()
                date_raw = (m.group("date") or "").strip()
                reg = (m.group("reg") or "").strip()
                if not name or not re.fullmatch(r"\d{4}\.\d{2}\.\d{2}", date_raw):
                    continue
                result["국가기술자격"].append(
                    {
                        "종목": name,
                        "합격일": _yyyy_mm_dd_to_iso(date_raw),
                        "등록번호": reg,
                    }
                )
        
        result["학력"] = _parse_education_from_combined_text(combined_text)
        
        # 교육훈련 파싱 (개선: 괄호 + 줄바꿈 완벽 처리)
        training_rows = _extract_training_rows_from_text(combined_text)
        print(f"  - 교육훈련 행(복원) 개수: {len(training_rows)}건")
        for row in training_rows:
            parsed = _parse_training_row(row)
            if parsed:
                result["교육훈련"].append(parsed)
        
        # 근무처: (1) 단일 컬럼(YYYY.MM 지원) 라인 기반 → (2) 기존 2열 라인 기반 → (3) 구형 정규식 보조
        wp_body = _workplace_body_lines_from_text(combined_text)
        for w in _parse_workplace_body_lines_single(wp_body):
            result["근무처"].append(w)
        for w in _parse_workplace_body_lines(wp_body):
            result["근무처"].append(w)

        work_pattern = r'(\d{4}\.\d{2}(?:\.\d{2})?)\s*~\s*(\d{4}\.\d{2}(?:\.\d{2})?|근\s*무\s*중)\s+((?:[가-힣]+\(주\)|\(주\)[가-힣]+))'
        for match in re.finditer(work_pattern, text_normalized):
            start_date = match.group(1)
            end_date = match.group(2).replace(' ', '')
            company_name = match.group(3).strip()
            match_end = match.end()

            current_company = ""
            remaining_text = text_normalized[match_end:]
            marker_alt = "|".join(re.escape(m) for m in get_company_change_markers()) or "現"
            current_match = re.match(
                rf"\s*(?:{marker_alt})\s*[:：]\s*([가-힣]+\(주\)|\(주\)[가-힣]+)",
                remaining_text,
            )
            if current_match:
                current_company = current_match.group(1).strip()
            else:
                # 2열/혼합 레이아웃에서 "종료일 + 사유:신상호"가 다음 토큰으로 바로 이어지는 케이스 보강
                # 예) "... 2003.09.21 ~ 2007.08.01 (주)A 2007.08.01 분할설립:(주)B ..."
                # → 직전 근무처(2003.09.21~2007.08.01)의 현재상호는 (주)B 로 간주해야 한다.
                boundary_match = re.match(
                    rf"\s*{re.escape(end_date)}\s+(?:{marker_alt})\s*[:：]\s*([가-힣]+\(주\)|\(주\)[가-힣]+)",
                    remaining_text,
                )
                if boundary_match:
                    current_company = boundary_match.group(1).strip()

            result['근무처'].append({
                "근무기간_시작": _workplace_date_to_iso(start_date),
                "근무기간_종료": "근무중" if ("근무중" in end_date or ("근" in end_date and "무" in end_date)) else _workplace_date_to_iso(end_date),
                "이전_상호명": "" if not current_company else company_name,
                "현재_상호명": current_company or company_name
            })

        result["근무처"] = _finalize_workplace_list(result["근무처"])
    
    except Exception as e:
        print(f"[ERROR] 제1-3쪽 텍스트 파싱 오류: {e}")
        import traceback
        traceback.print_exc()
    
    return result


def parse_page_1(ctx: DocumentContext, page_num: int = 0) -> Dict[str, Any]:
    """
    제1-3쪽 통합 파싱: 인적사항, 등급, 국가기술자격, 학력, 교육훈련, 상훈, 벌점 및 제재사항, 근무처
    (교육훈련이 1-3페이지에 걸쳐 있으므로 통합 파싱)
    
    Args:
        ctx: DocumentContext
        page_num: 시작 페이지 번호 (0부터 시작, 기본값 0)
    
    Returns:
        Dict: 추출된 데이터
    """
    result = {
        '인적사항': {
            '성명': '',
            '생년월일': '',
            '주소': '',
            '관리번호': ''
        },
        '서류출력일자': '',
        '등급': {
            '설계시공_등_직무분야': '',
            '설계시공_등_직무분야_등급': '',
            '설계시공_등_전문분야': '',
            '설계시공_등_전문분야_등급': '',
            '건설사업관리_직무분야': '',
            '건설사업관리_직무분야_등급': '',
            '건설사업관리_전문분야': '',
            '건설사업관리_전문분야_등급': '',
            '품질관리_등급': ''
        },
        '국가기술자격': [],
        '학력': [],
        '교육훈련': [],
        '상훈': [],
        '벌점및제재사항': {"벌점": "해당없음", "제재사항": "해당없음"},
        '근무처': []
    }
    
    try:
        # 교육훈련이 여러 페이지(최대 4페이지 이상)에 걸릴 수 있어,
        # '1. 기술경력' 섹션이 나오기 전까지(또는 상한) 텍스트를 통합한다.
        combined_text = ""
        page_indices: List[int] = []
        max_scan = min(8, ctx.total_pages)  # 상한: 8페이지까지만 스캔
        for i in range(max_scan):
            text = ctx.get_text(i) or ""
            page_indices.append(i)
            if text.strip():
                combined_text += text + "\n"
            if "1. 기술경력" in (text or ""):
                break
        
        if not combined_text:
            print(f"[WARN] 1-3페이지 텍스트를 찾을 수 없습니다.")
            return result
        
        print(f"  - 제1-3쪽 통합 파싱 중... (총 텍스트 길이: {len(combined_text)})")
        print(
            f"  - 제1쪽 표 기반 병합 (pdfplumber explicit_vertical_lines "
            f"X={PAGE1_VIRTUAL_LEFT_X}, X={PAGE1_VIRTUAL_RIGHT_X})..."
        )
        flat_rows = _collect_page1_flat_table_rows(ctx, page_indices)
        table_partial = (
            map_rows_to_existing_schema(flat_rows) if flat_rows else _fresh_page1_result()
        )
        text_result = parse_page_1_from_text(combined_text)
        result = _merge_page1_table_first_then_text(table_partial, text_result)
        result["근무처"] = _finalize_workplace_list(result.get("근무처") or [])
        print(f"    [OK] 교육훈련: {len(result['교육훈련'])}건 (표+텍스트 병합)")

        # 폴백(일반 규칙, 하드코딩 금지):
        # - 등급/국가기술자격은 "부분 누락"이 있어도 표/좌표 기반 파서로 보강한다.
        # - 벌점/제재는 표·텍스트가 모두 기본값이면 section_parsers 표 경로를 시도한다.

        page0 = ctx.get_page(page_num) if page_num is not None else None
        pdf_path = getattr(ctx, "pdf_path", None)

        # 0) 벌점 및 제재사항: 기본값이면 parse_penalty_and_sanction_info (표 기반)
        try:
            pen = result.get("벌점및제재사항") or {}
            bdef = str(pen.get("벌점") or "").strip() in ("", "해당없음")
            jraw = pen.get("제재사항")
            jdef = (jraw == "해당없음") or (
                isinstance(jraw, str) and not str(jraw).strip()
            ) or (isinstance(jraw, list) and len(jraw) == 0)
            if bdef and jdef and page0 is not None:
                p2 = parse_penalty_and_sanction_info(page0) or {}
                if isinstance(p2, dict) and p2:
                    if not isinstance(result["벌점및제재사항"], dict):
                        result["벌점및제재사항"] = {"벌점": "해당없음", "제재사항": "해당없음"}
                    if str(p2.get("벌점") or "").strip() and str(p2.get("벌점")).strip() != "해당없음":
                        result["벌점및제재사항"]["벌점"] = p2.get("벌점")
                    sj = p2.get("제재사항")
                    if isinstance(sj, list) and sj:
                        result["벌점및제재사항"]["제재사항"] = sj
                    elif str(sj or "").strip() and str(sj).strip() != "해당없음":
                        result["벌점및제재사항"]["제재사항"] = sj
        except Exception as ex:
            try:
                _page1_agent_log(
                    run_id="page1-table",
                    hypothesis_id="P",
                    location="page_1_parser.py:parse_page_1:penalty_fallback",
                    message="parse_penalty_and_sanction_info failed",
                    data={"error": repr(ex)},
                )
            except Exception:
                pass

        # 1) 등급: 텍스트 기반은 레이아웃/추출 편차(** 누락, 줄분리 등)에 취약하다.
        #    - 전부 비어있는 경우뿐 아니라, "부분 누락"이 있으면 표/좌표 기반 파서로 보강한다.
        try:
            grade = result.get("등급") or {}
            grade_vals = []
            if isinstance(grade, dict):
                grade_vals = [str(v or "").strip() for v in grade.values()]
            all_empty = (not grade_vals) or all((not v) for v in grade_vals)
            # 부분 누락도 보강 대상(직무/전문/등급 중 하나라도 비면 보강)
            need_any_fill = False
            if isinstance(grade, dict):
                expected_keys = [
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
                need_any_fill = any((not str(grade.get(k) or "").strip()) for k in expected_keys)

            if (all_empty or need_any_fill) and page0 is not None:
                g2 = parse_grade_info(page0, pdf_path=pdf_path, page_num=page_num) or {}
                if isinstance(g2, dict) and g2:
                    # section_parsers 출력 키 → JSON 스키마 키로 매핑
                    mapped = {
                        "설계시공_등_직무분야": str(g2.get("design_work_field") or "").strip(),
                        "설계시공_등_직무분야_등급": str(g2.get("design_work_grade") or "").strip(),
                        "설계시공_등_전문분야": str(g2.get("design_specialty") or "").strip(),
                        "설계시공_등_전문분야_등급": str(g2.get("design_specialty_grade") or "").strip(),
                        "건설사업관리_직무분야": str(g2.get("cm_work_field") or "").strip(),
                        "건설사업관리_직무분야_등급": str(g2.get("cm_work_grade") or "").strip(),
                        "건설사업관리_전문분야": str(g2.get("cm_specialty") or "").strip(),
                        "건설사업관리_전문분야_등급": str(g2.get("cm_specialty_grade") or "").strip(),
                        "품질관리_등급": str(g2.get("quality_grade") or "").strip(),
                    }
                    # 덮어쓰지 말고 "빈 값만" 보강한다(부분 누락/오탐 방지)
                    if isinstance(grade, dict):
                        merged = dict(grade)
                        for k, v in mapped.items():
                            if not str(merged.get(k) or "").strip() and str(v or "").strip():
                                merged[k] = v
                        result["등급"] = merged
                    else:
                        if any(v for v in mapped.values()):
                            result["등급"] = mapped
        except Exception:
            pass

        # 2) 국가기술자격: 0건이면 section_parsers.parse_license_info로 폴백
        try:
            lic = result.get("국가기술자격")
            if (not isinstance(lic, list)) or (len(lic) == 0):
                if page0 is not None:
                    l2 = parse_license_info(page0, pdf_path=pdf_path, page_num=page_num) or []
                    if isinstance(l2, list) and l2:
                        # section_parsers는 {type_and_grade, pass_date, registration_number} 형태를 사용
                        mapped_rows = []
                        for r in l2:
                            if not isinstance(r, dict):
                                continue
                            mapped_rows.append(
                                {
                                    "종목": str(r.get("type_and_grade") or "").strip(),
                                    "합격일": str(r.get("pass_date") or "").strip(),
                                    "등록번호": str(r.get("registration_number") or "").strip(),
                                }
                            )
                        # 완전 공백 레코드 제거
                        mapped_rows = [
                            r
                            for r in mapped_rows
                            if (r.get("종목") or "").strip()
                            or (r.get("합격일") or "").strip()
                            or (r.get("등록번호") or "").strip()
                        ]
                        if mapped_rows:
                            result["국가기술자격"] = mapped_rows
        except Exception:
            pass

        # 3) 국가기술자격 최종 정리(중복 제거 + 잘린 종목명 우선순위 해결)
        # - 동일 자격이 좌/우 컬럼 또는 표/좌표/텍스트 경로에서 중복 수집될 수 있다.
        # - 같은 (합격일, 등록번호) 조합에서 종목명이 잘린(짧은) 레코드가 섞이면 더 긴 종목명을 유지한다.
        try:
            rows = result.get("국가기술자격")
            if isinstance(rows, list) and rows:
                def _normalize_reg(s: str) -> str:
                    """
                    등록번호 셀/토큰이 추출 과정에서 깨지면서
                    '2008.09.08 08185010569A' 같이 합격일이 같이 붙는 케이스가 있다.
                    최종적으로는 '08185010569A' 같은 등록번호 토큰만 남겨 중복키가 안정적으로 맞도록 한다.
                    """
                    raw = str(s or "").strip()
                    if not raw:
                        return ""
                    # 공백/개행 정리
                    raw = re.sub(r"\s+", " ", raw).strip()
                    # 'YYYY.MM.DD' 토큰은 제거(등록번호 필드에 잘못 섞이는 케이스)
                    raw_wo_dates = re.sub(r"\b\d{4}\.\d{2}\.\d{2}\b", " ", raw)
                    raw_wo_dates = re.sub(r"\s+", " ", raw_wo_dates).strip()
                    # 후보: 영숫자/하이픈 조합(끝에 알파벳이 붙는 경우 포함)
                    cands = re.findall(r"\b[A-Z0-9][A-Z0-9\-]{5,}[A-Z0-9]?\b", raw_wo_dates)
                    if cands:
                        return cands[-1].strip()
                    # 숫자만 길게 남는 경우(최후 폴백)
                    cands2 = re.findall(r"\b\d{6,}\b", raw_wo_dates)
                    if cands2:
                        return cands2[-1].strip()
                    # 날짜만 있었던 경우는 비운다
                    if re.fullmatch(r"\d{4}\.\d{2}\.\d{2}", raw):
                        return ""
                    return raw_wo_dates

                def _norm_space(s: str) -> str:
                    return re.sub(r"\s+", " ", (s or "")).strip()

                tech_names = _load_national_tech_license_names()

                def _normalize_license_name(nm: str) -> str:
                    """
                    종목명이 접미어(기술사/기사/산업기사/기능사/기능장) 누락 등으로 잘리는 케이스를
                    `국가기술자격.csv` 목록으로 보정한다.
                    오탐 방지를 위해 "단일 후보로만 확정 가능"한 경우에만 치환한다.
                    """
                    n = _norm_space(nm)
                    if not n or not tech_names:
                        return n
                    if n in tech_names:
                        return n
                    # 접미어가 누락된 흔한 케이스를 보수적으로 보정
                    suffixes = ("기술사", "기사", "산업기사", "기능사", "기능장")
                    cands = [n + suf for suf in suffixes if (n + suf) in tech_names]
                    if len(cands) == 1:
                        return cands[0]
                    return n

                def _key(r: dict) -> tuple:
                    # // [수정] (합격일, 등록번호)가 같으면 동일 자격이다. 종목만 '기사' 등으로 쪼개진 경우
                    # (종목, 합격일) 키로는 2행이 남아 품질검증이 실패하므로 등록번호 우선 키를 쓴다.
                    dt = _norm_space(str(r.get("합격일") or ""))
                    reg_k = re.sub(r"\s+", "", _normalize_reg(str(r.get("등록번호") or "")))
                    if reg_k and len(reg_k) >= 8 and re.search(r"\d", reg_k):
                        return ("dt_reg", dt, reg_k.upper())
                    name = _norm_space(str(r.get("종목") or ""))
                    nm_key = re.sub(r"\s+", "", name)
                    return ("name_dt", nm_key, dt)

                best_by_key: dict[tuple, dict] = {}
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    nm = _normalize_license_name(str(r.get("종목") or ""))
                    dt = _norm_space(str(r.get("합격일") or ""))
                    reg = _normalize_reg(str(r.get("등록번호") or ""))
                    # 완전 공백 행은 버림
                    if (not nm) and (not dt) and (not reg):
                        continue
                    r2 = {
                        "종목": nm,
                        "합격일": dt,
                        "등록번호": reg,
                    }
                    k = _key(r2)
                    cur = best_by_key.get(k)
                    if cur is None:
                        best_by_key[k] = r2
                        continue
                    # 같은 키면 더 "완전한" 레코드 우선:
                    # // [수정] 접미사-only 종목명·등록-년도 정합·종목명 길이로 동일 (일+등록번호) 충돌 해소
                    def _row_rank(x: dict) -> tuple:
                        dtx = _norm_space(str(x.get("합격일") or ""))
                        nmx = str(x.get("종목") or "").strip()
                        rgx = _normalize_reg(str(x.get("등록번호") or ""))
                        qx = license_registration_quality_key(dtx, rgx)
                        return (
                            0 if is_standalone_license_grade_label(nmx) else 1,
                            qx[0],
                            qx[1],
                            qx[2],
                            len(nmx),
                            qx[3],
                        )

                    a = str(cur.get("종목") or "")
                    b = str(r2.get("종목") or "")
                    if a and b:
                        if a.replace(" ", "") in b.replace(" ", "") and len(b) >= len(a):
                            best_by_key[k] = r2
                            continue
                        if b.replace(" ", "") in a.replace(" ", "") and len(a) >= len(b):
                            continue
                    if _row_rank(r2) > _row_rank(cur):
                        best_by_key[k] = r2

                dedup = list(best_by_key.values())
                # // [수정] 텍스트/표 결합 후 동일 날짜·동일 종목으로 남은 상충 행 정리(section_parsers 2차 병합 정책과 동일 맥락)
                merged_qual: dict[tuple[str, str], dict] = {}
                for r_item in dedup:
                    iso2 = _norm_space(str(r_item.get("합격일") or ""))
                    nk = re.sub(r"\s+", "", str(r_item.get("종목") or "").strip())
                    kz = (iso2, nk)
                    prev = merged_qual.get(kz)
                    if prev is None:
                        merged_qual[kz] = r_item
                        continue

                    def _row_rank_inner(x: dict) -> tuple:
                        dtx = _norm_space(str(x.get("합격일") or ""))
                        nmx = str(x.get("종목") or "").strip()
                        rgx = _normalize_reg(str(x.get("등록번호") or ""))
                        qx = license_registration_quality_key(dtx, rgx)
                        return (
                            0 if is_standalone_license_grade_label(nmx) else 1,
                            qx[0],
                            qx[1],
                            qx[2],
                            len(nmx),
                            qx[3],
                        )

                    if _row_rank_inner(r_item) > _row_rank_inner(prev):
                        merged_qual[kz] = r_item

                dedup_final = list(merged_qual.values())
                dedup_final.sort(
                    key=lambda x: (str(x.get("합격일") or ""), str(x.get("종목") or ""))
                )
                result["국가기술자격"] = dedup_final
        except Exception:
            pass

        # 4) 상훈: 0건이면 parse_award_info로 폴백 (표/텍스트 기반, fitz 없이도 동작)
        try:
            if not result.get("상훈"):
                for pg_idx in range(min(4, ctx.total_pages)):
                    if "1. 기술경력" in (ctx.get_text(pg_idx) or ""):
                        break
                    pg = ctx.get_page(pg_idx)
                    if pg is None:
                        continue
                    aw = parse_award_info(pg)
                    if aw:
                        result["상훈"] = aw
                        break
                # 여전히 0건이면 schema 유지용 해당없음 1행
                if not result.get("상훈"):
                    result["상훈"] = [dict(AWARD_NOT_APPLICABLE_TEMPLATE)]
        except Exception:
            pass

        return result
    
    except Exception as e:
        print(f"[ERROR] 제1-3쪽 파싱 오류: {e}")
        import traceback
        traceback.print_exc()
    
    return result
