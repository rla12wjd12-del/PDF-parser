#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
제1쪽 파서: 인적사항, 등급, 국가기술자격, 학력, 교육훈련, 상훈, 벌점 및 제재사항, 근무처
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, Any
import csv
import re
from datetime import datetime
from parsers.section_parsers import (
    parse_award_info,
    AWARD_NOT_APPLICABLE_TEMPLATE,
    parse_penalty_and_sanction_info,
    parse_grade_info,
    parse_workplace_info,
    parse_license_info,
)
from parsers.document_context import DocumentContext


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
                    rows.append(" ".join(buf).strip())
                    buf = []
                in_table = True
                continue

            # 섹션 종료(기술경력 시작)면 전체 종료
            if "1. 기술경력" in ln:
                if buf:
                    rows.append(" ".join(buf).strip())
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
                    rows.append(" ".join(buf).strip())
                    buf = []
                in_table = False
                continue

            # 근무처 영역으로 넘어가면, 이번 페이지의 교육훈련 표는 끝난 것으로 보고 대기 상태로 전환.
            if workplace_like_pat.match(ln) or "근무기간" in ln or ln == "근무처":
                if buf:
                    rows.append(" ".join(buf).strip())
                    buf = []
                in_table = False
                continue

            if row_start_pat.match(ln):
                if buf:
                    rows.append(" ".join(buf).strip())
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
        rows.append(" ".join(buf).strip())
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
    """'근무기간'+'상호' 헤더 다음 ~ '근무처' 또는 각주 전까지 본문 줄."""
    lines = [(ln or "").strip() for ln in (raw or "").splitlines()]
    start_i = None
    for i, ln in enumerate(lines):
        if "근무기간" in ln and "상호" in ln:
            start_i = i + 1
            break
    if start_i is None:
        return []
    body: list[str] = []
    for j in range(start_i, len(lines)):
        ln = lines[j]
        if not ln:
            continue
        if ln.strip() == "근무처":
            break
        if "1. 기술경력" in ln:
            break
        if "본 증명서는" in ln and body:
            break
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
    # '現 :', '現: ' 등 공백 정리
    s = re.sub(r"現\s*:\s*", "現:", s)
    # '흡수합병 :', '흡수합병:' 등 공백 정리
    s = re.sub(r"흡수합병\s*:\s*", "흡수합병:", s)
    return s.strip()


def _normalize_company(s: str) -> tuple[str, str]:
    s = _normalize_company_markers(s)
    prev = s
    curr = ""
    # '現:' 또는 '흡수합병:' 이후를 "현재 상호"로 취급
    if "現:" in s:
        left, right = s.split("現:", 1)
        prev = left.strip()
        curr = right.strip()
    elif "흡수합병:" in s:
        left, right = s.split("흡수합병:", 1)
        prev = (left or "").strip()
        curr = (right or "").strip()
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
    return _yyyy_mm_dd_to_iso(end_raw)


def _parse_workplace_body_lines(body_lines: list[str]) -> list[dict]:
    """
    근무처 텍스트 블록을 단순 라인 기반으로 파싱한다.

    처리:
    - 2열(좌/우) 서식: '시작행'과 '종료행'을 pending으로 짝지어 2개 레코드를 생성
    - 특수행: 흡수합병/근무중
    """
    out: list[dict] = []
    pending: dict | None = None  # {"l_start","l_co","r_start","r_co"}

    start_row_pat = re.compile(
        r"^(\d{4}\.\d{2}\.\d{2})\s*~\s*(.+?)\s+(\d{4}\.\d{2}\.\d{2})\s*~\s*(.+)$"
    )
    end_row_pat = re.compile(
        r"^(\d{4}\.\d{2}\.\d{2})\s+"
        r"(?:現?:(.+?)\s+)?"
        r"(\d{4}\.\d{2}\.\d{2}|근\s*무\s*중)"
        r"\s*(?::\s*(.+))?$"
    )

    i = 0
    while i < len(body_lines):
        ln = body_lines[i]

        # 일부 추출에서는 라인 앞에 '근무처' 같은 라벨이 붙는다.
        dm = re.search(r"\d{4}\.\d{2}\.\d{2}", ln)
        if dm and dm.start() > 0:
            ln = ln[dm.start():].strip()

        merger = re.search(
            r"^(\d{4}\.\d{2}\.\d{2})\s+흡수합병:\s*(.+?)\s+(\d{4}\.\d{2}\.\d{2})\s*:\s*(.+)$",
            ln,
        )
        if merger:
            # 2열 서식에서 우측 컬럼의 종료행이 "흡수합병:" 형식으로 내려오는 경우가 있다.
            # 이때는 pending(좌/우 시작행)을 먼저 종료시킨 후, 별도 merger 레코드는 추가하지 않는다.
            if pending and (pending.get("r_start") == merger.group(1)):
                l_end = merger.group(1)  # 우측 시작일이 좌측 종료일로 내려오는 케이스
                r_end = merger.group(3)

                l_prev, l_curr = _normalize_company(str(pending.get("l_co") or ""))
                # 우측은 흡수합병을 현재상호로 반영
                r_prev, r_curr = _normalize_company(f"{merger.group(2).strip()} 흡수합병:{merger.group(4).strip()}")

                out.append(
                    {
                        "근무기간_시작": _yyyy_mm_dd_to_iso(str(pending.get("l_start") or "")),
                        "근무기간_종료": _end_value(l_end),
                        "이전_상호명": l_prev,
                        "현재_상호명": l_curr,
                    }
                )
                out.append(
                    {
                        "근무기간_시작": _yyyy_mm_dd_to_iso(str(pending.get("r_start") or "")),
                        "근무기간_종료": _end_value(r_end),
                        "이전_상호명": r_prev,
                        "현재_상호명": r_curr,
                    }
                )
                pending = None
            else:
                out.append({
                    "근무기간_시작": _yyyy_mm_dd_to_iso(merger.group(1)),
                    "근무기간_종료": _yyyy_mm_dd_to_iso(merger.group(3)),
                    "이전_상호명": merger.group(2).strip(),
                    "현재_상호명": merger.group(4).strip(),
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
            if m:
                l_end = m.group(1)
                l_curr_inline = (m.group(2) or "").strip()
                r_end = m.group(3)
                r_curr = (m.group(4) or "").strip()
            else:
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

            l_co = pending["l_co"]
            if l_curr_inline:
                l_co = l_co + " 現:" + l_curr_inline

            l_prev, l_curr = _normalize_company(l_co)
            r_prev, r_curr2 = _normalize_company(pending["r_co"] + (f" 現:{r_curr}" if r_curr else ""))

            out.append(
                {
                    "근무기간_시작": _yyyy_mm_dd_to_iso(pending["l_start"]),
                    "근무기간_종료": _end_value(l_end),
                    "이전_상호명": l_prev,
                    "현재_상호명": l_curr,
                }
            )
            out.append(
                {
                    "근무기간_시작": _yyyy_mm_dd_to_iso(pending["r_start"]),
                    "근무기간_종료": _end_value(r_end),
                    "이전_상호명": r_prev,
                    "현재_상호명": r_curr2,
                }
            )
            pending = None
            i += 1
            continue

        ongoing = re.match(r"^(\d{4}\.\d{2}\.\d{2})\s*~\s*(.+)$", ln)
        if ongoing:
            # 다음 줄에 '근무중'이 내려오는 일반 케이스
            if i + 1 < len(body_lines):
                nxt_compact = re.sub(r"\s+", "", body_lines[i + 1])
                if "근무중" in nxt_compact:
                    out.append({
                        "근무기간_시작": _yyyy_mm_dd_to_iso(ongoing.group(1)),
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
                    "근무기간_시작": _yyyy_mm_dd_to_iso(ongoing.group(1)),
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

    def _normalize_company_markers(s: str) -> str:
        """
        회사명 문자열 내 '現:' 표기를 추출 편차(전각 콜론/공백/개행)까지 흡수해 표준화한다.
        """
        s = (s or "")
        # 전각 콜론 → 반각 콜론
        s = s.replace("：", ":")
        # 개행/다중 공백 정리
        s = re.sub(r"\s+", " ", s)
        # '現 :', '現: ' 등 공백 정리
        s = re.sub(r"現\s*:\s*", "現:", s)
        # '흡수합병 :', '흡수합병:' 등 공백 정리
        s = re.sub(r"흡수합병\s*:\s*", "흡수합병:", s)
        return s.strip()

    def _normalize_company(s: str) -> tuple[str, str]:
        s = _normalize_company_markers(s)
        prev = s
        curr = ""
        # '現:' 또는 '흡수합병:' 이후를 "현재 상호"로 취급
        if "現:" in s:
            left, right = s.split("現:", 1)
            prev = left.strip()
            curr = right.strip()
        elif "흡수합병:" in s:
            left, right = s.split("흡수합병:", 1)
            prev = (left or "").strip()
            curr = (right or "").strip()
        else:
            # pdfplumber가 '現' 문자를 드롭한 경우: "구상호 :신상호" 패턴 감지
            # 예: "(주)창설토목건축사사무소 :(주)창설", "한일개발(주) :한진건설(주)"
            if ":" in s:
                left, right = s.split(":", 1)
                left = left.strip()
                right = right.strip()
                # 콜론이 상호 변경 표기일 가능성이 높은 경우만 분리
                # - 우측에 (주)가 포함되거나, 우측이 한글/영문/숫자/괄호로 일정 길이 이상인 경우
                if left and right and (
                    "(주)" in right
                    or re.search(r"[가-힣A-Za-z0-9]", right)
                ):
                    prev = left
                    curr = right
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
        return _yyyy_mm_dd_to_iso(end_raw)

    # 패턴: "좌시작 ~ 좌상호  우시작 ~ 우상호" (시작행)
    start_row_pat = re.compile(r'(\d{4}\.\d{2}\.\d{2})\s*~\s*(.+?)\s+(\d{4}\.\d{2}\.\d{2})\s*~\s*(.+)$')
    # 패턴: "좌종료 [現:좌현재상호] 우종료 [:우현재상호]" (종료행)
    # pdfplumber가 '現' 문자를 드롭해 " :(주)창설 2021.12.31" 형태가 되는 케이스도 처리
    end_row_pat = re.compile(
        r'^(\d{4}\.\d{2}\.\d{2})\s+'          # 좌종료일
        r'(?:現?:(.+?)\s+)?'                    # [옵션] 좌현재상호 (現: 또는 :로 시작)
        r'(\d{4}\.\d{2}\.\d{2}|근\s*무\s*중)'  # 우종료일
        r'\s*(?::\s*(.+))?$'                   # [옵션] 우현재상호
    )

    pending = None  # {"l_start","l_co","r_start","r_co"}

    for ln in table_lines:
        # 일부 추출에서는 라인 앞에 '근무처' 같은 라벨이 붙어 start_row_pat가 실패한다.
        # 첫 날짜 패턴부터 잘라내어 정규식을 안정화한다.
        dm = re.search(r"\d{4}\.\d{2}\.\d{2}", ln)
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
                "근무기간_시작": _yyyy_mm_dd_to_iso(pending["l_start"]),
                "근무기간_종료": _end_value(l_end),
                "이전_상호명": l_prev,
                "현재_상호명": l_curr
            })
            out.append({
                "근무기간_시작": _yyyy_mm_dd_to_iso(pending["r_start"]),
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


# 학력: 한 줄에 "졸업일 학교 … 학위[상태]"가 오고, 다음 줄이 "학력 YYYY.MM.DD …"처럼
# 섹션 라벨이 날짜 앞에 붙는 양식이 있다. 라벨이 있으면 새 레코드 시작으로 인식해야 하며,
# 이미 한 줄로 병합된 문자열은 날짜+학위 패턴으로 분리한다.
_EDU_DEG_BRACKET = re.compile(r"(학사|석사|박사과정|박사수료|박사|없음)\[([^\]]+)\]")
_EDU_ONE_LINE = re.compile(
    r"^(?:학력\s+)?(?P<date>\d{4}\.\d{2}\.\d{2})\s+"
    r"(?P<body>.+?(?:학사|석사|박사과정|박사수료|박사|없음)\[[^\]]+\])\s*$"
)
_EDU_SEG_FIND = re.compile(
    r"(?:^|\s)(?:학력\s+)?(?P<date>\d{4}\.\d{2}\.\d{2})\s+"
    r"(?P<rest>.+?(?:학사|석사|박사과정|박사수료|박사|없음)\[[^\]]+\])"
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
            '주소': ''
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
        
        # 등급 파싱 (9개 항목 구조)
        # 패턴: 토목 고급 ** 해당없음 ** 토목 특급 ** 생략 **
        grade_line_pattern = r'([\w가-힣]*)\s+(고급|중급|초급|특급|고급기술자|중급기술자|초급기술자|기술자|특급기술인|고급기술인)?\s*\*\*\s*(해당없음|생략|[\w가-힣]*)\s*\*\*\s*([\w가-힣]*)\s+(고급|중급|초급|특급|고급기술자|중급기술자|초급기술자|기술자|특급기술인|고급기술인)?\s*\*\*\s*(해당없음|생략|[\w가-힣]*)\s*\*\*'
        
        # 기본값으로 빈 문자열 초기화
        result['등급'] = {
            '설계시공_등_직무분야': '',
            '설계시공_등_직무분야_등급': '',
            '설계시공_등_전문분야': '',
            '설계시공_등_전문분야_등급': '',
            '건설사업관리_직무분야': '',
            '건설사업관리_직무분야_등급': '',
            '건설사업관리_전문분야': '',
            '건설사업관리_전문분야_등급': '',
            '품질관리_등급': ''
        }
        
        # 간단한 패턴으로 추출 시도
        simple_pattern = r'([\w가-힣]+)\s+(고급|중급|초급|특급)\s+\*\*\s*해당없음\s*\*\*\s+([\w가-힣]+)\s+(고급|중급|초급|특급)'
        match = re.search(simple_pattern, text_normalized)
        
        if match:
            # 설계시공 등
            result['등급']['설계시공_등_직무분야'] = match.group(1)
            result['등급']['설계시공_등_직무분야_등급'] = match.group(2)
            result['등급']['설계시공_등_전문분야'] = ''
            result['등급']['설계시공_등_전문분야_등급'] = ''
            
            # 건설사업관리
            result['등급']['건설사업관리_직무분야'] = match.group(3)
            result['등급']['건설사업관리_직무분야_등급'] = match.group(4)
            result['등급']['건설사업관리_전문분야'] = ''
            result['등급']['건설사업관리_전문분야_등급'] = ''
        
        # 국가기술자격 파싱 (텍스트 폴백: 표 추출 실패 케이스 대응)
        # - 기존 코드는 '토목기사' 하드코딩이라 대부분 누락됨
        # - "국가기술자격" 섹션을 잘라서 (종목, 합격일, 등록번호) 패턴을 반복 추출
        def _extract_license_section(raw: str) -> str:
            if not raw:
                return ""
            # 줄바꿈은 유지하되 과도한 공백만 축약
            t = re.sub(r"[ \t]+", " ", raw)
            # 섹션 컷: 국가기술자격 ~ 다음 섹션 전까지
            m = re.search(
                # 주의: re.MULTILINE에서 `$`는 "라인 끝"도 매칭하므로 섹션이 헤더 한 줄로 잘릴 수 있다.
                # 따라서 문자열 끝은 `\\Z`로 고정한다.
                r"(국가기술자격[\s\S]*?)(?=\n\s*(?:학력|교육훈련|상훈|벌점|제재사항|근무처)\b|\Z)",
                t,
                flags=0,
            )
            return (m.group(1) if m else "")

        license_section = _extract_license_section(combined_text)
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
        
        # 학력 파싱(멀티라인 전공 병합)
        # 원본 PDF 텍스트 추출은 대개:
        #   YYYY.MM.DD <학교명> <학과/전공...> <학위>[상태]
        # 형태이며, 학과/전공의 괄호가 다음 줄로 떨어질 수 있다(예: '전공)' 단독 라인).
        edu_rows: list[dict] = []

        edu_start_pat = re.compile(
            r"^(?:학력\s+)?(?P<date>\d{4}\.\d{2}\.\d{2})\s+"
            r"(?P<body>.+?(?:학사|석사|박사|없음)\[[^\]]+\])\s*$"
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
            # 헤더 라인 스킵
            if "졸업일" in line and "학교명" in line and ("학과" in line or "전공" in line) and "학위" in line:
                continue

            if edu_start_pat.match(line):
                if buf_line:
                    merged_lines.append(buf_line.strip())
                buf_line = line
                continue

            if buf_line:
                # continuation 후보:
                # - 새 학력/교육훈련/상훈/근무처/기술경력 시작이 아니고
                # - 날짜(졸업일)로 새로 시작하지 않으면
                # 직전 학력 라인에 이어붙인다.
                # FIX: '학력 YYYY.MM.DD …' 는 새 학력 행이므로 병합하지 않고 버퍼를 비운 뒤
                #      다음 루프에서 edu_start_pat에 걸리게 한다.
                if (not section_start_like.match(line)) and (
                    not re.match(r"^(?:학력\s+)?\d{4}\.\d{2}\.\d{2}\b", line)
                ):
                    buf_line = (buf_line + " " + line).strip()
                    continue
                merged_lines.append(buf_line.strip())
                buf_line = ""
                # 완결 학력 줄 또는 '날짜만 먼저 나오고 학위는 다음 줄'인 새 블록 시작
                if edu_start_pat.match(line):
                    buf_line = line
                elif re.match(r"^(?:학력\s+)?\d{4}\.\d{2}\.\d{2}\b", line):
                    buf_line = line
                continue

        if buf_line:
            merged_lines.append(buf_line.strip())

        # 복수 학력이 한 덩어리로 붙은 경우 분리 + 정규화 줄 목록
        edu_line_segments: list[str] = []
        for line in merged_lines:
            segs = _split_merged_education_line(line)
            if len(segs) > 1:
                edu_line_segments.extend(segs)
            elif len(segs) == 1:
                edu_line_segments.append(segs[0])
            else:
                # 분리 실패 시 기존 한 줄 시도(라벨만 제거)
                t = _strip_leading_hakryeok_label(line)
                if t != line.strip():
                    t2 = _split_merged_education_line(t)
                    edu_line_segments.extend(t2 if t2 else [t])
                else:
                    edu_line_segments.append(line.strip())

        for line in edu_line_segments:
            if not any(k in line for k in ["학사[", "석사[", "박사과정[", "박사수료[", "박사[", "없음["]):
                continue
            line = _strip_leading_hakryeok_label(line)
            # 패턴이 변형되어도(전공 ')'이 뒤로 붙는 등) 마지막 학위[상태] 토큰을 기준으로 분리한다.
            m_date = re.match(r"^(?P<date>\d{4}\.\d{2}\.\d{2})\s+(?P<rest>.+)$", line)
            if not m_date:
                continue
            date_raw = (m_date.group("date") or "").strip()
            rest = (m_date.group("rest") or "").strip()

            deg_hits = list(_EDU_DEG_BRACKET.finditer(rest))
            if not deg_hits:
                continue
            last = deg_hits[-1]
            degree = (last.group(1) or "").strip()
            status = (last.group(2) or "").strip()
            before = rest[: last.start()].strip()
            after = rest[last.end() :].strip()

            # 학교명에 괄호·공백이 섞이면 첫 토큰만 학교로 보면 깨짐
            # (예: "경상대학교( :경상국립대학교) 토목공학과" → 학교=앞부분 전체, 학과=마지막 토큰)
            words = [p for p in before.split(" ") if p]
            # 헤더 단어 '학력'이 학과 뒤에 토큰으로 붙는 경우
            if words and words[-1] == "학력":
                words = words[:-1]
            if len(words) < 2:
                continue
            major = words[-1].strip()
            school = " ".join(words[:-1]).strip()
            if after:
                major = (major + " " + after).strip()
            # 표 헤더 '학력' 이 학과 끝에 붙는 추출 오류 제거
            major = re.sub(r"\s+학력\s*$", "", (major or "").strip()).strip()

            if not school or not major:
                continue
            edu_rows.append(
                {
                    "졸업일": _yyyy_mm_dd_to_iso(date_raw),
                    "학교명": school,
                    "학과": major,
                    "학위": degree,
                    "상태": status,
                }
            )

        # 중복 제거 + 날짜순 정렬
        if edu_rows:
            seen = set()
            dedup = []
            for e in edu_rows:
                key = (e.get("졸업일", ""), e.get("학교명", ""), e.get("학과", ""), e.get("학위", ""), e.get("상태", ""))
                if key in seen:
                    continue
                seen.add(key)
                dedup.append(e)
            dedup.sort(key=lambda x: (x.get("졸업일") or ""))
            result["학력"] = dedup
        
        # 교육훈련 파싱 (개선: 괄호 + 줄바꿈 완벽 처리)
        training_rows = _extract_training_rows_from_text(combined_text)
        print(f"  - 교육훈련 행(복원) 개수: {len(training_rows)}건")
        for row in training_rows:
            parsed = _parse_training_row(row)
            if parsed:
                result["교육훈련"].append(parsed)
        
        # 근무처: 표준 서식(근무기간·상호 블록) 줄 단위 파싱 + 구형 단일 정규식 보조
        wp_body = _workplace_body_lines_from_text(combined_text)
        for w in _parse_workplace_body_lines(wp_body):
            result["근무처"].append(w)

        work_pattern = r'(\d{4}\.\d{2}\.\d{2})\s*~\s*(\d{4}\.\d{2}\.\d{2}|근\s*무\s*중)\s+((?:[가-힣]+\(주\)|\(주\)[가-힣]+))'
        for match in re.finditer(work_pattern, text_normalized):
            start_date = match.group(1)
            end_date = match.group(2).replace(' ', '')
            company_name = match.group(3).strip()
            match_end = match.end()

            current_company = ""
            remaining_text = text_normalized[match_end:]
            current_match = re.match(r'\s*(?:現:|흡수합병\s*:)\s*([가-힣]+\(주\)|\(주\)[가-힣]+)', remaining_text)
            if current_match:
                current_company = current_match.group(1).strip()

            result['근무처'].append({
                "근무기간_시작": _yyyy_mm_dd_to_iso(start_date),
                "근무기간_종료": "근무중" if ("근무중" in end_date or ("근" in end_date and "무" in end_date)) else _yyyy_mm_dd_to_iso(end_date),
                "이전_상호명": "" if not current_company else company_name,
                "현재_상호명": current_company or company_name
            })

        # 완전 동일 레코드 제거 후 정렬
        wp_seen: set[tuple[str, str, str, str]] = set()
        wp_dedup: list[dict] = []
        for w in result["근무처"]:
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
        wp_dedup.sort(key=lambda x: (x.get("근무기간_시작") or ""))
        result["근무처"] = wp_dedup
    
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
            '주소': ''
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
        max_scan = min(8, ctx.total_pages)  # 상한: 8페이지까지만 스캔
        for i in range(max_scan):
            text = ctx.get_text(i) or ""
            if text.strip():
                combined_text += text + "\n"
            if "1. 기술경력" in (text or ""):
                break
        
        if not combined_text:
            print(f"[WARN] 1-3페이지 텍스트를 찾을 수 없습니다.")
            return result
        
        print(f"  - 제1-3쪽 통합 파싱 중... (총 텍스트 길이: {len(combined_text)})")
        result = parse_page_1_from_text(combined_text)
        print(f"    [OK] 교육훈련: {len(result['교육훈련'])}건")

        # 폴백(일반 규칙, 하드코딩 금지):
        # - 텍스트 기반 파싱은 PDF 추출 편차에 취약하므로,
        #   등급/국가기술자격이 "전부 비어있는" 경우에 한해 표/좌표 기반 파서를 추가로 시도한다.
        # - 단, 다른 섹션(인적사항/학력/교육훈련/근무처 등)은 기존 정책대로 텍스트 기반을 우선한다.

        page0 = ctx.get_page(page_num) if page_num is not None else None
        pdf_path = getattr(ctx, "pdf_path", None)

        # 1) 등급: 전부 빈 값이면 section_parsers.parse_grade_info로 폴백
        try:
            grade = result.get("등급") or {}
            grade_vals = []
            if isinstance(grade, dict):
                grade_vals = [str(v or "").strip() for v in grade.values()]
            all_empty = (not grade_vals) or all((not v) for v in grade_vals)
            if all_empty and page0 is not None:
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
                    # 의미 있는 값이 하나라도 있으면 채택
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
                    # 등록번호가 있으면 가장 안정적인 키
                    dt = _norm_space(str(r.get("합격일") or ""))
                    reg = _normalize_reg(str(r.get("등록번호") or ""))
                    name = _norm_space(str(r.get("종목") or ""))
                    if reg:
                        return ("reg", dt, reg)
                    # 등록번호가 비는 문서도 있어 보조 키 사용
                    return ("name", dt, re.sub(r"\s+", "", name))

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
                    # 1) 종목명이 더 긴 쪽
                    # 2) 등록번호 유무
                    # 3) 합격일 유무
                    def _score(x: dict) -> tuple:
                        name_len = len(str(x.get("종목") or "").strip())
                        has_reg = 1 if str(x.get("등록번호") or "").strip() else 0
                        has_dt = 1 if str(x.get("합격일") or "").strip() else 0
                        return (name_len, has_reg, has_dt)

                    # 접두/접미 관계면 긴 쪽으로 승격(잘림 복원)
                    a = str(cur.get("종목") or "")
                    b = str(r2.get("종목") or "")
                    if a and b:
                        if a.replace(" ", "") in b.replace(" ", "") and len(b) >= len(a):
                            best_by_key[k] = r2
                            continue
                        if b.replace(" ", "") in a.replace(" ", "") and len(a) >= len(b):
                            continue
                    if _score(r2) > _score(cur):
                        best_by_key[k] = r2

                dedup = list(best_by_key.values())
                # 정렬: 합격일, 종목
                dedup.sort(key=lambda x: (str(x.get("합격일") or ""), str(x.get("종목") or "")))
                result["국가기술자격"] = dedup
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
