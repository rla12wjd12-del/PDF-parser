#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
요약 페이지 파서: 분야별 참여기간 인정일 및 건설기술진흥법령 외 자격·학력 등

PDF 텍스트 추출 시 공사종류별/직무전문분야별 인정일수가 2열로 한 줄에 나란히 나오는
레이아웃에 대응한다.

예)
  공사종류별 인정일수 현황 직무/전문분야별 인정일수 현황
  고속국도,장대교량(100m이상),터널 480 일 건축/건축품질관리 1,892 일
  고속도로,교량 984 일 토목/토목시공 1,973 일
  ...
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, List, Any
import re

from parsers.document_context import DocumentContext


_DAYS_ENTRY_RE = re.compile(r"(.+?)\s+([\d,]+)\s*일")
_SUMMARY_END_ANCHOR_RE = re.compile(r"(?m)^\s*2\.\s*건설기술진흥법령\s*외\b")

_SUMMARY_START_HINT_RE = re.compile(r"(공사종류별\s*인정일수\s*현황|분야별\s*참여기간\s*인정일)")
_SUMMARY_RIGHT_HEADER_RE = re.compile(r"(직무\s*/\s*전문분야별\s*인정일수\s*현황|직무/전문분야별\s*인정일수\s*현황)")
_SUMMARY_LEFT_HEADER_RE = re.compile(r"(공사종류별\s*인정일수\s*현황)")

_NUM_ONLY_RE = re.compile(r"^\s*[\d,]+\s*$")
_NUM_WITH_IL_RE = re.compile(r"^\s*[\d,]+\s*일\s*$")
_IL_ONLY_RE = re.compile(r"^\s*일\s*$")

# 좌측 컬럼의 인정일수가 라벨 없이 줄 시작에 등장하는 패턴.
# (좌측 라벨이 길어 위/아래 줄로 분리되고, 인정일수만 가운데 줄에 남는 경우 발생)
# 예) "7 일 토목/(미기재) 7,153 일"
_ORPHAN_LEFT_DAYS_RE = re.compile(r"^\s*\d+(?:,\d+)*\s*일\b")


def _is_footer_like_line(ln: str) -> bool:
    s = (ln or "").strip()
    if not s:
        return True
    if s.startswith("※"):
        return True
    if "자격" in s and "학력" in s:
        return True
    # 푸터/바코드 안내 문구 제외
    if any(kw in s for kw in ["문서하단", "바코드", "위·변조", "확인번호"]):
        return True
    return False


def _normalize_and_stitch_lines(text: str) -> list[str]:
    """
    요약 섹션 텍스트는 추출 품질에 따라 '라벨/숫자/일'이 줄바꿈으로 분해될 수 있다.
    이 함수는 기본적인 줄 정규화 + 대표적인 분리 패턴을 재조립한다.
    """
    raw = [re.sub(r"\s+", " ", (ln or "")).strip() for ln in (text or "").splitlines()]
    raw = [ln for ln in raw if ln]

    out: list[str] = []
    i = 0
    while i < len(raw):
        cur = raw[i].strip()
        nxt = raw[i + 1].strip() if (i + 1) < len(raw) else ""

        # 1) 숫자만 다음 줄에 '일'이 붙는 케이스: "123" + "일" -> "123 일"
        if _NUM_ONLY_RE.match(cur) and nxt and _IL_ONLY_RE.match(nxt):
            out.append(f"{cur} 일")
            i += 2
            continue

        # 2) 라벨만 있고 다음 줄이 숫자(+일)인 케이스: "항만" + "314 일" -> "항만 314 일"
        #    라벨 줄은 헤더/푸터/앵커/이미 일수 매칭이 있는 줄이 아니어야 한다.
        if (
            cur
            and (not _DAYS_ENTRY_RE.search(cur))
            and (not _SUMMARY_END_ANCHOR_RE.search(cur))
            and (not _SUMMARY_LEFT_HEADER_RE.search(cur))
            and (not _SUMMARY_RIGHT_HEADER_RE.search(cur))
            and (not _is_footer_like_line(cur))
            and nxt
            and (_NUM_WITH_IL_RE.match(nxt) or _NUM_ONLY_RE.match(nxt))
        ):
            if _NUM_ONLY_RE.match(nxt):
                # 다음 줄이 숫자만이면 그 다음에 '일'이 있을 수도 있음
                nn = raw[i + 2].strip() if (i + 2) < len(raw) else ""
                if nn and _IL_ONLY_RE.match(nn):
                    out.append(f"{cur} {nxt} 일")
                    i += 3
                    continue
                # 숫자만 있고 '일'이 없으면 보수적으로 결합하지 않는다.
            else:
                out.append(f"{cur} {nxt}")
                i += 2
                continue

        out.append(cur)
        i += 1

    # 좌측 컬럼 라벨이 길어서 PDF 렌더링 시 2~3줄에 걸쳐 분리되는 경우를 보정한다.
    # (요약 섹션 범위 안에서만 적용해 다른 섹션에 영향이 가지 않도록 한다.)
    return _stitch_wrapped_left_labels_in_section(out)


def _is_summary_label_only_line(ln: str) -> bool:
    """
    요약 섹션 내에서 '라벨만 있는 줄'(인정일수가 없는 줄) 여부를 판정한다.
    - 헤더/푸터/앵커/숫자(만)/'일'(만) 줄은 제외.
    """
    s = (ln or "").strip()
    if not s:
        return False
    if _DAYS_ENTRY_RE.search(s):
        return False
    if _SUMMARY_END_ANCHOR_RE.search(s):
        return False
    if _SUMMARY_LEFT_HEADER_RE.search(s):
        return False
    if _SUMMARY_RIGHT_HEADER_RE.search(s):
        return False
    if _is_footer_like_line(s):
        return False
    if _NUM_ONLY_RE.match(s) or _NUM_WITH_IL_RE.match(s) or _IL_ONLY_RE.match(s):
        return False
    if s.startswith("2."):
        return False
    return True


def _has_unbalanced_open_paren(s: str) -> bool:
    """
    문자열에 닫히지 않은 '(' 가 있는지 확인한다.
    - 한국어 PDF의 공사종류 라벨은 일반적으로 괄호가 균형있게 짝지어진다.
    - 라벨이 줄바꿈된 경우, 위쪽에는 '(' 만, 아래쪽에 ')' 만 분리되어 등장할 수 있다.
    """
    s = s or ""
    opens = s.count("(") + s.count("（")
    closes = s.count(")") + s.count("）")
    return opens > closes


def _stitch_wrapped_left_labels_in_section(lines: list[str]) -> list[str]:
    """
    공사종류별 인정일수 섹션 내에서, 좌측 라벨이 길어 줄바꿈된 패턴을 감지해 합친다.

    PDF 추출 패턴은 텍스트 추출기의 라인 분리 동작에 따라 두 가지로 나타난다.

    [CASE A] 좌측 인정일수가 가운데 줄에만 남는 경우 (orphan-days)
        관광휴게시설(공원.유원지.관광지부          <- 좌측 라벨 상단 (라벨만)
        7 일 토목/(미기재) 7,153 일                <- 좌측 인정일수 + 우측 항목 (라벨 없이 시작)
        수시 설)                                   <- 좌측 라벨 하단 (라벨만)

    [CASE B] 좌측 라벨 상단과 인정일수가 같은 줄로 합쳐졌고 하단만 분리된 경우
        관광휴게시설(공원.유원지.관광지부 161 일   <- 상단 라벨 + 인정일수 (괄호 미닫힘)
        수시 설)                                   <- 하단 라벨 (라벨만)

    합친 결과)
        관광휴게시설(공원.유원지.관광지부수시설) 7 일 토목/(미기재) 7,153 일
        관광휴게시설(공원.유원지.관광지부수시설) 161 일

    한국어 라벨은 줄바꿈 시 의도된 공백이 거의 발생하지 않는다(이 도메인의 공사종류
    값은 모두 공백 없는 형태). PDF 폰트 렌더링 시 인접 글자 사이 미세한 간격을
    pdfplumber가 단어 경계로 해석해 "수시 설)"처럼 잘못된 공백이 끼어드는 경우가
    있어, 합칠 때 각 라벨 조각의 내부 공백도 함께 제거한다.
    """
    if not lines:
        return lines

    # 섹션 범위 탐지: 좌측 헤더가 등장하면 시작, 종료 앵커("2. 건설기술진흥법령 외")가
    # 등장하거나 줄이 "2."로 시작하면 종료한다. 헤더가 없으면 처리하지 않는다.
    start_idx = -1
    end_idx = len(lines)
    for i, ln in enumerate(lines):
        if start_idx < 0 and _SUMMARY_LEFT_HEADER_RE.search(ln):
            start_idx = i
            continue
        if start_idx >= 0:
            if _SUMMARY_END_ANCHOR_RE.search(ln) or ln.startswith("2."):
                end_idx = i
                break

    if start_idx < 0:
        return lines

    head = list(lines[:start_idx])
    section = list(lines[start_idx:end_idx])
    tail = list(lines[end_idx:])

    new_section: list[str] = []
    i = 0
    while i < len(section):
        cur = section[i]

        # CASE A: 줄이 좌측 인정일수만으로 시작 → 상/하 라벨-only 줄을 합친다.
        if _ORPHAN_LEFT_DAYS_RE.match(cur):
            above_labels: list[str] = []
            while new_section and _is_summary_label_only_line(new_section[-1]):
                above_labels.insert(0, new_section.pop())
            below_labels: list[str] = []
            j = i + 1
            while j < len(section) and _is_summary_label_only_line(section[j]):
                below_labels.append(section[j])
                j += 1

            if above_labels or below_labels:
                merged_chunks = above_labels + below_labels
                merged_label = "".join(re.sub(r"\s+", "", c) for c in merged_chunks)
                stitched = (merged_label + " " + cur).strip() if merged_label else cur
                new_section.append(stitched)
                i = j
                continue

        # CASE B: 첫 인정일수 매칭의 라벨에 닫히지 않은 '('가 있고, 다음 줄이 라벨-only인 경우
        #         → 다음 줄(들)을 라벨 하단으로 흡수해 라벨을 보강한다.
        m = _DAYS_ENTRY_RE.search(cur)
        if m:
            label_part = (m.group(1) or "").strip()
            if _has_unbalanced_open_paren(label_part):
                below_labels: list[str] = []
                j = i + 1
                # 괄호가 균형을 이룰 때까지(또는 라벨-only 줄이 끝날 때까지) 흡수한다.
                while j < len(section) and _is_summary_label_only_line(section[j]):
                    below_labels.append(section[j])
                    extra_so_far = "".join(re.sub(r"\s+", "", c) for c in below_labels)
                    j += 1
                    if not _has_unbalanced_open_paren(label_part + extra_so_far):
                        break

                if below_labels:
                    extra = "".join(re.sub(r"\s+", "", c) for c in below_labels)
                    new_label = label_part + extra
                    # cur 의 라벨 부분만 교체. m.end(1) 이후(공백 + 인정일수 ...)는 그대로 유지.
                    new_cur = new_label + cur[m.end(1):]
                    new_section.append(new_cur)
                    i = j
                    continue

        new_section.append(cur)
        i += 1

    return head + new_section + tail


def _parse_summary_text(text: str) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {
        "공사종류별인정일수": [],
        "직무전문분야별인정일수": [],
    }
    if not text:
        return result

    lines = _normalize_and_stitch_lines(text)

    # 섹션 헤더 모드 추적
    # - left: 공사종류
    # - right: 직무/전문분야
    # - both: 같은 줄에 2개 항목이 나란히 등장할 수 있음(2열)
    mode: str | None = None
    in_section = False

    left_seen: set[str] = set()
    right_seen: set[str] = set()

    def _add_left(label: str, days_str: str) -> None:
        key = re.sub(r"\s+", " ", (label or "").strip())
        if not key:
            return
        if key in left_seen:
            return
        left_seen.add(key)
        result["공사종류별인정일수"].append({"공사종류": key, "인정일수": days_str})

    def _add_right(label: str, days_str: str) -> None:
        raw_label = re.sub(r"\s+", " ", (label or "").strip())
        if not raw_label:
            return
        # 우측(직무/전문분야) 컬럼은 문서에 따라 슬래시가 없을 수 있다.
        # 예: "... 고속도로 31 일 기타 531 일"에서 우측 라벨은 "기타"
        if "/" in raw_label:
            jf, sf = [p.strip() for p in raw_label.split("/", 1)]
        else:
            # '(미기재)' 같은 케이스는 전문분야로 두고 직무분야는 비운다.
            if raw_label.startswith("(") and "미기재" in raw_label:
                jf, sf = "", raw_label
            else:
                # 기본: 직무분야만 있는 것으로 간주
                jf, sf = raw_label, ""
        if not jf and not sf:
            return
        key = f"{jf}|{sf}"
        if key in right_seen:
            return
        right_seen.add(key)
        result["직무전문분야별인정일수"].append({"직무분야": jf, "전문분야": sf, "인정일수": days_str})

    for ln in lines:
        if not ln:
            continue

        # 섹션 종료: '2.'로 시작하는 다음 대섹션(자격/학력...) 시작 지점에서 중단한다.
        # NOTE: 페이지 제목 줄에도 "건설기술진흥법령 외 ..." 문구가 포함될 수 있으므로
        #       단순 부분 문자열 포함으로는 중단하지 않는다.
        if ln.startswith("2.") or _SUMMARY_END_ANCHOR_RE.search(ln):
            # 요약 섹션 밖에서의 '2.' 노이즈를 피하기 위해, 섹션에 들어온 후에만 종료한다.
            if in_section:
                break
            continue
        if _is_footer_like_line(ln):
            continue

        # 헤더 감지
        has_left_h = bool(_SUMMARY_LEFT_HEADER_RE.search(ln))
        has_right_h = bool(_SUMMARY_RIGHT_HEADER_RE.search(ln))
        if has_left_h or has_right_h:
            in_section = True
            if has_left_h and has_right_h:
                mode = "both"
            elif has_left_h:
                mode = "left"
            elif has_right_h:
                mode = "right"
            continue

        if not in_section:
            continue

        hits = list(_DAYS_ENTRY_RE.finditer(ln))
        if not hits:
            continue

        # 2열 케이스(한 줄에 2개 이상): 왼쪽=공사종류, 오른쪽=직무/전문분야
        if len(hits) >= 2:
            h0, h1 = hits[0], hits[1]
            lab0 = (h0.group(1) or "").strip()
            day0 = (h0.group(2) or "").replace(",", "").strip()
            lab1 = (h1.group(1) or "").strip()
            day1 = (h1.group(2) or "").replace(",", "").strip()
            if lab0 and day0 and not ("인정일수" in lab0 and "합계" in lab0):
                _add_left(lab0, day0)
            if lab1 and day1 and not ("인정일수" in lab1 and "합계" in lab1):
                _add_right(lab1, day1)
            continue

        # 단일 항목(1개 매칭): mode에 따라 분기
        h = hits[0]
        label = (h.group(1) or "").strip()
        days_str = (h.group(2) or "").replace(",", "").strip()
        if not label or not days_str:
            continue
        if "인정일수" in label and "합계" in label:
            continue

        if mode == "right":
            _add_right(label, days_str)
        elif mode == "left":
            _add_left(label, days_str)
        else:
            # mode를 확정 못한 경우: '/' 유무로만 보수적으로 판정
            if "/" in label:
                _add_right(label, days_str)
            else:
                _add_left(label, days_str)

    return result


_COORD_DAYS_NUM_RE = re.compile(r"^[\d,]+$")
_COORD_LEFT_COLUMN_FALLBACK_X = 300.0  # 좌측(공사종류별)·우측(직무/전문분야별) 열 경계(우측 머리글을 못 찾을 때)
_COORD_ROW_ATTACH_DY = 12.0  # 줄바꿈된 이름 줄을 숫자 행에 붙일 수 있는 최대 세로 거리(pt). 행 간격은 약 20pt


def _parse_left_column_by_coordinates(ctx: DocumentContext, page_num: int) -> List[Dict[str, Any]] | None:
    """
    [hardening stage5] '공사종류별 인정일수 현황'(좌측 열)을 PyMuPDF 단어 좌표로 읽는다.

    텍스트 줄 기반 파싱은 이름이 두 줄로 줄바꿈되면(숫자 행의 위·아래로 나뉨) 이웃 행 이름과 붙거나 잘린다.
    여기서는 각 행 오른쪽의 'N 일'을 기준점으로 삼고, 이름 줄을 세로로 가장 가까운 기준점 행에 붙인다.
    항목 합계가 표 아래 '인정일수 합계'와 정확히 같을 때만 결과를 돌려주고, 아니면 None.
    """
    fitz_doc = getattr(ctx, "_fitz_doc", None)
    if fitz_doc is None:
        return None
    rows: List[Dict[str, Any]] = []
    total_days: int | None = None
    right_x = _COORD_LEFT_COLUMN_FALLBACK_X
    started = False
    for pno in range(page_num, fitz_doc.page_count):
        words = fitz_doc[pno].get_text("words")
        # 페이지 본문 시작 y: 제목 줄('분야별 참여기간 인정일 및 …') 아래
        title_y = max((w[3] for w in words if w[4] == "분야별" and w[1] < 90), default=85.0)
        top_y = title_y + 2
        if not started:
            header = [w for w in words if w[4].startswith("공사종류별")]
            if not header:
                continue
            started = True
            top_y = max(top_y, header[0][3] + 2)
            rh = [w for w in words if w[4].startswith("직무/전문분야별")]
            if rh:
                # 우측 열 이름은 우측 머리글보다 왼쪽(약 x=315)에서 시작한다. 좌측 열의 끝은 좌측 열 'N 일'의
                # '일'(우측 머리글보다 충분히 왼쪽에 있는 것) 중 가장 오른쪽 끝으로 잡는다.
                left_il_ends = [w[2] for w in words if w[4] == "일" and w[0] < rh[0][0] - 40]
                if left_il_ends:
                    right_x = max(left_il_ends) + 2
                else:
                    left_header_end = max(w[2] for w in words if abs(w[1] - header[0][1]) < 2 and w[0] < rh[0][0])
                    right_x = min((left_header_end + rh[0][0]) / 2.0, _COORD_LEFT_COLUMN_FALLBACK_X)
        # 읽기 종료 y: 주석(※), 다음 대섹션('2.'), 쪽 하단 발급 안내문('본 증명서는 …')
        stop_y = min(
            [
                w[1]
                for w in words
                if w[4].startswith("※") or (w[4] == "2." and w[0] < 60) or w[4] == "증명서는"
            ]
            or [1e9]
        )
        left = sorted(
            [w for w in words if w[0] < right_x and top_y <= w[1] < stop_y],
            key=lambda w: (w[5], w[6], w[7]),
        )
        # 줄 단위로 묶기
        lines: Dict[tuple, List[Any]] = {}
        for w in left:
            lines.setdefault((w[5], w[6]), []).append(w)
        anchors: List[Dict[str, Any]] = []
        label_lines: List[tuple[float, str]] = []
        for ws in lines.values():
            ws.sort(key=lambda w: w[0])
            toks = [w[4] for w in ws]
            # 줄 끝의 'N 일' → 기준점, 앞쪽 단어는 같은 행의 이름
            if len(toks) >= 2 and toks[-1] == "일" and _COORD_DAYS_NUM_RE.match(toks[-2]):
                days = int(toks[-2].replace(",", ""))
                name = " ".join(toks[:-2]).strip()
                anchors.append({"y": ws[-2][1], "days": days, "parts": [(ws[0][1], name)] if name else []})
            else:
                label_lines.append((ws[0][1], " ".join(toks).strip()))
        for y, text in label_lines:
            if not anchors:
                return None
            near = min(anchors, key=lambda a: abs(a["y"] - y))
            if abs(near["y"] - y) > _COORD_ROW_ATTACH_DY:
                return None
            near["parts"].append((y, text))
        for a in sorted(anchors, key=lambda a: a["y"]):
            parts = [t for _, t in sorted(a["parts"])]
            label = "".join(p.replace(" ", "") for p in parts) if len(parts) > 1 else (parts[0] if parts else "")
            label = re.sub(r"\s+", " ", label).strip()
            if re.sub(r"\s+", "", label) == "인정일수합계":
                total_days = a["days"]
                break
            if not label:
                return None
            rows.append({"공사종류": label, "인정일수": str(a["days"])})
        if total_days is not None:
            break
    if total_days is None or not rows:
        return None
    if sum(int(r["인정일수"]) for r in rows) != total_days:
        return None
    return rows


def parse_page_summary(ctx: DocumentContext, page_num: int) -> Dict[str, List[Dict[str, Any]]]:
    """
    요약 페이지 파싱: 분야별 참여기간 인정일

    Args:
        ctx: DocumentContext
        page_num: 페이지 번호 (0부터 시작)

    Returns:
        Dict: 분야별 참여기간 인정일 데이터
    """
    result: Dict[str, List[Dict[str, Any]]] = {
        "공사종류별인정일수": [],
        "직무전문분야별인정일수": [],
    }

    try:
        if page_num >= ctx.total_pages:
            print(f"[WARN] 페이지 번호 오류: {page_num + 1}페이지는 존재하지 않습니다.")
            return result

        combined = ""
        merged_until = page_num
        end_anchor_seen = False

        # 증분 기반 종료: 새 페이지를 추가해도 추출 항목 수가 늘지 않는 상태가 연속되면 중단
        no_growth_streak = 0
        NO_GROWTH_STREAK_LIMIT = 2

        best_parsed: Dict[str, List[Dict[str, Any]]] = {
            "공사종류별인정일수": [],
            "직무전문분야별인정일수": [],
        }
        best_total = 0

        for i in range(page_num, ctx.total_pages):
            text = ctx.get_text(i) or ""
            if text.strip():
                combined += text + "\n"
            merged_until = i

            if _SUMMARY_END_ANCHOR_RE.search(text or ""):
                end_anchor_seen = True

            parsed = _parse_summary_text(combined)
            total = len(parsed.get("공사종류별인정일수") or []) + len(parsed.get("직무전문분야별인정일수") or [])

            if total > best_total:
                best_total = total
                best_parsed = parsed
                no_growth_streak = 0
            else:
                # 섹션 시작 힌트가 아직 안 잡히면 성급하게 종료하지 않는다.
                # (summary_start가 살짝 앞/뒤로 어긋나는 문서 대응)
                if _SUMMARY_START_HINT_RE.search(combined):
                    no_growth_streak += 1

            if _SUMMARY_START_HINT_RE.search(combined) and (no_growth_streak >= NO_GROWTH_STREAK_LIMIT):
                break

        end_page_display = merged_until + 1
        anchor_info = " (종료 앵커 감지)" if end_anchor_seen else ""
        growth_info = f" (증분종료: no_growth_streak={no_growth_streak})" if best_total > 0 else ""
        print(
            f"  - 분야별 참여기간 인정일 파싱 중... "
            f"(페이지 {page_num + 1}~{end_page_display}){anchor_info}{growth_info}"
        )

        result.update(best_parsed)

        # [hardening stage5] 좌측(공사종류별)은 좌표 기반 결과가 합계 검산을 통과하면 그것을 쓴다.
        try:
            coord_left = _parse_left_column_by_coordinates(ctx, page_num)
        except Exception as e:  # 좌표 방식 실패 시 텍스트 결과 유지
            coord_left = None
            print(f"[WARN] 요약 좌측 열 좌표 파싱 실패(텍스트 결과 유지): {e}")
        if coord_left is not None:
            result["공사종류별인정일수"] = coord_left

    except Exception as e:
        print(f"[ERROR] 요약 페이지 파싱 오류: {e}")

    return result
