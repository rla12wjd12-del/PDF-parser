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

    return out


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

    except Exception as e:
        print(f"[ERROR] 요약 페이지 파싱 오류: {e}")

    return result
