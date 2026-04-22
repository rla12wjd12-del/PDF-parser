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


def _parse_summary_text(text: str) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {
        "공사종류별인정일수": [],
        "직무전문분야별인정일수": [],
    }
    if not text:
        return result

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    in_section = False
    for ln in lines:
        if "공사종류별" in ln and "인정일수" in ln and "현황" in ln:
            in_section = True
            continue
        if not in_section:
            continue
        if "건설기술진흥법령 외" in ln or ln.startswith("2."):
            break
        if ln.startswith("※"):
            continue
        if "자격" in ln and "학력" in ln:
            break

        hits = list(_DAYS_ENTRY_RE.finditer(ln))
        if not hits:
            continue

        for h in hits:
            label = h.group(1).strip()
            days_str = h.group(2).replace(",", "")

            if "인정일수" in label and "합계" in label:
                continue
            if not label or not days_str:
                continue

            if "/" in label and not any(
                kw in label
                for kw in [
                    "도로,", "교량,", "터널", "하수", "항만", "댐,",
                    "공동주택", "발전소", "고속", "상수도", "하천",
                    "건설기계", "조경", "기타(", "철도", "철근",
                    "일반도로",
                ]
            ):
                parts = label.split("/", 1)
                jf = parts[0].strip()
                sf = parts[1].strip()
                result["직무전문분야별인정일수"].append({
                    "직무분야": jf,
                    "전문분야": sf,
                    "인정일수": days_str,
                })
            else:
                if label.startswith("(") and "미기재" in label:
                    result["직무전문분야별인정일수"].append({
                        "직무분야": "",
                        "전문분야": label,
                        "인정일수": days_str,
                    })
                    continue
                result["공사종류별인정일수"].append({
                    "공사종류": label,
                    "인정일수": days_str,
                })

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
        end_found = False
        merged_until = page_num
        for i in range(page_num, ctx.total_pages):
            text = ctx.get_text(i) or ""
            if text.strip():
                combined += text + "\n"
            merged_until = i

            # 종료 앵커(2. 건설기술진흥법령 외 ...)가 등장하면, 요약 입력 범위를
            # 해당 섹션 시작 직전까지만 남기고 병합을 중단한다.
            m = _SUMMARY_END_ANCHOR_RE.search(combined)
            if m:
                combined = combined[: m.start()].rstrip() + "\n"
                end_found = True
                break

        # 로그는 실제 병합 범위를 기준으로 표시한다.
        end_page_display = merged_until + 1
        anchor_info = " (종료 앵커 감지)" if end_found else ""
        print(
            f"  - 분야별 참여기간 인정일 파싱 중... "
            f"(페이지 {page_num + 1}~{end_page_display}){anchor_info}"
        )

        parsed = _parse_summary_text(combined)
        result.update(parsed)

    except Exception as e:
        print(f"[ERROR] 요약 페이지 파싱 오류: {e}")

    return result
