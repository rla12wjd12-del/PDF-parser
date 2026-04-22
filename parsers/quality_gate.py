#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
필드 단위 완전성(field-level completeness) 검증 게이트.

"누락 없음" = 필드 단위 완전성이라는 프로젝트 목표(ARCH_REVIEW.md P0-3)에 따라,
섹션별 필수 필드가 비어 있는 레코드를 탐지하고 구조화된 오류로 반환한다.

반환 형식(각 항목):
    {
        "type": "필드완전성",
        "section": "기술경력",
        "index": 2,          # 0-based 레코드 인덱스
        "field": "사업명",
        "error": "empty",
    }
"""
from __future__ import annotations

from typing import Any, Dict, List

# 섹션별 필수 필드 정의
# 이 목록은 "비어 있으면 파싱 오류로 간주"하는 최소 보장 필드다.
REQUIRED_FIELDS: Dict[str, List[str]] = {
    "기술경력": ["사업명", "참여기간_시작일", "참여기간_종료일", "인정일수"],
    "건설사업관리및감리경력": ["사업명", "참여기간_시작일", "참여기간_종료일", "인정일수"],
    "학력": ["학교명"],
    "국가기술자격": ["type_and_grade"],
    "근무처": ["근무기간_시작"],
}


def _is_empty(value: Any) -> bool:
    """값이 비어 있으면 True. 0은 유효한 값으로 허용한다."""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def check_field_completeness(result: dict) -> List[dict]:
    """
    result에서 REQUIRED_FIELDS 기준으로 비어 있는 필드를 탐지해 반환한다.

    Args:
        result: parse_full_document()가 반환한 딕셔너리

    Returns:
        비어 있는 필수 필드 목록. 완전하면 빈 리스트.
    """
    errors: List[dict] = []

    for section_key, required in REQUIRED_FIELDS.items():
        records = result.get(section_key)
        if not isinstance(records, list):
            continue
        for idx, record in enumerate(records):
            if not isinstance(record, dict):
                continue
            for field_name in required:
                if _is_empty(record.get(field_name)):
                    errors.append({
                        "type": "필드완전성",
                        "section": section_key,
                        "index": idx,
                        "field": field_name,
                        "error": "empty",
                    })

    return errors
