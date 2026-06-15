# -*- coding: utf-8 -*-
"""인적사항 '주소' 파싱 회귀 테스트.

검증 목표
- 주소에 '관리번호' 등 다른 항목 값이 섞여 들어오지 않을 것
- PDF 텍스트 레이어 중복 추출로 주소 내용이 반복되면 1회로 축약될 것
- 정상 주소(끝의 건물번호 등)는 손상 없이 보존될 것
"""

from parsers.core.page_1_parser import (
    _clean_personal_address,
    _extract_personal_address,
    parse_page_1_from_text,
    parse_personal_info_from_table,
)


def test_address_stops_before_management_number_label():
    text = (
        "성명(한글) 홍길동 생년월일 80.01.01 "
        "주소 서울특별시 강남구 테헤란로 123 "
        "관리번호 41000448"
    )
    assert _extract_personal_address(text) == "서울특별시 강남구 테헤란로 123"


def test_address_dedupes_duplicated_content():
    raw = "서울특별시 강남구 테헤란로 123 서울특별시 강남구 테헤란로 123"
    assert _clean_personal_address(raw) == "서울특별시 강남구 테헤란로 123"


def test_address_dedupes_and_strips_trailing_number():
    raw = (
        "경기도 성남시 분당구 정자동 178 "
        "경기도 성남시 분당구 정자동 178 4 1 0 0 0 4 4 8"
    )
    assert _clean_personal_address(raw) == "경기도 성남시 분당구 정자동 178"


def test_address_strips_hash_management_number():
    raw = "부산광역시 해운대구 센텀로 99 # 4 1 0 0 0 4 4 8"
    assert _clean_personal_address(raw) == "부산광역시 해운대구 센텀로 99"


def test_address_no_space_duplication():
    text = "주소 서울특별시강남구서울특별시강남구 연락처 010"
    assert _extract_personal_address(text) == "서울특별시강남구"


def test_address_preserves_building_number():
    text = "주소 인천광역시 연수구 송도과학로 32 연락처 010-1234-5678"
    assert _extract_personal_address(text) == "인천광역시 연수구 송도과학로 32"


def test_parse_page_1_from_text_address_clean():
    combined = (
        "관리번호 #41000448\n"
        "성명(한글) 김철수 생년월일 90.05.10\n"
        "주소 대전광역시 유성구 대학로 291 "
        "주소 대전광역시 유성구 대학로 291 관리번호 41000448\n"
        "등급\n"
    )
    out = parse_page_1_from_text(combined)
    assert out["인적사항"]["주소"] == "대전광역시 유성구 대학로 291"
    assert out["인적사항"]["관리번호"] == "#41000448"


def test_parse_personal_info_from_table_address_clean():
    rows = [
        ["관리번호", "#41000448"],
        ["성명(한글)", "이영희", "생년월일", "85.03.20"],
        ["주소", "광주광역시 북구 첨단과기로 123 광주광역시 북구 첨단과기로 123"],
    ]
    out = parse_personal_info_from_table(rows)
    assert out["인적사항"]["주소"] == "광주광역시 북구 첨단과기로 123"
    assert out["인적사항"]["관리번호"] == "#41000448"
