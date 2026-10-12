"""파싱 보강 1단계 회귀 테스트(합성 표 데이터, 개인정보 없음)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from parsers.core.page_3_parser import _is_cm_period_anchor_row  # noqa: E402
from parsers.utils.table_career_parser import build_career_block_positional  # noqa: E402
from parsers.utils.tech_career_table_only_core import _iter_page2_tech_records_by_period_rows  # noqa: E402

HEADER = [
    ["참여기간", "사업명", "", "직무분야", "담당업무", "비고"],
    ["", "발주자", "공사종류", "전문분야", "직위", ""],
    ["", "공사(용역)개요", "", "책임정도", "공사(용역)금액(백만원)", ""],
    ["", "적용 공법", "적용 융복합", "적용 신기술 등", "시설물 종류", ""],
]
E = ["", "", "", "", "", ""]


def test_positional_continuation_goes_to_same_physical_row():
    # 4줄 경력 + ┖→ 연장 블록 4줄: 연장 블록 4번째 줄의 신기술 글자는 '적용 신기술 등'(r3)으로 가야 한다.
    rows = [
        ["2023.10.05 ~ 근무중 (55일)", "사업A", "", "토목", "감리", ""],
        ["", "발주처A", "도로", "토목구조", "", ""],
        ["", "개요 앞부분", "", "기술지원기술인", "104,184", ""],
        ["", "", "", "교량거더 제2", "제1종", ""],
        ["┖→", "", "", "", "", ""],
        E,
        ["", "개요 뒷부분", "", "", "", ""],
        ["", "", "", "020-3-2호", "", ""],
    ]
    base = build_career_block_positional(rows)
    assert base is not None
    assert base[1][3] == "토목구조"  # 전문분야 오염 없음
    assert base[3][3] == "교량거더 제2020-3-2호"  # 적용 신기술 등에 이어붙음
    assert base[2][1] == "개요 앞부분개요 뒷부분"


def test_positional_keeps_empty_overview_row_in_place():
    # 개요 줄이 비어도 적용공법 줄이 개요 칸으로 밀려 올라가면 안 된다.
    rows = [
        ["2014.07.21 ~ 2015.03.30 (43일)", "사업B", "", "토목", "사업관리", ""],
        ["", "발주처B", "댐", "수자원개발", "처장", ""],
        E,
        ["", "기술용역", "", "", "", ""],
    ]
    base = build_career_block_positional(rows)
    assert base is not None
    assert base[2][1] == ""
    assert base[3][1] == "기술용역"


def test_positional_falls_back_when_structure_irregular():
    rows = [
        ["2020.01.01 ~ 2020.02.01 (31일)", "사업C", "", "토목", "설계", ""],
        ["", "발주처C", "도로", "도로및공항", "대리", ""],
        ["", "개요", "", "", "", ""],
        E,
        ["메모", "", "", "", "", ""],  # 연장 블록 첫 줄인데 기간 칸에 '┖→'가 아닌 글자 → 위치 판단 불가
    ]
    assert build_career_block_positional(rows) is None


def test_year_month_period_start_on_any_page():
    t6 = HEADER + [
        ["1991.04 ~ 1991.04 (0일) (1일)", "사업D", "", "토목", "설계", ""],
        ["", "발주처D", "공항", "토질·지질", "사원", ""],
        E,
        E,
        ["1991.01.14 ~ 1994.03.21 (560일)", "사업E", "", "토목", "설계", ""],
        ["", "발주처E", "도로", "토질·지질", "사원", ""],
        E,
        E,
    ]
    recs = list(_iter_page2_tech_records_by_period_rows(t6, header_start_row=0, allow_ym_period_start=True))
    assert [r[0][1] for r in recs] == ["사업D", "사업E"]
    assert recs[0][1][1] == "발주처D"


def test_cm_anchor_ignores_numbers_outside_period_column():
    # 개요 칸의 '2087.94'·'1587.65'(면적)는 날짜가 아니다.
    row = ["", "교육연구시설 : 연면적=2087.94㎡ 바닥면적=1587.65㎡", "", "기술지원기술인", "4,804", ""]
    assert _is_cm_period_anchor_row(row) is False
    assert _is_cm_period_anchor_row(["2024.12.05 ~ 2025.01.06 (7일)", "사업", "", "토목", "", ""]) is True
    assert _is_cm_period_anchor_row(["1994.09 ~ 1995.04 (20일)", "사업", "", "토목", "", ""]) is True
