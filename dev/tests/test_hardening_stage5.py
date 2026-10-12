"""파싱 보강 5단계: 요약표 좌측 열(공사종류별 인정일수)을 좌표 기준으로 읽기(합성 PDF)."""
import os
import sys
from types import SimpleNamespace

import fitz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from parsers.core.page_summary_parser import _parse_left_column_by_coordinates  # noqa: E402

FONT = "korea"


def _put(page, x, y, text):
    page.insert_text((x, y), text, fontname=FONT, fontsize=7)


def _make_doc(total: str):
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    _put(page, 150, 78, "분야별 참여기간 인정일 및 건설기술진흥법령 외 자격·학력 등")
    _put(page, 107, 130, "공사종류별 인정일수 현황")
    _put(page, 354, 130, "직무/전문분야별 인정일수 현황")
    _put(page, 55, 150, "공동주택(아파트)")
    _put(page, 243, 150, "1,606 일")
    _put(page, 315, 150, "조경/조경시공관리")  # 우측 열(섞이면 안 됨)
    # 두 줄로 줄바꿈된 이름: 숫자 행의 위·아래
    _put(page, 55, 165, "공동주택(아파트),조경시설물설치,조")
    _put(page, 252, 170, "807 일")
    _put(page, 55, 174, "경식 재")
    _put(page, 55, 185, "근린생활시설,조경시설물설치,조경식")
    _put(page, 257, 190, "61 일")
    _put(page, 55, 194, "재")
    _put(page, 55, 210, "인정일수 합계")
    _put(page, 243, 210, total)
    return doc


def test_wrapped_labels_attach_to_nearest_row():
    rows = _parse_left_column_by_coordinates(SimpleNamespace(_fitz_doc=_make_doc("2,474 일")), 0)
    assert rows == [
        {"공사종류": "공동주택(아파트)", "인정일수": "1606"},
        {"공사종류": "공동주택(아파트),조경시설물설치,조경식재", "인정일수": "807"},
        {"공사종류": "근린생활시설,조경시설물설치,조경식재", "인정일수": "61"},
    ]


def test_total_mismatch_returns_none():
    # 항목 합계가 '인정일수 합계'와 다르면 좌표 결과를 쓰지 않는다(기존 텍스트 결과 유지)
    assert _parse_left_column_by_coordinates(SimpleNamespace(_fitz_doc=_make_doc("9,999 일")), 0) is None
