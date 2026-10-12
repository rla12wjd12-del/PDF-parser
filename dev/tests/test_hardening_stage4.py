"""파싱 보강 4단계: 좌표 기반 독립 경력 수 검증(합성 PDF, 개인정보 없음)."""
import os
import sys
from types import SimpleNamespace

import fitz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import main  # noqa: E402


def _make_doc():
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    # 기간 칸(x<78) 안: 경력 2건(전체 날짜 1, 연·월 1)
    page.insert_text((30, 200), "2022.05.31", fontsize=7)
    page.insert_text((45, 210), "~", fontsize=7)
    page.insert_text((30, 300), "1991.04", fontsize=7)
    page.insert_text((45, 310), "~", fontsize=7)
    # 기간 칸 밖의 날짜·물결표(사업명 등)는 세지 않는다
    page.insert_text((120, 400), "2020.01.01", fontsize=7)
    page.insert_text((130, 410), "~", fontsize=7)
    # 기간 칸 안이지만 아래에 ~ 가 없는 날짜(종료일 등)는 세지 않는다
    page.insert_text((30, 500), "2023.12.30", fontsize=7)
    return doc


def test_coordinate_count_matches_period_pairs():
    ctx = SimpleNamespace(_fitz_doc=_make_doc())
    assert main._count_career_periods_by_coordinates(ctx, 0, 1) == 2


def test_coordinate_count_none_without_fitz():
    assert main._count_career_periods_by_coordinates(SimpleNamespace(_fitz_doc=None), 0, 1) is None
