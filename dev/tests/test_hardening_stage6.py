"""파싱 보강 6단계: pdfplumber 가 빠뜨린 한자를 PyMuPDF 단어로 복원(합성 PDF)."""
import os
import sys
from types import SimpleNamespace

import fitz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import main  # noqa: E402


def _ctx():
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    page.insert_text((80, 300), "울주型 스마트팜 단지", fontname="korea", fontsize=7)
    page.insert_text((80, 400), "새-로(路)프로젝트 (포장리모델링)", fontname="korea", fontsize=7)
    # 다음 단어(발주)가 사업명 뒤에 없어 lookahead로는 복원되지 않는 한자.
    page.insert_text((80, 500), "일산성현교회신축공사中석공사", fontname="korea", fontsize=7)
    page.insert_text((220, 500), "발주", fontname="korea", fontsize=7)
    return SimpleNamespace(_fitz_doc=doc)


def test_restore_dropped_and_blanked_ideographs():
    result = {
        "기술경력": [
            {"사업명": "울주 스마트팜 단지 용역", "_pdf_pages": [1]},
            {"사업명": "새-로( )프로젝트 (포장리모델링) 실시설계", "_pdf_pages": [1]},
            {"사업명": "울주 시청사 신축", "_pdf_pages": [1]},  # 뒤 문맥(스마트팜)이 다르면 바꾸지 않음
            {"사업명": "일산성현교회신축공사 석공사", "_pdf_pages": [1]},
        ]
    }
    n = main._restore_dropped_cjk_in_careers(_ctx(), result)
    rows = result["기술경력"]
    assert n == 3
    assert rows[0]["사업명"] == "울주型 스마트팜 단지 용역"
    assert rows[1]["사업명"] == "새-로(路)프로젝트 (포장리모델링) 실시설계"
    assert rows[2]["사업명"] == "울주 시청사 신축"
    assert rows[3]["사업명"] == "일산성현교회신축공사中석공사"


def test_fill_responsibility_below_start_date():
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    page.insert_text((30, 350), "2020.09.21", fontsize=8)
    page.insert_text((378, 390), "상주기술인", fontname="korea", fontsize=8)
    page.insert_text((30, 450), "2021.02.22", fontsize=8)
    page.insert_text((378, 490), "참여기술인", fontname="korea", fontsize=8)
    ctx = SimpleNamespace(_fitz_doc=doc)
    result = {
        "건설사업관리및감리경력": [
            {"참여기간_시작일": "2020-09-21", "책임정도": "", "_pdf_pages": [1]},
            {"참여기간_시작일": "2021-02-22", "책임정도": "참여기술인", "_pdf_pages": [1]},
        ]
    }
    filled = main._fill_empty_cm_responsibility(ctx, result)
    rows = result["건설사업관리및감리경력"]
    assert filled == 1
    assert rows[0]["책임정도"] == "상주기술인"
    assert rows[1]["책임정도"] == "참여기술인"


def test_license_column_below_header_before_education():
    from parsers.core.page_1_parser import _licenses_from_fitz_page

    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    page.insert_text((104, 240), "종목", fontname="korea", fontsize=8)
    page.insert_text((206, 240), "합격일", fontname="korea", fontsize=8)
    page.insert_text((263, 240), "등록번호", fontname="korea", fontsize=8)
    page.insert_text((72, 260), "건설안전기사", fontname="korea", fontsize=8)
    page.insert_text((197, 260), "2012.08.17", fontsize=8)
    page.insert_text((248, 260), "12202230426J", fontsize=8)
    page.insert_text((81, 320), "졸업일", fontname="korea", fontsize=8)
    rows = _licenses_from_fitz_page(page)
    assert rows == [
        {"종목": "건설안전기사", "합격일": "2012-08-17", "등록번호": "12202230426J"}
    ]
