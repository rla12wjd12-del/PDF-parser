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
    return SimpleNamespace(_fitz_doc=doc)


def test_restore_dropped_and_blanked_ideographs():
    result = {
        "기술경력": [
            {"사업명": "울주 스마트팜 단지 용역", "_pdf_pages": [1]},
            {"사업명": "새-로( )프로젝트 (포장리모델링) 실시설계", "_pdf_pages": [1]},
            {"사업명": "울주 시청사 신축", "_pdf_pages": [1]},  # 뒤 문맥(스마트팜)이 다르면 바꾸지 않음
        ]
    }
    n = main._restore_dropped_cjk_in_careers(_ctx(), result)
    rows = result["기술경력"]
    assert n == 2
    assert rows[0]["사업명"] == "울주型 스마트팜 단지 용역"
    assert rows[1]["사업명"] == "새-로(路)프로젝트 (포장리모델링) 실시설계"
    assert rows[2]["사업명"] == "울주 시청사 신축"
