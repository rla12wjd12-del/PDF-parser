"""파싱 보강 3단계: 페이지 넘김 연장(┖→)은 다음 쪽 첫 경력보다 위에 있을 때만 앞 쪽 경력에 붙인다."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from parsers.utils.tech_career_common import extract_tech_overview_continuation_from_page_text  # noqa: E402

HEAD = "Page : 2 / 9\n1. 기술경력\n사업명 직무분야 담당업무\n참여기간\n"


def test_top_of_page_continuation_is_taken():
    text = HEAD + "┖→ 앞 쪽 개요의 나머지 부분\n사업X 토목 설계\n2022.05.31\n~ 발주처 도로 토목구조 상무\n2023.12.30\n"
    assert extract_tech_overview_continuation_from_page_text(text).startswith("앞 쪽 개요의 나머지 부분")


def test_mid_page_continuation_is_not_taken():
    text = HEAD + "사업X 토목 설계\n2022.05.31\n~ 발주처 도로 토목구조 상무\n2023.12.30\n개요\n┖→ 이 쪽 경력의 연장\n"
    assert extract_tech_overview_continuation_from_page_text(text) == ""
