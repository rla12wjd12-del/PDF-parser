#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""상훈 종류/근거 정규화 — 표 칸 분할 꼬리 파편 제거 회귀."""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from parsers.core.section_parsers import (
    _normalize_award_type_text,
    _is_shattered_award_type_cell,
    _is_orphan_award_type_tail_fragment,
    _reconstruct_award_cells_from_fragment_extras,
    _drop_fragment_awards_when_well_formed_exists,
    _split_merged_award_triples,
    _split_institution_overflow_award_type,
    _sanitize_award_institution_field,
    _strip_institution_prefix_from_award_type,
)


class TestAwardTypeNormalize(unittest.TestCase):
    def test_strip_orphan_token_closing_bracket_tail(self):
        self.assertEqual(
            _normalize_award_type_text("표창장[제999호] 표창장 ]"),
            "표창장[제999호]",
        )
        self.assertEqual(
            _normalize_award_type_text("감사장[제1호] 감사장]"),
            "감사장[제1호]",
        )

    def test_keeps_distinct_complete_blocks(self):
        self.assertEqual(
            _normalize_award_type_text("표창장[제999호] 표창장[제444호]"),
            "표창장[제999호] 표창장[제444호]",
        )

    def test_shattered_cell_detection(self):
        self.assertTrue(_is_shattered_award_type_cell("]"))
        self.assertTrue(_is_shattered_award_type_cell("[제999호"))
        self.assertFalse(_is_shattered_award_type_cell("표창장[제999호]"))
        self.assertTrue(_is_orphan_award_type_tail_fragment("표창장 ]"))
        self.assertFalse(_is_orphan_award_type_tail_fragment("표창장[제999호]"))

    def test_reconstruct_shattered_row_fragments(self):
        inst, typ = _reconstruct_award_cells_from_fragment_extras(
            ["서울특별시장", "표창장", "[제999호", "]"]
        )
        self.assertEqual(inst, "서울특별시장")
        self.assertEqual(typ, "표창장[제999호]")

    def test_drop_fragment_rows_when_well_formed_row_exists_same_date(self):
        # 선 기반 표(온전한 셀) + 텍스트 기반 표(글자 단위 분할) 파편이 같은 수여일로 함께 잡힌 경우
        awards = [
            {"수여일": "2013-12-31", "수여기관": "진영국토관리사무소", "종류및근거": "표창장[제199호]"},
            {"수여일": "2013-12-31", "수여기관": "사무소 표창", "종류및근거": "호]"},
            {"수여일": "2009-12-10", "수여기관": "표창 질관", "종류및근거": "으로서철저한 ]"},
        ]
        out = _drop_fragment_awards_when_well_formed_exists(awards)
        self.assertEqual(
            out,
            [
                {"수여일": "2013-12-31", "수여기관": "진영국토관리사무소", "종류및근거": "표창장[제199호]"},
                {"수여일": "2009-12-10", "수여기관": "표창 질관", "종류및근거": "으로서철저한 ]"},
            ],
        )

    def test_stacked_cell_splits_institution_per_date(self):
        # 한 셀에 두 건이 줄바꿈으로 쌓인 행: 기관명은 줄 단위로, 종류는 새 종류 토큰 기준으로 나뉜다.
        triples = _split_merged_award_triples(
            "2013.12.31\n2009.12.10",
            "진영국토관리사무소\n부산광역시",
            "표창장[제199호]\n표창장[감리원으로서철저한시공과완벽한품\n질관리로해상교량건설에기여 ]",
        )
        split = [(d,) + _split_institution_overflow_award_type(i, t) for d, i, t in triples]
        self.assertEqual(split[0][:2], ("2013.12.31", "진영국토관리사무소"))
        self.assertEqual(split[0][2], "표창장[제199호]")
        self.assertEqual(split[1][:2], ("2009.12.10", "부산광역시"))
        self.assertTrue(split[1][2].startswith("표창장[감리원으로서"))

    def test_stacked_cell_one_line_per_date_keeps_tokenless_type(self):
        # 줄 수 == 수여일 수이고 대괄호가 다음 줄로 넘어가지 않으면, 토큰 없는 종류도 자기 건에 남는다.
        triples = _split_merged_award_triples(
            "1990.02.14\n1972.11.02",
            "강원도청\n서울특별시",
            "표창장[제49호]\n새서울상",
        )
        self.assertEqual(
            triples,
            [("1990.02.14", "강원도청", "표창장[제49호]"), ("1972.11.02", "서울특별시", "새서울상")],
        )

    def test_normalize_keeps_leading_gongro_prefix(self):
        self.assertEqual(_normalize_award_type_text("공로표창장[제195호]"), "공로표창장[제195호]")
        # 앞에 잡음이 붙은 경우는 여전히 가장 앞 토큰부터 자른다.
        self.assertEqual(_normalize_award_type_text("장[ 21337] 표창장[21337]"), "표창장[21337]")

    def test_normalize_keeps_prefix_glued_to_token(self):
        self.assertEqual(
            _normalize_award_type_text("석탑산업훈장(대통령)[제2501호]"), "석탑산업훈장(대통령)[제2501호]"
        )
        self.assertEqual(_normalize_award_type_text("최우수상(장관)[제2597호]"), "최우수상(장관)[제2597호]")

    def test_institution_prefix_strip_keeps_type_named_after_institution(self):
        self.assertEqual(
            _strip_institution_prefix_from_award_type("서울특별시", "서울특별시건설상[제196호]"),
            "서울특별시건설상[제196호]",
        )
        self.assertEqual(
            _strip_institution_prefix_from_award_type("한국수자원공사", "한국수자원공사 표창패"), "표창패"
        )
        self.assertEqual(
            _strip_institution_prefix_from_award_type("한국수자원공사", "한국수자원공사표창장[제1호]"),
            "표창장[제1호]",
        )

    def test_same_date_fragments_and_resplits_are_dropped(self):
        awards = [
            {"수여일": "2003-12-31", "수여기관": "부산지방국토관리청", "종류및근거": "표창장[제897호]"},
            {"수여일": "2003-12-31", "수여기관": "관리청", "종류및근거": "표창장[호]"},
            {"수여일": "2017-06-23", "수여기관": "국토교통부", "종류및근거": "최우수상(장관)[제2597호]"},
            {"수여일": "2017-06-23", "수여기관": "국토교통부최", "종류및근거": "우수상(장관)[제2597호]"},
        ]
        self.assertEqual(
            _drop_fragment_awards_when_well_formed_exists(awards),
            [awards[0], awards[2]],
        )

    def test_institution_with_slash_is_kept(self):
        self.assertEqual(_sanitize_award_institution_field("건설부/총무", "표창장"), "건설부/총무")

    def test_tokenless_single_chunk_type_is_well_formed(self):
        awards = [
            {"수여일": "1972-11-02", "수여기관": "서울특별시", "종류및근거": "새서울상"},
            {"수여일": "1972-11-02", "수여기관": "새서", "종류및근거": "서울특별시 울 상"},
        ]
        self.assertEqual(
            _drop_fragment_awards_when_well_formed_exists(awards),
            [{"수여일": "1972-11-02", "수여기관": "서울특별시", "종류및근거": "새서울상"}],
        )


if __name__ == "__main__":
    unittest.main()
