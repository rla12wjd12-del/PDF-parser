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


if __name__ == "__main__":
    unittest.main()
