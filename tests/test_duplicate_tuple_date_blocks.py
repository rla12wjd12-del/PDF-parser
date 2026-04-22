#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""동일 참여기간·일수 연속 행 → date_blocks 병합(_extract_date_blocks_from_text) 회귀."""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from parsers.page_2_parser import _extract_date_blocks_from_text


class TestDuplicateTupleDateBlocks(unittest.TestCase):
    def test_two_rows_same_period_merge_two_distinct_names(self):
        # 두 줄 모두 비기관·multi-token 사업명( combine 전 forward 후보가 실제 제목인 케이스 )
        text = (
            "첫번째사업명전용현장공사명표시\n"
            "1996.08.02\n"
            "~ 시청 도로 토목 기사\n"
            "1996.12.13\n"
            "(67일)\n"
            "(134일)\n"
            "두번째사업명전용현장공사명표시\n"
            "1996.08.02\n"
            "~ 시청 도로 토목 기사\n"
            "1996.12.13\n"
            "(67일)\n"
            "(134일)\n"
            "다음블록사업명전용현장공사\n"
            "1996.12.26\n"
            "~ 기관 발주 한줄\n"
            "1997.04.30\n"
            "(10일)\n"
            "(20일)\n"
        )
        blocks = _extract_date_blocks_from_text(text)
        self.assertEqual(len(blocks), 2, "동일 4튜플 2행은 1블록으로 합쳐져야 함")
        b0 = blocks[0]
        self.assertTrue(b0.get("_merged_duplicate_tuple"))
        names = b0.get("_사업명후보") or []
        self.assertEqual(len(names), 2, "행당 대표 사업명 2개")
        self.assertIn("첫번째사업명전용현장공사명표시", names[0])
        self.assertIn("두번째사업명전용현장공사명표시", names[1])

    def test_three_rows_same_period_keeps_middle_name(self):
        text = (
            "첫번째사업명전용현장공사명표시\n"
            "1996.08.02\n"
            "~ 시청 도로 토목 기사\n"
            "1996.12.13\n"
            "(67일)\n"
            "(134일)\n"
            "중간사업명전용현장공사명표시\n"
            "1996.08.02\n"
            "~ 시청 도로 토목 기사\n"
            "1996.12.13\n"
            "(67일)\n"
            "(134일)\n"
            "세번째사업명전용현장공사명표시\n"
            "1996.08.02\n"
            "~ 시청 도로 토목 기사\n"
            "1996.12.13\n"
            "(67일)\n"
            "(134일)\n"
            "다음블록사업명전용현장공사\n"
            "1996.12.26\n"
            "~ 기관 발주 한줄\n"
            "1997.04.30\n"
            "(10일)\n"
            "(20일)\n"
        )
        blocks = _extract_date_blocks_from_text(text)
        self.assertEqual(len(blocks), 2, "동일 4튜플 3행은 1블록으로 합쳐져야 함")
        names = (blocks[0].get("_사업명후보") or [])
        self.assertEqual(len(names), 3, "사업명 3개 유지")
        self.assertIn("중간사업명전용현장공사명표시", names[1])


if __name__ == "__main__":
    unittest.main()
