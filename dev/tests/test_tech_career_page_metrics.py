#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""기술경력 페이지 메트릭(_analyze_tech_career_page_metrics_from_lines) 단위 테스트."""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from main import _analyze_tech_career_page_metrics_from_lines
from parsers.page_2_parser import (
    _find_tilde_line_index_after_start_date,
    _flow_resolve_end_after_tilde,
)


class TestTechCareerPageMetrics(unittest.TestCase):
    def test_one_valid_tilde_no_arrows(self):
        lines = [
            "2020.01.01",
            "~ 참여 개요",
            "2021.12.31",
        ]
        m = _analyze_tech_career_page_metrics_from_lines(lines)
        self.assertEqual(m["n_tilde"], 1)
        self.assertEqual(m["n_arrow_border"], 0)
        self.assertEqual(m["n_arrow_table"], 0)
        self.assertEqual(m["n_arrow_total"], 0)
        self.assertEqual(m["capacity_6_minus_arrow"], 6)
        self.assertEqual(m["legacy_건수"], 1)

    def test_border_arrow_reduces_legacy_not_table_arrows(self):
        lines = [
            "┖→ 이전 페이지 이어짐",
            "2020.01.01",
            "~ 항목",
            "근무중",
        ]
        m = _analyze_tech_career_page_metrics_from_lines(lines)
        self.assertEqual(m["n_tilde"], 1)
        self.assertEqual(m["n_arrow_border"], 1)
        self.assertEqual(m["n_arrow_table"], 0)
        self.assertEqual(m["n_arrow_total"], 1)
        self.assertEqual(m["capacity_6_minus_arrow"], 5)
        self.assertEqual(m["legacy_건수"], 0)

    def test_unicode_arrow_and_capacity(self):
        lines = [
            "2020.01.01",
            "~ a",
            "2021.01.01",
            "\u2516\u2192 표 안 이어짐",
            "2020.06.01",
            "~ b",
            "2022.01.01",
        ]
        m = _analyze_tech_career_page_metrics_from_lines(lines)
        self.assertEqual(m["n_tilde"], 2)
        self.assertEqual(m["n_arrow_border"], 0)
        self.assertEqual(m["n_arrow_table"], 1)
        self.assertEqual(m["n_arrow_total"], 1)
        self.assertEqual(m["capacity_6_minus_arrow"], 5)


class TestFlowTildeAfterWrappedGongjong(unittest.TestCase):
    """공사종류 조각이 시작일과 ~ 사이에 끼는 extract_text 패턴."""

    def test_find_tilde_and_end_across_susigol_tail(self):
        lines = [
            "순천만자연생태공원조성실시설계용역 토목 설계",
            "2001.07.31",
            "관광휴게시설(공원.유원지.관광지부",
            "~ 순천시 토질·지질 과장",
            "수시설)",
            "2001.11.27",
            "(6일)",
            "(120일)",
        ]
        start_i = 1
        j = _find_tilde_line_index_after_start_date(lines, start_i)
        self.assertEqual(j, 3)
        end_iso, end_idx, day_cursor = _flow_resolve_end_after_tilde(lines, j)
        self.assertEqual(end_iso, "2001-11-27")
        self.assertEqual(end_idx, 5)
        self.assertEqual(day_cursor, 6)


if __name__ == "__main__":
    unittest.main()
