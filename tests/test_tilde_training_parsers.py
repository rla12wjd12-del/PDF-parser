#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""~ 라인(발주자 블록) 및 교육훈련 행 파서 회귀."""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from parsers.page_2_parser import _parse_tilde_line, _cleanup_tech_career_job_noise_row
from parsers.page_1_parser import _parse_training_row


class TestParseTildeLine(unittest.TestCase):
    def test_yongin_four_tokens_no_position(self):
        d = _parse_tilde_line("~ 용인시 상수도사업소 상수도 조경계획")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "용인시 상수도사업소")
        self.assertEqual(d.get("공사종류"), "상수도")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "")

    def test_goyang_three_tokens_specialty_and_noise_tail(self):
        d = _parse_tilde_line("~ 경기도고양시 조경계획 이사")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "경기도고양시")
        self.assertEqual(d.get("공사종류"), "")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "")

    def test_road_specialty_three_tokens(self):
        d = _parse_tilde_line("~ 한국도로공사 국도 토질·지질")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "한국도로공사")
        self.assertEqual(d.get("공사종류"), "국도")
        self.assertEqual(d.get("전문분야"), "토질·지질")
        self.assertEqual(d.get("직위"), "")

    def test_two_token_issuer_specialty(self):
        d = _parse_tilde_line("~ 한국도로공사 토질·지질")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "한국도로공사")
        self.assertEqual(d.get("전문분야"), "토질·지질")
        self.assertEqual(d.get("직위"), "")

    def test_issuer_compound_before_single_worktype(self):
        d = _parse_tilde_line("~ 경기도 양주시 공원 조경계획 부장")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "경기도 양주시")
        self.assertEqual(d.get("공사종류"), "공원")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "부장")

    def test_jeonbuk_jangsu_tourism_worktype(self):
        d = _parse_tilde_line("~ 전라북도 장수군 관광지 조경계획 부장")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "전라북도 장수군")
        self.assertEqual(d.get("공사종류"), "관광지")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "부장")

    def test_goyang_cityhall_danji_single(self):
        d = _parse_tilde_line("~ 고양시청 단지조성 조경계획 부장")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "고양시청")
        self.assertEqual(d.get("공사종류"), "단지조성")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "부장")

    def test_goyang_duplicate_danji_token_from_pdf(self):
        d = _parse_tilde_line("~ 고양시청 단지조성 단지조성 조경계획 부장")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "고양시청")
        self.assertEqual(d.get("공사종류"), "단지조성")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "부장")

    def test_four_tokens_with_position_at_end(self):
        d = _parse_tilde_line("~ 대전지방국토관리청 국도 도로및공항 사원")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "대전지방국토관리청")
        self.assertEqual(d.get("공사종류"), "국도")
        self.assertEqual(d.get("전문분야"), "도로및공항")
        self.assertEqual(d.get("직위"), "사원")

    def test_catalog_admin_prefix_before_non_worktype_tail(self):
        # 마지막 head 토큰이 짧은 공사종류 목록에 없을 때: 시도+시군구 접두를 카탈로그로 확장
        d = _parse_tilde_line(
            "~ 경기도 안양시 동안구 도시개발공사 기타시설 조경계획 사원"
        )
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "경기도 안양시")
        self.assertEqual(d.get("공사종류"), "동안구 도시개발공사 기타시설")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "사원")

    def test_catalog_public_institution_prefix(self):
        d = _parse_tilde_line(
            "~ 한국문학번역원 기획실 기타시설 조경계획 사원"
        )
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "한국문학번역원")
        self.assertEqual(d.get("공사종류"), "기획실 기타시설")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "사원")

    def test_non_catalog_issuer_token_conservation(self):
        d = _parse_tilde_line(
            "~ (주)전혀없는회사XYZ 부서A 부서B 조경계획 사원"
        )
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "(주)전혀없는회사XYZ")
        self.assertEqual(d.get("공사종류"), "부서A 부서B")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "사원")

    def test_strip_job_field_noise_gwangmyeong_four_tokens_no_position(self):
        d = _parse_tilde_line("~ 광명시 조경 조경계획")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "광명시")
        self.assertEqual(d.get("공사종류"), "")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "")

    def test_strip_job_field_noise_gwangmyeong_with_position(self):
        d = _parse_tilde_line("~ 광명시 조경 조경계획 대리")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "광명시")
        self.assertEqual(d.get("공사종류"), "")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "대리")

    def test_only_specialty_and_position_no_issuer(self):
        d = _parse_tilde_line("~ 조경계획 대리")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "")
        self.assertEqual(d.get("공사종류"), "")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "대리")

    def test_gimcheon_strip_trailing_jo(self):
        d = _parse_tilde_line("~ 김천시 조경 조경계획 대리")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("발주자"), "김천시")
        self.assertEqual(d.get("공사종류"), "")
        self.assertEqual(d.get("전문분야"), "조경계획")
        self.assertEqual(d.get("직위"), "대리")


class TestCleanupIssuerWorkOverlap(unittest.TestCase):
    def test_remove_duplicate_worktype_suffix_from_issuer(self):
        row = {
            "발주자": "서천군 관광지",
            "공사종류": "관광지",
            "전문분야": "조경계획",
        }
        _cleanup_tech_career_job_noise_row(row)
        self.assertEqual(row["발주자"], "서천군")
        self.assertEqual(row["공사종류"], "관광지")

    def test_multi_token_worktype_suffix(self):
        row = {
            "발주자": "경기도 안양시 동안구 도시개발공사",
            "공사종류": "동안구 도시개발공사",
            "전문분야": "조경계획",
        }
        _cleanup_tech_career_job_noise_row(row)
        self.assertEqual(row["발주자"], "경기도 안양시")
        self.assertEqual(row["공사종류"], "동안구 도시개발공사")

    def test_merged_issuer_cell_comma_list_worktypes(self):
        """카탈로그에 없는 콤마 목록이 발주자에 붙은 경우(이태협 PDF 등)."""
        row = {
            "발주자": "한국수자원공사 토공,관로공,상·하수도공",
            "공사종류": "",
            "전문분야": "토목시공",
        }
        _cleanup_tech_career_job_noise_row(row)
        self.assertEqual(row["발주자"], "한국수자원공사")
        self.assertEqual(row["공사종류"], "토공,관로공,상·하수도공")


class TestParseTrainingRow(unittest.TestCase):
    def test_university_institution(self):
        row = (
            "2023.06.23 ~ 2023.08.14 건설사업관리기술인 승급 전문교육 "
            "경복대학교 건설사업관리"
        )
        d = _parse_training_row(row)
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d.get("교육기간_시작"), "2023-06-23")
        self.assertEqual(d.get("교육기간_종료"), "2023-08-14")
        self.assertIn("경복대학교", d.get("교육기관명", ""))
        self.assertEqual(d.get("교육인정여부"), "건설사업관리")


if __name__ == "__main__":
    unittest.main()
