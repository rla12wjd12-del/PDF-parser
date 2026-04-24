#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
제2쪽 파서 (표 기반 단독 버전): 기술경력

원본 parsers/page_2_parser.py 와의 차이
----------------------------------------
- 본 모듈의 parse_page_2 는 `_parse_tech_careers_from_raw_table` 만 사용한다.
- 표 기반 추출이 빈 결과를 반환하면 그 자체로 빈 리스트를 반환한다.
  (텍스트/좌표 기반 폴백 경로는 호출하지 않는다.)
- 표 결과에 적용되던 후처리 4종은 원본과 동일하게 적용한다.

비교 목적
---------
- 원본 parse_page_2 가 거치는 5개 경로 중 0번(순수 표) 경로만 남겼을 때,
  실제 PDF 결과가 어떻게 달라지는지 확인하기 위한 실험용 파서이다.
- 후처리 로직은 검증된 원본 함수를 그대로 import 해서 사용하므로,
  결과 차이는 "메인 파싱 흐름"에서만 발생한다.

배치 위치
---------
- 본 파일은 원본과 동일한 디렉토리(parsers/)에 두는 것을 가정한다.
- 다른 위치에 두려면 아래 import 경로를 조정하면 된다.
"""

import os
import sys

# 원본과 같은 폴더가 아닌 경우를 위한 보조 sys.path 등록
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict, Any

from parsers.document_context import DocumentContext

# ──────────────────────────────────────────────────────────────────────────────
# 원본 page_2_parser 에서 "표 기반 추출 + 후처리"에 필요한 함수만 가져온다.
# (텍스트/좌표 기반 함수는 가져오지 않는다 → import 자체가 의도를 드러냄)
# ──────────────────────────────────────────────────────────────────────────────
from parsers.page_2_parser import (
    # 표 기반 추출 본체
    _parse_tech_careers_from_raw_table,
    # 후처리 (원본 parse_page_2 의 표 경로에서도 동일하게 적용되던 것)
    _sanitize_header_like_project_names,
    _sanitize_overview_like_project_names,
    _fix_shifted_fields_in_tech_career_rows,
    _cleanup_tech_career_job_noise_rows,
    # 디버그 로그(원본과 동일한 NDJSON 로그를 남기고 싶을 때만 사용)
    _agent_log,
)


def parse_page_2(ctx: DocumentContext, page_num: int) -> List[Dict[str, Any]]:
    """
    제2쪽 파싱 (표 기반 단독): 기술경력

    Args:
        ctx: DocumentContext
        page_num: 페이지 번호 (0부터 시작)

    Returns:
        List[Dict]: 기술경력 리스트
                    표 인식이 실패하면 빈 리스트 반환(폴백 없음).
    """
    careers: List[Dict[str, Any]] = []
    page_num_1based = page_num + 1

    try:
        # 1) 페이지 범위 검사
        if page_num >= ctx.total_pages:
            print(f"⚠️ 페이지 번호 오류: {page_num_1based}페이지는 존재하지 않습니다.")
            return careers

        page = ctx.pages[page_num]
        text = ctx.get_text(page_num) or ""

        print(f"  - 기술경력 파싱 중 (표 단독)... (페이지 {page_num_1based})")

        # 2) 빈 페이지 가드(원본과 동일)
        if not text.strip():
            return careers

        # 3) 표 기반 추출 — 유일한 추출 경로
        try:
            table_rows = (
                _parse_tech_careers_from_raw_table(
                    page,
                    page_num_1based=page_num_1based,
                    pdf_path=str(getattr(ctx, "pdf_path", "") or ""),
                )
                or []
            )
        except Exception as e:
            # 원본은 try/except 로 감싸고 빈 리스트로 처리하므로 동일하게 처리
            print(f"    [WARN] 표 기반 추출 예외(페이지 {page_num_1based}): {e}")
            table_rows = []

        # 4) 표 결과가 없으면 폴백 없이 빈 결과 반환 (원본과의 핵심 차이점)
        if not table_rows:
            print(f"    [INFO] 페이지 {page_num_1based}: 표 기반 추출 결과 없음 (폴백 안 함)")
            # 원본 디버그 로그 형식 유지
            try:
                _agent_log(
                    run_id="table-only",
                    hypothesis_id="T",
                    location="page_2_parser_table_only.py:parse_page_2:no_table_rows",
                    message="table-only parser returned empty (no fallback)",
                    data={"page_num_1based": page_num_1based, "text_len": len(text or "")},
                )
            except Exception:
                pass
            return careers

        # 5) 후처리 — 원본 parse_page_2 의 표 경로(3925~3933줄)와 완전 동일한 순서
        _sanitize_header_like_project_names(table_rows, page_num_1based=page_num_1based)
        _sanitize_overview_like_project_names(table_rows, page_num_1based=page_num_1based)
        _fix_shifted_fields_in_tech_career_rows(table_rows)
        _cleanup_tech_career_job_noise_rows(table_rows)

        # 6) _pdf_pages 부여 (원본과 동일)
        for r in table_rows:
            if isinstance(r, dict) and "_pdf_pages" not in r:
                r["_pdf_pages"] = [page_num_1based]

        careers = table_rows

        # 디버그 로그(원본과 같은 형식)
        try:
            _agent_log(
                run_id="table-only",
                hypothesis_id="T",
                location="page_2_parser_table_only.py:parse_page_2:return",
                message="table-only parser returning rows",
                data={
                    "page_num_1based": page_num_1based,
                    "n_rows": len(careers),
                },
            )
        except Exception:
            pass

    except Exception as e:
        # FIX: 원본과 동일하게 cp949 콘솔 호환을 위해 이모지 미사용
        print(f"[ERROR] 제2쪽 파싱 오류 (표 단독): {e}")

    return careers
