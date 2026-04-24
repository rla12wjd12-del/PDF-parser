#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
건설기술인 경력증명서 PDF → JSON 파서
메인 실행 스크립트
"""

import json
import argparse
import logging
import sys
from pathlib import Path
import zipfile
from datetime import datetime
import re

# 현재 디렉토리를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent))

from parsers.page_1_parser import parse_page_1
from parsers.page_2_parser_table_only import parse_page_2
from parsers.page_2_parser import merge_cross_page_tech_overviews, _strip_tail_job_duty, _is_footer_or_header_line
from parsers.page_3_parser import (
    parse_page_3,
    _parse_recent_1y_service_stats,
    _parse_cm_work_periods,
)
from parsers.page_summary_parser import parse_page_summary
from parsers.document_context import DocumentContext
from parsers.section_parsers import (
    parse_award_info,
    extract_award_section_text,
    count_award_data_lines_in_section_text,
    _find_award_table_header_idx,
    _is_award_table_boundary_row,
)
from excel_export import export_dict_to_excel_workbook
from parsers.quality_gate import check_field_completeness

BASE_DIR = Path(__file__).parent
JSON_OUTPUT_DIR = BASE_DIR / "json_output"
EXCEL_OUTPUT_DIR = BASE_DIR / "excel_output"


def count_expected_awards_from_pdf(ctx: DocumentContext, max_pages: int = 3) -> int:
    """
    상훈 기대 건수를 추정한다.

    FIX: 기존 텍스트 패턴 방식은 PDF 레이아웃에 따라 날짜+기관이 다른 줄에 추출되면
    0을 반환하는 문제가 있었다.
    개선 전략:
      1차) extract_tables()로 상훈 표를 찾아 데이터 행 수를 직접 카운팅 (parse_award_info와 동일 경로)
      2차) 텍스트 폴백 — extract_award_section_text + count_award_data_lines_in_section_text
    """
    # FIX: 상훈 기대 건수는 "섹션 파서"와 동일한 규칙으로 계산해야 한다.
    #      (표 추출/텍스트 추출 편차로 인해 간접 카운팅은 과소/과대 계상 가능)
    #      가장 안전한 기준은 제1쪽 통합 파서(`parse_page_1`)가 산출한 상훈 레코드 수다.
    try:
        p1 = parse_page_1(ctx, page_num=0) or {}
        aw = list(p1.get("상훈") or [])
        real = [
            a
            for a in aw
            if re.match(r"^\d{4}-\d{2}-\d{2}$", str((a or {}).get("수여일") or "").strip())
        ]
        if real:
            return len(real)
        return 0
    except Exception:
        pass

    # 폴백: 페이지 단위 상훈 섹션 파서로 계산
    awards: list[dict] = []
    for i in range(min(max_pages, ctx.total_pages)):
        p = ctx.pages[i]
        try:
            awards.extend(parse_award_info(p))
        except Exception:
            pass

    # '해당없음'만 있으면 기대 건수는 0으로 본다.
    real = [
        a
        for a in (awards or [])
        if re.match(r"^\d{4}-\d{2}-\d{2}$", str(a.get("수여일") or "").strip())
    ]
    if not real:
        return 0

    # 날짜+기관 기준으로 중복 제거(종류및근거가 더 긴 쪽 유지)
    dedup: dict[tuple[str, str], dict] = {}
    for a in real:
        dt = str(a.get("수여일") or "").strip()
        inst = re.sub(r"\s+", "", str(a.get("수여기관") or "").strip())
        if not dt:
            continue
        key = (dt, inst)
        cur = dedup.get(key)
        if cur is None:
            dedup[key] = a
            continue
        if len(str(a.get("종류및근거") or "")) > len(str(cur.get("종류및근거") or "")):
            dedup[key] = a
    return len(dedup)


def log_technical_career_field_issues(result: dict, pdf_path: str = "") -> list:
    """
    기술경력 및 건설사업관리경력의 사업명 누락·타입 오류를 콘솔에 남기고,
    오류 목록을 반환한다(JSON/Excel 저장용).
    """
    _label = pdf_path or "—"
    errors: list = []

    def _check_section(section_key: str) -> None:
        rows = result.get(section_key)
        if not isinstance(rows, list):
            msg = (
                f"{section_key} 섹션이 list가 아님: {type(rows).__name__} "
                f"(PDF: {_label})"
            )
            print(f"[ERROR] {msg}")
            errors.append({"섹션": section_key, "인덱스": -1, "오류유형": "타입오류(섹션)", "내용": msg})
            return
        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                msg = (
                    f"{section_key}[{i}] 레코드가 dict가 아님: {type(row).__name__} "
                    f"(PDF: {_label})"
                )
                print(f"[ERROR] {msg}")
                errors.append({"섹션": section_key, "인덱스": i, "오류유형": "타입오류(레코드)", "내용": msg})
                continue
            name = row.get("사업명")
            if not isinstance(name, str):
                msg = (
                    f"{section_key}[{i}] 사업명 타입 오류: 기대 str, 실제 {type(name).__name__} "
                    f"(PDF: {_label})"
                )
                print(f"[ERROR] {msg}")
                errors.append({"섹션": section_key, "인덱스": i, "오류유형": "사업명타입오류", "내용": msg,
                               "참여기간_시작일": str(row.get("참여기간_시작일", "")),
                               "참여기간_종료일": str(row.get("참여기간_종료일", ""))})
                continue
            if not name.strip():
                msg = (
                    f"{section_key}[{i}] 사업명이 비어 있음 "
                    f"(참여기간: {row.get('참여기간_시작일', '')} ~ {row.get('참여기간_종료일', '')}, "
                    f"PDF: {_label})"
                )
                print(f"[ERROR] {msg}")
                errors.append({"섹션": section_key, "인덱스": i, "오류유형": "사업명누락",
                               "참여기간_시작일": str(row.get("참여기간_시작일", "")),
                               "참여기간_종료일": str(row.get("참여기간_종료일", "")),
                               "내용": msg})

    _check_section("기술경력")
    _check_section("건설사업관리및감리경력")
    return errors


def pick_pdf_file_via_dialog(initial_dir: str | None = None) -> str | None:
    """
    OS 파일 선택 대화상자를 통해 PDF 파일 경로를 선택합니다.

    Returns:
        str | None: 선택된 파일 경로(취소 시 None)
    """
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        # 일부 환경에서 콘솔 창 맨 뒤로 가는 문제 완화
        try:
            root.attributes("-topmost", True)
            root.update()
        except Exception:
            pass

        file_path = filedialog.askopenfilename(
            title="PDF 파일 선택",
            initialdir=initial_dir or "",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
        )
        root.destroy()
        return file_path or None
    except Exception:
        # tkinter 미설치/GUI 불가 환경 등
        return None


def pick_json_file_via_dialog(initial_dir: str | None = None) -> str | None:
    """파일 선택 대화상자로 JSON 파일 경로를 선택합니다."""
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        try:
            root.attributes("-topmost", True)
            root.update()
        except Exception:
            pass

        file_path = filedialog.askopenfilename(
            title="JSON 파일 선택",
            initialdir=initial_dir or "",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        root.destroy()
        return file_path or None
    except Exception:
        return None


def _filter_career_page_details_for_parse_error(page_details: list[dict]) -> list[dict]:
    """
    항목수불일치 시 JSON '_파싱오류'에 넣을 페이지 상세를 줄인다.
    ~·건수·┖→가 모두 0인 페이지는 PDF 경력 행 카운트와 무관하므로 제외한다.
    """
    out: list[dict] = []
    for d in page_details or []:
        if not isinstance(d, dict):
            continue
        try:
            n_tilde = int(d.get("~개수") or 0)
            n_rows = int(d.get("건수") or 0)
            n_border = int(d.get("페이지경계┖→") or 0)
            n_table = int(d.get("표내┖→") or 0)
        except (TypeError, ValueError):
            continue
        if n_tilde > 0 or n_rows > 0 or n_border > 0 or n_table > 0:
            out.append(d)
    return out


_DATE_STRICT_CAREER = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")


def _line_starts_with_career_continue_arrow(s: str) -> bool:
    """기술경력 개요 연장 ┖→ (유니코드 동형 포함)."""
    t = (s or "").strip()
    if not t:
        return False
    if t.startswith("┖→"):
        return True
    if t.startswith("\u2516\u2192"):
        return True
    return False


def _tech_career_page_content_lines(text: str) -> list[str]:
    """헤더/푸터 제거 후 기술경력 페이지 본문 줄."""
    raw_lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in (text or "").splitlines()]
    return [ln for ln in raw_lines if ln and not _is_footer_or_header_line(ln)]


def _analyze_tech_career_page_metrics_from_lines(lines: list[str]) -> dict[str, int]:
    """
    한 페이지(전처리된 줄 목록)에서 유효 ~ 개수·┖→ 분류·페이지별 검증용 상한을 계산한다.

    - n_tilde: 참여기간 블록으로 인정되는 ~ 줄 수
    - legacy_건수: 전역 항목수 검증용 (기존) = n_tilde - 페이지경계┖→
    - capacity_6_minus_arrow: 레이아웃 상한 max(0, 6 - ┖→전체)
    """
    tilde_count = 0
    border_arrow_count = 0
    table_arrow_count = 0
    career_block_seen = False
    _PLACEHOLDER_DATE_LINE = re.compile(r"^\*{2,}\.\*{2,}\.\*{2,}$")

    for i, line in enumerate(lines):
        s = line.strip()

        if _line_starts_with_career_continue_arrow(s):
            if not career_block_seen:
                border_arrow_count += 1
            else:
                table_arrow_count += 1
            continue

        if s.lstrip().startswith("~"):
            has_prev_date = False
            for k in range(i - 1, max(-1, i - 13), -1):
                prev = lines[k].strip()
                if not prev:
                    continue
                # FIX: 일부 PDF에는 "****.**.**" 같은 마스킹/플레이스홀더 날짜 블록이 끼어있다.
                #      이 블록의 '~'는 실제 경력 1행을 의미하지 않으므로 유효 ~ 카운트에서 제외해야 한다.
                #      (플레이스홀더를 넘어 이전/다음 날짜를 잡아 ~를 카운트하면 PDF 원본 건수가 과대계상됨)
                if _PLACEHOLDER_DATE_LINE.match(prev):
                    break
                if _line_starts_with_career_continue_arrow(prev):
                    break
                if _DATE_STRICT_CAREER.match(prev):
                    has_prev_date = True
                    break
                if prev.lstrip().startswith("~"):
                    break

            has_next_date = False
            for k in range(i + 1, min(len(lines), i + 9)):
                nxt = lines[k].strip()
                if not nxt:
                    continue
                if _PLACEHOLDER_DATE_LINE.match(nxt):
                    break
                if _DATE_STRICT_CAREER.match(nxt):
                    has_next_date = True
                    break
                if re.sub(r"\s+", "", nxt) in ("근무중", "재직중"):
                    has_next_date = True
                    break
                if nxt.lstrip().startswith("~"):
                    break

            if has_prev_date and has_next_date:
                tilde_count += 1
                career_block_seen = True

    n_arrow_total = border_arrow_count + table_arrow_count
    # legacy_건수(검증용): 기존 방식과 동일하게 "페이지경계┖→"만 차감한다.
    # (단, 표내 ┖→는 같은 행의 연장 표시일 수 있어 차감하지 않는다.)
    legacy_건수 = max(0, tilde_count - border_arrow_count)
    capacity_6_minus_arrow = max(0, 6 - n_arrow_total)

    return {
        "n_tilde": tilde_count,
        "n_arrow_border": border_arrow_count,
        "n_arrow_table": table_arrow_count,
        "n_arrow_total": n_arrow_total,
        "capacity_6_minus_arrow": capacity_6_minus_arrow,
        "legacy_건수": legacy_건수,
    }


def _verify_tech_career_per_page_against_pdf(
    ctx: DocumentContext,
    tech_page_snapshots: list[tuple[int, int]],
) -> tuple[list[dict], list[dict]]:
    """
    기술경력 구간 각 페이지: PDF에서 센 유효 ~ 개수 vs 해당 페이지 parse_page_2 누적 차이.

    Returns:
        (전체_페이지_행_목록, 파싱오류용_항목_목록)
    """
    rows: list[dict] = []
    err_items: list[dict] = []
    if not tech_page_snapshots:
        return rows, err_items

    prev_cum = 0
    for page_idx, cum in tech_page_snapshots:
        parsed_n = cum - prev_cum
        prev_cum = cum
        if page_idx >= ctx.total_pages:
            continue
        text = ctx.get_text(page_idx) or ""
        lines = _tech_career_page_content_lines(text)
        m = _analyze_tech_career_page_metrics_from_lines(lines)
        n_tilde = m["n_tilde"]
        cap = m["capacity_6_minus_arrow"]
        n_arrow_total = m["n_arrow_total"]
        match = parsed_n == n_tilde
        overflow = n_tilde > cap

        row = {
            "페이지": page_idx + 1,
            "PDF_유효틸드수": n_tilde,
            "파싱건수": parsed_n,
            "페이지경계┖→": m["n_arrow_border"],
            "표내┖→": m["n_arrow_table"],
            "┖→전체": n_arrow_total,
            "상한6에서차감후": cap,
            "틸드_파싱_일치": match,
            "상한초과의심": overflow,
        }
        rows.append(row)

        if not match:
            print(
                f"    [WARN][기술경력_페이지검증] 페이지 {page_idx + 1}: "
                f"PDF 유효 ~ {n_tilde}건 vs 파싱 {parsed_n}건 (불일치)"
            )
            err_items.append(
                {
                    "오류유형": "기술경력_페이지별_틸드파싱불일치",
                    "섹션": "기술경력",
                    "페이지": page_idx + 1,
                    "PDF_유효틸드수": n_tilde,
                    "파싱건수": parsed_n,
                    "차이": parsed_n - n_tilde,
                    "내용": (
                        f"기술경력 페이지 {page_idx + 1}: "
                        f"PDF 유효 ~ {n_tilde}건 vs 해당 페이지 파싱 {parsed_n}건"
                    ),
                }
            )
        if overflow:
            print(
                f"    [WARN][기술경력_페이지검증] 페이지 {page_idx + 1}: "
                f"유효 ~ {n_tilde}건 > 상한(6-┖→전체) {cap}건 (상한 초과 의심)"
            )
            err_items.append(
                {
                    "오류유형": "기술경력_페이지별_상한초과의심",
                    "섹션": "기술경력",
                    "페이지": page_idx + 1,
                    "PDF_유효틸드수": n_tilde,
                    "┖→전체": n_arrow_total,
                    "상한6에서차감후": cap,
                    "내용": (
                        f"기술경력 페이지 {page_idx + 1}: "
                        f"유효 ~ {n_tilde}건이 6-{n_arrow_total}={cap} 상한을 초과"
                    ),
                }
            )

    return rows, err_items


def count_career_rows_by_tilde(
    ctx: DocumentContext,
    start_page: int,
    end_page: int,
) -> tuple[int, list[dict]]:
    """
    PDF 페이지들에서 경력 행 수를 계산한다.

    반환값: (총 경력 수, 페이지별 상세 결과 리스트)
    - 상세 결과 각 항목: {"페이지": N, "~개수": N, "페이지경계┖→": N, "표내┖→": N, "건수": N}

    ┖→ 분류 기준:
      - 페이지경계 ┖→: 해당 페이지에서 날짜(YYYY.MM.DD)+~ 패턴이 아직 한 번도 나오지 않은
        상태에서 등장 → 이전 경력의 개요 연장이므로 차감
      - 표내 ┖→: 날짜+~ 이후에 등장 → 표 셀 내 개요 연장이므로 차감 안 함

    건수(전역 검증용): ~개수 − 페이지경계┖→ (레거시 합산, 페이지별 틸드=파싱 검증과 별개).

    헤더/푸터 제거 후 처리하여 "Page : N", "성명 :" 등이 first_content_seen 판정을
    방해하는 기존 문제를 해결한다.
    """
    # NOTE:
    # - total_tilde: 페이지별 "유효 ~" 합계 (parse_page_2/_verify_tech_career_per_page_against_pdf 기준과 동일)
    # - total_legacy: 기존 레거시 방식(~ - 페이지경계┖→) 합계 (참고/로그용)
    total_tilde = 0
    total_legacy = 0
    page_details: list[dict] = []

    for page_idx in range(start_page, min(end_page, ctx.total_pages)):
        text = ctx.get_text(page_idx) or ""
        lines = _tech_career_page_content_lines(text)
        m = _analyze_tech_career_page_metrics_from_lines(lines)

        tilde_count = m["n_tilde"]
        border_arrow_count = m["n_arrow_border"]
        table_arrow_count = m["n_arrow_table"]
        page_count_legacy = m["legacy_건수"]
        total_tilde += tilde_count
        total_legacy += page_count_legacy

        detail = {
            "페이지": page_idx + 1,
            "~개수": tilde_count,
            "페이지경계┖→": border_arrow_count,
            "표내┖→": table_arrow_count,
            # 검증 기준(권장): 유효 ~ 합계
            "건수_틸드": tilde_count,
            # 레거시(참고): 유효 ~ - 페이지경계┖→
            "건수_legacy": page_count_legacy,
        }
        page_details.append(detail)

        arrow_summary = ""
        if border_arrow_count > 0 or table_arrow_count > 0:
            arrow_summary = (
                f", 페이지경계┖→ {border_arrow_count}개"
                + (f", 표내┖→ {table_arrow_count}개" if table_arrow_count > 0 else "")
            )
        print(
            f"    [검증] 페이지 {page_idx + 1}: ~ {tilde_count}개"
            f"{arrow_summary} → legacy {page_count_legacy}건"
        )

    # 반환 total은 "유효 ~ 합계"로 통일해 파서 건수와 직접 비교한다.
    return total_tilde, page_details


def count_pdf_items(ctx: DocumentContext, tech_start: int, cm_start: int,
                    cm_end: int, summary_start: int | None,
                    total_pages: int) -> dict[str, int]:
    """
    PDF 원본 텍스트에서 섹션별 항목 수를 패턴 기반으로 카운팅한다.

    기술경력·건설사업관리및감리경력은 parse_page_2/3와 동일한 파서 호출이 아닌
    페이지별 ~ 개수에서 '페이지경계┖→'만 빼 합산하는 레거시 방식이다
    (건수 = ~개수 − 페이지경계┖→; 표내┖→는 차감하지 않음).
    페이지마다 PDF 유효 ~ = 파싱 건수 여부는 별도 `_검증_기술경력_페이지별`로 검증한다.

    Returns:
        dict: {"교육훈련": N, "근무처": N, "국가기술자격": N, "상훈": N,
               "기술경력": N, "건설사업관리및감리경력": N}
    """
    _EDU_DATE_PAT = re.compile(r"\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}\.\d{2}\.\d{2}")
    _WP_START_PAT = re.compile(r"\d{4}\.\d{2}\.\d{2}\s*~")
    _LICENSE_PAT = re.compile(
        r"(기사|기술사|산업기사|기능사|기능장).*?\d{4}\.\d{2}\.\d{2}"
    )

    # 제1쪽 계열 텍스트 (0 ~ tech_start)
    page1_texts = []
    for i in range(min(tech_start, total_pages, ctx.total_pages)):
        page1_texts.append(ctx.get_text(i) or "")
    page1_combined = "\n".join(page1_texts)

    # --- 교육훈련 ---
    # FIX: 단순 텍스트 정규식(_EDU_DATE_PAT) 기반 카운팅은
    #      PDF 추출에서 날짜 범위가 다른 섹션 텍스트와 합쳐지면 과대/과소 계상될 수 있다.
    #      검증 목적에서는 제1쪽 통합 파서 결과와 동일한 기준(레코드 수)을 사용한다.
    edu_count = 0
    try:
        p1 = parse_page_1(ctx, page_num=0) or {}
        edu_count = len(p1.get("교육훈련") or [])
    except Exception:
        edu_count = len(_EDU_DATE_PAT.findall(page1_combined))

    # --- 근무처: '근무기간' 헤더 ~ 푸터 사이에서 'YYYY.MM.DD ~' 패턴 ---
    wp_count = 0
    for page_txt in page1_texts:
        lines = page_txt.splitlines()
        in_wp = False
        for ln in lines:
            stripped = ln.strip()
            if "근무기간" in stripped and "상호" in stripped:
                in_wp = True
                continue
            if in_wp:
                if "본 증명서는" in stripped or "인터넷으로" in stripped:
                    in_wp = False
                    continue
                hits = _WP_START_PAT.findall(stripped)
                wp_count += len(hits)

    # --- 국가기술자격: 종목 키워드 + 합격일 날짜가 있는 줄 ---
    license_count = 0
    for page_txt in page1_texts:
        lines = page_txt.splitlines()
        in_license = False
        for ln in lines:
            stripped = ln.strip()
            if "종목" in stripped and "합격일" in stripped:
                in_license = True
                continue
            if in_license:
                if any(kw in stripped for kw in ["학력", "졸업일", "교육기간", "교육훈련"]):
                    in_license = False
                    continue
                license_count += len(_LICENSE_PAT.findall(stripped))

    # --- 기술경력: ~ 개수 기반 독립 카운팅 (순환참조 없음) ---
    print("  [검증] 기술경력 페이지별 ~ 카운팅 시작...")
    tech_count, tech_page_details = count_career_rows_by_tilde(
        ctx, tech_start, min(cm_start, total_pages)
    )

    # --- 건설사업관리및감리경력: ~ 개수 기반 독립 카운팅 ---
    print("  [검증] 건설사업관리및감리경력 페이지별 ~ 카운팅 시작...")
    cm_count, cm_page_details = count_career_rows_by_tilde(
        ctx, cm_start, min(cm_end, total_pages)
    )

    # --- 상훈: 제1~3쪽 텍스트 블록에서 단일 수여일 데이터 줄 수 ---
    award_expected = 0
    try:
        award_expected = count_expected_awards_from_pdf(
            ctx, max_pages=min(3, total_pages)
        )
    except Exception:
        award_expected = 0

    return {
        "교육훈련": edu_count,
        "근무처": wp_count,
        "국가기술자격": license_count,
        "상훈": award_expected,
        "기술경력": tech_count,
        "건설사업관리및감리경력": cm_count,
        "_기술경력_페이지상세": tech_page_details,
        "_건설사업관리_페이지상세": cm_page_details,
    }


def parse_full_document(pdf_path: str) -> dict:
    """
    전체 PDF 문서 파싱
    
    Args:
        pdf_path: PDF 파일 경로
    
    Returns:
        dict: 전체 파싱 결과
    """
    print(f"\n{'='*60}")
    print(f"[INFO] PDF 파싱 시작: {pdf_path}")
    print(f"{'='*60}\n")
    
    result = {
        "인적사항": {},
        "등급": {
            "설계시공_등_직무분야": "",
            "설계시공_등_직무분야_등급": "",
            "설계시공_등_전문분야": "",
            "설계시공_등_전문분야_등급": "",
            "건설사업관리_직무분야": "",
            "건설사업관리_직무분야_등급": "",
            "건설사업관리_전문분야": "",
            "건설사업관리_전문분야_등급": "",
            "품질관리_등급": ""
        },
        "국가기술자격": [],
        "학력": [],
        "교육훈련": [],
        "상훈": [],
        "벌점및제재사항": {"벌점": "해당없음", "제재사항": "해당없음"},
        "근무처": [],
        "기술경력": [],
        "건설사업관리및감리경력": [],
        # CM 섹션 하단 별도 요약(각 경력 행에 주입 금지)
        "업무수행기간": {},
        "용역완성비율": {},
        "공사종류별인정일수": [],
        "직무전문분야별인정일수": []
    }
    
    try:
        with DocumentContext.open(pdf_path) as ctx:
            total_pages = ctx.total_pages
            print(f"[INFO] 총 페이지 수: {total_pages}")
        
            # 1. 제1쪽 파싱 (인적사항, 등급, 자격, 학력 등)
            print(f"\n[1/4] 제1쪽 파싱 시작...")
            page1_data = parse_page_1(ctx, page_num=0)
            result.update(page1_data)
            print("[OK] 제1쪽 파싱 완료")
        
            # 2. 제2쪽 파싱 (기술경력)
            print(f"\n[2/4] 기술경력 파싱 시작...")
            tech_careers = []

            # 문서마다 섹션 페이지 범위가 달라질 수 있어, 키워드로 동적으로 범위를 찾는다.
            def _find_first_page_index(keywords: list[str]) -> int | None:
                for idx in range(ctx.total_pages):
                    t = ctx.get_text(idx) or ""
                    if all(k in t for k in keywords):
                        return idx
                return None

            def _find_first_page_index_any(keyword_sets: list[list[str]]) -> int | None:
                for ks in keyword_sets:
                    hit = _find_first_page_index(ks)
                    if hit is not None:
                        return hit
                return None

            tech_start = _find_first_page_index_any([["1. 기술경력"], ["기술경력"]])
            cm_start = _find_first_page_index_any([["2. 건설사업관리 및 감리경력"], ["건설사업관리", "감리경력"]])
            summary_start = _find_first_page_index_any([["분야별 참여기간 인정일"], ["공사종류별 인정일수"], ["직무/전문분야별 인정일수"]])

            if tech_start is None:
                # 기존 가정 폴백
                tech_start = 3 if total_pages > 3 else 0
            tech_end = cm_start if cm_start is not None else (summary_start if summary_start is not None else total_pages)
            tech_end = max(min(tech_end, total_pages), tech_start)

            tech_page_snapshots: list[tuple[int, int]] = []
            tech_per_page_verify_errors: list[dict] = []
            for page_idx in range(tech_start, tech_end):
                careers = parse_page_2(ctx, page_idx)
                tech_careers.extend(careers)
                tech_page_snapshots.append((page_idx, len(tech_careers)))
            merge_cross_page_tech_overviews(
                ctx, tech_careers, tech_start, tech_end, tech_page_snapshots
            )

            # 중복(사업명+기간) 표시만: 삭제/병합하지 않고 플래그/그룹/건수만 기록한다.
            def _mark_duplicates_by_name_and_period(rows: list[dict], *, section: str) -> None:
                import re
                from collections import defaultdict

                def _norm(s: str) -> str:
                    return re.sub(r"\s+", " ", (s or "")).strip()

                groups: defaultdict[tuple, list[int]] = defaultdict(list)
                for i, r in enumerate(rows or []):
                    if not isinstance(r, dict):
                        continue
                    key = (
                        _norm(str(r.get("사업명") or "")),
                        _norm(str(r.get("참여기간_시작일") or "")),
                        _norm(str(r.get("참여기간_종료일") or "")),
                    )
                    if key[0] and key[1] and key[2]:
                        groups[key].append(i)

                dup_keys = [k for k, idxs in groups.items() if len(idxs) >= 2]
                dup_keys.sort()
                key_to_gid = {k: f"{section}:dup:{n+1}" for n, k in enumerate(dup_keys)}

                for k, idxs in groups.items():
                    if len(idxs) < 2:
                        continue
                    gid = key_to_gid.get(k) or f"{section}:dup:unknown"
                    for i in idxs:
                        rows[i]["_dup_name_period"] = True
                        rows[i]["_dup_name_period_group"] = gid
                        rows[i]["_dup_name_period_count"] = len(idxs)
                        rows[i]["_dup_name_period_key"] = {
                            "사업명": k[0],
                            "참여기간_시작일": k[1],
                            "참여기간_종료일": k[2],
                        }

            # Excel/검증용 보조 필드: 리스트/복합키를 사람이 읽기 쉬운 문자열로도 제공
            def _finalize_row_meta(rows: list[dict]) -> None:
                import re

                def _norm(s: str) -> str:
                    return re.sub(r"\s+", " ", (s or "")).strip()

                for r in (rows or []):
                    if not isinstance(r, dict):
                        continue
                    # pages -> string
                    pp = r.get("_pdf_pages")
                    if isinstance(pp, list):
                        try:
                            ints = []
                            for x in pp:
                                try:
                                    xi = int(x)
                                except Exception:
                                    continue
                                if xi > 0:
                                    ints.append(xi)
                            ints = sorted(set(ints))
                            r["_pdf_pages"] = ints
                            r["_pdf_pages_str"] = ",".join(str(x) for x in ints)
                        except Exception:
                            # 변환 실패 시 원본 유지
                            pass
                    # dup key -> string
                    if r.get("_dup_name_period"):
                        nm = _norm(str(r.get("사업명") or ""))
                        s = _norm(str(r.get("참여기간_시작일") or ""))
                        e = _norm(str(r.get("참여기간_종료일") or ""))
                        if nm and s and e:
                            r["_dup_name_period_key_str"] = f"{nm}|{s}|{e}"
            result["기술경력"] = tech_careers
            _mark_duplicates_by_name_and_period(result["기술경력"], section="기술경력")
            _finalize_row_meta(result["기술경력"])
            tph_rows, tph_errs = _verify_tech_career_per_page_against_pdf(
                ctx, tech_page_snapshots
            )
            result["_검증_기술경력_페이지별"] = tph_rows
            tech_per_page_verify_errors = tph_errs
            print(f"[OK] 기술경력 파싱 완료 (총 {len(tech_careers)}건)")
        
            # 3. 제3쪽 파싱 (건설사업관리 및 감리경력)
            print(f"\n[3/4] 건설사업관리 및 감리경력 파싱 시작...")
            cm_careers = []
            if cm_start is None:
                # 기존 가정 폴백
                cm_start = 7 if total_pages > 7 else tech_end
            cm_end = summary_start if summary_start is not None else total_pages
            cm_end = max(min(cm_end, total_pages), cm_start)

            for page_idx in range(cm_start, cm_end):
                careers = parse_page_3(ctx, page_idx)
                cm_careers.extend(careers)
            result["건설사업관리및감리경력"] = cm_careers
            _mark_duplicates_by_name_and_period(result["건설사업관리및감리경력"], section="건설사업관리및감리경력")
            _finalize_row_meta(result["건설사업관리및감리경력"])

            # CM 섹션 하단 요약 블록(업무수행기간/최근 1년 용역완성비율)은
            # "각 경력 행"이 아니라 "별도 항목"으로 JSON 최상위에 분리 저장한다.
            if cm_end > cm_start:
                try:
                    last_cm_txt = ctx.get_text(cm_end - 1) or ""
                except Exception:
                    last_cm_txt = ""
                try:
                    result["업무수행기간"] = _parse_cm_work_periods(last_cm_txt)
                except Exception:
                    result["업무수행기간"] = {}
                try:
                    result["용역완성비율"] = _parse_recent_1y_service_stats(last_cm_txt)
                except Exception:
                    result["용역완성비율"] = {}
            print(f"[OK] 건설사업관리 및 감리경력 파싱 완료 (총 {len(cm_careers)}건)")

            # 최종 사업명 꼬리 정제(직무분야/담당업무 오염 제거)
            for section_key in ["기술경력", "건설사업관리및감리경력"]:
                for row in result.get(section_key, []):
                    nm = str(row.get("사업명") or "").strip()
                    if not nm:
                        continue
                    # 사업명 선두 서비스 타입(기술용역 등) 제거
                    if nm.startswith("기술용역") and len(nm) > len("기술용역") + 1:
                        nm2 = re.sub(r"\s+", " ", nm[len("기술용역") :]).strip()
                        if nm2:
                            nm = nm2
                            row["사업명"] = nm
                    jf_known = str(row.get("직무분야") or "").strip()
                    dt_known = str(row.get("담당업무") or "").strip()
                    # 에러4 보강: 사업명 중간에 "직무분야 담당업무"가 끼어있는 경우도 제거
                    if jf_known and dt_known:
                        pat = re.compile(
                            rf"{re.escape(jf_known)}\s*{re.escape(dt_known)}"
                        )
                        hits = list(pat.finditer(nm))
                        if hits:
                            h = hits[-1]
                            nm2 = (nm[: h.start()] + nm[h.end() :]).strip()
                            nm2 = re.sub(r"\s+", " ", nm2).strip()
                            # 콤마 뒤 공백 정규화(가독성 + '...,모로코' 복원)
                            nm2 = re.sub(r",\s*", ", ", nm2)
                            nm2 = re.sub(r"\s+,", ",", nm2)
                            if nm2:
                                nm = nm2
                                row["사업명"] = nm
                    # 기존 꼬리 제거 로직
                    cleaned, jf, dt = _strip_tail_job_duty(nm)
                    if cleaned != nm:
                        row["사업명"] = cleaned
                        if jf and not jf_known:
                            row["직무분야"] = jf
                        if dt and not dt_known:
                            row["담당업무"] = dt
                    # 가독성 정규화: 콤마 뒤 공백(사업명에서만)
                    nm3 = str(row.get("사업명") or "").strip()
                    if nm3 and "," in nm3:
                        nm3 = re.sub(r",(?=\S)", ", ", nm3)
                        nm3 = re.sub(r"\s+", " ", nm3).strip()
                        row["사업명"] = nm3

            # 공사종류 보정: 일부 문서에서 공사종류가 이전 행 컨텍스트로 밀리는 사례가 있어
            # 사업명 키워드(도로/상수도/하수도 등)로 최소한의 교정을 수행한다.
            def _normalize_worktype_from_project_name(
                project_name: str, current_worktype: str
            ) -> str:
                name = re.sub(r"\s+", "", str(project_name or ""))
                cur = str(current_worktype or "").strip()
                cur_n = re.sub(r"\s+", "", cur)
                if not name:
                    return cur

                def _has_any(s: str, needles: list[str]) -> bool:
                    return any(n in s for n in needles)

                # 상하수도(복합): 사업명에 상하수도가 명시된 경우 단일 분류로 두면 오해 소지가 커
                # 가장 보수적으로 '상수도,하수도'로 통일한다.
                if "상하수도" in name:
                    if ("상수도" not in cur_n) or ("하수" not in cur_n):
                        return "상수도,하수도"

                # 상수도/정수장
                if "정수장" in name and ("정수장" not in cur_n):
                    return "상수도(정수장)"
                if _has_any(name, ["상수도", "광역상수도", "급수", "배수지"]) and ("상수도" not in cur_n):
                    return "상수도"
                # 하수도
                if _has_any(name, ["하수도", "하수처리", "하수관로"]) and ("하수" not in cur_n):
                    return "하수도"
                # 하천/골재
                if _has_any(name, ["하천", "골재", "부존량", "준설"]) and ("하천" not in cur_n):
                    return "하천"
                # 도로/교량
                if _has_any(name, ["고속도로", "국도", "지방도", "도로"]) and not _has_any(
                    cur_n, ["고속도로", "국도", "지방도", "도로"]
                ):
                    # 교량 키워드가 함께 있으면 조합으로 보강
                    if "교" in name or "교량" in name:
                        return "도로,교량"
                    return "도로"
                # 철도
                if "철도" in name and ("철도" not in cur_n):
                    return "철도"
                # 항만
                if "항만" in name and ("항만" not in cur_n):
                    return "항만"
                return cur

            for section_key in ["기술경력", "건설사업관리및감리경력"]:
                rows = result.get(section_key) or []
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    nm = str(row.get("사업명") or "").strip()
                    if not nm:
                        continue
                    # 발주자/공사종류 필드 밀림 보정:
                    # 발주자 칸에 '고속도로/국도/도로/상수도/하수도...' 같은 공사종류 토큰이 들어가고
                    # 공사종류가 비어있는 경우가 실제로 관측됨 → 공사종류로 이동.
                    issuer = str(row.get("발주자") or "").strip()
                    wt = str(row.get("공사종류") or "").strip()
                    if issuer and (not wt):
                        if issuer in {
                            "고속도로",
                            "국도",
                            "지방도",
                            "도로",
                            "상수도",
                            "하수도",
                            "하천",
                            "항만",
                            "철도",
                            "교량",
                            "공원",
                        }:
                            row["공사종류"] = issuer
                            row["발주자"] = ""
                            wt = issuer
                    new_wt = _normalize_worktype_from_project_name(nm, wt)
                    if new_wt and new_wt != wt:
                        row["공사종류"] = new_wt

            # 4. 요약 페이지 파싱 (분야별 참여기간 인정일)
            print(f"\n[4/4] 요약 페이지 파싱 시작...")
            if summary_start is not None:
                summary_data = parse_page_summary(ctx, summary_start)
                result["공사종류별인정일수"] = summary_data.get("공사종류별인정일수", [])
                result["직무전문분야별인정일수"] = summary_data.get(
                    "직무전문분야별인정일수", []
                )
            print("[OK] 요약 페이지 파싱 완료")

            # 5. 항목 수 검증 (PDF 원본 vs 파싱 결과)
            print(f"\n[5/5] 항목 수 검증 시작...")
            parsing_errors: list = []
            parsing_errors.extend(tech_per_page_verify_errors)
            try:
                pdf_counts = count_pdf_items(
                    ctx, tech_start, cm_start, cm_end, summary_start, total_pages
                )
                verification = {}
                sections = [
                    "교육훈련",
                    "근무처",
                    "국가기술자격",
                    "상훈",
                    "기술경력",
                    "건설사업관리및감리경력",
                ]
                mismatch_items = []
                for sec in sections:
                    pdf_n = pdf_counts.get(sec, 0)
                    # FIX: 상훈은 '해당없음'을 스키마 유지용 1행으로 둘 수 있으므로
                    #      건수 검증에서는 실제 날짜(YYYY-MM-DD) 데이터 행만 카운트한다.
                    if sec == "상훈":
                        parsed_n = sum(
                            1
                            for a in (result.get("상훈") or [])
                            if re.match(
                                r"^\d{4}-\d{2}-\d{2}$",
                                str((a or {}).get("수여일") or "").strip(),
                            )
                        )
                    else:
                        parsed_n = len(result.get(sec, []))
                    match = pdf_n == parsed_n
                    verification[sec] = {
                        "PDF원본": pdf_n,
                        "파싱결과": parsed_n,
                        "일치": match,
                    }
                    if not match:
                        mismatch_items.append((sec, pdf_n, parsed_n))

                result["_검증"] = verification

                if mismatch_items:
                    print(f"[WARN] 항목 수 불일치 발견!")
                    for sec, pdf_n, parsed_n in mismatch_items:
                        diff = parsed_n - pdf_n
                        direction = f"초과 +{diff}" if diff > 0 else f"누락 {diff}"
                        print(
                            f"[WARN]   {sec}: PDF원본 {pdf_n}건 vs 파싱결과 {parsed_n}건 ({direction})"
                        )

                        # 페이지별 ┖→ 상세 정보 첨부
                        page_detail_key = (
                            "_기술경력_페이지상세"
                            if sec == "기술경력"
                            else "_건설사업관리_페이지상세"
                            if sec == "건설사업관리및감리경력"
                            else None
                        )
                        page_details_for_sec = (
                            pdf_counts.get(page_detail_key, []) if page_detail_key else []
                        )

                        # ┖→가 있는 페이지 목록 추출 (로그용)
                        arrow_pages_info = []
                        for pd_item in page_details_for_sec:
                            b_arr = pd_item.get("페이지경계┖→", 0)
                            t_arr = pd_item.get("표내┖→", 0)
                            if b_arr > 0 or t_arr > 0:
                                arrow_pages_info.append(
                                    f"p{pd_item['페이지']}(경계┖→{b_arr}개, 표내┖→{t_arr}개)"
                                )
                        arrow_info_str = (
                            ", ".join(arrow_pages_info) if arrow_pages_info else "없음"
                        )

                        err_item = {
                            "오류유형": "항목수불일치",
                            "섹션": sec,
                            "PDF원본건수": pdf_n,
                            "파싱결과건수": parsed_n,
                            "차이": diff,
                            "내용": f"{sec}: PDF원본 {pdf_n}건 vs 파싱결과 {parsed_n}건 ({direction})",
                            "┖→감지페이지": arrow_info_str,
                        }
                        if page_details_for_sec:
                            filtered_pages = (
                                _filter_career_page_details_for_parse_error(
                                    page_details_for_sec
                                )
                            )
                            err_item["페이지별상세"] = filtered_pages
                            if len(filtered_pages) < len(page_details_for_sec):
                                err_item["페이지별상세_기록범위"] = (
                                    "활성 페이지만 기록 (~·건수·┖→ 중 하나라도 있는 페이지): "
                                    f"{len(filtered_pages)}/{len(page_details_for_sec)}페이지"
                                )
                        parsing_errors.append(err_item)

                        if arrow_pages_info:
                            print(f"[WARN]   ┖→ 감지 페이지: {arrow_info_str}")
                else:
                    print(f"[OK] 항목 수 검증 통과 ({len(sections)}개 항목 모두 일치)")
            except Exception as e:
                print(f"[WARN] 항목 수 검증 중 오류 발생 (파싱 결과에는 영향 없음): {e}")
                import traceback

                traceback.print_exc()

            # 사업명 필드 오류 검증 및 결과 기록
            field_errors = log_technical_career_field_issues(result, pdf_path)
            parsing_errors.extend(field_errors)

            # 추출 단계 예외(조용한 누락 방지): ctx.errors 병합
            if ctx.errors:
                parsing_errors.extend(ctx.errors)
                print(f"[WARN] 추출 단계 오류 {len(ctx.errors)}건이 감지되었습니다.")

            # 필드 단위 완전성 게이트
            completeness_errors = check_field_completeness(result)
            if completeness_errors:
                parsing_errors.extend(completeness_errors)
                print(f"[WARN] 필드 완전성 오류 {len(completeness_errors)}건이 감지되었습니다.")

            if parsing_errors:
                result["_파싱오류"] = parsing_errors
                print(
                    f"[WARN] 총 {len(parsing_errors)}건의 파싱 오류가 감지되었습니다. '_파싱오류' 항목에 기록되었습니다."
                )
            else:
                print(f"[OK] 검증 통과 (추출 오류·사업명·필드완전성 모두 이상 없음)")

    except Exception as e:
        print(f"\n[ERROR] 파싱 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("[OK] 전체 파싱 완료")
    print(f"{'='*60}\n")
    
    return result


def validate_output(result: dict, pdf_path: str) -> dict:
    """
    파싱 결과 검증
    
    Args:
        result: 파싱 결과 딕셔너리
        pdf_path: PDF 파일 경로
    
    Returns:
        dict: 검증 리포트
    """
    report = {
        "총_키_개수": len(result),
        "누락_키": [],
        "빈_배열": [],
        "세부_통계": {}
    }
    
    # 필수 키 체크
    required_keys = [
        "인적사항", "등급", "국가기술자격", "학력", "교육훈련", "상훈",
        "벌점및제재사항", "근무처", "기술경력", "건설사업관리및감리경력",
        "업무수행기간", "용역완성비율",
        "공사종류별인정일수", "직무전문분야별인정일수"
    ]
    
    for key in required_keys:
        if key not in result:
            report["누락_키"].append(key)
        elif isinstance(result[key], list) and len(result[key]) == 0:
            report["빈_배열"].append(key)
    
    # 세부 통계
    report["세부_통계"] = {
        "국가기술자격_개수": len(result.get("국가기술자격", [])),
        "학력_개수": len(result.get("학력", [])),
        "교육훈련_개수": len(result.get("교육훈련", [])),
        "상훈_개수": len(result.get("상훈", [])),
        "근무처_개수": len(result.get("근무처", [])),
        "기술경력_개수": len(result.get("기술경력", [])),
        "건설사업관리및감리경력_개수": len(result.get("건설사업관리및감리경력", [])),
    }
    
    return report


def main():
    """메인 함수"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(BASE_DIR / "parse.log"), encoding="utf-8"),
        ],
    )

    parser = argparse.ArgumentParser(
        description='건설기술인 경력증명서 PDF → JSON 파서'
    )
    parser.add_argument(
        'pdf_paths',
        nargs='*',
        default=None,
        help='PDF 파일 경로(들). 여러 개를 주면 순차 처리합니다.'
    )
    parser.add_argument(
        '--pick',
        action='store_true',
        help='파일 선택 창을 열어 PDF 파일을 선택합니다 (pdf_path 보다 우선)'
    )
    parser.add_argument(
        '--out',
        '-o',
        default=None,
        help='출력 JSON 파일 경로 (미지정 시: {성명 또는 PDF파일명}_{오늘(파싱일자)}.json)'
    )
    parser.add_argument(
        '--validate',
        '-v',
        action='store_true',
        help='검증 리포트 출력'
    )
    parser.add_argument(
        '--excel',
        action='store_true',
        default=True,
        help='파싱 결과(또는 JSON 입력)를 섹션별 시트가 있는 하나의 .xlsx로 보냅니다 (기본: 켜짐)'
    )
    parser.add_argument(
        '--no-excel',
        dest='excel',
        action='store_false',
        help='엑셀(.xlsx) 생성을 하지 않습니다'
    )
    parser.add_argument(
        '--excel-dir',
        default=None,
        help='엑셀 저장 폴더 (미지정 시 JSON과 같은 폴더, 파일명은 {JSON stem}.xlsx)'
    )
    parser.add_argument(
        '--excel-out',
        default=None,
        metavar='PATH',
        help='엑셀 파일 전체 경로 (지정 시 --excel-dir 보다 우선)'
    )
    parser.add_argument(
        '--from-json',
        default=None,
        metavar='PATH',
        help='지정한 JSON을 읽어 --excel 등 후처리만 합니다 (--pick-json 과 병행 가능)'
    )
    parser.add_argument(
        '--pick-json',
        action='store_true',
        help='JSON 파일 선택 창을 엽니다 (--excel 과 함께, --from-json 보다 우선)'
    )
    
    args = parser.parse_args()

    # JSON만 읽어 엑셀보내기
    if args.pick_json or args.from_json is not None:
        EXCEL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        if not args.excel and not args.validate:
            print("[ERROR] 실행할 작업이 없습니다. 엑셀 생성은 기본 켜짐이며, 끄려면 --no-excel 을 사용하세요.")
            sys.exit(1)
        if args.pick_json:
            json_path_str = pick_json_file_via_dialog()
            if not json_path_str:
                print("[ERROR] JSON 파일 선택이 취소되었거나 선택 창을 열 수 없습니다.")
                sys.exit(1)
        else:
            json_path_str = args.from_json
        json_path = Path(json_path_str)
        if not json_path.exists():
            print(f"[ERROR] JSON 파일을 찾을 수 없습니다: {json_path}")
            sys.exit(1)
        try:
            with open(json_path, encoding="utf-8") as f:
                result = json.load(f)
        except Exception as e:
            print(f"[ERROR] JSON 로드 실패: {e}")
            sys.exit(1)
        if not isinstance(result, dict):
            print("[ERROR] JSON 최상위는 객체(dict)여야 합니다.")
            sys.exit(1)
        json_resolved = json_path.resolve()
        if args.excel:
            if args.excel_out:
                excel_path = Path(args.excel_out)
                if not excel_path.is_absolute():
                    excel_path = EXCEL_OUTPUT_DIR / excel_path
            elif args.excel_dir:
                excel_dir = Path(args.excel_dir)
                if not excel_dir.is_absolute():
                    excel_dir = BASE_DIR / excel_dir
                excel_path = excel_dir / f"{json_path.stem}.xlsx"
            else:
                excel_path = EXCEL_OUTPUT_DIR / f"{json_path.stem}.xlsx"
            try:
                excel_path.parent.mkdir(parents=True, exist_ok=True)
                out = export_dict_to_excel_workbook(result, excel_path)
                sheet_count = len(result)
                print(f"[OK] 엑셀 저장 완료: {out}")
                print(f"[INFO] 시트 수(최상위 키): {sheet_count}")
            except Exception as e:
                print(f"[ERROR] 엑셀 저장 실패: {e}")
                sys.exit(1)
        if args.validate:
            print("\n" + "=" * 60)
            print("[INFO] 검증 리포트 (JSON→엑셀 모드, PDF 경로 없음)")
            print("=" * 60)
            report = validate_output(result, str(json_path))
            print(json.dumps(report, ensure_ascii=False, indent=2))
            print("=" * 60)
        return 0

    # 기술경력/건설사업관리및감리경력: 문자열 내 줄바꿈 제거 후 저장
    # - 파서 내부에서 가독성을 위해 '\n'로 누적하는 필드(개요/적용공법/비고 등)가 있어
    #   JSON 저장 시에는 줄바꿈을 공백으로 치환해 한 줄로 정리한다.
    def _squash_newlines_in_obj(obj):
        if isinstance(obj, str):
            if ("\n" not in obj) and ("\r" not in obj):
                return obj
            # 줄바꿈은 "공백으로 치환"하면 줄바꿈 자리에 불필요한 공백 1칸이 남을 수 있어
            # 줄바꿈 + 앞뒤 공백까지 통째로 제거한다.
            # 예) "A\nB" -> "AB", "A \n B" -> "AB"
            s = re.sub(r"[ \t]*\r?\n[ \t]*", "", obj)
            # 남아있는 공백/탭은 1칸으로 정리
            s = re.sub(r"[ \t]+", " ", s).strip()
            return s
        if isinstance(obj, list):
            return [_squash_newlines_in_obj(v) for v in obj]
        if isinstance(obj, dict):
            return {k: _squash_newlines_in_obj(v) for k, v in obj.items()}
        return obj

    def _parse_one_pdf(pdf_path: str) -> int:
        # PDF 파일 존재 확인
        if not pdf_path or not Path(pdf_path).exists():
            print(f"[ERROR] PDF 파일을 찾을 수 없습니다: {pdf_path}")
            return 1

        pdf_path_obj = Path(pdf_path)
        if pdf_path_obj.suffix.lower() != ".pdf":
            print(f"[ERROR] 입력 파일은 PDF(.pdf)여야 합니다: {pdf_path}")
            return 1
        if zipfile.is_zipfile(pdf_path_obj):
            print(f"[ERROR] ZIP 파일은 지원하지 않습니다. PDF 파일을 입력해주세요: {pdf_path}")
            return 1

        # PDF 파싱
        result = parse_full_document(pdf_path)

        for _k in ["기술경력", "건설사업관리및감리경력"]:
            if isinstance(result.get(_k), list):
                result[_k] = _squash_newlines_in_obj(result[_k])

        # JSON 파일로 저장
        JSON_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        if args.out:
            output_path = Path(args.out)
            if not output_path.is_absolute():
                output_path = JSON_OUTPUT_DIR / output_path
        else:
            name = str((result.get("인적사항") or {}).get("성명") or "").strip()
            # FIX: 결과 파일명 날짜는 "현재 파싱일자"를 사용한다.
            # (서류출력일자=PDF 발급일과 다를 수 있어 사용자 기대와 어긋나는 사례가 있음)
            issue_date = datetime.now().strftime("%Y%m%d")

            safe_name = re.sub(r'[<>:"/\\\\|?*\\s]+', "_", name).strip("_")
            safe_date = re.sub(r"[^0-9]", "", issue_date)  # YYYYMMDD 형태로 정규화

            if not safe_name:
                # 성명 파싱이 실패한 경우: 입력 PDF 파일명을 사용(가장 예측 가능)
                safe_name = re.sub(r'[<>:"/\\\\|?*\\s]+', "_", pdf_path_obj.stem).strip("_") or "output"
            if not safe_date:
                safe_date = datetime.now().strftime("%Y%m%d")

            stem = f"{safe_name}_{safe_date}"
            output_path = JSON_OUTPUT_DIR / f"{stem}.json"

            # 파일명 충돌 방지: 같은 이름이 있으면 _2, _3 ... 추가
            if output_path.exists():
                n = 2
                while True:
                    candidate = JSON_OUTPUT_DIR / f"{stem}_{n}.json"
                    if not candidate.exists():
                        output_path = candidate
                        break
                    n += 1
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"[OK] 결과 저장 완료: {output_path}")
            print(f"[INFO] 파일 크기: {output_path.stat().st_size:,} bytes")
        except Exception as e:
            print(f"[ERROR] JSON 저장 실패: {e}")
            return 1

        if args.excel:
            EXCEL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            if args.excel_out:
                excel_path = Path(args.excel_out)
                if not excel_path.is_absolute():
                    excel_path = EXCEL_OUTPUT_DIR / excel_path
            elif args.excel_dir:
                excel_dir = Path(args.excel_dir)
                if not excel_dir.is_absolute():
                    excel_dir = BASE_DIR / excel_dir
                excel_path = excel_dir / f"{output_path.stem}.xlsx"
            else:
                excel_path = EXCEL_OUTPUT_DIR / f"{output_path.stem}.xlsx"
            try:
                excel_path.parent.mkdir(parents=True, exist_ok=True)
                out = export_dict_to_excel_workbook(result, excel_path)
                print(f"[OK] 엑셀 저장 완료: {out}")
                print(f"[INFO] 시트 수(최상위 키): {len(result)}")
            except Exception as e:
                print(f"[ERROR] 엑셀 저장 실패: {e}")
                return 1

        # 검증 리포트 출력
        if args.validate:
            print("\n" + "="*60)
            print("[INFO] 검증 리포트")
            print("="*60)
            report = validate_output(result, pdf_path)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            print("="*60)

        return 0

    # PDF 경로 결정: --pick 우선, 아무 경로도 없으면 선택창 시도
    pdf_paths = list(args.pdf_paths or [])
    if args.pick or not pdf_paths:
        picked = pick_pdf_file_via_dialog()
        if picked:
            pdf_paths = [picked]
        else:
            print("[ERROR] 파일 선택이 취소되었거나 선택 창을 열 수 없습니다.")
            sys.exit(1)

    # 여러 PDF 순차 처리
    overall_rc = 0
    for p in pdf_paths:
        rc = _parse_one_pdf(p)
        if rc != 0:
            overall_rc = rc
    return overall_rc


if __name__ == "__main__":
    sys.exit(main())
