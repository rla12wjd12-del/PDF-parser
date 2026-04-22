#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_validator.py
=================
여러 PDF를 한 번에 파싱하고, 파싱 결과(JSON)의 품질을 자동으로 검증·리포팅한다.

검증 항목:
  1. 필수 키 존재 여부 (인적사항, 기술경력, 건설사업관리및감리경력, 상훈, 근무처)
  2. 사업명 품질 — 공란("") / fallback("(사업명 미상)", "본사") 비율
  3. _검증 섹션의 PDF원본 vs 파싱결과 불일치 건수
  4. 날짜 형식 오류 (YYYY-MM-DD 형식이 아닌 값)
  5. 발주자·공사종류 공란 비율 (데이터 밀림 지표)

사용법:
  python test_validator.py                  # originalPDF/ 의 모든 PDF 파싱 후 검증
  python test_validator.py --no-reparse     # json_output/ 의 기존 JSON만 검증
  python test_validator.py --pdf 홍성달.pdf  # 특정 파일만 처리
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── 프로젝트 루트를 sys.path에 추가 ──────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# // FIX: 공사종류 카탈로그 기반 경계 오염 탐지/리포팅 지원
try:
    from parsers.worktype_classifier import (
        is_worktype_phrase,
        split_issuer_and_worktype_by_catalog,
    )
except Exception:  # pragma: no cover
    is_worktype_phrase = None
    split_issuer_and_worktype_by_catalog = None

# ── 상수 ─────────────────────────────────────────────────────────────────
PDF_DIR = ROOT / "originalPDF"
JSON_DIR = ROOT / "json_output"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# FIX: 일부 PDF는 날짜를 마스킹(****.**.**) 처리하며, 파서에서는 이를 '****-**-**'로 보존한다.
MASKED_ISO_DATE = "****-**-**"
# FIX: "본사"는 본사 대기/소속 기간으로 실제 유효 경력이므로 fallback에서 제거.
# 진짜 파싱 실패는 빈 문자열("")과 "(사업명 미상)"뿐이다.
FALLBACK_NAMES = {"(사업명 미상)", ""}
REQUIRED_KEYS = ["인적사항", "기술경력", "건설사업관리및감리경력", "상훈", "근무처"]

# FIX: 필드 밀림(개요→사업명 오염 등) 탐지를 위한 휴리스틱
SHIFT_BAD_IN_NAME_RE = re.compile(
    r"(\(\s*\d[\d,]*\s*일\s*\))|(^\d{4}\.\d{2})|(\s~\s)|(^~)|(\b┖→\b)|(\d{1,3}(?:,\d{3})+\s*$)"
)

# 개요가 "다음 사업명"으로 오염되는 케이스는 보통
# - 개요가 짧고(제목 한 줄 수준)
# - 끝이 '...용역/실시설계/건설공사'로 끊기며
# - 수치/단위/규모 정보(km, ㎡ 등)가 거의 없는 형태로 나타난다.
OVERVIEW_TITLELIKE_END_RE = re.compile(r"(기본\s*및\s*실시설계|실시설계|건설공사|용역)\s*$")
OVERVIEW_HAS_SCALE_HINT_RE = re.compile(r"(\d|km|㎞|m\b|㎡|m2|ha|D\d|L=|B=|V=|Q=|H=|%|~|:|-)")


# ═══════════════════════════════════════════════════════════════════════════
# 유틸리티
# ═══════════════════════════════════════════════════════════════════════════

def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return None


def _parse_pdf(pdf_path: Path) -> Optional[Dict[str, Any]]:
    """main.py 의 parse_full_document를 호출해 JSON 딕셔너리를 반환한다."""
    try:
        from main import parse_full_document
        result = parse_full_document(str(pdf_path))
        return result
    except Exception as e:
        print(f"  [ERROR] 파싱 실패: {e}")
        return None


def _date_fields_in_career(row: Dict[str, Any]) -> List[str]:
    return [
        str(row.get("참여기간_시작일") or "").strip(),
        str(row.get("참여기간_종료일") or "").strip(),
    ]


# ═══════════════════════════════════════════════════════════════════════════
# 검증 로직
# ═══════════════════════════════════════════════════════════════════════════

class ValidationResult:
    def __init__(self, pdf_name: str):
        self.pdf_name = pdf_name
        self.issues: List[str] = []          # WARN/FAIL 메시지 목록
        self.stats: Dict[str, Any] = {}
        self.level = "PASS"                   # PASS / WARN / FAIL

    def warn(self, msg: str):
        self.issues.append(f"  [WARN]  {msg}")
        if self.level == "PASS":
            self.level = "WARN"

    def fail(self, msg: str):
        self.issues.append(f"  [FAIL]  {msg}")
        self.level = "FAIL"

    def summary_line(self) -> str:
        icon = {"PASS": "OK", "WARN": "!!", "FAIL": "XX"}.get(self.level, "?")
        stats_str = "  |  ".join(
            f"{k}: {v}" for k, v in self.stats.items()
        )
        return f"[{self.level}] {icon} {self.pdf_name}  —  {stats_str}"


def validate(data: Dict[str, Any], pdf_name: str) -> ValidationResult:
    vr = ValidationResult(pdf_name)

    # ── 1. 필수 키 존재 ──────────────────────────────────────────────────
    for key in REQUIRED_KEYS:
        if key not in data:
            vr.fail(f"필수 키 누락: '{key}'")

    # ── 2. 기술경력 검증 ─────────────────────────────────────────────────
    tech = data.get("기술경력") or []
    tech_total = len(tech)
    tech_fallback = sum(
        1 for r in tech
        if str(r.get("사업명") or "").strip() in FALLBACK_NAMES
    )
    tech_no_issuer = sum(
        1 for r in tech
        if not str(r.get("발주자") or "").strip()
    )
    tech_bad_date = sum(
        1 for r in tech
        for d in _date_fields_in_career(r)
        if d and d != "근무중" and d != MASKED_ISO_DATE and not DATE_RE.match(d)
    )

    # ── 2.5 필드 시프트(개요/기간 토큰이 사업명에 섞임) ───────────────────
    tech_name_shift_suspects = 0
    tech_overview_shift_suspects = 0
    for r in tech:
        name = " ".join(str(r.get("사업명") or "").split()).strip()
        overview = str(r.get("공사(용역)개요") or "")
        if name and SHIFT_BAD_IN_NAME_RE.search(name):
            tech_name_shift_suspects += 1
        # FIX: 너무 광범위한 suffix 매칭은 정상 개요도 대량 WARN이므로,
        # '짧고(title-like) + 규모 힌트가 거의 없는' 케이스만 시프트 의심으로 본다.
        ov = " ".join(str(overview or "").split()).strip()
        # FIX: 개요가 정상적으로 "…계획수립, …기본및실시설계"처럼 서술형인 문서가 많아
        # 길이 제한을 더 강하게 둬서 "제목만 들어간 것 같은" 케이스만 의심한다.
        if ov and len(ov) <= 40 and OVERVIEW_TITLELIKE_END_RE.search(ov) and not OVERVIEW_HAS_SCALE_HINT_RE.search(ov):
            tech_overview_shift_suspects += 1

    # // FIX: 발주자/공사종류 경계 오염 탐지
    tech_worktype_empty_but_issuer_has_tail = 0
    tech_issuer_contains_worktype = 0
    if tech and is_worktype_phrase and split_issuer_and_worktype_by_catalog:
        for r in tech:
            issuer = " ".join(str(r.get("발주자") or "").split()).strip()
            wt = " ".join(str(r.get("공사종류") or "").split()).strip()
            if not issuer:
                continue
            # 1) 공사종류가 비었는데, 발주자 tail에서 공사종류가 추출되면 경계 붕괴로 카운트
            if not wt:
                try:
                    iss2, wt2 = split_issuer_and_worktype_by_catalog(str(r.get("발주자") or ""), project_root=str(ROOT))
                except TypeError:
                    # 함수 시그니처 변경/불일치 방어
                    iss2, wt2 = issuer, ""
                if wt2:
                    tech_worktype_empty_but_issuer_has_tail += 1
            # 2) 발주자 전체가 카탈로그 공사종류로 인식되는(또는 포함되는) 경우는 오염 가능성이 높음
            #    단, 발주자 자체가 공사종류 단어인 특이 케이스가 거의 없으므로 WARN 지표로만 사용.
            if is_worktype_phrase(issuer, project_root=str(ROOT)):
                tech_issuer_contains_worktype += 1

    vr.stats["기술경력"] = tech_total
    if tech_fallback:
        ratio = tech_fallback / max(tech_total, 1) * 100
        msg = f"사업명 fallback {tech_fallback}/{tech_total}건 ({ratio:.0f}%)"
        (vr.fail if ratio >= 30 else vr.warn)(msg)
    if tech_bad_date:
        vr.fail(f"기술경력 날짜 형식 오류 {tech_bad_date}건")
    if tech_name_shift_suspects:
        vr.warn(f"기술경력 사업명 필드 시프트 의심(기간/일수/┖→/금액 토큰 포함) {tech_name_shift_suspects}건")
    if tech_overview_shift_suspects:
        vr.warn(f"기술경력 개요 필드 시프트 의심(개요 끝이 '...용역' 등으로 끊김) {tech_overview_shift_suspects}건")
    if tech_worktype_empty_but_issuer_has_tail:
        vr.warn(
            f"기술경력 공사종류 경계 의심(공사종류 비어있고 발주자 꼬리에서 공사종류 감지) "
            f"{tech_worktype_empty_but_issuer_has_tail}건"
        )
    if tech_issuer_contains_worktype:
        vr.warn(
            f"기술경력 발주자 전체가 공사종류로 인식되는 케이스 {tech_issuer_contains_worktype}건"
        )

    # ── 3. 건설사업관리 검증 ─────────────────────────────────────────────
    cm = data.get("건설사업관리및감리경력") or []
    cm_total = len(cm)
    cm_fallback = sum(
        1 for r in cm
        if str(r.get("사업명") or "").strip() in FALLBACK_NAMES
    )
    cm_bad_date = sum(
        1 for r in cm
        for d in _date_fields_in_career(r)
        if d and d != "근무중" and d != MASKED_ISO_DATE and not DATE_RE.match(d)
    )

    vr.stats["건설사업관리"] = cm_total
    if cm_fallback:
        ratio = cm_fallback / max(cm_total, 1) * 100
        msg = f"CM 사업명 fallback {cm_fallback}/{cm_total}건 ({ratio:.0f}%)"
        (vr.fail if ratio >= 30 else vr.warn)(msg)
    if cm_bad_date:
        vr.fail(f"건설사업관리 날짜 형식 오류 {cm_bad_date}건")

    # ── 4. 상훈 검증 ─────────────────────────────────────────────────────
    awards = data.get("상훈") or []
    vr.stats["상훈"] = len(awards)

    # _검증 섹션의 상훈 PDF원본 건수와 비교
    # FIX: count_expected_awards_from_pdf()가 표 기반으로 개선되었으므로
    # 불일치가 발생하면 파싱 오류로 간주.
    # PDF원본 0건이고 파싱도 0건이면 정상("수여 이력 없음"으로 처리).
    verify = data.get("_검증") or {}
    award_verify = verify.get("상훈") or {}
    pdf_count = award_verify.get("PDF원본")
    parsed_count = award_verify.get("파싱결과")
    if pdf_count is not None and parsed_count is not None:
        if pdf_count != parsed_count:
            vr.warn(f"상훈 건수 불일치 — PDF원본 {pdf_count}건 vs 파싱 {parsed_count}건")
    # FIX: 상훈 0건 자체는 경고하지 않음 — PDF에 수여 이력이 없는 경우가 정상

    # ── 5. _검증 섹션 불일치 ─────────────────────────────────────────────
    mismatch_sections = []
    for section, vdata in verify.items():
        if not isinstance(vdata, dict):
            continue
        # FIX: 상훈은 위 4번에서 이미 처리했으므로 중복 경고 방지
        if section == "상훈":
            continue
        if not vdata.get("일치", True):
            pdf_n = vdata.get("PDF원본", "?")
            parsed_n = vdata.get("파싱결과", "?")
            mismatch_sections.append(f"{section}(PDF:{pdf_n} / 파싱:{parsed_n})")
    if mismatch_sections:
        vr.warn(f"_검증 불일치: {', '.join(mismatch_sections)}")

    # ── 6. 근무처 날짜 형식 ──────────────────────────────────────────────
    workplaces = data.get("근무처") or []
    wp_bad_date = 0
    for wp in workplaces:
        for field in ["근무기간_시작", "근무기간_종료"]:
            d = str(wp.get(field) or "").strip()
            if d and d != "근무중" and d != MASKED_ISO_DATE and not DATE_RE.match(d):
                wp_bad_date += 1
    if wp_bad_date:
        vr.fail(f"근무처 날짜 형식 오류 {wp_bad_date}건")

    vr.stats["근무처"] = len(workplaces)
    return vr


# ═══════════════════════════════════════════════════════════════════════════
# 메인 실행
# ═══════════════════════════════════════════════════════════════════════════

def run(pdf_files: List[Path], no_reparse: bool) -> List[ValidationResult]:
    results = []
    for pdf_path in pdf_files:
        stem = pdf_path.stem
        print(f"\n{'─'*60}")
        print(f"처리 중: {pdf_path.name}")

        data = None

        if no_reparse:
            # json_output/ 에서 이름이 포함된 JSON 찾기.
            # JSON 파일명은 "[이름]_[날짜].json" 형식이므로
            # JSON의 이름 부분(첫 토큰)이 PDF 파일명 stem 안에 포함되는지 확인.
            # FIX: 동일 이름/날짜의 JSON이 여러 개면(예: _2, _3) 가장 최신 파일을 우선 사용한다.
            candidates = sorted(JSON_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            for c in candidates:
                json_name_part = c.stem.split("_")[0]  # 예: "홍성달"
                if json_name_part and json_name_part in stem:
                    data = _load_json(c)
                    if data:
                        try:
                            print(f"  기존 JSON 로드: {c.name}")
                        except UnicodeEncodeError:
                            pass
                        break
            if data is None:
                # 역방향: PDF stem의 일부가 JSON stem에 포함되는지도 확인
                for c in candidates:
                    if c.stem in stem or stem in c.stem:
                        data = _load_json(c)
                        if data:
                            break
        else:
            data = _parse_pdf(pdf_path)
            if data:
                # 결과를 json_output에 저장
                name = (data.get("인적사항") or {}).get("성명", stem)
                issue_date = str((data.get("서류출력일자") or "")).replace("-", "")
                out_name = f"{name}_{issue_date}.json" if issue_date else f"{name}.json"
                out_path = JSON_DIR / out_name
                JSON_DIR.mkdir(exist_ok=True)
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"  JSON 저장: {out_path.name}")

        if data is None:
            vr = ValidationResult(pdf_path.name)
            vr.fail("파싱 실패 또는 JSON 없음")
            results.append(vr)
            continue

        vr = validate(data, pdf_path.name)
        results.append(vr)

    return results


def print_report(results: List[ValidationResult]) -> str:
    lines = []
    lines.append("\n" + "=" * 60)
    lines.append("  파싱 검증 리포트")
    lines.append("=" * 60)

    pass_n = sum(1 for r in results if r.level == "PASS")
    warn_n = sum(1 for r in results if r.level == "WARN")
    fail_n = sum(1 for r in results if r.level == "FAIL")

    for vr in results:
        lines.append(vr.summary_line())
        for issue in vr.issues:
            lines.append(issue)

    lines.append("-" * 60)
    lines.append(
        f"총 {len(results)}건  PASS: {pass_n}  WARN: {warn_n}  FAIL: {fail_n}"
    )

    report = "\n".join(lines)
    # FIX: Windows cp949 터미널에서 한글/특수문자 인코딩 오류 방지
    try:
        print(report)
    except UnicodeEncodeError:
        print(report.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(sys.stdout.encoding or "utf-8", errors="replace"))
    return report


def save_report(report: str) -> None:
    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = ROOT / f"validation_report_{today}.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    try:
        print(f"\n리포트 저장: {report_path.name}")
    except UnicodeEncodeError:
        pass


def main():
    parser = argparse.ArgumentParser(description="PDF 파서 자동 검증")
    parser.add_argument(
        "--no-reparse",
        action="store_true",
        help="PDF를 다시 파싱하지 않고 기존 json_output/ JSON만 검증",
    )
    parser.add_argument(
        "--pdf",
        metavar="FILENAME",
        help="특정 PDF 파일 이름만 처리 (예: 홍성달.pdf)",
    )
    parser.add_argument(
        "--dir",
        metavar="DIR",
        help="특정 디렉터리 하위의 PDF만 처리 (originalPDF 기준 상대경로 또는 절대경로)",
    )
    args = parser.parse_args()

    if args.pdf and args.dir:
        print("[ERROR] --pdf 와 --dir 은 동시에 사용할 수 없습니다.")
        sys.exit(1)

    if args.dir:
        # // FIX: 회귀 검증을 위해 특정 폴더만 선택 실행 지원 (예: originalPDF/test2)
        d = Path(args.dir)
        if not d.is_absolute():
            d = (PDF_DIR / d).resolve()
        if not d.exists() or not d.is_dir():
            print(f"[ERROR] 디렉터리를 찾을 수 없습니다: {args.dir}")
            sys.exit(1)
        pdf_files = sorted(d.rglob("*.pdf"))
        if not pdf_files:
            print(f"[ERROR] 디렉터리 내 PDF가 없습니다: {d}")
            sys.exit(1)

    elif args.pdf:
        pdf_files = [PDF_DIR / args.pdf]
        if not pdf_files[0].exists():
            # // FIX: originalPDF 하위 폴더(예: originalPDF/전주국토/)에 PDF가 있는 케이스 지원
            # 부분 이름 매칭 (재귀)
            pdf_files = [p for p in PDF_DIR.rglob("*.pdf") if args.pdf in p.name]
        if not pdf_files:
            print(f"[ERROR] PDF 파일을 찾을 수 없습니다: {args.pdf}")
            sys.exit(1)
    else:
        # // FIX: originalPDF 하위 폴더까지 재귀적으로 수집
        pdf_files = sorted(PDF_DIR.rglob("*.pdf"))
        if not pdf_files:
            # json_output의 JSON만 있는 경우
            pdf_files = [
                Path(p.stem + ".pdf") for p in sorted(JSON_DIR.glob("*.json"))
            ]
            args.no_reparse = True

    if not pdf_files:
        print("[ERROR] 처리할 파일이 없습니다.")
        sys.exit(1)

    print(f"대상 파일 {len(pdf_files)}건 {'(재파싱 없음)' if args.no_reparse else '(파싱 포함)'}")

    results = run(pdf_files, no_reparse=args.no_reparse)
    report = print_report(results)
    save_report(report)

    # FAIL이 1건이라도 있으면 exit code 1
    sys.exit(1 if any(r.level == "FAIL" for r in results) else 0)


if __name__ == "__main__":
    main()
