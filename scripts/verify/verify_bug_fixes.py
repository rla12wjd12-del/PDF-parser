#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
에러1·에러2·에러3 수정 검증 스크립트

사용 예:
  python verify_bug_fixes.py "originalPDF/김경태 경력증명서(2025.09.11).pdf"
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


# ──────────────────────────────────────────────────────────────────────────────
# 공통 유틸
# ──────────────────────────────────────────────────────────────────────────────

def _yn(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


# ──────────────────────────────────────────────────────────────────────────────
# 검증 1: 에러1 — 경력 밀림 (phantom row) 탐지
# ──────────────────────────────────────────────────────────────────────────────

def check_error1_career_sliding(careers: list[dict]) -> tuple[bool, list[str]]:
    """
    에러1 탐지: 동일 날짜 블록에서 사업명만 달라 'phantom row'가 생성된 경우 탐지.

    검사 기준:
    - 연속된 두 경력 행이 (참여기간_시작일, 참여기간_종료일, 인정일수, 참여일수) 가
      모두 동일한데 사업명이 서로 다른 경우 → 에러1 의심 레코드
    - 단, 실제로 같은 기간에 두 프로젝트를 동시에 수행하는 합법적 케이스가 존재할 수 있어
      "의심(WARN)" 수준으로 보고한다.
    """
    issues: list[str] = []
    for i in range(len(careers) - 1):
        a = careers[i]
        b = careers[i + 1]
        a_key = (
            str(a.get("참여기간_시작일") or ""),
            str(a.get("참여기간_종료일") or ""),
            str(a.get("인정일수") or ""),
            str(a.get("참여일수") or ""),
        )
        b_key = (
            str(b.get("참여기간_시작일") or ""),
            str(b.get("참여기간_종료일") or ""),
            str(b.get("인정일수") or ""),
            str(b.get("참여일수") or ""),
        )
        if "" in (a_key[0], b_key[0]):
            continue
        if a_key != b_key:
            continue
        nm_a = str(a.get("사업명") or "").strip()
        nm_b = str(b.get("사업명") or "").strip()
        if nm_a == nm_b:
            continue
        # 발주자가 서로 달라도 같은 기간 → 합법적 케이스 가능성 체크
        iss_a = str(a.get("발주자") or "").strip()
        iss_b = str(b.get("발주자") or "").strip()
        if iss_a and iss_b and iss_a != iss_b:
            # 발주자가 다르면 진짜 동시 참여 가능성이 높음 → 경고 수준
            issues.append(
                f"  [WARN] 인덱스 {i}↔{i+1}: 동일기간({a_key[0]}~{a_key[1]}) 다른 사업명 "
                f"(발주자 상이 → 동시 참여 가능성)"
                f"\n      사업명A: {nm_a[:50]}"
                f"\n      사업명B: {nm_b[:50]}"
            )
        else:
            # 발주자까지 같은데 사업명이 다르면 phantom row 의심
            issues.append(
                f"  [ERROR] 인덱스 {i}↔{i+1}: 동일기간·동일발주자에 다른 사업명 → phantom row 의심"
                f"\n      기간: {a_key[0]} ~ {a_key[1]}, 인정={a_key[2]}, 참여={a_key[3]}"
                f"\n      사업명A: {nm_a[:60]}"
                f"\n      사업명B: {nm_b[:60]}"
            )
    errors_only = [x for x in issues if "[ERROR]" in x]
    return (len(errors_only) == 0), issues


# ──────────────────────────────────────────────────────────────────────────────
# 검증 2: 에러2 — ┖→ 연장 텍스트가 사업명에 포함된 케이스 탐지
# ──────────────────────────────────────────────────────────────────────────────

def check_error2_continuation_in_name(careers: list[dict]) -> tuple[bool, list[str]]:
    """
    에러2 탐지: 이전 경력의 공사(용역)개요 내용이 다음 경력의 사업명에 포함된 경우 탐지.

    검사 기준:
    - 개요 필드의 마지막 30자 이상이 다음 행의 사업명 앞에 등장하는 경우
    """
    issues: list[str] = []
    for i in range(len(careers) - 1):
        prev_ov = re.sub(r"\s+", "", str(careers[i].get("공사(용역)개요") or "")).strip()
        next_nm = re.sub(r"\s+", "", str(careers[i + 1].get("사업명") or "")).strip()
        if len(prev_ov) < 30 or not next_nm:
            continue
        # 이전 개요의 마지막 30자가 다음 사업명의 앞부분에 있으면 ┖→ 오파싱 의심
        tail = prev_ov[-30:]
        if next_nm.startswith(tail):
            issues.append(
                f"  [ERROR] 인덱스 {i}→{i+1}: 이전 개요 꼬리가 다음 사업명 앞에 포함 → ┖→ 연장 미처리"
                f"\n      이전 개요 끝: ...{prev_ov[-50:]}"
                f"\n      다음 사업명 앞: {next_nm[:60]}..."
            )
    return (len(issues) == 0), issues


# ──────────────────────────────────────────────────────────────────────────────
# 검증 3: 에러3 — 사업명에 불필요한 띄어쓰기 패턴 탐지
# ──────────────────────────────────────────────────────────────────────────────

def check_error3_project_name_spaces(careers: list[dict]) -> tuple[bool, list[str]]:
    """
    에러3 탐지: 한국어 사업명 내에 단어 중간에 공백이 삽입된 패턴 탐지.

    검사 기준:
    - 숫자나 한글 뒤에 공백, 그 뒤에 한글/숫자가 오는 패턴 중
      공백을 제거하면 합리적인 단어가 되는 경우 → 두 줄 이어붙이기 오류 의심.
    - 특히 "~" 없는 일반 한글 단어 중간의 공백을 검사.
    """
    # 기술 용어(발주자명, 공법 등)는 의도적 공백이 많아 너무 광범위하게 탐지하면
    # 오탐이 많다. 실용적으로: 사업명 내 "한글 공백 한글" 패턴에서
    # 공백을 제거한 결과가 알려진 사업명 패턴과 맞는지는 어렵다.
    # 대신, "숫자/공백/한글" 또는 "한글/공백/숫자" 중간 공백만 체크한다.
    # 예: "제15 공구" → "제15공구", "가물막이 검토" → 탐지 불가(두 단어 모두 한글)
    issues: list[str] = []
    # 두 줄로 나뉜 뒤 공백으로 이어붙인 특징적 패턴:
    # 단어 끝에 공백 + 연속 문자(알파벳 소문자/대문자 혼합 없는 순한글)이 오는 케이스
    # → 현실적으로 탐지 범위를 좁힘: 개행 이어붙임 시 자주 나타나는 "한글끝 공백 한글시작" 중
    #   공백 앞/뒤가 각각 의미 있는 단어처럼 끊긴 경우를 보고.
    # 실제로는 JSON 결과에서 사업명에 공백이 "불필요하게" 있는 경우를 직접 검사하기
    # 어려우므로, 간단한 휴리스틱으로 "XX 로" "XX 의" 등의 조사 앞 공백을 탐지.
    # 보다 직접적인 방법: 기존 JSON과 비교. 여기서는 단순 패턴만.
    _SPLIT_HINT = re.compile(
        r"(?<=[가-힣\d])\s(?=[가-힣])"  # 한글/숫자 뒤 공백 뒤 한글
    )
    for i, r in enumerate(careers):
        nm = str(r.get("사업명") or "")
        matches = list(_SPLIT_HINT.finditer(nm))
        if not matches:
            continue
        # 모든 공백이 의심스러운 건 아님 — 4자 미만의 단편(단독 조사 등) 앞 공백만 보고
        for m in matches:
            after_space = nm[m.end():]
            word_after = re.split(r"[\s\(]", after_space)[0]
            if len(word_after) <= 2:  # 짧은 단편(조사, 구분자)이면 오류 의심
                issues.append(
                    f"  [WARN] 인덱스 {i}: 사업명에 단편 공백 의심"
                    f"\n      사업명: {nm[:80]}"
                )
                break
    return (len(issues) == 0), issues


# ──────────────────────────────────────────────────────────────────────────────
# JSON 기반 검증 (파싱 없이 결과 JSON 파일만으로도 실행 가능)
# ──────────────────────────────────────────────────────────────────────────────

def run_checks_on_json(json_path: Path) -> int:
    with open(json_path, encoding="utf-8") as f:
        result = json.load(f)

    careers = result.get("기술경력") or []
    cm_careers = result.get("건설사업관리및감리경력") or []

    all_pass = True
    for section_name, section_data in [("기술경력", careers), ("건설사업관리및감리경력", cm_careers)]:
        print(f"\n{'='*60}")
        print(f"[검증 섹션] {section_name} ({len(section_data)}건)")
        print(f"{'='*60}")

        ok1, issues1 = check_error1_career_sliding(section_data)
        print(f"\n[에러1] 경력 밀림(phantom row) 검사: {_yn(ok1)}")
        for msg in issues1:
            print(msg)

        ok2, issues2 = check_error2_continuation_in_name(section_data)
        print(f"\n[에러2] ┖→ 연장텍스트 사업명 혼입 검사: {_yn(ok2)}")
        for msg in issues2:
            print(msg)

        ok3, issues3 = check_error3_project_name_spaces(section_data)
        print(f"\n[에러3] 사업명 불필요 공백 검사: {_yn(ok3)}")
        for msg in issues3:
            print(msg)

        if not (ok1 and ok2 and ok3):
            all_pass = False

    # 기존 _검증 결과도 출력
    v = result.get("_검증") or {}
    if v:
        print(f"\n{'='*60}")
        print("[내부 _검증 결과 (파서 자체 카운팅)]")
        print(f"{'='*60}")
        for sec, info in v.items():
            match = info.get("일치", False)
            print(
                f"  {sec}: PDF원본 {info.get('PDF원본')}건 vs 파싱 {info.get('파싱결과')}건 "
                f"→ {_yn(match)}"
            )
            if not match:
                all_pass = False

    print(f"\n{'='*60}")
    print(f"[최종 결과] {'[전체 PASS]' if all_pass else '[일부 FAIL 존재]'}")
    print(f"{'='*60}\n")
    return 0 if all_pass else 1


# ──────────────────────────────────────────────────────────────────────────────
# PDF 파싱 후 검증
# ──────────────────────────────────────────────────────────────────────────────

def run_checks_on_pdf(pdf_path: Path) -> int:
    from main import parse_full_document

    print(f"\n[INFO] PDF 파싱 중: {pdf_path}")
    result = parse_full_document(str(pdf_path))

    # 임시 JSON 저장
    tmp_json = ROOT / "json_output" / f"_verify_{pdf_path.stem}.json"
    tmp_json.parent.mkdir(exist_ok=True)
    with open(tmp_json, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"[INFO] 검증용 JSON 저장: {tmp_json}")

    return run_checks_on_json(tmp_json)


# ──────────────────────────────────────────────────────────────────────────────
# 진입점
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="에러1·에러2·에러3 수정 결과 검증"
    )
    ap.add_argument(
        "path",
        nargs="?",
        default=None,
        help="검증할 PDF 또는 JSON 경로 (생략 시 파일 선택 창)",
    )
    ap.add_argument("--json", action="store_true", help="입력이 JSON 파일임을 명시")
    args = ap.parse_args()

    path_str = args.path
    if not path_str:
        try:
            import tkinter as tk
            from tkinter import filedialog
            root_tk = tk.Tk()
            root_tk.withdraw()
            root_tk.attributes("-topmost", True)
            root_tk.update()
            path_str = filedialog.askopenfilename(
                title="PDF 또는 JSON 선택",
                filetypes=[("PDF/JSON", "*.pdf *.json"), ("All", "*.*")],
            )
            root_tk.destroy()
        except Exception:
            pass

    if not path_str:
        print("[ERROR] 파일 경로를 지정하거나 파일 선택 창에서 선택하세요.")
        return 2

    p = Path(path_str)
    if not p.exists():
        print(f"[ERROR] 파일 없음: {p}")
        return 2

    if p.suffix.lower() == ".json" or args.json:
        return run_checks_on_json(p)
    else:
        return run_checks_on_pdf(p)


if __name__ == "__main__":
    sys.exit(main())
