#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
폴더 내 모든 PDF에 대해 table-only 후처리 ON/OFF 파싱 결과를 비교한다.

비교 포인트
- 최상위 결과 dict 전체 equality (완전 동일 여부)
- 주요 섹션별(기술경력/건설사업관리및감리경력/근무처/국가기술자격/교육훈련/상훈) 동일 여부
- 기술경력/CM: 인덱스 정렬 기준으로 필드별 차이 카운트 + 예시 일부
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

# 프로젝트 루트를 sys.path에 추가
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from main import parse_full_document  # noqa: E402


def _norm(s: Any) -> str:
    return " ".join(str(s or "").split())


def _stable_row_key(r: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        _norm(r.get("사업명")),
        _norm(r.get("참여기간_시작일")),
        _norm(r.get("참여기간_종료일")),
    )


@dataclass
class CompareResult:
    pdf: str
    ok: bool
    equal_full: bool
    section_equal: Dict[str, bool]
    tech_diff: Dict[str, Any]
    cm_diff: Dict[str, Any]


def _diff_rows_by_index(rows_on: list, rows_off: list, *, label: str) -> Dict[str, Any]:
    fields = [
        "사업명",
        "발주자",
        "공사종류",
        "참여기간_시작일",
        "참여기간_종료일",
        "인정일수",
        "참여일수",
        "직무분야",
        "전문분야",
        "담당업무",
        "책임정도",
        "직위",
        "공사(용역)금액(백만원)",
        "공사(용역)개요",
        "적용 공법",
        "적용 융복합건설기술",
        "적용 신기술 등",
        "시설물 종류",
        "비고",
    ]
    A = rows_on or []
    B = rows_off or []
    out: Dict[str, Any] = {
        "label": label,
        "n_on": len(A),
        "n_off": len(B),
        "n_row_key_diff": 0,
        "field_diffs": {},
        "examples": [],
    }
    n = max(len(A), len(B))
    for i in range(n):
        ra = A[i] if i < len(A) else None
        rb = B[i] if i < len(B) else None
        if not isinstance(ra, dict) or not isinstance(rb, dict):
            if ra != rb:
                out["n_row_key_diff"] += 1
            continue
        if _stable_row_key(ra) != _stable_row_key(rb):
            out["n_row_key_diff"] += 1
        for f in fields:
            va = _norm(ra.get(f))
            vb = _norm(rb.get(f))
            if va != vb:
                out["field_diffs"][f] = out["field_diffs"].get(f, 0) + 1
                if len(out["examples"]) < 5:
                    out["examples"].append(
                        {"idx": i, "field": f, "on": va[:160], "off": vb[:160]}
                    )
    return out


def _parse_with_postprocess(pdf_path: Path, enabled: bool) -> Dict[str, Any]:
    os.environ["PDFPARSER_TABLE_ONLY_POSTPROCESS"] = "1" if enabled else "0"
    return parse_full_document(str(pdf_path))


def compare_one(pdf_path: Path) -> CompareResult:
    on = _parse_with_postprocess(pdf_path, True)
    off = _parse_with_postprocess(pdf_path, False)

    section_keys = [
        "인적사항",
        "등급",
        "국가기술자격",
        "학력",
        "교육훈련",
        "상훈",
        "근무처",
        "기술경력",
        "건설사업관리및감리경력",
        "업무수행기간",
        "용역완성비율",
        "공사종류별인정일수",
        "직무전문분야별인정일수",
        "_검증",
        "_파싱오류",
    ]
    sec_equal: Dict[str, bool] = {}
    for k in section_keys:
        sec_equal[k] = (on.get(k) == off.get(k))

    tech_diff = _diff_rows_by_index(on.get("기술경력") or [], off.get("기술경력") or [], label="기술경력")
    cm_diff = _diff_rows_by_index(
        on.get("건설사업관리및감리경력") or [],
        off.get("건설사업관리및감리경력") or [],
        label="건설사업관리및감리경력",
    )
    equal_full = (on == off)
    ok = equal_full
    return CompareResult(
        pdf=str(pdf_path),
        ok=ok,
        equal_full=equal_full,
        section_equal=sec_equal,
        tech_diff=tech_diff,
        cm_diff=cm_diff,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="table-only 후처리 ON/OFF 배치 비교")
    ap.add_argument("folder", help="PDF 폴더 경로")
    ap.add_argument("--out", default=str(BASE_DIR / "docs" / "TABLE_ONLY_POSTPROCESS_BATCH_REPORT.json"))
    args = ap.parse_args()

    folder = Path(args.folder)
    if not folder.exists():
        print(f"[ERROR] 폴더가 없습니다: {folder}")
        return 2

    pdfs = sorted([p for p in folder.glob("*.pdf") if p.is_file()], key=lambda p: p.name)
    if not pdfs:
        print(f"[ERROR] PDF가 없습니다: {folder}")
        return 3

    results: List[dict] = []
    n_equal = 0
    for i, p in enumerate(pdfs, 1):
        print(f"\n[{i}/{len(pdfs)}] compare: {p.name}")
        r = compare_one(p)
        if r.equal_full:
            n_equal += 1
        results.append(
            {
                "pdf": r.pdf,
                "equal_full": r.equal_full,
                "section_equal": r.section_equal,
                "tech_diff": r.tech_diff,
                "cm_diff": r.cm_diff,
            }
        )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_payload = {
        "folder": str(folder),
        "n_pdfs": len(pdfs),
        "n_equal_full": n_equal,
        "n_diff_full": len(pdfs) - n_equal,
        "results": results,
    }
    out_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[OK] report saved: {out_path}")
    print(f"[INFO] equal_full: {n_equal}/{len(pdfs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

