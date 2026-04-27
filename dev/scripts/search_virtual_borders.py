#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
가상 좌/우 explicit_vertical_lines x 좌표를 스윕하며,
원시 표에서 기술경력/건설사업관리및감리경력 헤더에
 - '참여기간'
 - '비고'
가 동시에 살아나는지(컬럼 생성 신호) 점검한다.

원칙:
 - 하드코딩된 페이지 번호 대신, 페이지 텍스트에서 섹션 앵커를 찾아 해당 페이지를 평가한다.
 - pdfplumber lines 전략 + explicit_vertical_lines만 대상으로 빠르게 평가한다.

사용:
  python scripts/search_virtual_borders.py "originalPDF/황규철 경력증명서(2025.07.24).pdf"
  python scripts/search_virtual_borders.py "..." --left 24 40 --right 555 585 --step 0.5
"""

from __future__ import annotations

import argparse
import itertools
import re
import sys
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from parsers.table_settings import LINE_TABLE_SETTINGS, safe_extract_tables  # noqa: E402


def _find_section_pages(pdf: pdfplumber.PDF) -> tuple[int | None, int | None]:
    tech = None
    cm = None
    for i, p in enumerate(pdf.pages):
        try:
            t = (p.extract_text() or "").replace(" ", "")
        except Exception:
            t = ""
        if tech is None and ("1.기술경력" in t or "1.기술 경력" in t):
            tech = i
        if cm is None and ("2.건설사업관리및감리경력" in t or "2.건설사업관리" in t and "감리경력" in t):
            cm = i
        if tech is not None and cm is not None:
            break
    return tech, cm


def _table_has_tokens(table: list[list[object]], tokens: list[str]) -> bool:
    if not table:
        return False
    joined = " ".join(str(c) for r in table for c in (r or []) if c)
    return all(tok in joined for tok in tokens)


def _score_tables(tables: list[list[list[object]]]) -> tuple[int, int, int]:
    """
    Return (score, best_cols, best_rows)
    score:
      +400 if table contains both 참여기간 and 비고
      +cols*10 + non_empty_rows
    """
    best = (-10_000, 0, 0)
    for t in tables or []:
        if not t:
            continue
        cols = max((len(r) for r in t if r), default=0)
        non_empty = sum(1 for r in t if r and any(str(c or "").strip() for c in r))
        base = cols * 10 + non_empty
        if _table_has_tokens(t, ["참여기간", "비고"]):
            base += 400
        if base > best[0]:
            best = (base, cols, non_empty)
    return best


def _extract_tables_for_page(page, left: float, right: float) -> list[list[list[object]]]:
    settings = dict(LINE_TABLE_SETTINGS)
    settings["explicit_vertical_lines"] = [float(left), float(right)]
    return safe_extract_tables(page, settings)


def main() -> int:
    ap = argparse.ArgumentParser(description="가상 좌/우 x 좌표 스윕(참여기간/비고 컬럼 생성 확인)")
    ap.add_argument("pdf", type=Path)
    ap.add_argument("--left", nargs=2, type=float, default=[24.0, 40.0], metavar=("MIN", "MAX"))
    ap.add_argument("--right", nargs=2, type=float, default=[555.0, 585.0], metavar=("MIN", "MAX"))
    ap.add_argument("--step", type=float, default=0.5)
    ap.add_argument("--topk", type=int, default=10)
    args = ap.parse_args()

    if not args.pdf.is_file():
        print(f"[ERROR] 파일 없음: {args.pdf}", file=sys.stderr)
        return 2

    with pdfplumber.open(str(args.pdf)) as pdf:
        tech_i, cm_i = _find_section_pages(pdf)
        print("pdf:", args.pdf)
        print("tech_page_idx:", tech_i, "cm_page_idx:", cm_i)
        if tech_i is None or cm_i is None:
            print("[ERROR] 섹션 페이지를 찾지 못했습니다(텍스트 추출 실패/양식 차이).", file=sys.stderr)
            return 2

        left_min, left_max = args.left
        right_min, right_max = args.right
        lefts = [round(x, 2) for x in frange(left_min, left_max, args.step)]
        rights = [round(x, 2) for x in frange(right_min, right_max, args.step)]

        results: list[tuple[int, float, float, tuple[int, int, int], tuple[int, int, int]]] = []
        p_tech = pdf.pages[tech_i]
        p_cm = pdf.pages[cm_i]
        for l, r in itertools.product(lefts, rights):
            if r <= l + 100:
                continue
            ttech = _extract_tables_for_page(p_tech, l, r)
            tcm = _extract_tables_for_page(p_cm, l, r)
            stech = _score_tables(ttech)
            scm = _score_tables(tcm)
            # 두 섹션 모두 참여기간/비고가 잡히는 쪽을 최우선
            both_ok = (stech[0] >= 400) and (scm[0] >= 400)
            score = (10_000 if both_ok else 0) + stech[0] + scm[0]
            results.append((score, l, r, stech, scm))

        results.sort(key=lambda x: x[0], reverse=True)
        print("\nTOP results:")
        for score, l, r, stech, scm in results[: max(1, args.topk)]:
            tech_ok = "OK" if stech[0] >= 400 else "NO"
            cm_ok = "OK" if scm[0] >= 400 else "NO"
            print(
                f"score={score}  left={l} right={r}  "
                f"tech({tech_ok} cols={stech[1]} rows={stech[2]} base={stech[0]})  "
                f"cm({cm_ok} cols={scm[1]} rows={scm[2]} base={scm[0]})"
            )

        best = results[0]
        print("\nBEST:")
        print("left=", best[1], "right=", best[2])
        return 0


def frange(a: float, b: float, step: float):
    x = a
    # include b
    while x <= b + 1e-9:
        yield x
        x += step


if __name__ == "__main__":
    raise SystemExit(main())

