#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
디버그 전용: parsers.core.section_parsers.parse_award_info()의 내부 흐름을
코드 변경 없이(동일 로직 호출) 추적 출력한다.

목표:
- 어떤 테이블이 상훈 후보로 인식되는지
- 헤더 인덱스/열 인덱스가 무엇인지
- 각 row가 어떻게 (수여일/수여기관/종류및근거)로 매핑/보강되는지
- 텍스트 블록 파싱/병합이 실제로 어떤 영향을 주는지
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Any

import pdfplumber

# repo root를 sys.path에 추가
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from parsers.table_settings import extract_tables_merged, table_set_has_header_signals  # noqa: E402
import parsers.core.section_parsers as sp  # noqa: E402


def _flat_table_text(table: list[list[Any]]) -> str:
    return " ".join(str(c) for r in (table or []) for c in (r or []) if c)


def _safe_cell(row: list[Any], idx: int) -> str:
    if idx < 0 or idx >= len(row):
        return ""
    return str(row[idx] or "").strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf_path")
    ap.add_argument("--page", type=int, default=1, help="1-based page number to inspect")
    ap.add_argument("--print-tables", action="store_true", help="print full candidate tables (can be large)")
    args = ap.parse_args()

    pdf_path = args.pdf_path
    if not os.path.exists(pdf_path):
        raise SystemExit(f"PDF not found: {pdf_path!r}")

    page_idx = max(0, int(args.page) - 1)

    with pdfplumber.open(pdf_path) as pdf:
        if page_idx >= len(pdf.pages):
            raise SystemExit(f"Page out of range: {args.page} (total {len(pdf.pages)})")
        page = pdf.pages[page_idx]

        print("=" * 110)
        print(f"PDF={pdf_path!r}")
        print(f"PAGE={args.page}/{len(pdf.pages)}")

        page_text = page.extract_text() or ""
        print(f"PAGE_TEXT_LEN={len(page_text)}")

        award_block = sp.extract_award_section_text(page_text)
        print(f"AWARD_BLOCK_LEN={len(award_block)}")
        if award_block.strip():
            lines = [ln for ln in award_block.splitlines()]
            print("[AWARD_BLOCK_HEAD]")
            for ln in lines[:25]:
                print(ln)
            if len(lines) > 40:
                print("... (snip) ...")
            print("[AWARD_BLOCK_TAIL]")
            for ln in lines[-12:]:
                print(ln)
        else:
            print("[AWARD_BLOCK] <empty>")

        # --- 테이블 후보 수집(원 로직 동일 게이트) ---
        tables = extract_tables_merged(page) or []
        print(f"TABLES_TOTAL={len(tables)}")
        has_signals = bool(tables) and table_set_has_header_signals(tables, ["수여일", "수여기관", "상훈"])
        print(f"HAS_HEADER_SIGNALS={has_signals}")

        # cand: "수여일" 포함 테이블만(원 로직과 동일한 1차 스킵 조건과 유사하게)
        cand = []
        for ti, t in enumerate(tables):
            flat = _flat_table_text(t)
            if ("수여일" in flat) or ("상훈" in flat):
                cand.append((ti, t, flat))
        print(f"CAND_TABLES={len(cand)} (contains '수여일' or '상훈')")

        # --- parse_award_info 테이블 분기(내부를 따라가며 출력) ---
        awards: list[dict[str, Any]] = []
        if has_signals:
            print("[TABLE_PARSE] begin")
            for ti, table, flat in cand:
                table_flat = _flat_table_text(table)
                if "수여일" not in table_flat:
                    print(f"  - table#{ti}: SKIP (no '수여일' in table_flat)")
                    continue

                header_idx = sp._find_award_table_header_idx(table)
                print(f"  - table#{ti}: header_idx={header_idx} rows={len(table)} cols_max={max((len(r or []) for r in table), default=0)}")
                if header_idx < 0:
                    continue

                header_row = table[header_idx]
                date_col = sp.find_column_index(header_row, "수여일")
                inst_col = sp.find_column_index(header_row, "수여기관")
                type_col = sp.find_column_index(header_row, "종류")
                if type_col < 0:
                    type_col = sp.find_column_index(header_row, "근거")
                if date_col < 0:
                    date_col = 0
                if inst_col < 0:
                    inst_col = 1
                if type_col < 0:
                    type_col = 2

                print(f"    header_row={header_row}")
                print(f"    cols(date,inst,type)={date_col},{inst_col},{type_col}")

                next_section_idx = sp.find_next_section_header(
                    table,
                    header_idx,
                    ["근무처", "벌점", "교육훈련", "국가기술자격", "학력"],
                )
                print(f"    next_section_idx={next_section_idx}")

                # row 처리 로직을 그대로 따라가며, "어떤 값이 어떤 이유로 들어갔는지" 출력
                for ri in range(header_idx + 1, next_section_idx):
                    row = table[ri]
                    if not row:
                        continue
                    row_text = " ".join([str(cell) for cell in row if cell])
                    if sp._is_award_table_boundary_row(row_text):
                        print(f"    row#{ri}: BOUNDARY break ({row_text!r})")
                        break

                    award_date_raw = _safe_cell(row, date_col)
                    row_has_award_date = bool(
                        award_date_raw and re.match(r"\d{4}\.\d{2}\.\d{2}", award_date_raw)
                    )

                    # continuation 병합(원 로직)
                    if (not row_has_award_date) and awards:
                        inst_cand = _safe_cell(row, inst_col)
                        type_cand = _safe_cell(row, type_col)
                        extras = []
                        for ci, cell in enumerate(row):
                            if ci in [date_col]:
                                continue
                            v = str(cell or "").replace("\n", " ").strip()
                            if v:
                                extras.append(v)
                        if (not type_cand) and extras:
                            type_cand = " ".join(extras).strip()
                        if inst_cand or type_cand:
                            prev = awards[-1]
                            if re.match(r"^\d{4}-\d{2}-\d{2}$", str(prev.get("수여일") or "").strip()):
                                print(
                                    f"    row#{ri}: CONTINUATION merge into prev (inst_cand={inst_cand!r}, type_cand={type_cand!r})"
                                )
                                if inst_cand and (inst_cand not in str(prev.get("수여기관") or "")):
                                    prev["수여기관"] = (str(prev.get("수여기관") or "").strip() + " " + inst_cand).strip()
                                if type_cand and (type_cand not in str(prev.get("종류및근거") or "")):
                                    prev["종류및근거"] = (str(prev.get("종류및근거") or "").strip() + " " + type_cand).strip()
                                continue

                    if ("해당없음" in row_text) and (not row_has_award_date):
                        print(f"    row#{ri}: NOT_APPLICABLE append")
                        awards.append(sp._award_not_applicable_row())
                        continue

                    inst = _safe_cell(row, inst_col)
                    typ = _safe_cell(row, type_col)

                    extras = []
                    for ci, cell in enumerate(row):
                        if ci in [date_col, inst_col]:
                            continue
                        v = str(cell or "").replace("\n", " ").strip()
                        if v:
                            extras.append(v)
                    used_extras = False
                    if not typ and extras:
                        typ = " ".join(extras).strip()
                        used_extras = True

                    triples = sp._split_merged_award_triples(award_date_raw, inst, typ)
                    if not triples:
                        continue
                    for d_raw, inst_p, typ_p in triples:
                        typ_fill = typ_p
                        if not typ_fill:
                            typ_fill = "<EMPTY_AFTER_TABLE_AND_EXTRAS>"
                        ok_date = bool(d_raw and re.match(r"\d{4}\.\d{2}\.\d{2}", d_raw))
                        print(
                            f"    row#{ri}: date_raw={d_raw!r} inst_cell={inst!r} type_cell={typ!r} used_extras={used_extras} extras={extras!r} -> inst_p={inst_p!r} typ_p={typ_p!r}"
                        )
                        if ok_date:
                            awards.append(
                                {
                                    "수여일": sp.convert_date_format(d_raw),
                                    "수여기관": str(inst_p).replace("\n", " ").strip(),
                                    "종류및근거": str(typ_fill).replace("\n", " ").strip(),
                                }
                            )
            print(f"[TABLE_PARSE] end -> awards_count={len(awards)}")
        else:
            print("[TABLE_PARSE] skipped (no header signals)")

        # --- 텍스트 파싱(원 로직 호출) ---
        text_awards = sp._parse_awards_from_text_block(page_text)
        print(f"TEXT_AWARDS_COUNT={len(text_awards)}")
        for i, a in enumerate(text_awards[:10]):
            print(f"  text_award#{i}: {a}")

        merged = sp._merge_award_lists(awards, text_awards)
        print(f"MERGED_COUNT={len(merged)}")
        for i, a in enumerate(merged):
            print(f"  merged#{i}: {a}")

        # --- 최종 parse_award_info 결과와 비교(원 함수 호출) ---
        final = sp.parse_award_info(page)
        print(f"FINAL(parse_award_info)_COUNT={len(final)}")
        for i, a in enumerate(final):
            print(f"  final#{i}: {a}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

