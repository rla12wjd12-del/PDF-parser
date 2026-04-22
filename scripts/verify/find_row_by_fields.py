#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
parse_page_2/3 결과에서 특정 필드 조건에 맞는 행을 찾아 출력한다.

예:
  python scripts/verify/find_row_by_fields.py <pdf_path> --mode tech --start 120 --end 220 --start-date 2009-04-10 --issuer 김포도시공사
"""

from __future__ import annotations

import os
import sys
import argparse
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from parsers.document_context import DocumentContext
from parsers.page_2_parser import parse_page_2
from parsers.page_3_parser import parse_page_3


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf_path")
    ap.add_argument("--mode", choices=["tech", "cm"], required=True)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--end", type=int, default=200)
    ap.add_argument("--start-date", default="")
    ap.add_argument("--end-date", default="")
    ap.add_argument("--issuer", default="")
    ap.add_argument("--limit", type=int, default=5)
    args = ap.parse_args()

    found = []
    with DocumentContext.open(args.pdf_path) as ctx:
        end = min(args.end, ctx.total_pages)
        for p in range(max(0, args.start), max(0, end)):
            rows = []
            try:
                rows = parse_page_2(ctx, p) if args.mode == "tech" else parse_page_3(ctx, p)
            except Exception:
                continue
            for r in rows or []:
                if args.start_date and str(r.get("참여기간_시작일") or "").strip() != args.start_date:
                    continue
                if args.end_date and str(r.get("참여기간_종료일") or "").strip() != args.end_date:
                    continue
                if args.issuer and args.issuer not in str(r.get("발주자") or ""):
                    continue
                found.append((p, r))
                if len(found) >= args.limit:
                    break
            if len(found) >= args.limit:
                break

    print(f"found={len(found)} mode={args.mode}")
    for p, r in found:
        print(f"\n[page_idx={p}]\n" + json.dumps(r, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

