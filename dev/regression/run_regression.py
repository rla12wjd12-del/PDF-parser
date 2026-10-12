#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
파서 회귀 테스트 도구.

1) run     : PDF 폴더의 모든 PDF를 지정한 파서 코드(--root)로 파싱해 결과 JSON을 저장한다.
2) compare : 기준(baseline) 결과와 후보(candidate) 결과를 비교해 바뀐 필드를 모두 나열한다.

PDF와 결과 JSON에는 개인정보가 들어 있으므로 저장소 밖 폴더를 사용한다.
파일은 정렬 순서대로 01, 02, ... 번호로 부르며, 번호와 파일명 대응은 out-dir/index.json에만 남는다.

예)
  python dev/regression/run_regression.py run --root . --pdf-dir <PDF폴더> --out-dir <결과폴더> --jobs 8
  python dev/regression/run_regression.py compare --baseline <기준폴더> --candidate <후보폴더> --report <보고서.txt>
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

CAREER_SECTIONS = ("기술경력", "건설사업관리및감리경력")
IGNORE_KEYS = {"_elapsed", "_pdf_pages_str"}


# ── run ──────────────────────────────────────────────────────────────────────

def _parse_one(args: tuple[str, str, str]) -> tuple[str, float, str | None]:
    root, pdf_path, out_path = args
    if root not in sys.path:
        sys.path.insert(0, root)
    os.chdir(root)
    import main  # noqa: WPS433 (작업 프로세스마다 지정 root의 main을 불러온다)

    t0 = time.perf_counter()
    error = None
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            result = main.parse_full_document(pdf_path)
    except Exception as exc:  # 예외도 결과로 기록해 비교한다
        result = {}
        error = f"{type(exc).__name__}: {exc}"
    elapsed = round(time.perf_counter() - t0, 3)
    result["_error"] = error
    result["_elapsed"] = elapsed
    Path(out_path).write_text(json.dumps(result, ensure_ascii=False, default=str), encoding="utf-8")
    return Path(out_path).stem, elapsed, error


def cmd_run(ns: argparse.Namespace) -> int:
    root = str(Path(ns.root).resolve())
    out_dir = Path(ns.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    pdfs = sorted(Path(ns.pdf_dir).glob("*.pdf"))
    index = {f"{i + 1:02d}": p.name for i, p in enumerate(pdfs)}
    (out_dir / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=1), encoding="utf-8")
    tasks = [(root, str(p), str(out_dir / f"{i + 1:02d}.json")) for i, p in enumerate(pdfs)]
    t0 = time.perf_counter()
    errors = 0
    total = 0.0
    with ProcessPoolExecutor(max_workers=max(1, ns.jobs)) as pool:
        for key, elapsed, error in pool.map(_parse_one, tasks):
            total += elapsed
            if error:
                errors += 1
            print(f"#{key} {elapsed:6.2f}s{'  ERROR ' + error if error else ''}", flush=True)
    wall = time.perf_counter() - t0
    summary = {"files": len(tasks), "errors": errors, "sum_elapsed": round(total, 1), "wall": round(wall, 1), "jobs": ns.jobs}
    (out_dir / "_run_summary.json").write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 1 if errors else 0


# ── compare ──────────────────────────────────────────────────────────────────

def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _walk_diff(a, b, path: str, out: list[tuple[str, object, object]]) -> None:
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k in IGNORE_KEYS:
                continue
            _walk_diff(a.get(k), b.get(k), f"{path}.{k}" if path else k, out)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append((f"{path}[건수]", len(a), len(b)))
        for i, (x, y) in enumerate(zip(a, b)):
            _walk_diff(x, y, f"{path}[{i}]", out)
    elif a != b:
        out.append((path, a, b))


def cmd_compare(ns: argparse.Namespace) -> int:
    base, cand = Path(ns.baseline), Path(ns.candidate)
    keys = sorted(p.stem for p in base.glob("[0-9][0-9].json"))
    lines: list[str] = []
    changed_files = 0
    total_changes = 0
    field_counter: dict[str, int] = {}
    time_b = time_c = 0.0
    problems = 0
    for key in keys:
        pb, pc = base / f"{key}.json", cand / f"{key}.json"
        if not pc.exists():
            lines.append(f"#{key} 후보 결과 없음")
            problems += 1
            continue
        a, b = _load(pb), _load(pc)
        time_b += a.get("_elapsed") or 0
        time_c += b.get("_elapsed") or 0
        if b.get("_error"):
            lines.append(f"#{key} 예외: {b['_error']}")
            problems += 1
        for sec in CAREER_SECTIONS:
            na, nb = len(a.get(sec) or []), len(b.get(sec) or [])
            if na != nb:
                lines.append(f"#{key} {sec} 건수 변경 {na} → {nb}")
                problems += 1
        diffs: list[tuple[str, object, object]] = []
        _walk_diff(a, b, "", diffs)
        if diffs:
            changed_files += 1
            total_changes += len(diffs)
            lines.append(f"#{key} 변경 {len(diffs)}건")
            for path, x, y in diffs[: ns.max_per_file]:
                field = path.split(".")[-1].split("[")[0]
                field_counter[field] = field_counter.get(field, 0) + 1
                lines.append(f"   {path}: {str(x)[:120]!r} → {str(y)[:120]!r}")
            for path, _x, _y in diffs[ns.max_per_file:]:
                field = path.split(".")[-1].split("[")[0]
                field_counter[field] = field_counter.get(field, 0) + 1
    head = [
        f"비교 파일 {len(keys)}개 / 변경된 파일 {changed_files}개 / 변경 항목 {total_changes}건 / 예외·건수 문제 {problems}건",
        f"처리 시간 합계: 기준 {time_b:.1f}s → 후보 {time_c:.1f}s",
        "필드별 변경 건수: " + ", ".join(f"{k} {v}" for k, v in sorted(field_counter.items(), key=lambda x: -x[1])),
        "",
    ]
    text = "\n".join(head + lines)
    if ns.report:
        Path(ns.report).write_text(text, encoding="utf-8")
    print("\n".join(head[:3]))
    return 1 if problems else 0


def main_cli() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--root", required=True, help="파서 코드 루트(main.py가 있는 폴더)")
    r.add_argument("--pdf-dir", required=True)
    r.add_argument("--out-dir", required=True)
    r.add_argument("--jobs", type=int, default=4, help="동시에 처리할 파일 수")
    c = sub.add_parser("compare")
    c.add_argument("--baseline", required=True)
    c.add_argument("--candidate", required=True)
    c.add_argument("--report")
    c.add_argument("--max-per-file", type=int, default=40)
    ns = ap.parse_args()
    return cmd_run(ns) if ns.cmd == "run" else cmd_compare(ns)


if __name__ == "__main__":
    sys.exit(main_cli())
