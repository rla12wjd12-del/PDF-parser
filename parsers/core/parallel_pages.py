#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[hardening stage7] 페이지 단위 병렬 처리.

파싱 시간의 대부분은 pdfplumber(pdfminer)가 페이지 내용을 순수 파이썬으로 해석하는 데 쓰인다.
페이지는 서로 독립적이므로, 문서를 시작할 때 작업 프로세스들이 페이지를 나눠 맡아 한 번에 처리한다.
  - 모든 페이지: 텍스트(get_text)
  - 모든 페이지(1쪽 제외): 기술경력 행(parse_page_2), CM 행(parse_page_3) — 섹션 범위는 텍스트를
    본 뒤에야 정해지므로 미리 둘 다 계산하고, 본 프로세스는 실제 범위에 해당하는 결과만 꺼내 쓴다.
  - 제1쪽 파싱(parse_page_1)도 별도 작업으로 동시에 실행한다.
파싱 로직은 바꾸지 않고 기존 함수를 각 프로세스에서 그대로 실행하며, 출력(print)과 ctx.errors 도
해당 결과를 실제로 쓸 때만 본 프로세스에 옮긴다(미리 계산했지만 쓰지 않은 결과의 로그·오류는 버린다).

안전장치: 병렬 처리 중 어떤 문제가 생겨도(작업 프로세스 비정상 종료, 시간 초과 등) 결과를 버리고
기존 순차 방식으로 처리한다. 결과가 달라지는 경우는 없고, 느려질 뿐이다.

환경변수
  - PDFPARSER_WORKERS: 작업 프로세스 수. 0 또는 1이면 병렬 처리를 하지 않는다(기존 순차 방식).
    지정하지 않으면 min(8, CPU-1).
  - PDFPARSER_PARALLEL_MIN_PAGES: 이 쪽수 미만 문서는 순차 처리(기본 20). 프로세스 준비 비용이 더 크기 때문.
  - PDFPARSER_PARALLEL_TIMEOUT: 병렬 처리 제한 시간(초, 기본 600). 넘으면 작업 프로세스를 끝내고 순차 처리.
"""
from __future__ import annotations

import contextlib
import io
import logging
import multiprocessing
import os
import sys
import threading
from concurrent.futures import FIRST_EXCEPTION, ProcessPoolExecutor, wait
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

log = logging.getLogger(__name__)

_IN_WORKER_ENV = "PDFPARSER_IN_WORKER"
_DEFAULT_MIN_PAGES = 20
_DEFAULT_TIMEOUT = 600.0

# 동시에 병렬 처리하는 문서는 하나로 제한(동시 요청마다 프로세스 묶음이 생기지 않도록). 나머지는 순차 처리.
_SLOT = threading.BoundedSemaphore(1)
# 병렬 처리가 한 번 실패하면 이 프로세스에서는 다시 시도하지 않는다(실패 원인이 환경이면 매번 시간만 든다).
_DISABLED_REASON: Optional[str] = None
_MAIN_SAFE: Optional[bool] = None


def _env_int(key: str, default: int) -> int:
    raw = str(os.getenv(key, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    raw = str(os.getenv(key, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _main_module_is_spawn_safe() -> bool:
    """
    Windows 는 작업 프로세스를 띄울 때 호출한 스크립트를 다시 실행한다. 스크립트에
    `if __name__ == "__main__":` 보호 구문이 없으면 작업 프로세스마다 스크립트 전체가 다시 돌므로 병렬 처리를 하지 않는다.
    uvicorn(python -m), 대화형 실행처럼 스크립트 파일이 아닌 경우는 안전하다.
    """
    global _MAIN_SAFE
    if _MAIN_SAFE is None:
        main = sys.modules.get("__main__")
        path = getattr(main, "__file__", None)
        spec = getattr(main, "__spec__", None)
        if not path or spec is not None or not str(path).lower().endswith(".py"):
            _MAIN_SAFE = True
        else:
            try:
                with open(path, encoding="utf-8", errors="replace") as fh:
                    src = fh.read()
                _MAIN_SAFE = "__name__" in src and "__main__" in src
            except OSError:
                _MAIN_SAFE = False
    return _MAIN_SAFE


def configured_workers(total_pages: int) -> int:
    """이 문서에 쓸 작업 프로세스 수(1이면 순차 처리)."""
    if _DISABLED_REASON:
        return 1
    # 작업 프로세스 안이거나, 작업 프로세스가 호출 스크립트를 다시 실행하는 중이면 다시 병렬로 들어가지 않는다.
    if os.getenv(_IN_WORKER_ENV) == "1" or getattr(multiprocessing.current_process(), "_inheriting", False):
        return 1
    workers = _env_int("PDFPARSER_WORKERS", min(8, max(1, (os.cpu_count() or 2) - 1)))
    if workers <= 1 or total_pages < _env_int("PDFPARSER_PARALLEL_MIN_PAGES", _DEFAULT_MIN_PAGES):
        return 1
    if not _main_module_is_spawn_safe():
        return 1
    return min(workers, 61)  # Windows ProcessPoolExecutor 상한


def _chunks(items: Sequence[int], n: int) -> List[List[int]]:
    """연속 페이지 묶음으로 나눈다(같은 프로세스가 이웃 페이지를 처리)."""
    n = max(1, min(n, len(items)))
    size, extra = divmod(len(items), n)
    out, start = [], 0
    for i in range(n):
        end = start + size + (1 if i < extra else 0)
        if end > start:
            out.append(list(items[start:end]))
        start = end
    return out


# ── 작업 프로세스에서 실행되는 함수(모듈 최상위여야 pickle 가능) ─────────────────

def _init_worker() -> None:
    os.environ[_IN_WORKER_ENV] = "1"


def _run_captured(ctx: Any, fn: Callable[[], Any]) -> Tuple[str, Any, str, List[dict]]:
    """fn 을 실행해 (상태, 결과, 출력, 이번에 늘어난 ctx.errors) 를 돌려준다. 예외는 결과로 담는다."""
    buf = io.StringIO()
    before = len(ctx.errors)
    try:
        with contextlib.redirect_stdout(buf):
            value = fn()
        return "ok", value, buf.getvalue(), list(ctx.errors[before:])
    except Exception as e:  # 본 프로세스가 같은 페이지를 순차 방식으로 다시 처리한다
        return "error", repr(e), buf.getvalue(), list(ctx.errors[before:])


def _worker_page1(pdf_path: str) -> Tuple[str, Any, str, List[dict]]:
    from parsers.core.document_context import DocumentContext
    from parsers.page_1_parser import parse_page_1

    with contextlib.redirect_stdout(io.StringIO()):
        ctx = DocumentContext.open(pdf_path)
    with ctx:
        return _run_captured(ctx, lambda: parse_page_1(ctx, page_num=0))


def _worker_scan(args: Tuple[str, List[int]]) -> Dict[int, Dict[str, Any]]:
    pdf_path, pages = args
    from parsers.core.document_context import DocumentContext
    from parsers.page_2_parser import parse_page_2
    from parsers.page_3_parser import parse_page_3

    out: Dict[int, Dict[str, Any]] = {}
    with contextlib.redirect_stdout(io.StringIO()):
        ctx = DocumentContext.open(pdf_path)
    with ctx:
        for p in pages:
            before = len(ctx.errors)
            text = ctx.get_text(p)
            entry: Dict[str, Any] = {"text": text, "text_errors": list(ctx.errors[before:])}
            if p > 0:
                entry["tech"] = _run_captured(ctx, lambda: parse_page_2(ctx, p))
                entry["cm"] = _run_captured(ctx, lambda: parse_page_3(ctx, p))
            out[p] = entry
    return out


# ── 본 프로세스 ─────────────────────────────────────────────────────────────

def _is_cache_error(err: Any) -> bool:
    """캐시되는 추출 단계 오류(페이지당 한 번만 생김). 작업 프로세스마다 따로 생겨 중복될 수 있다."""
    stage = str(err.get("stage", "")) if isinstance(err, dict) else ""
    return stage.startswith("pdfplumber.") or stage.startswith("fitz.")


def _merge_errors(ctx: Any, errors: List[dict]) -> None:
    for err in errors:
        if _is_cache_error(err) and err in ctx.errors:
            continue
        ctx.errors.append(err)


class ParallelScan:
    """작업 프로세스들이 미리 계산한 결과. 결과를 실제로 쓸 때 출력과 오류를 본 프로세스에 옮긴다."""

    def __init__(self, page1: Tuple[str, Any, str, List[dict]], pages: Dict[int, Dict[str, Any]]):
        self._page1 = page1
        self._pages = pages

    def fill_text_cache(self, ctx: Any) -> None:
        for p in sorted(self._pages):
            if p not in ctx._text_cache:
                _merge_errors(ctx, self._pages[p]["text_errors"])
                ctx._text_cache[p] = self._pages[p]["text"]

    @staticmethod
    def _take(ctx: Any, item: Optional[Tuple[str, Any, str, List[dict]]], fallback: Callable[[], Any]) -> Any:
        if item is None or item[0] != "ok":
            return fallback()
        _, value, out, errors = item
        if out:
            sys.stdout.write(out)
        _merge_errors(ctx, errors)
        return value

    def page1(self, ctx: Any, fallback: Callable[[], Any]) -> Any:
        return self._take(ctx, self._page1, fallback)

    def rows(self, ctx: Any, kind: str, page_idx: int, fallback: Callable[[], Any]) -> Any:
        return self._take(ctx, self._pages.get(page_idx, {}).get(kind), fallback)


def _terminate(pool: ProcessPoolExecutor) -> None:
    for proc in list((getattr(pool, "_processes", None) or {}).values()):
        with contextlib.suppress(Exception):
            proc.terminate()
    with contextlib.suppress(Exception):
        pool.shutdown(wait=False, cancel_futures=True)


def scan_document(ctx: Any) -> Optional[ParallelScan]:
    """
    병렬로 문서를 미리 처리한다. 병렬 처리를 하지 않거나 실패하면 None(호출 측은 기존 순차 방식으로 처리).
    성공하면 ctx 텍스트 캐시를 채운다.
    """
    global _DISABLED_REASON
    workers = configured_workers(ctx.total_pages)
    if workers <= 1:
        return None
    if not _SLOT.acquire(blocking=False):
        return None
    pool: Optional[ProcessPoolExecutor] = None
    ok = False
    try:
        pages = list(range(ctx.total_pages))
        pool = ProcessPoolExecutor(max_workers=workers, initializer=_init_worker)
        page1_future = pool.submit(_worker_page1, ctx.pdf_path)
        scan_futures = [pool.submit(_worker_scan, (ctx.pdf_path, ch)) for ch in _chunks(pages, workers * 3)]
        futures = [page1_future, *scan_futures]
        done, pending = wait(futures, timeout=_env_float("PDFPARSER_PARALLEL_TIMEOUT", _DEFAULT_TIMEOUT),
                             return_when=FIRST_EXCEPTION)
        if pending:
            failed = next((f for f in done if f.exception() is not None), None)
            raise failed.exception() if failed else TimeoutError("병렬 처리 제한 시간 초과")
        merged: Dict[int, Dict[str, Any]] = {}
        for f in scan_futures:
            merged.update(f.result())
        scan = ParallelScan(page1_future.result(), merged)
        scan.fill_text_cache(ctx)
        ok = True
        return scan
    except Exception as e:
        _DISABLED_REASON = repr(e)
        msg = f"[WARN] 병렬 처리 실패 → 순차 방식으로 처리합니다(이후 병렬 처리 중지): {e!r}"
        print(msg)
        log.warning(msg)
        return None
    finally:
        if pool is not None:
            if ok:
                pool.shutdown(wait=True)
            else:
                _terminate(pool)
        _SLOT.release()
