"""파싱 보강 7단계: 페이지 단위 병렬 처리의 결과 동일성과 안전장치(합성 PDF)."""
import os
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import fitz
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from parsers.core import parallel_pages as pp  # noqa: E402
from parsers.core.document_context import DocumentContext  # noqa: E402
from parsers.page_2_parser import parse_page_2  # noqa: E402


@pytest.fixture
def pdf_path(tmp_path):
    doc = fitz.open()
    for i in range(24):
        page = doc.new_page(width=595, height=842)
        page.insert_text((60, 100), f"page {i + 1} 1. 기술경력", fontname="korea", fontsize=9)
        page.insert_text((60, 130), f"2020.01.0{i % 9 + 1} ~ 2020.02.01 (32일)", fontname="korea", fontsize=8)
    path = tmp_path / "synthetic.pdf"
    doc.save(path)
    doc.close()
    return str(path)


@pytest.fixture
def parallel_env(monkeypatch):
    monkeypatch.setenv("PDFPARSER_WORKERS", "2")
    monkeypatch.setenv("PDFPARSER_PARALLEL_MIN_PAGES", "1")
    monkeypatch.delenv("PDFPARSER_PARALLEL_TIMEOUT", raising=False)
    monkeypatch.setattr(pp, "_DISABLED_REASON", None)
    monkeypatch.setattr(pp, "_MAIN_SAFE", True)


def test_scan_matches_sequential(pdf_path, parallel_env):
    with DocumentContext.open(pdf_path) as ctx:
        scan = pp.scan_document(ctx)
        assert scan is not None
        texts = dict(ctx._text_cache)
        rows = {p: scan.rows(ctx, "tech", p, lambda: pytest.fail("fallback")) for p in range(1, ctx.total_pages)}
    with DocumentContext.open(pdf_path) as ref:
        assert texts == {p: ref.get_text(p) for p in range(ref.total_pages)}
        assert rows == {p: parse_page_2(ref, p) for p in range(1, ref.total_pages)}


def test_small_document_stays_sequential(monkeypatch):
    monkeypatch.setenv("PDFPARSER_WORKERS", "8")
    monkeypatch.delenv("PDFPARSER_PARALLEL_MIN_PAGES", raising=False)
    monkeypatch.setattr(pp, "_DISABLED_REASON", None)
    monkeypatch.setattr(pp, "_MAIN_SAFE", True)
    assert pp.configured_workers(19) == 1
    assert pp.configured_workers(20) == 8
    monkeypatch.setenv("PDFPARSER_WORKERS", "0")
    assert pp.configured_workers(200) == 1


def test_no_nested_parallel_inside_worker(monkeypatch, parallel_env):
    monkeypatch.setenv("PDFPARSER_IN_WORKER", "1")
    assert pp.configured_workers(200) == 1


def test_worker_failure_falls_back_and_disables(tmp_path, parallel_env):
    ctx = SimpleNamespace(pdf_path=str(tmp_path / "missing.pdf"), total_pages=24, _text_cache={}, errors=[])
    assert pp.scan_document(ctx) is None
    assert ctx._text_cache == {} and ctx.errors == []
    assert pp._DISABLED_REASON
    assert pp.configured_workers(200) == 1


def test_timeout_falls_back(pdf_path, parallel_env, monkeypatch):
    monkeypatch.setenv("PDFPARSER_PARALLEL_TIMEOUT", "0.01")
    with DocumentContext.open(pdf_path) as ctx:
        assert pp.scan_document(ctx) is None
        assert ctx._text_cache == {}
    assert "TimeoutError" in pp._DISABLED_REASON


def test_fallback_used_when_page_failed():
    scan = pp.ParallelScan(("error", "ValueError()", "", []), {3: {"tech": ("error", "X", "", [])}})
    ctx = SimpleNamespace(errors=[])
    assert scan.page1(ctx, lambda: {"seq": 1}) == {"seq": 1}
    assert scan.rows(ctx, "tech", 3, lambda: ["seq"]) == ["seq"]
    assert scan.rows(ctx, "cm", 9, lambda: ["seq"]) == ["seq"]


def test_merge_errors_dedupes_cache_stages_only():
    ctx = SimpleNamespace(errors=[{"stage": "fitz.open", "page": -1, "error": "E"}])
    pp._merge_errors(ctx, [
        {"stage": "fitz.open", "page": -1, "error": "E"},
        {"stage": "page2", "page": 3, "error": "A"},
        {"stage": "page2", "page": 3, "error": "A"},
    ])
    assert [e["stage"] for e in ctx.errors] == ["fitz.open", "page2", "page2"]


@pytest.mark.parametrize("guarded,expected", [(True, "2"), (False, "1")])
def test_unguarded_script_stays_sequential(tmp_path, guarded, expected):
    body = "from parsers.core import parallel_pages as pp\nprint(pp.configured_workers(200))\n"
    if guarded:
        body = "if __name__ == '__main__':\n" + textwrap.indent(body, "    ")
    script = tmp_path / "caller.py"
    script.write_text(f"import sys\nsys.path.insert(0, {ROOT!r})\n" + body, encoding="utf-8")
    env = dict(os.environ, PDFPARSER_WORKERS="2", PDFPARSER_PARALLEL_MIN_PAGES="1")
    env.pop("PDFPARSER_IN_WORKER", None)
    out = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, env=env, timeout=120)
    assert out.stdout.strip().splitlines()[-1] == expected
