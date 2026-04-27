"""
호환용 re-export 모듈.

기존 import 경로(`parsers.page_summary_parser`)를 유지하면서 실제 구현은
`parsers.core.page_summary_parser`로 이동했다.
"""

from __future__ import annotations

from parsers.core import page_summary_parser as _impl


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__():
    return sorted(set(dir(_impl)))

