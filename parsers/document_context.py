"""
호환용 re-export 모듈.

기존 import 경로(`parsers.document_context`)를 유지하면서 실제 구현은
`parsers.core.document_context`로 이동했다.
"""

from __future__ import annotations

from parsers.core import document_context as _impl


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__():
    return sorted(set(dir(_impl)))

