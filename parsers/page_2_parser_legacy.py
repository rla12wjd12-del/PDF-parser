"""
호환용 re-export 모듈.

기존 import 경로(`parsers.page_2_parser_legacy`)를 유지하면서 실제 구현은
`parsers.experimental.page_2_parser_legacy`로 이동했다.
"""

from __future__ import annotations

from parsers.experimental import page_2_parser_legacy as _impl


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__():
    return sorted(set(dir(_impl)))

