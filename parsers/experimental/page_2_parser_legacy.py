"""
레거시 제2쪽(기술경력) 파서 호환 래퍼.

- 실제 구현은 `parsers.experimental.page_2_parser_legacy_impl`로 이동했다.
- 목적: 레거시 모듈 파일 자체는 작게 유지하고, 과거 import 경로를 깨지 않기.
"""

from __future__ import annotations

from parsers.experimental import page_2_parser_legacy_impl as _impl


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__():
    return sorted(set(dir(_impl)))

