"""
호환용 re-export 모듈.

기존 import 경로(`parsers.tech_career_common`)를 유지하면서 실제 구현은
`parsers.utils.tech_career_common`로 이동했다.
"""

from __future__ import annotations

from parsers.utils import tech_career_common as _impl


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__():
    return sorted(set(dir(_impl)))

