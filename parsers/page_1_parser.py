"""
호환용 re-export 모듈.

기존 import 경로(`parsers.page_1_parser`)를 유지하면서 실제 구현은
`parsers.core.page_1_parser`를 사용한다.
"""

from __future__ import annotations

from parsers.core.page_1_parser import *  # noqa: F401,F403

