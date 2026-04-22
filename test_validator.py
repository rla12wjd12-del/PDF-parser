#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
루트 진입점: 다중 PDF 파싱 결과 자동 검증 스크립트.

기존 구현은 `scripts/verify/test_validator.py`에 있으며,
요청한 파일명(`test_validator.py`)으로 루트에서 바로 실행할 수 있게 래핑한다.

사용 예:
  python test_validator.py
  python test_validator.py --no-reparse
  python test_validator.py --pdf 경력증명서_오철수.pdf
"""

from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    target = here / "scripts" / "verify" / "test_validator.py"
    runpy.run_path(str(target), run_name="__main__")

