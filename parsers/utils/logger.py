#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""공통 로거 팩토리. main.py에서 1회 basicConfig 후 이 로거를 사용한다."""
from __future__ import annotations

import json
import logging
import time


def get_parser_logger(name: str = "pdf_parser") -> logging.Logger:
    return logging.getLogger(name)


# ──────────────────────────────────────────────────────────────────────────────
# NDJSON agent debug logger (page2/page3 파서 공용)
# ──────────────────────────────────────────────────────────────────────────────
_AGENT_DEBUG_LOG_PATH = "debug-dcc858.log"
_AGENT_DEBUG_SESSION_ID = "dcc858"


def agent_debug_log(
    *,
    run_id: str,
    hypothesis_id: str,
    location: str,
    message: str,
    data: dict,
) -> None:
    """NDJSON 형식의 디버그 로그를 파일에 추가한다 (예외 무시)."""
    try:
        payload = {
            "sessionId": _AGENT_DEBUG_SESSION_ID,
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        with open(_AGENT_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        return
