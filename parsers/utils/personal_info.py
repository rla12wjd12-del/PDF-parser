#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""날짜 형식 변환 유틸. section_parsers.py에서 convert_date_format을 import한다."""

import re


def convert_date_format(date_str: str) -> str:
    """날짜 형식 변환: YYYY.MM.DD → YYYY-MM-DD"""
    if not date_str:
        return date_str
    try:
        m = re.match(r'(\d{4})\.(\d{2})\.(\d{2})', date_str)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        m = re.match(r'(\d{4})\.(\d{1,2})', date_str)
        if m:
            return f"{m.group(1)}-{m.group(2).zfill(2)}"
    except Exception:
        pass
    return date_str
