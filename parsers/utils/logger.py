#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""공통 로거 팩토리. main.py에서 1회 basicConfig 후 이 로거를 사용한다."""
from __future__ import annotations

import logging


def get_parser_logger(name: str = "pdf_parser") -> logging.Logger:
    return logging.getLogger(name)
