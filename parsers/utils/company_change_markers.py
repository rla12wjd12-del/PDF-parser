#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path

_CACHE: list[str] | None = None


def _default_markers() -> list[str]:
    # 폴백(파일이 없거나 읽기 실패 시): 기존 동작을 유지하기 위한 기본 목록
    return [
        "現",
        "현",
        "흡수합병",
        "분할설립",
        "상호변경",
        "법인전환",
        "합병",
        "양수도",
        "양도양수",
    ]


def get_company_change_markers(project_root: str | Path | None = None) -> list[str]:
    """
    근무처 '상호 변경 사유' 구분 키워드 목록을 로드한다.
    우선순위:
    1) {project_root}/data/company_change_markers.json
    2) 이 모듈 기준 repo 루트 추정 후 data/company_change_markers.json
    3) 하드코딩 폴백(_default_markers)
    """
    global _CACHE
    if _CACHE is not None:
        return list(_CACHE)

    try:
        root = Path(project_root) if project_root else Path(__file__).resolve().parents[2]
        path = root / "data" / "company_change_markers.json"
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                markers = [str(x).strip() for x in raw if str(x).strip()]
                # 중복 제거(순서 유지)
                seen: set[str] = set()
                out: list[str] = []
                for m in markers:
                    if m in seen:
                        continue
                    seen.add(m)
                    out.append(m)
                if out:
                    _CACHE = out
                    return list(_CACHE)
    except Exception:
        pass

    _CACHE = _default_markers()
    return list(_CACHE)

