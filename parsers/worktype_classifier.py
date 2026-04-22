#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공사종류(WorkType) 판별 유틸.

목표:
- 표/텍스트 추출에서 발주자·공사종류 경계가 흔들릴 때, 카탈로그 기반으로 공사종류를 식별한다.
- 카탈로그(Construction_categories_tree.json, building_categories_tree.json) + 구분자 변형(공백/콤마/가운뎃점/점) 허용.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional


_SPLIT_RE = re.compile(r"[,\u00b7\u318d/]+")  # , · ㆍ /
_AND_RE = re.compile(r"\s*및\s*")
_WS_RE = re.compile(r"\s+")


def _norm(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # 공백 제거 + 구분자/점류 제거(카탈로그는 '항만관개수로'인데 PDF는 '항만·관개수로' 등)
    s = _WS_RE.sub("", s)
    s = s.replace("·", "").replace("ㆍ", "").replace(".", "").replace("．", "")
    return s


def _iter_tree_nodes(obj) -> Iterable[str]:
    """
    tree json 내에서 main/sub/detailCategory에 해당하는 문자열을 모두 순회.
    (키 이름은 파일마다 다를 수 있어 보편적으로 처리)
    """
    if obj is None:
        return
    if isinstance(obj, str):
        yield obj
        return
    if isinstance(obj, dict):
        for k in ("mainCategory", "subCategory", "detailCategory"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                yield v
        # 자식 배열 키들
        for k in ("subCategories", "detailCategories", "children", "items"):
            v = obj.get(k)
            if isinstance(v, list):
                for x in v:
                    yield from _iter_tree_nodes(x)
        return
    if isinstance(obj, list):
        for x in obj:
            yield from _iter_tree_nodes(x)


@lru_cache(maxsize=1)
def _load_catalog_norm_set(project_root: str) -> frozenset[str]:
    root = Path(project_root)
    paths = [
        root / "data" / "Construction_categories_tree.json",
        root / "data" / "building_categories_tree.json",
    ]
    out: set[str] = set()
    for p in paths:
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        for s in _iter_tree_nodes(raw):
            nk = _norm(s)
            if nk:
                out.add(nk)
    return frozenset(out)


def is_worktype_phrase(text: str, *, project_root: str) -> bool:
    """
    공사종류 후보 문자열이 카탈로그 기반으로 유효한지 판별.
    - 단일: '도로', '교량'
    - 콤마/가운뎃점/슬래시: '도로,교량', '통신·전력구'
    - 괄호: '상수도시설(상수도,정수장)' (main+parts 모두 카탈로그에 있으면 True)
    """
    s = (text or "").strip()
    if not s:
        return False
    cat = _load_catalog_norm_set(project_root)

    # 괄호(main(parts...)) 형태 처리
    m = re.fullmatch(r"\s*([^\(\)]+)\s*\(\s*([^\(\)]+)\s*\)\s*", s)
    if m:
        main = _norm(m.group(1))
        inside = m.group(2)
        parts = []
        for chunk in _AND_RE.split(inside):
            parts.extend([c for c in _SPLIT_RE.split(chunk) if c and c.strip()])
        parts_n = [_norm(x) for x in parts if _norm(x)]
        if main and main in cat and parts_n and all(pn in cat for pn in parts_n):
            return True
        # main 자체만으로도 유효하면 공사종류로 인정(내부가 일부 누락될 수 있어 완화)
        if main and main in cat:
            return True

    # 구분자 조합형: 각각이 카탈로그에 있으면 True
    parts = []
    for chunk in _AND_RE.split(s):
        parts.extend([c for c in _SPLIT_RE.split(chunk) if c and c.strip()])
    parts_n = [_norm(x) for x in parts if _norm(x)]
    if len(parts_n) >= 2 and all(pn in cat for pn in parts_n):
        return True

    # 단일
    return _norm(s) in cat


def extract_worktype_suffix_from_tokens(
    tokens: list[str],
    *,
    project_root: str,
    max_suffix_tokens: int = 4,
) -> tuple[int, str]:
    """
    토큰 리스트의 suffix 중 공사종류로 볼 수 있는 가장 긴 구간을 찾는다.
    Returns: (suffix_token_count, suffix_text). 못 찾으면 (0, "").
    """
    toks = [t for t in (tokens or []) if (t or "").strip()]
    if not toks:
        return 0, ""
    max_k = min(max_suffix_tokens, len(toks))
    for k in range(max_k, 0, -1):
        cand = " ".join(toks[-k:]).strip()
        if is_worktype_phrase(cand, project_root=project_root):
            return k, cand
    return 0, ""


def split_issuer_and_worktype_by_catalog(
    issuer_text: str,
    *,
    project_root: str,
    max_suffix_tokens: int = 4,
) -> tuple[str, str]:
    """
    발주자 문자열 끝부분에 공사종류가 붙어 있는 경우를 카탈로그로 분리한다.
    (공사종류가 이미 별도 필드로 존재할 수도 있으므로, 반환값을 호출부에서 병합/우선순위 처리)
    """
    iss = " ".join((issuer_text or "").split()).strip()
    if not iss:
        return "", ""
    toks = iss.split(" ")
    k, wt = extract_worktype_suffix_from_tokens(toks, project_root=project_root, max_suffix_tokens=max_suffix_tokens)
    if k <= 0 or not wt:
        return iss, ""
    issuer = " ".join(toks[:-k]).strip()
    return issuer, wt.strip()

