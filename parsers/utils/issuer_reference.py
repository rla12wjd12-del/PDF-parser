# -*- coding: utf-8 -*-
"""행정구역·공공기관 카탈로그: 발주자 접두 매칭(틸드 head 분리 보조).

classify_issuer_reference 등은 내부·디버그용이며 parse_full_document JSON에는 넣지 않는다.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, List

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 단독 시군구명으로 쓰면 동명이의가 많은 짧은 구명(접두 매칭에서 제외)
_AMBIGUOUS_SIGUNGU_STANDALONE = frozenset(
    {"중구", "동구", "서구", "남구", "북구"}
)

# 기관명 부분 일치 시 너무 짧은 문자열 제외
_MIN_INSTITUTION_LEN = 5


def _walk_collect_region_prefixes(nodes: Any, out: set[str]) -> None:
    if not isinstance(nodes, list):
        return
    for node in nodes:
        if not isinstance(node, dict):
            continue
        typ = (node.get("type") or "").strip()
        name = (node.get("name") or "").strip()
        if typ == "시도" and name:
            out.add(name)
            for ch in node.get("children") or []:
                if not isinstance(ch, dict):
                    continue
                if (ch.get("type") or "").strip() != "시군구":
                    continue
                cname = (ch.get("name") or "").strip()
                if not cname:
                    continue
                out.add(f"{name} {cname}")
                if cname not in _AMBIGUOUS_SIGUNGU_STANDALONE and len(cname) >= 2:
                    out.add(cname)


@lru_cache(maxsize=1)
def _admin_prefix_strings() -> frozenset[str]:
    path = _PROJECT_ROOT / "data" / "korea_regions_tree.json"
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    out: set[str] = set()
    _walk_collect_region_prefixes(data, out)
    return frozenset(out)


def _collect_institution_names(obj: Any, acc: set[str]) -> None:
    if isinstance(obj, dict):
        v = obj.get("name")
        if isinstance(v, str) and v.strip():
            acc.add(v.strip())
        for vv in obj.values():
            _collect_institution_names(vv, acc)
    elif isinstance(obj, list):
        for it in obj:
            _collect_institution_names(it, acc)


@lru_cache(maxsize=1)
def _public_institution_names_sorted() -> tuple[str, ...]:
    path = _PROJECT_ROOT / "data" / "public_institutions_tree.json"
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    acc: set[str] = set()
    _collect_institution_names(data, acc)
    return tuple(sorted(acc, key=len, reverse=True))


def longest_admin_prefix_token_count(tokens: List[str]) -> int:
    """
    tokens 앞에서부터 이어 붙인 문자열이 행정구역 카탈로그(시도·시군구·시도+시군구·비모호 단독 시군구)와
    일치하는 가장 긴 토큰 개수(1..len). 없으면 0.
    """
    if not tokens:
        return 0
    tt = tuple(str(t).strip() for t in tokens if str(t).strip())
    if not tt:
        return 0
    admin = _admin_prefix_strings()
    for k in range(len(tt), 0, -1):
        s = " ".join(tt[:k])
        if s in admin:
            return k
    return 0


def _issuer_string_matches_institution(s: str) -> bool:
    s = " ".join((s or "").split())
    if len(s) < _MIN_INSTITUTION_LEN:
        return False
    for n in _public_institution_names_sorted():
        if len(n) < _MIN_INSTITUTION_LEN:
            continue
        if s == n:
            return True
        if s.endswith(n):
            return True
    return False


def longest_institution_prefix_token_count(tokens: List[str]) -> int:
    """
    head 앞쪽 m개 토큰이 공공기관 카탈로그 name과 정확 일치 또는 접미 일치하는
    가장 큰 m (1 .. len-1). 없으면 0.
    """
    tt = [str(t).strip() for t in tokens if str(t).strip()]
    if len(tt) < 2:
        return 0
    for m in range(len(tt) - 1, 0, -1):
        s = " ".join(tt[:m])
        if _issuer_string_matches_institution(s):
            return m
    return 0


