#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
레거시 의존 없는(page_2_parser_legacy_impl 미참조) 공용 유틸.

목표
- `parsers.core.page_3_parser`가 과거 page2(기술경력) 레거시 구현에 기대던 일부 유틸을
  `parsers/utils/`에서 독립적으로 제공한다.
- 레거시를 완전히 격리(`parsers/experimental/*` 미-import)한다.

주의
- 본 모듈은 "page3가 필요로 하는 최소 유틸"만 제공한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import re

from parsers.tech_career_heuristics import load_tech_career_heuristics
from parsers.tech_career_common import _is_annotation_or_footnote_line, _is_footer_or_header_line


_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_TC_H = load_tech_career_heuristics(_PROJECT_ROOT)

_DUTY_WORDS = frozenset(_TC_H.duty_words or ())
_JOB_FIELD_HINTS = frozenset(_TC_H.job_field_hints or ())
_POSITION_TOKENS = frozenset(_TC_H.table_col3_position_tokens or ())
_ISSUER_POS_EXTRA = frozenset(_TC_H.issuer_position_extra_tokens or ())

_DATE_RE = re.compile(r"^\d{4}\.\d{2}(?:\.\d{2})?$")
_DAYS_RE = re.compile(r"^\(?\s*(\d[\d,]*)?\s*일\s*\)?$")
_DUTY_PAREN_RE = re.compile(r"^\([^)]+\)$")


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", "", (s or "").strip())


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _line_starts_with_overview_continue_marker(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return False
    return t.startswith("┖→") or t.startswith("\u2516\u2192")


def _looks_like_position_token(s: str) -> bool:
    t = _norm_space(s)
    if not t:
        return False
    return (t in _POSITION_TOKENS) or (t in _ISSUER_POS_EXTRA)


def _parse_job_and_duty_line_stacked(line: str) -> Optional[Tuple[str, str]]:
    s = _norm_space(line)
    if not s or s == "┖→":
        return None
    if s.startswith("~") or _DATE_RE.match(s) or _DAYS_RE.match(s):
        return None
    parts = s.split(" ")
    if len(parts) != 2:
        return None
    a, b = parts[0].strip(), parts[1].strip()
    if not a or not b:
        return None
    if a in _JOB_FIELD_HINTS and (b in _DUTY_WORDS or _DUTY_PAREN_RE.match(b)):
        return a, b
    if b in _JOB_FIELD_HINTS and (a in _DUTY_WORDS or _DUTY_PAREN_RE.match(a)):
        return b, a
    return None


def _parse_issuer_line_stacked(line: str) -> Optional[Dict[str, str]]:
    """
    stacked 레이아웃에서 발주자/공사종류/전문분야/직위를 한 줄에서 복원하는 최소 구현.
    예) "대전지방국토관리청 국도 도로및공항 사원"
    """
    s = _norm_space(line)
    if not s or s == "┖→":
        return None
    if s.startswith("~") or _DATE_RE.match(s) or _DAYS_RE.match(s):
        return None
    parts = s.split(" ")
    if len(parts) < 4:
        return None
    pos = parts[-1].strip()
    if not _looks_like_position_token(pos):
        return None
    specialty = parts[-2].strip()
    work_type = parts[-3].strip()
    issuer = " ".join(parts[:-3]).strip()
    if not issuer:
        return None
    return {"발주자": issuer, "공사종류": work_type, "전문분야": specialty, "직위": pos}


def _looks_like_technical_overview_line(line: str) -> bool:
    s = _norm_space(line)
    if not s:
        return False
    if s.startswith("- "):
        return True
    if "○" in s or "\u25cb" in s:
        return True
    if "내용:" in s or "내용 :" in s:
        return True
    if re.match(r"^\d+\)\s*", s):
        return True
    if re.search(r"공법\s*\([^)]*개착", s):
        return True
    if re.search(r"공법\s*\([^)]*비개착", s):
        return True
    compact = _norm_key(s)
    if "공법" in compact and len(compact) <= 24:
        if not (compact.endswith("공사") or compact.endswith("용역") or compact.endswith("사업")):
            return True
    if re.fullmatch(r"[A-Z]{2,8}", compact):
        return True
    if re.fullmatch(r"[A-Za-z0-9.,/+\-]{2,20}공법", compact):
        return True
    if "검토 설계" in s and "용역" not in s:
        return True
    if re.match(r"^장\s*:\s*\d", s):
        return True
    if re.match(r"^점\s+", s) and "접속" in s:
        return True
    return False


def _technical_overview_compact_prefix(prefix_compact: str) -> bool:
    if not prefix_compact or len(prefix_compact) < 4:
        return False
    if "공법" in prefix_compact and ("개착" in prefix_compact or "비개착" in prefix_compact):
        return True
    if "검토설계" in prefix_compact:
        return True
    if prefix_compact.startswith("조물") and "공법" in prefix_compact:
        return True
    return False


def _table_name_is_trusted_suffix_of_flow(flow_nm: str, table_nm: str) -> bool:
    a = _norm_key(flow_nm)
    b = _norm_key(table_nm)
    if not b or len(b) < 8:
        return False
    if not a.endswith(b):
        return False
    if len(a) <= len(b) + 2:
        return False
    pref = a[: len(a) - len(b)]
    return _technical_overview_compact_prefix(pref)


def _is_probable_project_name_line(line: str) -> bool:
    s = _norm_space(line)
    if not s or s == "┖→":
        return False
    if s.startswith("~") or _DATE_RE.match(s) or _DAYS_RE.match(s):
        return False
    if _is_footer_or_header_line(s):
        return False
    if _looks_like_position_token(s):
        return False
    if _parse_job_and_duty_line_stacked(s):
        return False
    if _parse_issuer_line_stacked(s):
        return False
    if len(s) <= 1:
        return False
    korean_chars = re.findall(r"[가-힣]", s)
    if len(korean_chars) < 2:
        alphanumeric = re.findall(r"[A-Za-z0-9]", s)
        if len(alphanumeric) < 3:
            return False
    if re.search(r"[㎥㎞㎡㎝㎜㎥㏃]|Q=|q=|m³|m²|㎥/일|㎥/s", s):
        return False
    if re.search(r"\bD\d+~[D]?\d+\w*\b", s):
        return False
    if re.search(r"\bL=\d+\.?\d*\s*(m|km|mm|cm)\b", s, re.IGNORECASE):
        return False
    if len(s) <= 15 and re.search(r"\.\s*[가-힣A-Z]", s):
        return False
    if re.match(r"^계\(", s) or re.match(r"^등[,\s]", s):
        return False
    if _looks_like_technical_overview_line(s):
        return False
    return True


def _strip_tail_job_duty(name: str) -> tuple[str, str, str]:
    s = _norm_space(name)
    if not s:
        return s, "", ""
    parts = s.split(" ")
    if len(parts) >= 3 and parts[-2] in _JOB_FIELD_HINTS:
        project = " ".join(parts[:-2]).strip()
        if project:
            return project, parts[-2], parts[-1]
    if len(parts) >= 2 and parts[-1] in _JOB_FIELD_HINTS:
        candidate = " ".join(parts[:-1]).strip()
        if candidate:
            return candidate, parts[-1], ""
    return s, "", ""


def _parse_project_line(line: str) -> Optional[dict]:
    s = _norm_space(line)
    if not s or s == "┖→":
        return None
    if s.startswith("~"):
        return None
    parts = s.split(" ")
    if len(parts) < 2:
        return None
    if "/" in s and len(parts) <= 2:
        return None
    duty_i: int | None = None
    duty_val = ""
    for i in range(len(parts) - 1, max(-1, len(parts) - 4), -1):
        cand = parts[i].strip()
        if cand == "설계담당":
            cand = "설계"
        if cand in _DUTY_WORDS or _DUTY_PAREN_RE.match(cand):
            duty_i = i
            duty_val = cand
            break
    if duty_i is None:
        return None
    if duty_i - 1 < 0:
        return None
    job_val = parts[duty_i - 1].strip()
    if not job_val:
        return None
    if job_val not in _JOB_FIELD_HINTS:
        return None
    project = " ".join(parts[: duty_i - 1]).strip()
    if not project:
        return None
    return {"사업명": project, "직무분야": job_val, "담당업무": duty_val}


def _apply_parse_project_line_to_row(r: Dict[str, Any], raw_name: str) -> None:
    s = str(raw_name or "").strip()
    if not s:
        return
    ph = _parse_project_line(s)
    if ph:
        nm = str(ph.get("사업명") or "").strip()
        if nm:
            r["사업명"] = nm
        if ph.get("직무분야") and not str(r.get("직무분야") or "").strip():
            r["직무분야"] = str(ph.get("직무분야") or "").strip()
        if ph.get("담당업무") and not str(r.get("담당업무") or "").strip():
            r["담당업무"] = str(ph.get("담당업무") or "").strip()
        return
    cleaned, jf, dt = _strip_tail_job_duty(s)
    if cleaned != s:
        r["사업명"] = cleaned
        if jf and not str(r.get("직무분야") or "").strip():
            r["직무분야"] = jf
        if dt and not str(r.get("담당업무") or "").strip():
            r["담당업무"] = dt


def _enrich_from_table_by_project_name(
    rows: List[Dict[str, Any]], table_projects: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    if not rows or not table_projects:
        return rows

    def _tp_ctx_fk(tp: Dict[str, Any]) -> str:
        return "|".join(
            [
                _norm_key(str(tp.get("발주자") or "")),
                _norm_key(str(tp.get("전문분야") or "")),
                _norm_key(str(tp.get("직위") or "")),
                _norm_key(str(tp.get("공사종류") or "")),
            ]
        )

    def _apply_tp_to_row(r: Dict[str, Any], tp: Dict[str, Any], *, replace_name: bool) -> None:
        if replace_name:
            nm = str(tp.get("사업명") or "").strip()
            cur = str(r.get("사업명") or "").strip()
            if nm:
                if not cur:
                    r["사업명"] = nm
                else:
                    a, b = _norm_key(cur), _norm_key(nm)
                    if a == b:
                        if cur.count(" ") > nm.count(" "):
                            r["사업명"] = nm
                    elif a in b:
                        r["사업명"] = nm
                    elif b in a:
                        if _table_name_is_trusted_suffix_of_flow(cur, nm):
                            r["사업명"] = nm
                    elif len(cur) < len(nm):
                        r["사업명"] = nm
                _apply_parse_project_line_to_row(r, str(r.get("사업명") or ""))
        for k, v in tp.items():
            if k in ["참여기간_시작일", "참여기간_종료일", "인정일수", "참여일수"]:
                continue
            if k == "사업명" and not replace_name:
                continue
            if not str(r.get(k) or "").strip() and str(v or "").strip():
                r[k] = v

    consumed: set[int] = set()

    for r in rows:
        has_evidence = any(
            str(r.get(k) or "").strip()
            for k in ["발주자", "공사종류", "전문분야", "직무분야", "담당업무", "직위"]
        )
        if not has_evidence:
            continue

        nk = _norm_key(str(r.get("사업명") or ""))
        idx: int | None = None
        matched_by_name = False

        if nk:
            for i, tp in enumerate(table_projects):
                if i in consumed:
                    continue
                if _norm_key(str(tp.get("사업명") or "")) == nk:
                    idx = i
                    matched_by_name = True
                    break

        if idx is None and _norm_key(str(r.get("발주자") or "")):
            fk = _tp_ctx_fk(r)
            if fk != "|||":
                for i, tp in enumerate(table_projects):
                    if i in consumed:
                        continue
                    if _tp_ctx_fk(tp) == fk:
                        idx = i
                        break

        issuer_n = _norm_key(str(r.get("발주자") or ""))
        if idx is None and nk and len(nk) >= 3 and issuer_n:
            for i, tp in enumerate(table_projects):
                if i in consumed:
                    continue
                if _norm_key(str(tp.get("발주자") or "")) != issuer_n:
                    continue
                tpn = _norm_key(str(tp.get("사업명") or ""))
                if not tpn:
                    continue
                if tpn.endswith(nk) or (len(nk) >= 4 and nk in tpn) or (len(tpn) >= 4 and tpn in nk):
                    idx = i
                    break

        if idx is None:
            continue

        consumed.add(idx)
        tp = table_projects[idx]
        tp_nm = str(tp.get("사업명") or "").strip()
        cur_nm = str(r.get("사업명") or "").strip()
        replace_name = (not matched_by_name) and bool(tp_nm)
        if matched_by_name and tp_nm and cur_nm:
            if cur_nm.count(" ") > tp_nm.count(" ") and _norm_key(cur_nm) == _norm_key(tp_nm):
                replace_name = True
        if tp_nm and cur_nm and _table_name_is_trusted_suffix_of_flow(cur_nm, tp_nm):
            replace_name = True
        _apply_tp_to_row(r, tp, replace_name=replace_name)

    return rows


def _collect_flow_block_project_name_backward(
    lines_in: List[str],
    date_idx: int,
    max_lookback: int = 12,
    confirmed_end_indices: Optional[set] = None,
) -> tuple[str, Optional[tuple[str, str]]]:
    segments: List[str] = []
    jd_from_name: Optional[tuple[str, str]] = None
    bi = date_idx - 1
    back_n = 0
    while bi >= 0 and back_n < max_lookback:
        if confirmed_end_indices and bi in confirmed_end_indices:
            break
        sback = (lines_in[bi] or "").strip()
        if _DATE_RE.match(sback):
            break
        if _DAYS_RE.match(sback):
            break
        if sback.lstrip().startswith("~"):
            break
        if _line_starts_with_overview_continue_marker(sback):
            break
        if bi >= 1 and _line_starts_with_overview_continue_marker((lines_in[bi - 1] or "").strip()):
            break
        if _is_annotation_or_footnote_line(sback):
            bi -= 1
            back_n += 1
            continue
        if _parse_issuer_line_stacked(sback):
            break
        if _parse_job_and_duty_line_stacked(sback):
            bi -= 1
            back_n += 1
            continue
        if _is_probable_project_name_line(sback):
            ph = _parse_project_line(sback)
            frag = (ph.get("사업명") if ph else sback) or ""
            frag = str(frag).strip()
            if frag:
                segments.append(frag)
            if (
                jd_from_name is None
                and ph
                and str(ph.get("직무분야") or "").strip()
                and str(ph.get("담당업무") or "").strip()
            ):
                jd_from_name = (
                    str(ph.get("직무분야") or "").strip(),
                    str(ph.get("담당업무") or "").strip(),
                )
            bi -= 1
            back_n += 1
            continue
        break

    if not segments:
        return "", jd_from_name

    merged = "".join(reversed(segments)).strip()
    merged_one = _norm_space(merged)
    # 개요 꼬리 제거는 page3에서는 과도하게 공격적일 수 있어 최소화(공법/개착 꼬리 등만).
    merged_one = merged_one  # keep
    ph2 = _parse_project_line(merged_one)
    if ph2 and str(ph2.get("사업명") or "").strip():
        merged = str(ph2.get("사업명") or "").strip()
        if (
            jd_from_name is None
            and str(ph2.get("직무분야") or "").strip()
            and str(ph2.get("담당업무") or "").strip()
        ):
            jd_from_name = (
                str(ph2.get("직무분야") or "").strip(),
                str(ph2.get("담당업무") or "").strip(),
            )
    return merged, jd_from_name

