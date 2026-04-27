#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기술경력(제2쪽) 파싱 휴리스틱/토큰 설정.

- page_2_parser.py에서 자주 쓰는 약어/토큰/규칙을 한 곳에서 관리하기 위해 분리했다.
- 필요 시 `data/tech_career_heuristics.json`으로 일부 값을 덮어쓸 수 있다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import json
import re
from typing import Iterable


@dataclass(frozen=True)
class TechCareerHeuristics:
    """
    기술경력 파서에서 사용하는 휴리스틱 토큰 묶음.
    """

    # 사업명 앞에 붙어서 "개요 조각"으로 자주 등장하는 약어/토큰(사업명으로 흡수되면 밀림 발생)
    overview_prefix_hints: tuple[str, ...] = (
        "NATM",
        "STEEL",
        "Steel",
        "GIRDER",
        "Girder",
        "BOX",
        "PSC",
        "PC BOX",
        "ILM",
        "FCM",
        "DCM",
    )

    # 발주자 셀/라인에 들어오지만 실제로는 직위인 토큰(표 추출/줄바꿈에 따라 swap 필요)
    # - 기본 직위 토큰은 page_2_parser의 `_looks_like_position_token`로 판단하고,
    #   여기에는 그 판정이 약한 "추가"만 둔다.
    issuer_position_extra_tokens: tuple[str, ...] = (
        "상무보",
    )

    # '~' 라인에서 공사종류 칸에 자주 오는 짧은 토큰(전문분야와 3토큰 혼선 시에만 사용)
    tilde_short_worktype_tokens: tuple[str, ...] = (
        "상수도",
        "하수도",
        "상하수도",
        "국도",
        "지방도",
        "시도",
        "농로",
        "시군도",
        "국토",
        "해안",
        "항만",
        "하천",
        "댐",
        "철도",
        "터널",
        "교량",
        "도로",
        "국가지방도",
        "공원",
        "녹지",
        "광장",
        "체육",
        "유원지",
        "관광지",
        "관광단지",
        "단지조성",
        "주택",
        "산업단지",
        "택지",
    )

    # 담당업무(직무/담당 라인 파싱)에서 담당업무로 인정하는 토큰 집합
    duty_words: tuple[str, ...] = (
        "설계",
        "설계담당",
        "설계감리",
        "계획",
        "사업관리",
        "실시설계",
        "도면작성",
        "조사",
        "감리",
        "시공",
        "분야기술인",
        "참여기술인",
        "기술지원",
        "자문",
        "지하안전영향평가",
        "안전영향평가",
        "환경영향평가",
        "사전재해영향평가",
        "소규모환경영향평가",
        "소규모지하안전영향평가",
        "안전관리",
    )

    # 직무분야 힌트 토큰(스택형 레이아웃에서 직무/담당 분리 보조)
    job_field_hints: tuple[str, ...] = (
        "토목",
        "건축",
        "기계",
        "전기",
        "통신",
        "조경",
        "환경",
        "안전",
        "품질",
        "안전관리",
        "품질관리",
    )

    # 표(col3)에서 직위로 볼 수 있는 토큰(발주자 행 vs 사업 헤더 구분에 사용)
    table_col3_position_tokens: tuple[str, ...] = (
        "사외이사",
        "상무이사",
        "전무이사",
        "부사장",
        "사장",
        "대표",
        "이사장",
        "상무",
        "전무",
        "부장",
        "차장",
        "과장",
        "계장",
        "대리",
        "주임",
        "사원",
        "선임",
        "책임",
        "수석",
        "전임",
        "소장",
        "실장",
        "팀장",
        "본부장",
        "단장",
        "감리원",
        "원장",
        "반장",
        "조장",
        "검사원",
        "관리원",
        "기사"
    )

    # 발주자 기관명처럼 보이는 힌트 정규식(문서 종류별로 확장 가능)
    issuer_name_hint_regex: str = (
        r"(시청|군청|구청|도청|관리청|해운항만청|도로공사|전력공사|수자원공사|"
        r"농어촌공사|환경공단|시설공단|도시공사|주택공사|토지공사|"
        r"건설본부|종합건설본부|지하철건설본부|국토관리청|지방국토)"
    )

    # 프로젝트명(사업명) 본문에 흔히 등장하는 "끝맺음/마커" — 개요/연장 텍스트와 분리할 때 기준.
    # (하드코딩이 아니라, 문서 전반에서 공통으로 안정적인 단어 위주)
    project_title_markers: tuple[str, ...] = (
        "공사",
        "용역",
        "사업",
        "정비",
        "건설",
        "개설",
        "확장",
        "개량",
        "설치",
        "조성",
        "실시설계",
        "기본설계",
        "기본 및 실시설계",
        "기본계획",
        "조사",
        "측량",
    )

    # 사업명 문자열 안에서 "진짜 프로젝트 제목이 시작되는" 강한 신호(정규식)
    # - 개요/연장 텍스트(… 1식, … 개소 등)가 사업명 문자열 앞에 붙는 케이스를 분리할 때 사용
    project_start_regexes: tuple[str, ...] = (
        r"도로건설공사",
        r"건설공사",
        r"실시설계용역",
        r"기본\s*및\s*실시설계용역",
        r"기본\s*및\s*실시설계",
        r"기본설계용역",
        r"기본계획",
    )

    # “개요 연장” 텍스트에서 흔히 보이는 패턴(규격/수치) — 사업명과 분리할 때 사용
    overview_measurement_patterns: tuple[str, ...] = (
        r"\bD\d+~[D]?\d+\w*\b",          # D300~1500mm 등
        r"(?:^|[^A-Za-z0-9])L=\d+\.?\d*\s*(m|km|mm|cm)\b",  # L=5.43km 등 (앞 글자에 붙어도 탐지)
        r"\bQ=\d+[\d,]*",               # Q=2,800 등
        r"[㎥㎞㎡㎝㎜]|m³|m²|㎥/일|㎥/s",
    )

    # 개요 연장 텍스트로 볼 만한 "목록형" 힌트(콜론/쉼표/개소/1식 등)
    overview_listish_patterns: tuple[str, ...] = (
        r":\s*\d",       # "가펌프장 : 16개소" 같은 형태
        r"\b\d+\s*개소\b",
        r"\b1\s*식\b",
        r",\s*",         # 콤마 다수
    )

    # “및” 토큰 오염 보정에 쓰는 키워드(사업명 복원)
    and_token: str = "및"
    and_repair_phrase: str = "및 실시설계용역"

    # 발주자 셀 마지막 토큰이 공사종류 목록으로 보일 때(쉼표 없는 단일 토큰 등) 보조 힌트
    issuer_cell_worktype_tail_keywords: tuple[str, ...] = ()


def _as_tuple_of_str(v: object) -> tuple[str, ...]:
    if v is None:
        return tuple()
    if isinstance(v, (list, tuple)):
        out = []
        for x in v:
            s = str(x).strip()
            if s:
                out.append(s)
        return tuple(out)
    s = str(v).strip()
    return (s,) if s else tuple()


def load_tech_career_heuristics(project_root: str | Path) -> TechCareerHeuristics:
    """
    기본값 + data/tech_career_heuristics.json(있으면) 오버라이드로 휴리스틱 로드.
    """
    root = Path(project_root)
    cfg_path = root / "data" / "tech_career_heuristics.json"
    base = TechCareerHeuristics()
    if not cfg_path.exists():
        return base

    try:
        obj = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return base

    if not isinstance(obj, dict):
        return base

    # 허용 키만 반영(오타/예상치 못한 키는 무시)
    kw = {}
    if "overview_prefix_hints" in obj:
        kw["overview_prefix_hints"] = _as_tuple_of_str(obj.get("overview_prefix_hints"))
    if "issuer_position_extra_tokens" in obj:
        kw["issuer_position_extra_tokens"] = _as_tuple_of_str(obj.get("issuer_position_extra_tokens"))
    if "tilde_short_worktype_tokens" in obj:
        kw["tilde_short_worktype_tokens"] = _as_tuple_of_str(obj.get("tilde_short_worktype_tokens"))
    if "duty_words" in obj:
        kw["duty_words"] = _as_tuple_of_str(obj.get("duty_words"))
    if "job_field_hints" in obj:
        kw["job_field_hints"] = _as_tuple_of_str(obj.get("job_field_hints"))
    if "table_col3_position_tokens" in obj:
        kw["table_col3_position_tokens"] = _as_tuple_of_str(obj.get("table_col3_position_tokens"))
    if "issuer_name_hint_regex" in obj and str(obj.get("issuer_name_hint_regex") or "").strip():
        kw["issuer_name_hint_regex"] = str(obj.get("issuer_name_hint_regex") or "").strip()
    if "project_title_markers" in obj:
        kw["project_title_markers"] = _as_tuple_of_str(obj.get("project_title_markers"))
    if "project_start_regexes" in obj:
        kw["project_start_regexes"] = _as_tuple_of_str(obj.get("project_start_regexes"))
    if "overview_measurement_patterns" in obj:
        kw["overview_measurement_patterns"] = _as_tuple_of_str(obj.get("overview_measurement_patterns"))
    if "overview_listish_patterns" in obj:
        kw["overview_listish_patterns"] = _as_tuple_of_str(obj.get("overview_listish_patterns"))
    if "and_token" in obj and str(obj.get("and_token") or "").strip():
        kw["and_token"] = str(obj.get("and_token") or "").strip()
    if "and_repair_phrase" in obj and str(obj.get("and_repair_phrase") or "").strip():
        kw["and_repair_phrase"] = str(obj.get("and_repair_phrase") or "").strip()
    if "issuer_cell_worktype_tail_keywords" in obj:
        kw["issuer_cell_worktype_tail_keywords"] = _as_tuple_of_str(
            obj.get("issuer_cell_worktype_tail_keywords")
        )

    try:
        return TechCareerHeuristics(**{**base.__dict__, **kw})
    except Exception:
        return base


def compiled_any(patterns: Iterable[str]) -> re.Pattern | None:
    """
    패턴 목록을 OR로 묶어 컴파일. 빈 입력이면 None.
    """
    pats = [p for p in (patterns or []) if str(p).strip()]
    if not pats:
        return None
    return re.compile("|".join(f"(?:{p})" for p in pats), re.IGNORECASE)

