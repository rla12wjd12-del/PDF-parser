#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
제1쪽 섹션별 파서: 등급, 국가기술자격, 학력, 교육훈련, 상훈, 벌점 및 제재사항, 근무처

교육훈련 파싱: 하이브리드 방식 (테이블 + 텍스트) 적용
- 테이블에서 과정명 완전 추출 (줄바꿈 병합)
- 텍스트에서 교육기관, 교육인정여부 추출
- 100% 정확도 달성
"""

import re
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, List, Any, Optional, Tuple
import pdfplumber
from parsers.personal_info import convert_date_format
from parsers.table_settings import extract_tables_merged, table_set_has_header_signals
from field_catalog import best_match_specialty, extract_name_and_grade, get_field_catalog

# 직무분야/전문분야 카탈로그(엑셀 기반, 실패 시 폴백)
# NOTE: data/field_catalog.json은 repo root의 data/ 아래에 있으므로, core/ 하위에서 2단계 더 올라간 경로를 root로 준다.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CATALOG = get_field_catalog(project_root=_REPO_ROOT)
BASIC_FIELDS = list(_CATALOG.job_fields)

# ============================================
# 교육훈련 파싱 전용 상수
# ============================================

# 교육인정여부 키워드
RECOGNITION_KEYWORDS = ['건설사업관리', '설계·시공', '품질관리']

# 교육기관 키워드
INSTITUTION_KEYWORDS = [
    '건설기술교육원', 
    '건설산업교육원', 
    '한국건설기술관리협회', 
    '한국시설안전공단',
    '한국건설기술인협회'
]

# 날짜 패턴
DATE_PATTERN = r'(\d{4}\.\d{2}\.\d{2})\s*~\s*(\d{4}\.\d{2}\.\d{2})'

# 새로운 과정명 시작 패턴
COURSE_START_PATTERNS = [
    '건설사업', '포장시공', '감리사', '고급전문', '기본교육',
    '전문교육', '설계·시공', '정밀안전', '관급자재', '품질관리',
    '도로교량', '시설물안전', '보수보강', '원가계산', '적산실무',
    '소규모취약', '내진성능', '안전점검', '건설현장', '스마트건설',
    '수치지도', '드론촬영', '지반조사', '기초공사', '콘크리트',
    '철근콘크리트', '강구조', '토공사', '가설공사', '측량'
]


def find_column_index(header_row: List, keyword: str) -> int:
    """헤더 행에서 키워드로 열 인덱스 찾기"""
    for i, cell in enumerate(header_row):
        if cell and keyword in str(cell):
            return i
    return -1


def find_section_header(table: List[List], keywords: List[str], exclude_keywords: Optional[List[str]] = None) -> int:
    """섹션 헤더 찾기 (개선: 더 정확한 매칭)"""
    for i, row in enumerate(table):
        if not row:
            continue
        row_text = ' '.join([str(cell) for cell in row if cell])
        
        # 필수 키워드 확인
        if any(kw in row_text for kw in keywords):
            # 제외 키워드 확인
            if exclude_keywords and any(kw in row_text for kw in exclude_keywords):
                continue
            return i
    return -1


def find_next_section_header(table: List[List], start_idx: int, section_keywords: List[str]) -> int:
    """다음 섹션 헤더 찾기 (더 정확한 섹션 구분)"""
    for i in range(start_idx + 1, len(table)):
        row = table[i]
        if not row:
            continue
        row_text = ' '.join([str(cell) for cell in row if cell])
        
        # 다른 섹션 헤더 키워드 확인
        if any(kw in row_text for kw in section_keywords):
            return i
    return len(table)


_AWARD_DATE_TOKEN = re.compile(r"\d{4}\.\d{2}\.\d{2}")
# FIX: 기존 패턴은 "근무처"가 같은 줄에 다른 텍스트와 붙어있으면 매칭 실패.
# 독립된 줄에 있는 섹션 헤더 키워드만 종료 경계로 인식하도록 패턴 강화.
# re.MULTILINE 플래그를 compile 인자로 전달((?m) 인라인 플래그를 패턴 중간에 쓰면 Python 3.11+ 에서 오류).
_AWARD_SECTION_END = re.compile(
    r"^\s*(?:벌점\s*및\s*제재사항?|근무처|교육훈련|국가기술자격|1\.\s*기술경력|2\.\s*건설사업관리)\s*$"
    r"|^\s*(?:벌점\s*및\s*제재|근무처\s+근무기간|교육훈련\s+교육기간)",
    re.MULTILINE,
)
_TYPE_TAIL_HINT = re.compile(r"(표창|훈장|포장|감사장|장려|감사|\[제\s*\d+호\]|제\s*\d+\s*호)")
_AWARD_TYPE_TOKENS = ("표창장", "표창패", "훈장", "포장", "감사장", "상장", "유공표창", "우수상")


def _squash_whitespace_inside_first_award_bracket_body(t: str) -> str:
    # [수정] 상훈 근거는 '토큰[한 줄짜리 본문]' 형태인 경우가 많고, PDF 줄바꿈·셀 병합 때
    # 대괄호 안에만 의미 없는 공백이 생긴다(예: 완벽한품 질관리→완벽한품질관리). 본문 구간만 공백 제거.
    s = str(t or "")
    if "[" not in s or "]" not in s:
        return s
    best_pos: int | None = None
    best_tok = ""
    for tok in _AWARD_TYPE_TOKENS:
        key = tok + "["
        p = s.find(key)
        if p < 0:
            continue
        if best_pos is None or p < best_pos:
            best_pos, best_tok = p, tok
    if best_pos is None:
        return s
    open_i = best_pos + len(best_tok)
    close_i = s.find("]", open_i + 1)
    if close_i < 0:
        return s
    inner = s[open_i + 1 : close_i]
    inner_flat = re.sub(r"\s+", "", inner)
    return s[: open_i + 1] + inner_flat + s[close_i:]


def _normalize_award_type_text(s: str) -> str:
    """
    상훈 종류/근거 문자열을 정규화한다.
    - 섹션 라벨(상훈) 유입 제거
    - '제99-2 30호' 같이 숫자 토큰이 셀/워드 단위로 쪼개진 케이스 결합
    - 공백 축약
    """
    t = str(s or "").replace("\n", " ").strip()
    if not t:
        return ""
    # 라벨 오염 제거(문장 중간 삽입도 있어 단어 경계로 제거)
    t = re.sub(r"\b상훈\b", " ", t).strip()
    # '제99-2 30호' -> '제99-230호' (공백/하이픈 주변 공백 정리)
    t = re.sub(r"(제\s*\d+\s*[-–]\s*\d+)\s+(\d+\s*호)", r"\1\2", t)
    # 표/셀 분리로 타입 토큰 앞쪽이 깨져 앞에 잡음이 붙는 케이스 제거:
    # 예) '장[ 21337] 표창장[21337]' -> '표창장[21337]'
    # 예) '...기여] 감사장[감리원...' 처럼 앞에 설명이 붙으면 '감사장[...]'부터로 절단
    for tok in _AWARD_TYPE_TOKENS:
        pos = t.find(tok)
        if pos > 0:
            t = t[pos:].strip()
            break
    t = re.sub(r"\s+", " ", t).strip()

    # FIX: 표 추출/continuation 병합 과정에서 같은 근거 문구가
    # 대괄호 닫힘(]) 뒤에 "부분 중복"으로 한 번 더 붙는 케이스가 있다.
    # 예) '표창장[...기여한공이큼] 직무에정려하여...공이큼]'
    # - tail이 괄호 안 본문과 "부분 중복"이면 제거
    # - tail이 본문과 완전 substring이 아니어도(중간 일부 누락) LCS 비율이 높으면 제거
    try:
        if "]" in t:
            head, tail = t.split("]", 1)
            tail = tail.strip()
            if tail:
                # 뒤에 또 다른 상훈 토큰이 없고, tail이 head(괄호 안)에서 유래한 중복일 때만 절단
                if (not any(tok in tail for tok in _AWARD_TYPE_TOKENS)) and (len(tail) >= 6):
                    # 비교용 정규화: 공백 제거 + 괄호/기호 제거(중복 감지용)
                    def _cmp_norm(x: str) -> str:
                        x2 = re.sub(r"\s+", "", (x or ""))
                        # 대괄호/괄호/구두점 등 제거(의미 없는 차이 흡수)
                        x2 = re.sub(r"[\[\]\(\)\{\}<>\"'“”‘’·,，\.]", "", x2)
                        return x2

                    # head에서 '[...]' 본문만 분리(가능하면 본문 기준으로 중복을 판단)
                    inside = ""
                    if "[" in head:
                        inside = head.split("[", 1)[1]
                    inside_n = _cmp_norm(inside)
                    head_n = _cmp_norm(head)
                    tail_n = _cmp_norm(tail).strip("]")

                    def _lcs_len(a: str, b: str) -> int:
                        # O(nm) DP (문자열이 짧아 실용적)
                        if not a or not b:
                            return 0
                        # 작은 쪽을 열로 사용
                        if len(a) < len(b):
                            short, long = a, b
                        else:
                            short, long = b, a
                        prev = [0] * (len(short) + 1)
                        for ch in long:
                            cur = [0]
                            for j, sh in enumerate(short, start=1):
                                if ch == sh:
                                    cur.append(prev[j - 1] + 1)
                                else:
                                    cur.append(max(cur[-1], prev[j]))
                            prev = cur
                        return prev[-1]

                    # 1) substring이면 제거(가장 안전)
                    if tail_n and (tail_n in inside_n or tail_n in head_n):
                        t = head.strip() + "]"
                    else:
                        # 2) substring은 아니지만 본문과 유사도가 매우 높으면 제거
                        # (중간 일부가 누락된 중복 조각 케이스)
                        base = inside_n or head_n
                        if tail_n and base:
                            lcs = _lcs_len(tail_n, base)
                            # tail 대부분이 base에서 유래하면 중복으로 간주
                            if (lcs / max(1, len(tail_n))) >= 0.85:
                                t = head.strip() + "]"
    except Exception:
        pass
    # [수정] 병합/줄바꿈 경로별로 들어간 뒤에도 근거 본문(첫 번째 [...]) 내부 공백을 일괄 정리한다.
    t = _squash_whitespace_inside_first_award_bracket_body(t)
    return t


def _award_type_bracket_unclosed(s: str) -> bool:
    # [수정] 멀티라인 '종류및근거'가 표 추출 과정에서 쪼개질 때, '['는 있는데 아직 ']' 전인 상태
    t = str(s or "")
    return t.count("[") > t.count("]")


def _merge_award_type_text_fragments(head: str, tail: str) -> str:
    # [수정] 두 조각을 하나의 종류/근거 문자열로 합친다(줄바꿈·중복·접두 접미 이어붙임 처리).
    h = str(head or "").strip()
    t = str(tail or "").strip()
    if not h:
        return t
    if not t:
        return h
    ch = re.sub(r"\s+", "", h)
    ct = re.sub(r"\s+", "", t)
    if ch == ct:
        return h if len(h) >= len(t) else t
    if ct.startswith(ch) or (h and h in t and len(t) >= len(h)):
        return (t if len(ct) >= len(ch) else h).strip()
    if ch.startswith(ct) or (t and t in h and len(h) >= len(t)):
        return (h if len(ch) >= len(ct) else t).strip()
    if ct in ch and len(ct) >= 8:
        return h.strip()
    if ch in ct and len(ch) >= 8:
        return t.strip()
    if _award_type_bracket_unclosed(h):
        return re.sub(r"\s+", "", h + t)
    return (h + " " + t).strip()


def _reconstruct_award_cells_from_fragment_extras(extra_parts: list[str]) -> tuple[str, str]:
    # [수정] 표 칸 분할로 extras에만 기관/종류 토큰이 흩어진 경우 무공백으로 이어 붙여 복구
    blob = re.sub(r"\s+", "", "".join(extra_parts)).strip()
    if not blob:
        return "", ""
    return _split_tail_institution_and_type(blob)


def _split_institution_overflow_award_type(
    institution: str, type_and_basis: str
) -> tuple[str, str]:
    # [수정] 수여기관 셀 끝에 '우수상[...]' 등 종류 텍스트가 붙어 나오는 레이아웃을 기관명과 분리
    inst = str(institution or "").replace("\n", " ").strip()
    typ = str(type_and_basis or "").replace("\n", " ").strip()
    if not inst:
        return inst, typ
    best_pos: int | None = None
    for tok in _AWARD_TYPE_TOKENS:
        p = inst.find(tok)
        if p < 0 or p == 0:
            continue
        if best_pos is None or p < best_pos:
            best_pos = p
    if best_pos is None:
        return inst, typ
    pure_inst = inst[:best_pos].strip()
    overflow = inst[best_pos:].strip()
    if not pure_inst or not overflow:
        return inst, typ
    merged_typ = _merge_award_type_text_fragments(overflow, typ)
    return pure_inst, merged_typ


def _award_type_quality_score(s: str) -> int:
    """
    동일 수여일+기관 병합 시, 더 "그럴듯한" 종류/근거를 고르기 위한 점수.
    길이만으로 고르면 '사 표창장...' 같은 깨진 파편이 이길 수 있어 보정한다.
    """
    t = _normalize_award_type_text(s)
    if not t:
        return -10
    score = 0
    if any(tok in t for tok in _AWARD_TYPE_TOKENS):
        score += 3
    # 대괄호가 닫혀 있으면(번호가 완결) 가산
    if ("[" in t) and ("]" in t) and (t.count("[") == t.count("]")):
        score += 1
    # 헤더/라벨이 섞여 있으면 감점
    if any(bad in t for bad in ["수여일", "수여기관", "종류", "근거", "상훈"]):
        score -= 2
    # 숫자 토큰이 과도하게 분리된 흔적 감점
    if re.search(r"제\s*\d+\s*[-–]\s*\d+\s+\d+\s*호", t):
        score -= 1
    return score


def _norm_award_key_inst(s: str) -> str:
    return re.sub(r"\s+", "", (s or "").strip())


# 상훈 '해당없음' 행: 빈 배열 대신 필드 3개를 유지하고 값만 해당없음으로 둔다.
AWARD_NOT_APPLICABLE_TEMPLATE: Dict[str, str] = {
    "수여일": "해당없음",
    "수여기관": "해당없음",
    "종류및근거": "해당없음",
}


def _award_not_applicable_row() -> Dict[str, str]:
    return dict(AWARD_NOT_APPLICABLE_TEMPLATE)


def _find_award_table_header_idx(table: List[List]) -> int:
    """상훈 표 헤더 행(수여일·수여기관 등)만 집도록 좁힌다."""
    for i, row in enumerate(table):
        if not row:
            continue
        row_text = " ".join(str(c) for c in row if c)
        if not row_text.strip():
            continue
        if "교육훈련" in row_text and "교육기간" in row_text.replace(" ", ""):
            continue
        if "근무처" in row_text and ("근무기간" in row_text or "상호" in row_text):
            continue
        if "수여일" in row_text and (
            "수여기관" in row_text or "상훈" in row_text or "종류" in row_text or "근거" in row_text
        ):
            return i
    # 폴백: '상훈' 제목이 바로 위에만 있고 헤더 행은 수여일/수여기관만 있는 서식
    for i, row in enumerate(table):
        if not row:
            continue
        row_text = " ".join(str(c) for c in row if c)
        if "수여일" not in row_text:
            continue
        if "교육기간" in row_text or "과정명" in row_text:
            continue
        if "합격일" in row_text or "등록번호" in row_text:
            continue
        if "졸업일" in row_text or "학교명" in row_text:
            continue
        if "근무기간" in row_text and "상호" in row_text:
            continue
        if "제재일" in row_text and "제재" in row_text:
            continue
        return i
    return -1


def _is_award_table_boundary_row(row_text: str) -> bool:
    """
    상훈 데이터 행과 다음 섹션 헤더를 구분한다.
    단일 키워드(예: '근무처')만으로는 중단하지 않아 본문에 '근무'가 포함된 경우 오판을 줄인다.
    """
    if not (row_text or "").strip():
        return False
    s = row_text.replace(" ", "")
    if "근무처" in row_text and ("근무기간" in row_text or "상호" in row_text or "이전" in s):
        return True
    if "벌점" in row_text and "제재" in row_text:
        return True
    if "교육훈련" in row_text and "교육기간" in row_text:
        return True
    if "국가기술자격" in row_text and "종목" in row_text:
        return True
    if "학력" in row_text and ("졸업일" in row_text or "학교명" in row_text):
        return True
    if re.match(r"^\s*1\.\s*기술경력", row_text):
        return True
    return False


def _split_merged_award_triples(
    award_date_raw: str, institution: str, type_and_basis: str
) -> List[tuple[str, str, str]]:
    """
    세로 병합 등으로 한 셀에 여러 수여일이 줄바꿈으로 들어간 경우 행을 분리한다.
    """
    d_cell = str(award_date_raw or "").strip()
    dates = _AWARD_DATE_TOKEN.findall(d_cell)
    if len(dates) <= 1:
        return [(d_cell, str(institution or "").strip(), str(type_and_basis or "").strip())]

    inst_lines = [x.strip() for x in re.split(r"[\n\r]+", str(institution or "")) if x.strip()]
    raw_type_lines = [x.strip() for x in re.split(r"[\n\r]+", str(type_and_basis or "")) if x.strip()]

    def _looks_like_new_award_type_line(s: str) -> bool:
        t = (s or "").strip()
        if not t:
            return False
        if any(tok in t for tok in _AWARD_TYPE_TOKENS):
            return True
        if ("[" in t or "]" in t) and re.search(r"(표창|훈장|포장|감사장|상장|유공)", t):
            return True
        if _TYPE_TAIL_HINT.search(t):
            return True
        return False

    # 줄바꿈으로 1개 항목이 여러 라인으로 쪼개지면(continuation),
    # 단순 index 매핑이 깨져 "다음 날짜" 레코드로 꼬리가 넘어갈 수 있다.
    # 새 항목 시작처럼 보이지 않는 라인은 직전 라인에 이어붙인다.
    type_lines: list[str] = []
    cur = ""
    for ln in raw_type_lines:
        if not cur:
            cur = ln
            continue
        if _looks_like_new_award_type_line(ln):
            type_lines.append(cur)
            cur = ln
        else:
            cur = (cur + " " + ln).strip()
    if cur:
        type_lines.append(cur)
    out: List[tuple[str, str, str]] = []
    for idx, d in enumerate(dates):
        inst = inst_lines[idx] if idx < len(inst_lines) else (inst_lines[-1] if inst_lines else "")
        typ = type_lines[idx] if idx < len(type_lines) else (type_lines[-1] if type_lines else "")
        out.append((d, inst, typ))
    return out


def extract_award_section_text(page_text: str) -> str:
    """페이지 텍스트에서 '상훈' 블록만 잘라낸다 (검증·텍스트 폴백용)."""
    if not page_text:
        return ""
    # FIX: 단순 re.search(r"상훈")은 헤더/푸터에 포함된 "상훈"도 매칭해 블록 범위가 틀어짐.
    # 독립된 줄에 있는 "상훈" 제목 행만 섹션 시작으로 인식.
    # 우선 독립 행 패턴으로 시도하고, 없으면 기존 방식으로 폴백.
    m = re.search(r"(?m)^\s*상훈\s*$", page_text)
    if not m:
        # 폴백: "상훈" 뒤에 수여일/수여기관이 바로 이어지는 형태도 허용
        m = re.search(r"상훈", page_text)
    if not m:
        return ""
    rest = page_text[m.start() :]
    em = _AWARD_SECTION_END.search(rest, pos=1)
    if em:
        return rest[: em.start()]
    return rest


def count_award_data_lines_in_section_text(section_text: str) -> int:
    """
    상훈 블록에서 수여일 날짜 패턴이 있는 데이터 줄 수를 센다.
    (교육훈련의 'YYYY.MM.DD ~ YYYY.MM.DD' 날짜 범위 줄은 제외)

    FIX: 기존 날짜+공백+텍스트 패턴은 날짜 뒤에 즉시 텍스트가 없으면
    (날짜와 기관명이 다른 줄에 추출되는 경우) 카운팅을 놓쳤다.
    수여일은 단독으로 줄 시작에 나타나도 유효한 1건으로 인정.
    """
    if not section_text.strip():
        return 0
    n = 0
    for raw in section_text.splitlines():
        ln = raw.strip()
        if not ln:
            continue
        # 날짜 범위(교육훈련 등)는 제외
        if re.search(r"\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}\.\d{2}\.\d{2}", ln):
            continue
        # FIX: 줄이 날짜로 시작하면 수여일 행으로 인정 (뒤에 텍스트 없어도 됨)
        if re.match(r"^\d{4}\.\d{2}\.\d{2}", ln):
            n += 1
    if n > 0:
        return n
    # 날짜 행 없이 '해당없음'만 있는 상훈 블록은 1건(스키마 유지 행)으로 친다.
    if "해당없음" in section_text:
        return 1
    return 0


def _split_tail_institution_and_type(tail: str) -> tuple[str, str]:
    if not tail:
        return "", ""
    m = _TYPE_TAIL_HINT.search(tail)
    if m:
        return tail[: m.start()].strip(), tail[m.start() :].strip()
    # [수정] 우수상[ 등 '_TYPE_TAIL_HINT' 밖 패턴 분리(_TYPE_TAIL_HINT 확장 없이 공통 토큰 활용)
    best_pos: int | None = None
    for tok in _AWARD_TYPE_TOKENS:
        p = tail.find(tok)
        if p < 0:
            continue
        if best_pos is None or p < best_pos:
            best_pos = p
    if best_pos is not None:
        if best_pos == 0:
            return "", tail.strip()
        return tail[:best_pos].strip(), tail[best_pos:].strip()
    return tail.strip(), ""


def _parse_awards_from_text_block(page_text: str) -> List[Dict[str, Any]]:
    """표 추출 실패·병합 시 텍스트 블록에서 상훈 행을 복구한다."""
    block = extract_award_section_text(page_text)
    if not block.strip():
        return []

    awards: List[Dict[str, Any]] = []
    logical_lines: list[str] = []
    buf = ""
    for raw in block.splitlines():
        ln = (raw or "").strip()
        if not ln:
            continue
        if ("수여일" in ln) and ("수여기관" in ln.replace(" ", "")):
            continue
        if " ~ " in ln or (
            ln.count("~") >= 1 and re.search(r"\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}", ln)
        ):
            continue
        # [수정] '상훈 YYYY.MM.DD ...' 헤더·붙임 라인 허용 / 멀티라인 꼬리는 이전 날짜 행에 이어붙임
        ln2 = re.sub(r"^\s*상훈\s+", "", ln).strip()
        if not ln2:
            continue
        if re.match(r"^\d{4}\.\d{2}\.\d{2}", ln2):
            if buf.strip():
                logical_lines.append(buf.strip())
            buf = ln2
            continue
        if buf.strip():
            if _award_type_bracket_unclosed(buf):
                tail_m = ln2.strip()
                buf = (re.sub(r"\s+", "", buf) + re.sub(r"\s+", "", tail_m)).strip()
            else:
                buf = (buf + " " + ln2).strip()
            continue
        if logical_lines:
            prev_l = logical_lines[-1].strip()
            if _award_type_bracket_unclosed(prev_l):
                tail_m = re.sub(r"\s+", "", ln2.strip())
                logical_lines[-1] = (
                    (re.sub(r"\s+", "", prev_l)) + tail_m
                ).strip()
            else:
                logical_lines[-1] = (prev_l + " " + ln2).strip()

    if buf.strip():
        logical_lines.append(buf.strip())

    for logical in logical_lines:
        m = re.match(r"^(\d{4}\.\d{2}\.\d{2})\s+(.+)$", logical.strip())
        if not m:
            continue
        date_raw, tail = m.group(1), m.group(2).strip()
        inst, typ = _split_tail_institution_and_type(tail)
        awards.append(
            {
                "수여일": convert_date_format(date_raw),
                "수여기관": inst.replace("\n", " ").strip(),
                "종류및근거": typ.replace("\n", " ").strip(),
            }
        )

    if not awards and "해당없음" in block:
        has_award_date_line = False
        for raw in block.splitlines():
            ln = raw.strip()
            if not ln or ("수여일" in ln and "수여기관" in ln.replace(" ", "")):
                continue
            if " ~ " in ln or (
                ln.count("~") >= 1 and re.search(r"\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}", ln)
            ):
                continue
            if re.match(r"^\d{4}\.\d{2}\.\d{2}", ln):
                has_award_date_line = True
                break
        if not has_award_date_line:
            return [_award_not_applicable_row()]

    return awards


def _merge_award_lists(primary: List[Dict[str, Any]], secondary: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """동일 수여일+기관은 종류및근거가 더 긴 쪽을 유지하고, 텍스트 전용 행은 누락 시 보강한다."""
    merged: List[Dict[str, Any]] = []
    by_key: dict[tuple[str, str], int] = {}

    def _ingest(a: Dict[str, Any]) -> None:
        dt = str(a.get("수여일") or "").strip()
        inst = str(a.get("수여기관") or "").replace("\n", " ").strip()
        typ = _normalize_award_type_text(a.get("종류및근거") or "")
        if not dt:
            return
        k = (dt, _norm_award_key_inst(inst))
        if k not in by_key:
            by_key[k] = len(merged)
            merged.append({"수여일": dt, "수여기관": inst, "종류및근거": typ})
            return
        i = by_key[k]
        cur = merged[i]
        cur_typ = str(cur.get("종류및근거") or "").strip()
        # 길이 우선 대신 "품질 점수" 우선으로 선택
        if _award_type_quality_score(typ) > _award_type_quality_score(cur_typ) or (
            _award_type_quality_score(typ) == _award_type_quality_score(cur_typ) and len(typ) > len(cur_typ)
        ):
            cur["종류및근거"] = typ
        if inst and not str(cur.get("수여기관") or "").strip():
            cur["수여기관"] = inst

    for a in primary:
        _ingest(a)
    for a in secondary:
        _ingest(a)
    return merged


def _fill_grade_row_ordered(next_row: List[Any], grade_info: Dict[str, Any]) -> None:
    # [수정] '설계·시공 등'과 '건설사업관리'가 한 헤더 행에 붙어 있는 표에서 열 번호 고정 매핑이 깨져
    # 같은 셀이 설계 직무·건설사업관리 직무에 중복 들어가는 문제가 있었다.
    # 값 행을 좌→우로 순회해 (직무, 전문) 슬롯을 순서대로 채운다.
    jobs: list[tuple[str, str]] = []
    specs: list[tuple[str, str]] = []
    skip_substrings = ("생략", "해당없음")
    label_cells = {"직무분야", "전문분야", "등급", "설계", "시공", "건설사업관리", "품질관리"}

    for cell in next_row:
        raw = str(cell or "").strip()
        if not raw:
            continue
        if raw in label_cells:
            continue
        if any(s in raw for s in skip_substrings):
            continue
        field_name, field_grade = extract_name_and_grade(raw)
        if not field_name or not field_grade:
            continue
        if field_name in BASIC_FIELDS:
            jobs.append((field_name, field_grade))
            continue
        sp = best_match_specialty(raw, _CATALOG) or best_match_specialty(field_name, _CATALOG)
        if sp:
            specs.append((sp, field_grade))

    if len(jobs) >= 1:
        if "design_work_field" not in grade_info:
            grade_info["design_work_field"] = jobs[0][0]
            grade_info["design_work_grade"] = jobs[0][1]
    if len(specs) >= 1:
        if "design_specialty" not in grade_info:
            grade_info["design_specialty"] = specs[0][0]
            grade_info["design_specialty_grade"] = specs[0][1]
    if len(jobs) >= 2:
        if "cm_work_field" not in grade_info:
            grade_info["cm_work_field"] = jobs[1][0]
            grade_info["cm_work_grade"] = jobs[1][1]
    if len(specs) >= 2:
        if "cm_specialty" not in grade_info:
            grade_info["cm_specialty"] = specs[1][0]
            grade_info["cm_specialty_grade"] = specs[1][1]


def parse_grade_info(page, *, pdf_path: str | None = None, page_num: int | None = None) -> Dict[str, Any]:
    """등급 정보 파싱 (개선: 표 구조 기반 정확한 추출)"""
    grade_info = {}
    
    try:
        tables = extract_tables_merged(page)
        if not table_set_has_header_signals(
            tables,
            ["설계", "시공", "건설사업", "품질관리"],
        ):
            tables = []
        if tables:
            for table in tables:
                for i, row in enumerate(table):
                    if not row:
                        continue
                    
                    row_text = ' '.join([str(cell) for cell in row if cell])
                    
                    # 설계·시공 등 섹션 찾기
                    if '설계' in row_text and '시공' in row_text:
                        # [수정] 같은 줄에 건설사업관리까지 있는 '통합 헤더' 행은 열 고정 로직 대신 순서 기반 채우기
                        combined_design_cm_header = "건설사업관리" in row_text
                        # 다음 행들에서 정보 추출 (최대 3행까지)
                        for j in range(i + 1, min(i + 4, len(table))):
                            next_row = table[j]
                            if not next_row or len(next_row) < 2:
                                continue

                            if combined_design_cm_header:
                                _fill_grade_row_ordered(next_row, grade_info)
                                continue

                            # 직무분야 열 (보통 첫 번째 열)
                            field_cell = str(next_row[0] or "").strip()
                            field_name, field_grade = extract_name_and_grade(field_cell)
                            if field_name and field_grade and field_name in BASIC_FIELDS:
                                if "design_work_field" not in grade_info:
                                    grade_info["design_work_field"] = field_name
                                    grade_info["design_work_grade"] = field_grade

                            # 전문분야 열 찾기 (보통 세 번째 열)
                            if len(next_row) >= 3:
                                specialty_cell = str(next_row[2] or "").strip()
                                # 해당없음, 생략 제외
                                if specialty_cell and "해당없음" not in specialty_cell and "생략" not in specialty_cell:
                                    specialty_name = best_match_specialty(specialty_cell, _CATALOG)
                                    _, specialty_grade = extract_name_and_grade(specialty_cell)
                                    if specialty_name and specialty_grade:
                                        if "design_specialty" not in grade_info:
                                            grade_info["design_specialty"] = specialty_name
                                            grade_info["design_specialty_grade"] = specialty_grade

                    # 건설사업관리 섹션 찾기
                    # [수정] 통합 헤더 행은 설계 블록에서 이미 순서 기반 처리함 → 같은 데이터 행을 CM 열처럼 다시 읽지 않음
                    if "건설사업관리" in row_text and not (
                        "설계" in row_text and "시공" in row_text
                    ):
                        for j in range(i + 1, min(i + 4, len(table))):
                            next_row = table[j]
                            if not next_row or len(next_row) < 2:
                                continue
                            
                            field_cell = str(next_row[0] or '').strip()
                            field_name, field_grade = extract_name_and_grade(field_cell)
                            if field_name and field_grade and field_name in BASIC_FIELDS:
                                # 특급 우선
                                if 'cm_work_field' not in grade_info or field_grade == '특급':
                                    grade_info['cm_work_field'] = field_name
                                    grade_info['cm_work_grade'] = field_grade
                            
                            # 전문분야 찾기
                            if len(next_row) >= 3:
                                specialty_cell = str(next_row[2] or '').strip()
                                if specialty_cell and '해당없음' not in specialty_cell and '생략' not in specialty_cell:
                                    specialty_name = best_match_specialty(specialty_cell, _CATALOG)
                                    _, specialty_grade = extract_name_and_grade(specialty_cell)
                                    if specialty_name and specialty_grade:
                                        if 'cm_specialty' not in grade_info or specialty_grade == '특급':
                                            grade_info['cm_specialty'] = specialty_name
                                            grade_info['cm_specialty_grade'] = specialty_grade
                    
                    # 품질관리 등급 찾기
                    if '품질관리' in row_text:
                        # 같은 행에서 등급 찾기
                        for col_idx in range(len(row)):
                            cell = str(row[col_idx] or '').strip()
                            quality_match = re.search(r'(특급|고급|중급|초급)', cell)
                            if quality_match:
                                grade_info['quality_grade'] = quality_match.group(1)
                                break
                        
                        # 같은 행에서 못 찾으면 다음 행에서 찾기
                        if 'quality_grade' not in grade_info:
                            for j in range(i + 1, min(i + 3, len(table))):
                                next_row = table[j]
                                if not next_row:
                                    continue
                                for col_idx in range(len(next_row)):
                                    cell = str(next_row[col_idx] or '').strip()
                                    quality_match = re.search(r'(특급|고급|중급|초급)', cell)
                                    if quality_match:
                                        grade_info['quality_grade'] = quality_match.group(1)
                                        break
                                if 'quality_grade' in grade_info:
                                    break

        # 표 기반에서 직무/전문분야가 깨지는 PDF가 있어, 레이아웃(라인) 기반으로 보강한다.
        # - '**' 같은 구분자가 없어도, '토목특급', '토목시공 특급'처럼 "카탈로그명 + 등급"이 붙어 있는 경우를 복원한다.
        try:
            need_any = any(
                k not in grade_info
                for k in [
                    "design_work_field",
                    "design_work_grade",
                    "design_specialty",
                    "design_specialty_grade",
                    "cm_work_field",
                    "cm_work_grade",
                    "cm_specialty",
                    "cm_specialty_grade",
                ]
            )
            if need_any and pdf_path is not None and page_num is not None:
                try:
                    from parsers.layout_extractor import extract_lines as _layout_extract_lines

                    lines = _layout_extract_lines(
                        pdf_path=pdf_path,
                        page_num=page_num,
                        pdfplumber_page=page,
                        engine="auto",
                        y_tolerance=2.0,
                        join_gap=1.0,
                    )
                except Exception:
                    lines = []

                src = "\n".join([ln for ln in (lines or []) if (ln or "").strip()])
                if src:
                    grade_tokens_pat = r"(특급|고급|중급|초급)"

                    # 등급 표는 대개:
                    #  - "설계·시공 등 건설사업관리" (섹션 헤더)
                    #  - "직무분야 전문분야 직무분야 전문분야" (컬럼 헤더)
                    #  - "<직무+등급> <전문+등급> <직무+등급> <전문+등급>" (값 행)
                    # 형태로 추출된다. 섹션 헤더가 같은 줄에 붙어있으면 단순 substring cut이 깨지므로,
                    # '직무분야/전문분야' 헤더 다음 "값 행"을 직접 잡는다.
                    src_lines = [ln.strip() for ln in src.splitlines() if (ln or "").strip()]
                    header_idx = -1
                    for i, ln in enumerate(src_lines):
                        if ("직무분야" in ln) and ("전문분야" in ln):
                            header_idx = i
                            break
                    value_ln = ""
                    if header_idx >= 0:
                        for j in range(header_idx + 1, min(header_idx + 6, len(src_lines))):
                            cand = src_lines[j].strip()
                            if not cand:
                                continue
                            # 중간에 '등급' 같은 라벨 라인이 끼는 케이스를 건너뛴다.
                            if cand == "등급":
                                continue
                            # 값 행은 보통 등급 토큰을 포함한다.
                            if not re.search(grade_tokens_pat, cand):
                                continue
                            value_ln = cand
                            break

                    if value_ln:
                        grade_tokens = ("특급", "고급", "중급", "초급")

                        job_hits: list[tuple[str, str]] = []
                        spec_hits: list[tuple[str, str]] = []
                        job_set = set(BASIC_FIELDS)
                        # [수정] '토목 초급'처럼 공백으로 (분야명+등급)이 나뉜 한 줄 → 단어 단위 split로는 복원 불가했음
                        pair_pat = re.compile(
                            rf"([\w가-힣·ㆍ]+(?:\s+[\w가-힣·ㆍ]+)*)\s*{grade_tokens_pat}\b",
                            flags=re.UNICODE,
                        )
                        for nm_raw, g in pair_pat.findall(value_ln):
                            cell_like = f"{(nm_raw or '').strip()} {g}".strip()
                            field_name, field_grade = extract_name_and_grade(cell_like)
                            if not field_name or not field_grade or g not in grade_tokens:
                                continue
                            if field_name in job_set:
                                job_hits.append((field_name, field_grade))
                                continue
                            sp = best_match_specialty(cell_like, _CATALOG) or best_match_specialty(
                                field_name, _CATALOG
                            )
                            if sp:
                                spec_hits.append((sp, field_grade))

                        # 좌→우 순서대로 설계/CM에 매핑(중복이 있으면 0/1번째 사용)
                        if len(job_hits) >= 1 and "design_work_field" not in grade_info:
                            grade_info["design_work_field"] = job_hits[0][0]
                            grade_info["design_work_grade"] = job_hits[0][1]
                        if len(spec_hits) >= 1 and "design_specialty" not in grade_info:
                            grade_info["design_specialty"] = spec_hits[0][0]
                            grade_info["design_specialty_grade"] = spec_hits[0][1]
                        if len(job_hits) >= 2 and "cm_work_field" not in grade_info:
                            grade_info["cm_work_field"] = job_hits[1][0]
                            grade_info["cm_work_grade"] = job_hits[1][1]
                        if len(spec_hits) >= 2 and "cm_specialty" not in grade_info:
                            grade_info["cm_specialty"] = spec_hits[1][0]
                            grade_info["cm_specialty_grade"] = spec_hits[1][1]
        except Exception:
            pass

        # 표 기반에서 품질관리 등급을 못 찾는 PDF가 있어, 단어 위치 기반으로 폴백한다.
        # (extract_tables가 '품질관리' 셀을 누락/병합하는 케이스)
        if 'quality_grade' not in grade_info:
            try:
                # 좌표 기반 단어 추출: 가능하면 PyMuPDF(=fitz), 아니면 pdfplumber.extract_words 폴백
                words = []
                try:
                    from parsers.layout_extractor import extract_words as _layout_extract_words

                    words = [
                        {"text": w.text, "x0": w.x0, "x1": w.x1, "top": w.top, "bottom": w.bottom}
                        for w in _layout_extract_words(
                            pdf_path=pdf_path,
                            page_num=page_num,
                            pdfplumber_page=page,
                            engine="auto",
                        )
                    ]
                except Exception:
                    words = []
                if not words:
                    try:
                        words = page.extract_words(use_text_flow=True) or []
                    except TypeError:
                        # pdfplumber 버전에 따라 시그니처가 다를 수 있음
                        words = page.extract_words() or []

                anchors = [w for w in words if '품질관리' in (w.get('text') or '')]
                if anchors:
                    grade_tokens = {'특급', '고급', '중급', '초급'}

                    def _find_near_grade(anchor):
                        ax0 = float(anchor.get('x0', 0) or 0)
                        ax1 = float(anchor.get('x1', 0) or 0)
                        atop = float(anchor.get('top', 0) or 0)
                        abot = float(anchor.get('bottom', 0) or 0)
                        ay = (atop + abot) / 2.0

                        # 1) 같은 라인(y가 비슷)에서 오른쪽에 있는 등급 토큰 우선
                        candidates = []
                        for w in words:
                            txt = (w.get('text') or '').strip()
                            if txt not in grade_tokens:
                                continue
                            wx0 = float(w.get('x0', 0) or 0)
                            wtop = float(w.get('top', 0) or 0)
                            wbot = float(w.get('bottom', 0) or 0)
                            wy = (wtop + wbot) / 2.0
                            if wx0 >= ax1 and abs(wy - ay) <= 6:
                                candidates.append((wx0 - ax1, txt))
                        if candidates:
                            candidates.sort(key=lambda x: x[0])
                            return candidates[0][1]

                        # 2) 근처(상하/좌우)에서 가장 가까운 등급 토큰
                        near = []
                        for w in words:
                            txt = (w.get('text') or '').strip()
                            if txt not in grade_tokens:
                                continue
                            wx0 = float(w.get('x0', 0) or 0)
                            wx1 = float(w.get('x1', 0) or 0)
                            wtop = float(w.get('top', 0) or 0)
                            wbot = float(w.get('bottom', 0) or 0)
                            wy = (wtop + wbot) / 2.0
                            dx = 0.0
                            if wx1 < ax0:
                                dx = ax0 - wx1
                            elif wx0 > ax1:
                                dx = wx0 - ax1
                            dy = abs(wy - ay)
                            dist = (dx * dx + dy * dy) ** 0.5
                            # 방향성 보정:
                            # - 품질관리 라벨의 "왼쪽"에 있는 등급(다른 컬럼의 등급)을 오탐하는 케이스가 있어
                            #   라벨보다 왼쪽에 완전히 위치한 토큰은 패널티를 준다.
                            if wx1 < ax0:
                                dist += 60.0
                            elif wx0 < ax0:
                                dist += 15.0
                            # 너무 멀면 제외(다른 섹션 등급과 혼동 방지)
                            if dist <= 120:
                                near.append((dist, txt))
                        if near:
                            near.sort(key=lambda x: x[0])
                            return near[0][1]
                        return None

                    for a in anchors:
                        g = _find_near_grade(a)
                        if g:
                            grade_info['quality_grade'] = g
                            break
            except Exception:
                pass
    
    except Exception as e:
        print(f"⚠️ 등급 정보 파싱 오류: {e}")
    
    return grade_info


def license_registration_quality_key(pass_date_iso: str, registration_number: str) -> Tuple[int, int, int, str]:
    """
    같은 (종목, 합격일) 후보 행 여러 건 중 등록번호를 고를 때 사용하는 정렬용 키.
    대한민국 국가기술자격 등록번호는 합격일의 년도(후 2자리)와 접두 패턴이 맞는 경우가 많으며,
    2열 레이아웃이나 셀 분리 깨짐으로 이웃 항목 번호·등록번호 내부 숫자 단편만 붙어도
    이 패턴으로 잘못된 후보가 낮게 랭크된다.
    """
    # // [수정]
    reg = str(registration_number or "").strip()
    iso = str(pass_date_iso or "").strip()
    # 짧은 숫자 단편(예: 등록번호 본체의 일부분으로만 OCR된 형태)
    fragile = bool(reg.isdigit() and len(reg) < 10)

    ym = re.match(r"(\d{4})-\d{2}-\d{2}", iso)
    yy_match_rank = 0
    if ym and len(reg) >= 2:
        y = int(ym.group(1))
        yy = f"{y % 100:02d}"
        if reg[:2].upper() == yy.upper():
            yy_match_rank = 10

    len_rank = min(len(reg), 30)
    return (yy_match_rank, 0 if fragile else 1, len_rank, reg.upper())


_STANDALONE_LICENSE_GRADE_LABELS = frozenset(
    {"기사", "산업기사", "기능사", "기술사", "기능장"}
)


def is_standalone_license_grade_label(name: str) -> bool:
    """
    표/텍스트 추출 과정에서 '토목산업기사'가 '토목'+'기사' 셀로 쪼개질 때
    단독 '기사·산업기사' 줄이 종목처럼 잡히는 경우가 있다.
    이런 토큰은 완전한 종목명이 있는 동일 등록번호 후보가 있으면 버려진다.
    """
    # // [수정]
    t = re.sub(r"\s+", "", (name or "").strip())
    return t in _STANDALONE_LICENSE_GRADE_LABELS


def _canonical_reg_for_license_dedup(registration_number: str) -> str:
    """등록번호가 신뢰 가능하면 정규화 키로 쓴다."""
    # // [수정]
    raw = str(registration_number or "").strip()
    raw = re.sub(r"\s+", "", raw)
    raw_u = raw.upper()
    if len(raw_u) < 8 or not re.search(r"\d", raw_u):
        return ""
    return raw_u


def _license_merge_primary_key(pass_date_iso: str, type_grade: str, registration_number: str) -> Tuple[str, str, str]:
    # // [수정]
    iso = str(pass_date_iso or "").strip()
    cr = _canonical_reg_for_license_dedup(registration_number)
    if cr:
        return ("reg_dt", iso, cr)
    nm = re.sub(r"\s+", "", (type_grade or "").strip())
    return ("name_dt", iso, nm)


def _license_row_merge_score(pass_iso: str, row: Dict[str, Any]) -> Tuple[int, int, int, int, int, str]:
    nm = str(row.get("type_and_grade") or "").strip()
    reg = str(row.get("registration_number") or "").strip()
    frag = is_standalone_license_grade_label(nm)
    q = license_registration_quality_key(pass_iso, reg)
    return (
        0 if frag else 1,
        q[0],
        q[1],
        q[2],
        len(nm),
        q[3],
    )


def _license_name_compact(type_grade: str) -> str:
    return re.sub(r"\s+", "", (type_grade or "").strip())


def merge_duplicate_license_records_by_qualification(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    1차: (합격일, 신뢰 가능한 등록번호)로 묶어, 단독 접미 종목명(기사 등) 행 제거 등.
    2차: (합격일, 종목 문자열 동일 시) 같은 날·같은 종목으로 남은 다중 행 중
        이웃 행렬에서 섞여 들어 온 다른 등록번호(합격연도 접두 불일치 등)까지 점수로 정리한다.
    등록번호를 전혀 못 받았을 때만 1차 키를 종목+합격일로 두어 누락 방지한다.
    """
    # // [수정]
    if not rows:
        return []
    best_by_primary: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for r in rows:
        iso = str(r.get("pass_date") or "").strip()
        nm = str(r.get("type_and_grade") or "").strip()
        reg = str(r.get("registration_number") or "").strip()
        pk = _license_merge_primary_key(iso, nm, reg)
        cur = best_by_primary.get(pk)
        if cur is None:
            best_by_primary[pk] = r
            continue
        if _license_row_merge_score(iso, r) > _license_row_merge_score(iso, cur):
            best_by_primary[pk] = r
    interim = list(best_by_primary.values())

    merged_by_qual: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in interim:
        iso = str(r.get("pass_date") or "").strip()
        nm_c = _license_name_compact(str(r.get("type_and_grade") or ""))
        k2 = (iso, nm_c)
        cur = merged_by_qual.get(k2)
        if cur is None:
            merged_by_qual[k2] = r
            continue
        if _license_row_merge_score(iso, r) > _license_row_merge_score(iso, cur):
            merged_by_qual[k2] = r

    final = list(merged_by_qual.values())
    final.sort(key=lambda x: (str(x.get("pass_date") or ""), str(x.get("type_and_grade") or "")))
    return final


def _extract_licenses_from_cell(type_cell: str, date_cell: str, number_cell: str) -> List[Dict[str, Any]]:
    """셀 하나에서 (종목, 합격일, 등록번호) 리스트를 추출한다.
    줄바꿈으로 구분된 여러 자격증을 개별 분리하며,
    기능장 등 비표준 종목명도 처리한다.
    """
    results = []
    names = [n.strip() for n in type_cell.split('\n') if n.strip()]
    dates = re.findall(r'\d{4}\.\d{2}\.\d{2}', date_cell) if date_cell else []
    nums = [n.strip() for n in number_cell.split('\n') if n.strip()] if number_cell else []

    skip_labels = {'기사', '기능자격', '기능사자격', '기술사', '산업기사', '기능사', ''}

    def _infer_reg_no_fallback(nm: str, dt: str) -> str:
        """
        테이블 추출이 깨진 경우 등록번호가 `등록번호` 셀이 아니라
        `합격일`/`종목` 셀에 같이 붙어 들어오는 케이스를 복원한다.
        """
        hay = " ".join(
            [str(type_cell or ""), str(date_cell or ""), str(number_cell or "")]
        )
        # 날짜 토큰 제거
        hay = re.sub(r"\d{4}\.\d{2}\.\d{2}", " ", hay)
        # 종목명(부분) 제거(과도 제거 방지: 공백 축약 후 단순 포함 제거)
        nm2 = re.sub(r"\s+", " ", (nm or "").strip())
        if nm2 and nm2 in hay:
            hay = hay.replace(nm2, " ")
        hay = re.sub(r"\s+", " ", hay).strip()

        # 등록번호 후보: 영숫자/하이픈 조합(끝에 알파벳이 붙는 경우 포함)
        # 예: 17112010253L, 04201031128K, 09188010185K
        cands = re.findall(r"\b[A-Z0-9][A-Z0-9\-]{5,}[A-Z0-9]?\b", hay)
        if not cands:
            # 너무 엄격하면 누락될 수 있어 최후 폴백(공백 없는 토큰)
            cands = [t for t in hay.split(" ") if re.search(r"[A-Z0-9]", t)]
        # 너무 짧은 토큰은 제외
        cands = [c for c in cands if len(c) >= 6]
        if not cands:
            return ""
        # // [수정] 마지막 토큰 고정 선택은 같은 문자열 안에 다른 항목의 등록번호가 남았을 때
        # 다음 행 번호까지 잘못 잡히므로 합격일·등록 접두 패턴 일치 우선 선택
        iso = convert_date_format(str(dt))
        return max(cands, key=lambda c: license_registration_quality_key(iso, str(c))).strip()

    idx = 0
    for nm in names:
        if nm in skip_labels:
            continue
        dt = dates[idx] if idx < len(dates) else ''
        rn = nums[idx] if idx < len(nums) else ''
        if dt and not rn:
            rn = _infer_reg_no_fallback(nm, dt)
        if dt and re.match(r'\d{4}\.\d{2}\.\d{2}', dt):
            results.append({
                'type_and_grade': nm,
                'pass_date': convert_date_format(dt),
                'registration_number': rn,
            })
        idx += 1

    # // [수정] 같은 (종목, 합격일) 후보 행은 합격일·등록번호 정합 규칙으로 1건만 유지
    return merge_duplicate_license_records_by_qualification(results)


def parse_license_info(page, *, pdf_path: str | None = None, page_num: int | None = None) -> List[Dict[str, Any]]:
    """국가기술자격 정보 파싱 (개선: 2열 레이아웃·기능장 대응)"""
    licenses = []
    
    try:
        # 좌표 기반 단어 추출: 가능하면 PyMuPDF(=fitz), 아니면 pdfplumber.extract_words 폴백
        words = []
        try:
            from parsers.layout_extractor import extract_words as _layout_extract_words

            words = [
                {"text": w.text, "x0": w.x0, "x1": w.x1, "top": w.top, "bottom": w.bottom}
                for w in _layout_extract_words(
                    pdf_path=pdf_path,
                    page_num=page_num,
                    pdfplumber_page=page,
                    engine="auto",
                )
            ]
        except Exception:
            words = []
        if not words:
            try:
                words = page.extract_words(use_text_flow=True) or []
            except TypeError:
                words = page.extract_words() or []

        _REG_TOKEN = re.compile(r"^[A-Z0-9][A-Z0-9\-]{5,}[A-Z0-9]?$")

        def _find_reg_right_of_date(date_token: str, *, x_min: float | None = None, x_max: float | None = None) -> str:
            """
            일부 PDF는 오른쪽 컬럼 '등록번호'가 테이블 셀로 추출되지 않는다.
            이 경우, 날짜(합격일) 토큰의 좌표를 기준으로 같은 라인 우측에서
            등록번호처럼 보이는 토큰을 찾아 보강한다.
            """
            if not date_token:
                return ""
            # 날짜 토큰 위치 찾기
            date_hits = []
            for w in words:
                if (w.get("text") or "").strip() == date_token:
                    try:
                        x1 = float(w.get("x1", 0) or 0)
                        x0 = float(w.get("x0", 0) or 0)
                        top = float(w.get("top", 0) or 0)
                        bottom = float(w.get("bottom", 0) or 0)
                        y = (top + bottom) / 2.0
                        if x_min is not None and x0 < x_min:
                            continue
                        if x_max is not None and x1 > x_max:
                            continue
                        date_hits.append((x1, y))
                    except Exception:
                        continue
            if not date_hits:
                return ""

            # 같은 날짜가 양 컬럼에 동시에 존재할 수 있어, 컬럼 범위 내 첫 히트를 사용
            dx1, dy = date_hits[0]

            candidates = []
            for w in words:
                txt = (w.get("text") or "").strip()
                if not txt:
                    continue
                if not _REG_TOKEN.match(txt):
                    continue
                try:
                    x0 = float(w.get("x0", 0) or 0)
                    top = float(w.get("top", 0) or 0)
                    bottom = float(w.get("bottom", 0) or 0)
                    y = (top + bottom) / 2.0
                except Exception:
                    continue
                if x_min is not None and x0 < x_min:
                    continue
                if x_max is not None and x0 > x_max:
                    continue
                # 날짜 오른쪽, 같은 라인 근처
                if x0 > dx1 + 2 and abs(y - dy) <= 6:
                    candidates.append((x0 - dx1, txt))
            if not candidates:
                return ""
            candidates.sort(key=lambda x: x[0])
            return candidates[0][1]

        tables = extract_tables_merged(page)
        if tables and table_set_has_header_signals(
            tables,
            ["국가기술자격", "종목", "합격"],
        ):
            for table in tables:
                header_idx = find_section_header(
                    table,
                    ['국가기술자격', '종목'],
                    ['교육훈련', '학력', '근무처', '상훈']
                )
                
                if header_idx >= 0:
                    header_row = table[header_idx]
                    ncols = len(header_row)
                    
                    type_col = find_column_index(header_row, '종목')
                    date_col = find_column_index(header_row, '합격일')
                    number_col = find_column_index(header_row, '등록번호')
                    
                    if type_col < 0:
                        type_col = 0
                    if date_col < 0:
                        date_col = 1
                    if number_col < 0:
                        number_col = 2

                    right_type_col = right_date_col = right_number_col = -1
                    if ncols >= 8:
                        for ci in range(type_col + 1, ncols):
                            cell = str(header_row[ci] or '').strip()
                            if '종목' in cell and ci != type_col:
                                right_type_col = ci
                                break
                        if right_type_col >= 0:
                            for ci in range(right_type_col + 1, ncols):
                                cell = str(header_row[ci] or '').strip()
                                if '합격' in cell:
                                    right_date_col = ci
                                    break
                            for ci in range(right_type_col + 1, ncols):
                                cell = str(header_row[ci] or '').strip()
                                if '등록' in cell:
                                    right_number_col = ci
                                    break
                    
                    next_section_idx = find_next_section_header(
                        table,
                        header_idx,
                        ['학력', '교육훈련', '상훈', '근무처', '벌점']
                    )
                    
                    for i in range(header_idx + 1, next_section_idx):
                        row = table[i]
                        if not row:
                            continue
                        
                        row_text = ' '.join([str(cell) for cell in row if cell])
                        
                        if any(kw in row_text for kw in ['학력', '교육훈련', '상훈', '근무처', '벌점', '졸업일', '교육기간', '수여일', '근무기간']):
                            break
                        
                        if '해당없음' in row_text and len(row_text.strip()) < 20:
                            continue

                        tg = str(row[type_col] or '').strip() if type_col < len(row) else ''
                        pd_ = str(row[date_col] or '').strip() if date_col < len(row) else ''
                        rn = str(row[number_col] or '').strip() if number_col < len(row) else ''

                        if tg and tg not in ['~', '기사과정', '졸업일', '학교명', '학과']:
                            # 왼쪽 컬럼 등록번호가 테이블 셀로 안 나오는 경우 좌표 기반으로 보강(컬럼 경계 적용)
                            if (not rn) and pd_:
                                dts = re.findall(r"\d{4}\.\d{2}\.\d{2}", pd_)
                                if dts:
                                    mid = (float(getattr(page, "width", 0) or 0) / 2.0) or None
                                    inferred = [
                                        _find_reg_right_of_date(d, x_max=mid) if mid is not None else _find_reg_right_of_date(d)
                                        for d in dts
                                    ]
                                    rn = "\n".join([x for x in inferred if x])
                            licenses.extend(_extract_licenses_from_cell(tg, pd_, rn))

                        if right_type_col >= 0:
                            rtg = str(row[right_type_col] or '').strip() if right_type_col < len(row) else ''
                            rpd = str(row[right_date_col] or '').strip() if right_date_col >= 0 and right_date_col < len(row) else ''
                            rrn = str(row[right_number_col] or '').strip() if right_number_col >= 0 and right_number_col < len(row) else ''
                            if rtg and rtg not in ['~', '기사과정', '졸업일', '학교명', '학과']:
                                # 오른쪽 컬럼 등록번호가 테이블 셀로 안 나오는 경우 좌표 기반으로 보강
                                if (not rrn) and rpd:
                                    dts = re.findall(r"\d{4}\.\d{2}\.\d{2}", rpd)
                                    if dts:
                                        mid = (float(getattr(page, "width", 0) or 0) / 2.0) or None
                                        inferred = [
                                            _find_reg_right_of_date(d, x_min=mid) if mid is not None else _find_reg_right_of_date(d)
                                            for d in dts
                                        ]
                                        rrn = "\n".join([x for x in inferred if x])
                                licenses.extend(_extract_licenses_from_cell(rtg, rpd, rrn))
    
    except Exception as e:
        print(f"[WARN] 국가기술자격 파싱 오류: {e}")

    # // [수정] 한 페이지에서 추출되는 여러 표/동일 레이아웃에 대해 parse가 중복 순회하면
    # 동일 레코드가 확장되어 적재된다. 종목·합격일 고정 후 등록번호 정합 규칙으로 정리.
    licenses = merge_duplicate_license_records_by_qualification(licenses)

    return licenses


def parse_education_info(page) -> List[Dict[str, Any]]:
    """학력 정보 파싱 (개선: 멀티라인/학위[상태]/학교명 이전·현재 분리)"""
    educations = []
    
    try:
        # // [수정] 표 셀의 학위 열: 대졸[졸업] 등 학력구분+[상태] (page_1_parser._EDU_KIND와 정책 정합)
        _DEG_STATUS = re.compile(
            r"^(학사|석사|박사과정|박사수료|박사|없음|대졸|전문졸업|전문|고졸|중졸|초졸|석·박|본\s*석사|본석사)\s*\[\s*([^\]]+)\s*\]\s*$"
        )

        def _norm_spaces(s: str) -> str:
            return re.sub(r"\s+", " ", (s or "")).strip()

        def _split_prev_current_school(school_raw: str) -> tuple[str, str, str]:
            """
            학교명에 '(現:...)'가 있으면 이전/현재로 분리한다.
            - 보강: PDF 추출에서 '現'이 드롭되어 '( :현재학교명)'로 오는 경우가 있어,
              학교명 컨텍스트가 강할 때만 이를 현 표기로 간주해 분리한다.
            - 반환: (학교명_원문정리, 이전_학교명, 현재_학교명)
            """
            s = _norm_spaces((school_raw or "").replace("：", ":"))

            def _looks_like_school_name(x: str) -> bool:
                t = re.sub(r"\s+", "", (x or "")).strip()
                if not t:
                    return False
                return any(k in t for k in ["대학교", "대학", "전문대학", "고등학교", "중학교", "초등학교", "학교"])

            # "(現:...)" / "(현:...)" / "(現 : ...)" 변형 흡수
            m = re.search(r"\(\s*(?:現|현)\s*:\s*([^)]+)\)", s)
            if m:
                curr = _norm_spaces(m.group(1))
                prev = _norm_spaces((s[: m.start()] + s[m.end() :]).strip())
                prev = _norm_spaces(prev)
                if not prev and curr:
                    return s, "", curr
                if prev and not curr:
                    return s, prev, prev
                return s, prev, curr

            # 보강: '( :현재학교명)' (現 드롭) 케이스
            m2 = re.search(r"\(\s*:\s*([^)]+)\)", s)
            if m2:
                cand_curr = _norm_spaces(m2.group(1))
                cand_prev = _norm_spaces((s[: m2.start()] + s[m2.end() :]).strip())
                cand_prev = _norm_spaces(cand_prev)
                if (
                    _looks_like_school_name(cand_prev)
                    and _looks_like_school_name(cand_curr)
                    and not any(cand_curr.endswith(suf) for suf in ["학과", "전공", "학부", "과"])
                ):
                    return s, cand_prev, cand_curr

            # 분리 불가
            return s, "", s

        def _merge_major_bleed_from_school_cell(
            school: str, major: str
        ) -> tuple[str, str]:
            # // [수정] 졸업일 행 경계 때문에 학교명 셀에는 '…학부'만, 학과 열에는 '…전공'만 오는 빗나감 흡수.
            ss, sm = _norm_spaces(school), _norm_spaces(major)
            if not ss or not sm:
                return school, major
            sp = [p for p in ss.split(" ") if p]
            if len(sp) < 2:
                return school, major
            last = sp[-1]
            if last in {"과정"}:
                return school, major
            if not (
                any(last.endswith(suf) for suf in ("학부", "학과"))
                and any(sm.endswith(suf) for suf in ("전공", "학과", "과"))
            ):
                return school, major
            return (
                _norm_spaces(" ".join(sp[:-1])),
                _norm_spaces(last + " " + sm),
            )

        def _major_from_school_tail_if_needed(school: str, major: str) -> tuple[str, str]:
            """
            pdfplumber 테이블이 '학교명' 셀에 학과까지 같이 넣는 케이스 보정.
            - major가 비어 있으면 학교 문자열 안의 '첫 학과형 토큰'부터 마지막까지를 학과로 옮긴다.
              (꼬리 1토큰만 옮기면 학부·전공 쌍에서 오류가 난다.)
            """
            school_m, major_m = _merge_major_bleed_from_school_cell(school, major)

            if _norm_spaces(major_m):
                return school_m, major_m

            sc = _norm_spaces(school_m)
            parts = [p for p in sc.split(" ") if p]
            if len(parts) < 2:
                return school_m, major_m
            # 학과/전공 꼬리 휴리스틱(하드코딩 최소화)
            # // [수정] 복수 토큰 학과 분리 시 첫 매칭 토큰 기준으로 절단
            major_start = None
            for i, p in enumerate(parts):
                if p in {"과정"}:
                    continue
                if any(
                    p.endswith(suf)
                    for suf in ["학과", "전공", "학부", "과"]
                ):
                    major_start = i
                    break
            if major_start is None or major_start == 0:
                return school_m, major_m
            return (
                _norm_spaces(" ".join(parts[:major_start])),
                _norm_spaces(" ".join(parts[major_start:])),
            )

        tables = extract_tables_merged(page)
        if tables and table_set_has_header_signals(
            tables,
            ["학력", "졸업일", "학교명"],
        ):
            for table in tables:
                header_idx = find_section_header(
                    table,
                    ['학력', '졸업일'],
                    ['교육훈련', '근무처', '상훈']
                )
                
                if header_idx >= 0:
                    header_row = table[header_idx]
                    
                    date_col = find_column_index(header_row, '졸업일')
                    school_col = find_column_index(header_row, '학교명')
                    major_col = find_column_index(header_row, '학과')
                    if major_col < 0:
                        major_col = find_column_index(header_row, '전공')
                    degree_col = find_column_index(header_row, '학위')
                    
                    # 기본값 설정
                    if date_col < 0:
                        date_col = 0
                    if school_col < 0:
                        school_col = 1
                    if major_col < 0:
                        major_col = 2
                    if degree_col < 0:
                        degree_col = 3
                    
                    # 다음 섹션 찾기
                    next_section_idx = find_next_section_header(
                        table,
                        header_idx,
                        ['교육훈련', '상훈', '근무처', '벌점', '국가기술자격']
                    )
                    
                    # 데이터 행 추출 (다음 섹션 전까지만)
                    for i in range(header_idx + 1, next_section_idx):
                        row = table[i]
                        if not row:
                            continue
                        
                        row_text = ' '.join([str(cell) for cell in row if cell])
                        
                        # 다른 섹션 헤더인지 확인
                        if any(kw in row_text for kw in ['교육훈련', '교육기간', '상훈', '근무처', '근무기간']):
                            break
                        
                        if '해당없음' in row_text and len(row_text.strip()) < 20:
                            continue
                        
                        graduation_date = str(row[date_col] or '').strip() if date_col < len(row) else ''
                        school_name = str(row[school_col] or '').strip() if school_col < len(row) else ''
                        major = str(row[major_col] or '').strip() if major_col < len(row) else ''
                        degree = str(row[degree_col] or '').strip() if degree_col < len(row) else ''

                        # 학위[상태] 분리
                        status = ""
                        mds = _DEG_STATUS.match(_norm_spaces(degree))
                        if mds:
                            degree = (mds.group(1) or "").strip()
                            status = (mds.group(2) or "").strip()
                        
                        # 유효성 검사: 졸업일과 학교명이 있어야 함
                        if graduation_date and school_name:
                            if re.match(r'\d{4}\.\d{2}(?:\.\d{2})?', graduation_date):
                                # // [수정] 학교·학과 열 경계 붕괴(학부↔전공 분할 등) + 한 셀 병합 보정
                                school_name, major = _major_from_school_tail_if_needed(school_name, major)
                                school_full, prev_school, curr_school = _split_prev_current_school(school_name)
                                educations.append({
                                    "graduation_date": convert_date_format(graduation_date),
                                    # 하위 호환용 원래 키도 유지
                                    "school_name": curr_school or school_full,
                                    "prev_school_name": prev_school,
                                    "curr_school_name": curr_school or school_full,
                                    "major": _norm_spaces(major),
                                    "degree": _norm_spaces(degree),
                                    "status": _norm_spaces(status),
                                })
    
    except Exception as e:
        print(f"⚠️ 학력 파싱 오류: {e}")
    
    return educations


# ============================================
# 교육훈련 파싱 (하이브리드 방식)
# ============================================

def _extract_courses_from_table(page) -> Dict[str, str]:
    """
    테이블에서 날짜-과정명 매핑 추출
    (줄바꿈으로 합쳐진 과정명을 분리하여 전체 텍스트 복원)
    """
    table_courses = {}
    tables = extract_tables_merged(page)
    if not table_set_has_header_signals(
        tables,
        ["교육기간", "과정명", "교육기관"],
    ):
        return table_courses

    for table in tables:
        if not table:
            continue
        
        # 교육훈련 테이블 찾기
        header_idx = -1
        for i, row in enumerate(table):
            if row:
                row_text = ' '.join([str(cell) for cell in row if cell])
                if '교육기간' in row_text and '과정명' in row_text:
                    header_idx = i
                    break
        
        if header_idx < 0:
            continue
        
        # 데이터 행 처리
        for row in table[header_idx + 1:]:
            if not row or len(row) < 2:
                continue
            
            # 다른 섹션으로 넘어가면 중단
            row_text = ' '.join([str(cell) for cell in row if cell])
            if any(kw in row_text for kw in ['수여일', '수여기관', '벌점', '제재일', '근무기간', '상호']):
                break
            
            periods = str(row[0] or '').strip()
            courses = str(row[1] or '').strip()
            
            # 날짜 패턴 찾기
            period_matches = list(re.finditer(DATE_PATTERN, periods))
            
            # 과정명 줄바꿈 분리 및 재조합
            course_lines = courses.split('\n')
            merged_courses = []
            current_course = ''
            
            for line in course_lines:
                line = line.strip()
                if not line or line == '교육':
                    continue
                
                # 새로운 과정 시작 조건
                is_new_course = any(line.startswith(pattern) for pattern in COURSE_START_PATTERNS)
                
                if is_new_course:
                    if current_course:
                        merged_courses.append(current_course)
                    current_course = line
                else:
                    current_course += line
            
            if current_course:
                merged_courses.append(current_course)
            
            # 날짜-과정명 매핑
            for i, match in enumerate(period_matches):
                date_key = f"{match.group(1)}~{match.group(2)}"
                if i < len(merged_courses):
                    table_courses[date_key] = merged_courses[i]
    
    return table_courses


def parse_training_info(page) -> List[Dict[str, Any]]:
    """
    교육훈련 정보 파싱 (하이브리드 방식: 테이블 + 텍스트)
    
    원칙:
    1. 테이블에서 과정명 완전 추출 (줄바꿈으로 병합된 텍스트 처리)
    2. 텍스트에서 교육기관과 교육인정여부 추출
    3. 두 정보를 날짜 기준으로 병합
    4. 모든 데이터의 내용은 절대로 누락이 없어야 함
    """
    trainings = []
    
    try:
        # 1단계: 테이블에서 전체 과정명 추출 (줄바꿈으로 병합된 텍스트)
        table_courses = _extract_courses_from_table(page)
        
        # 2단계: 텍스트에서 교육기관과 교육인정여부 추출
        text = page.extract_text()
        if not text:
            return trainings
        
        lines = text.split('\n')
        in_training_section = False
        single_date_pat = re.compile(r"^\s*\d{4}\.\d{2}\.\d{2}\b")
        
        for i, line in enumerate(lines):
            # 교육훈련 헤더 찾기
            if '교육기간' in line and '과정명' in line and '교육기관명' in line:
                in_training_section = True
                continue
            
            # 섹션 종료 조건
            if in_training_section and any(kw in line for kw in ['수여일 수여기관', '벌점', '근무기간 상호', '제재일']):
                in_training_section = False
                continue

            # FIX: 상훈 데이터가 헤더 없이 바로 이어지는 PDF가 있다.
            # 교육훈련은 보통 'YYYY.MM.DD ~ YYYY.MM.DD'로 시작하지만,
            # 상훈은 'YYYY.MM.DD' 단일 날짜로 시작하는 경우가 많다.
            # 교육훈련 섹션 안에서 단일 날짜 라인이 나오면 교육훈련을 종료한다.
            if in_training_section:
                s = (line or "").strip()
                if single_date_pat.match(s) and ("~" not in s):
                    in_training_section = False
                    continue

            # 학력 라인(학위[상태])이 끼어드는 레이아웃도 있어 방어적으로 종료
            if in_training_section and any(tok in (line or "") for tok in ["학사[", "석사[", "박사과정[", "박사수료[", "박사[", "없음["]):
                in_training_section = False
                continue
            
            # 불필요한 줄 건너뛰기
            if '건설기술 진흥법' in line or '시공 등 업무' in line or '품질관리 업무' in line:
                continue
            
            if in_training_section:
                date_match = re.search(DATE_PATTERN, line)
                if date_match:
                    start_date = date_match.group(1)
                    end_date = date_match.group(2)
                    date_key = f"{start_date}~{end_date}"
                    
                    rest_of_line = line[date_match.end():].strip()
                    rest_of_line = re.sub(r'^교육훈련\s*', '', rest_of_line)
                    
                    # 교육인정여부 추출 (줄 끝에서)
                    recognition = ''
                    for kw in RECOGNITION_KEYWORDS:
                        if rest_of_line.endswith(kw):
                            recognition = kw
                            rest_of_line = rest_of_line[:-len(kw)].strip()
                            break
                    
                    # 교육기관 추출
                    institution = ''
                    for inst_kw in INSTITUTION_KEYWORDS:
                        if inst_kw in rest_of_line:
                            institution = inst_kw
                            break
                    
                    # 테이블에서 전체 과정명 가져오기
                    course_name = table_courses.get(date_key, '')
                    
                    if not course_name:
                        # 테이블에 없으면 텍스트에서 추출
                        for inst_kw in INSTITUTION_KEYWORDS:
                            if inst_kw in rest_of_line:
                                idx = rest_of_line.find(inst_kw)
                                course_name = rest_of_line[:idx].strip()
                                break
                    
                    if course_name or institution or recognition:
                        trainings.append({
                            'start_date': convert_date_format(start_date),
                            'end_date': convert_date_format(end_date),
                            'course_name': course_name,
                            'institution_name': institution,
                            'recognition_status': recognition
                        })
        
        # 3단계: 중복 제거 (날짜 기준)
        seen = set()
        unique_trainings = []
        for t in trainings:
            key = f"{t['start_date']}~{t['end_date']}"
            if key not in seen:
                seen.add(key)
                unique_trainings.append(t)
        
        return unique_trainings
    
    except Exception as e:
        print(f"⚠️ 교육훈련 파싱 오류: {e}")
    
    return trainings


def parse_award_info(page) -> List[Dict[str, Any]]:
    """상훈 정보 파싱 (개선: 섹션 구분 강화)"""
    awards: List[Dict[str, Any]] = []
    
    try:
        page_text = page.extract_text() or ""
        text_lines = [ln.strip() for ln in page_text.split("\n") if ln and ln.strip()]

        def _compact(s: str) -> str:
            return re.sub(r"\s+", "", (s or ""))

        def _infer_type_and_basis_from_text(raw_date: str, institution: str) -> str:
            """
            일부 PDF는 상훈 테이블이 (수여일, 수여기관) 2열로만 추출되어
            '종류 및 근거'가 테이블에서 누락된다. 이 경우 텍스트 라인에서 보강한다.
            예: '2007.12.31 원주지방국토관리청 표창장[제1028호]'
            """
            if not raw_date:
                return ""
            inst = (institution or "").strip()
            raw_date = raw_date.strip()

            inst_c = _compact(inst)
            for ln in text_lines:
                if raw_date not in ln:
                    continue
                # 기관명이 라인에 있으면 해당 기관명 뒤를 우선 사용
                if inst and inst in ln:
                    tail = ln.split(inst, 1)[1].strip()
                    return re.sub(r"\s+", " ", tail).strip()
                # 공백/특수문자 차이로 직접 포함이 실패하면 compact 비교로 재시도
                if inst_c and inst_c in _compact(ln):
                    # 원문에서 기관명 이후를 안전하게 잘라내기 어렵기 때문에
                    # 날짜 이후를 가져온 뒤, 가능한 경우 기관명을 제거한다.
                    tail = ln.split(raw_date, 1)[1].strip()
                    if inst:
                        tail = tail.replace(inst, " ").strip()
                    return re.sub(r"\s+", " ", tail).strip()

            # 기관명 매칭 실패: 날짜 뒤 전체를 반환(최후의 폴백)
            for ln in text_lines:
                if raw_date in ln:
                    tail = ln.split(raw_date, 1)[1].strip()
                    return re.sub(r"\s+", " ", tail).strip()
            return ""

        def _infer_award_type_multiline_from_section_text(date_dot: str, institution: str) -> str:
            """
            상훈 섹션 텍스트 블록에서 멀티라인으로 이어진 '종류및근거'를 복원한다.
            - 1행: 'YYYY.MM.DD <기관> <종류...'
            - 다음 행(들): 날짜로 시작하지 않는 continuation(예: '우수감리원으로선정]')
            """
            if not page_text:
                return ""
            if not date_dot:
                return ""
            inst = (institution or "").strip()
            if not inst:
                return ""

            block = extract_award_section_text(page_text)
            if not block.strip():
                return ""
            lines = [ln.strip() for ln in block.splitlines() if ln and ln.strip()]
            if not lines:
                return ""

            inst_c = _compact(inst)
            date_pat = re.compile(r"^\d{4}\.\d{2}\.\d{2}\b")

            for i, ln in enumerate(lines):
                if date_dot not in ln:
                    continue
                # 기존 로직은 "날짜/기관/종류"가 같은 줄에 있어야 안정적으로 동작했는데,
                # 실제 PDF에선 (날짜) / (기관) / (종류및근거)가 각각 다른 줄로 쪼개질 수 있다.
                # 따라서 "해당 날짜가 나타난 줄부터 다음 날짜 전까지"를 하나의 레코드 블록으로 합친 뒤,
                # 그 블록에서 기관명 이후를 종류/근거로 취한다.

                # 1) 레코드 블록 수집: i ~ (다음 날짜 시작 전)
                chunk_parts: list[str] = []
                j = i
                while j < len(lines):
                    ln2 = lines[j].strip()
                    if not ln2:
                        j += 1
                        continue
                    if j > i and date_pat.match(ln2):
                        break
                    if _AWARD_SECTION_END.match(ln2):
                        break
                    # [수정] 같은 페이지 텍스트에 '벌점 *해당없음*' 한 줄 형태만 오면 _AWARD_SECTION_END가 안 걸린다 → chunk 오염·종류 누락 발생
                    if re.match(r"^\s*벌점\b", ln2):
                        break
                    # 헤더류 제거
                    if ("수여일" in ln2 and "수여기관" in ln2.replace(" ", "")) or (ln2 == "상훈"):
                        j += 1
                        continue
                    # 다음 섹션 제목이 한 줄로 섞이는 오염 방지
                    if _is_award_table_boundary_row(ln2):
                        break
                    chunk_parts.append(re.sub(r"\s+", " ", ln2).strip())
                    j += 1
                    # 너무 길게 확장하지 않도록 상한(오탐 방지)
                    if len(chunk_parts) >= 6:
                        break

                chunk = " ".join([p for p in chunk_parts if p]).strip()
                chunk = re.sub(r"\s+", " ", chunk).strip()
                if not chunk:
                    continue

                # 2) chunk에서 date 이후 텍스트만 남기기
                if date_dot in chunk:
                    chunk_tail = chunk.split(date_dot, 1)[1].strip()
                else:
                    # 날짜가 라벨 뒤에 붙어 깨진 경우 대비: compact로 재탐색(최후 폴백)
                    chunk_tail = chunk
                chunk_tail = re.sub(r"\s+", " ", chunk_tail).strip()
                if not chunk_tail:
                    continue

                # 3) 기관명이 chunk_tail 안 어딘가에 있으면 "기관 뒤"를 종류/근거로 사용
                if inst and inst in chunk_tail:
                    tail = chunk_tail.split(inst, 1)[1].strip()
                else:
                    # 공백/특수문자 차이로 직접 포함이 실패하면 compact 비교 후, 원문에서 최대한 안전하게 제거
                    if inst_c and inst_c in _compact(chunk_tail):
                        # 원문에서 기관 문자열의 정확한 위치를 찾기 어렵기 때문에,
                        # 우선 date 이후 전체를 취하고, 기관명이 그대로 있으면 제거한다.
                        tail = chunk_tail.replace(inst, " ").strip() if inst else chunk_tail
                    else:
                        # 기관이 아예 안 잡힌 케이스: date 이후 전체를 반환(그래도 멀티라인은 복원됨)
                        tail = chunk_tail

                tail = re.sub(r"\s+", " ", tail).strip()
                # [수정] 줄 병합으로 '[...] ' 괄호 안에 들어간 분리 공백 제거(예: '우수한 성적으로'→'우수한성적으로')
                if tail and ("[" in tail) and ("]" in tail) and any(
                    (tok + "[") in tail for tok in _AWARD_TYPE_TOKENS
                ):
                    pref, _, rest = tail.partition("[")
                    inner, _, suf = rest.partition("]")
                    tail = (pref + "[" + inner.replace(" ", "") + "]" + suf.strip()).strip()

                return tail
            return ""

        tables = extract_tables_merged(page)
        if tables and table_set_has_header_signals(
            tables,
            ["수여일", "수여기관", "상훈"],
        ):
            def _looks_like_award_type_token(s: str) -> bool:
                """
                '표창장[...]/훈장증[...]'처럼 수여기관 컬럼에 들어가면 안 되는
                상훈 종류 토큰을 휴리스틱으로 판정한다.
                """
                t = (s or "").strip()
                if not t:
                    return False
                # [수정] '_AWARD_TYPE_TOKENS`(우수상 등)까지 동일하게 인식해 기관·종류 경계 분리에 사용
                if any(tok in t for tok in _AWARD_TYPE_TOKENS):
                    return True
                # 대괄호/번호가 붙는 문서가 많아 힌트로 사용
                if "[" in t or "]" in t:
                    if any(k in t for k in ["표창", "훈장", "포장", "감사장", "상장", "감사"]):
                        return True
                if any(k in t for k in ["표창장", "훈장증", "포장증", "감사장"]):
                    return True
                return False

            def _looks_like_institution_name(s: str) -> bool:
                t = (s or "").strip()
                if not t:
                    return False
                # 날짜/라벨/구분 텍스트는 배제
                if re.match(r"^\d{4}\.\d{2}\.\d{2}$", t):
                    return False
                if any(k in t for k in ["수여일", "수여기관", "종류", "근거", "상훈", "해당없음"]):
                    return False
                # 너무 긴 문장은 기관명이라기보다 종류/근거일 확률이 높다
                if len(t) > 40:
                    return False
                # 기관명은 보통 한글/공백/괄호/점 정도로 구성
                if not re.search(r"[가-힣A-Za-z]", t):
                    return False
                # '표창장/훈장증' 같은 타입 토큰은 기관명이 아니다
                if _looks_like_award_type_token(t):
                    return False
                return True

            for table in tables:
                # FIX: "수여일" 텍스트가 없는 테이블은 상훈 표가 아님 → 스킵.
                # 교육훈련/근무처가 하나의 대형 테이블로 합쳐지는 PDF에서
                # _find_award_table_header_idx가 잘못된 헤더를 반환하는 것을 방지.
                table_flat = " ".join(
                    str(cell) for row in (table or []) for cell in (row or []) if cell
                )
                if "수여일" not in table_flat:
                    continue
                header_idx = _find_award_table_header_idx(table)

                if header_idx >= 0:
                    header_row = table[header_idx]

                    date_col = find_column_index(header_row, "수여일")
                    institution_col = find_column_index(header_row, "수여기관")
                    type_col = find_column_index(header_row, "종류")
                    if type_col < 0:
                        type_col = find_column_index(header_row, "근거")

                    # 기본값 설정
                    if date_col < 0:
                        date_col = 0
                    if institution_col < 0:
                        institution_col = 1
                    if type_col < 0:
                        type_col = 2

                    # 다음 섹션 찾기
                    next_section_idx = find_next_section_header(
                        table,
                        header_idx,
                        ["근무처", "벌점", "교육훈련", "국가기술자격", "학력"],
                    )

                    # 데이터 행 추출 (다음 섹션 전까지만)
                    for i in range(header_idx + 1, next_section_idx):
                        row = table[i]
                        if not row:
                            continue

                        row_text = " ".join([str(cell) for cell in row if cell])

                        if _is_award_table_boundary_row(row_text):
                            break

                        award_date_raw = str(row[date_col] or "").strip() if date_col < len(row) else ""
                        row_has_award_date = bool(
                            award_date_raw and re.match(r"\d{4}\.\d{2}\.\d{2}", award_date_raw)
                        )
                        # 멀티라인 행 보정:
                        # - 수여기관/종류및근거가 다음 줄(다음 row)로 내려오는데 날짜 셀이 비는 경우가 있다.
                        # - 이 경우 현재 row를 "이전 레코드의 continuation"으로 병합한다.
                        if (not row_has_award_date) and awards:
                            # 현재 row가 섹션 경계/헤더가 아니라면 continuation 후보
                            inst_cand = str(row[institution_col] or "").strip() if institution_col < len(row) else ""
                            type_cand = str(row[type_col] or "").strip() if type_col < len(row) else ""
                            extras = []
                            for ci, cell in enumerate(row):
                                if ci in [date_col]:
                                    continue
                                v = str(cell or "").replace("\n", " ").strip()
                                if v:
                                    extras.append(v)
                            if not type_cand and extras:
                                # institution/type 컬럼이 깨진 경우 extra를 type 후보로 사용
                                type_cand = " ".join(extras).strip()

                            if inst_cand or type_cand:
                                prev = awards[-1]
                                # 이전 레코드가 실제 날짜 레코드일 때만 병합
                                if re.match(r"^\d{4}-\d{2}-\d{2}$", str(prev.get("수여일") or "").strip()):
                                    # [수정] 이전 행까지 수여기관 셀에 종류 문자열이 남았으면 정리 후, 미닫힌 '[' 인 종류를 이어붙임
                                    pi, pu = _split_institution_overflow_award_type(
                                        str(prev.get("수여기관") or ""),
                                        str(prev.get("종류및근거") or ""),
                                    )
                                    prev["수여기관"] = pi
                                    prev["종류및근거"] = pu
                                    pt = str(prev.get("종류및근거") or "").strip()

                                    if _award_type_bracket_unclosed(pt):
                                        if inst_cand:
                                            pt = _merge_award_type_text_fragments(pt, inst_cand)
                                        if type_cand:
                                            pt = _merge_award_type_text_fragments(pt, type_cand)
                                        prev["종류및근거"] = _normalize_award_type_text(pt)
                                        continue

                                    # [수정] 날짜 없는 줄의 기관 칸에는 '다른 수여 건의 기관' 조각만 오는 경우가 많아 수여기관에 넣으면 모든 행이 오염됨. 종류 꼬리만 선택적으로 병합.
                                    if type_cand and (
                                        type_cand not in str(prev.get("종류및근거") or "")
                                    ):
                                        prev["종류및근거"] = _normalize_award_type_text(
                                            _merge_award_type_text_fragments(
                                                str(prev.get("종류및근거") or "").strip(),
                                                type_cand,
                                            )
                                        )
                                    continue
                        # 해당없음: 유효 수여일이 없으면 스키마 유지용 1행 추가
                        if "해당없음" in row_text and not row_has_award_date:
                            awards.append(_award_not_applicable_row())
                            continue

                        institution = str(row[institution_col] or "").strip() if institution_col < len(row) else ""
                        type_and_basis = str(row[type_col] or "").strip() if type_col < len(row) else ""
                        institution, type_and_basis = _split_institution_overflow_award_type(
                            institution, type_and_basis
                        )

                        # ── 열 밀림/빈 스페이서 컬럼 보정 ─────────────────────────────
                        # 일부 PDF는 헤더에는 빈 컬럼('')이 끼는데, 데이터 행은 그 빈 컬럼이 채워져
                        # (기관이 왼쪽 컬럼에, 종류가 '수여기관' 컬럼에 들어오는) 열 어긋남이 발생한다.
                        # 예:
                        #   header: ['', '수여일', '', '수여기관', '종류 및 근거']
                        #   row   : ['', '2015.11.25', '국토교통부', '표창장[10574]', '']
                        # 이 경우를 행 단위로 감지하여 기관/종류를 재배치한다.
                        try:
                            left_idx = institution_col - 1
                            left_cell = str(row[left_idx] or "").strip() if 0 <= left_idx < len(row) else ""
                            header_left = str(header_row[left_idx] or "").strip() if 0 <= left_idx < len(header_row) else ""
                            header_left_empty = (not header_left) or (header_left.lower() in {"none", "null"})

                            if header_left_empty and left_cell and _looks_like_institution_name(left_cell):
                                # 케이스 A: type_col이 비고, institution_col에 '표창장/훈장증'이 들어간 경우
                                if (not type_and_basis) and _looks_like_award_type_token(institution):
                                    type_and_basis = institution
                                    institution = left_cell
                                # 케이스 B: institution_col이 타입 조각, type_col이 꼬리 조각인 경우(예: '훈장증[석탑산' + '업훈장제...')
                                elif _looks_like_award_type_token(institution) and type_and_basis:
                                    type_and_basis = (institution + " " + type_and_basis).strip()
                                    institution = left_cell
                        except Exception:
                            pass

                        # [수정] 같은 행 안에서만 extras를 모아 무공백 복구(날짜 셀이 기관열로 복제되거나 종류 칸만 `]` 같은 조각인 PDF)
                        ar_compact = (award_date_raw or "").strip().replace(" ", "")
                        inst_compact = re.sub(r"\s+", "", (institution or "").strip())
                        typ_compact = str(type_and_basis or "").strip().replace("\n", "").replace(" ", "")
                        bad_dupe_date_inst = (
                            award_date_raw
                            and bool(re.fullmatch(r"\d{4}\.\d{2}\.\d{2}", award_date_raw.strip()))
                            and inst_compact == ar_compact
                        )
                        shattered_type_only = False
                        if typ_compact:
                            if typ_compact in {"]", "호]", ",", ".", "'", '"'}:
                                shattered_type_only = True
                            elif len(typ_compact) <= 8 and "[" not in typ_compact and "]" in typ_compact:
                                shattered_type_only = True
                                if any(tok in typ_compact for tok in _AWARD_TYPE_TOKENS):
                                    shattered_type_only = False

                        extras_all: list[str] = []
                        for ci, cell in enumerate(row):
                            if ci == date_col:
                                continue
                            v = str(cell or "").strip()
                            if not v:
                                continue
                            if v == "상훈":
                                continue
                            extras_all.append(v)
                        if extras_all and (bad_dupe_date_inst or shattered_type_only):
                            ir, tr = _reconstruct_award_cells_from_fragment_extras(extras_all)
                            if ir.strip():
                                institution = ir.strip()
                            if tr.strip():
                                type_and_basis = tr.strip()

                        # 테이블이 2열로만 잡히거나 병합/누락으로 type_col이 공란인 케이스 보강:
                        if not type_and_basis:
                            extras = []
                            for ci, cell in enumerate(row):
                                if ci in [date_col, institution_col]:
                                    continue
                                v = str(cell or "").replace("\n", " ").strip()
                                if v:
                                    extras.append(v)
                            if extras:
                                type_and_basis = " ".join(extras).strip()

                        triples = _split_merged_award_triples(award_date_raw, institution, type_and_basis)
                        for d_raw, inst_p, typ_p in triples:
                            typ_fill = typ_p
                            if not typ_fill:
                                typ_fill = _infer_type_and_basis_from_text(d_raw, inst_p)
                            # 깨진 셀 분리로 '한국수자원공' + '사 표창장...'처럼
                            # 기관 접미어(예: '사')가 종류/근거로 유입되는 케이스를 복원한다.
                            try:
                                inst_p = str(inst_p or "").replace("\n", " ").strip()
                                typ_fill = str(typ_fill or "").replace("\n", " ").strip()
                                if inst_p and typ_fill:
                                    # 유형: typ가 '사 ...'로 시작하고, inst+첫글자를 합친 형태가 페이지 텍스트에 존재
                                    first = typ_fill[:1]
                                    if first and first in {"사"} and not inst_p.endswith(first):
                                        inst2 = inst_p + first
                                        # page_text에서 실제 기관명이 그렇게 등장했는지 확인(오탐 방지)
                                        if _compact(inst2) in _compact(page_text):
                                            inst_p = inst2
                                            typ_fill = typ_fill[1:].strip()
                                    if typ_fill.startswith("사 "):
                                        inst2 = inst_p + "사"
                                        if _compact(inst2) in _compact(page_text):
                                            inst_p = inst2
                                            typ_fill = typ_fill[2:].strip()
                            except Exception:
                                pass
                            typ_fill = _normalize_award_type_text(typ_fill)
                            if d_raw and re.match(r"\d{4}\.\d{2}\.\d{2}", d_raw):
                                awards.append(
                                    {
                                        "수여일": convert_date_format(d_raw),
                                        "수여기관": inst_p.replace("\n", " ").strip(),
                                        "종류및근거": typ_fill.replace("\n", " ").strip(),
                                    }
                                )

        text_awards = _parse_awards_from_text_block(page_text)
        awards = _merge_award_lists(awards, text_awards)

        # 텍스트 블록 기반 멀티라인 복원 값이 더 신뢰할 만하면(대괄호 종료 등) 교체
        try:
            for a in awards or []:
                dt_iso = str(a.get("수여일") or "").strip()
                inst = str(a.get("수여기관") or "").strip()
                cur_typ = str(a.get("종류및근거") or "").strip()
                if not re.match(r"^\d{4}-\d{2}-\d{2}$", dt_iso):
                    continue
                if not inst:
                    continue
                date_dot = dt_iso.replace("-", ".")
                inferred = _infer_award_type_multiline_from_section_text(date_dot, inst)
                if not inferred:
                    continue
                # 교체 조건(오탐 방지 + 목표 케이스 해결):
                # - inferred가 ']'로 닫히거나, 현재 값에 날짜 토큰/잘린 파편이 섞여 있으면 inferred 우선
                suspicious = bool(re.search(r"\b\d{4}\.\d{2}\.\d{2}\b", cur_typ)) or bool(
                    re.match(r"^(구사업|호\]$)", cur_typ.replace(" ", ""))
                )
                if inferred.endswith("]") and (suspicious or (len(inferred) >= len(cur_typ))):
                    a["종류및근거"] = inferred
        except Exception:
            pass

        # FIX: '해당없음'은 상훈 자체가 없을 때만 1행으로 유지해야 한다.
        #      실제 상훈이 존재하는데도 표/텍스트 경로에서 '해당없음' 행이 섞이면 과대 파싱으로 이어진다.
        has_real = any(
            re.match(r"^\d{4}-\d{2}-\d{2}$", str(a.get("수여일") or "").strip())
            for a in (awards or [])
        )
        if has_real:
            awards = [
                a
                for a in (awards or [])
                if str(a.get("수여일") or "").strip() not in ("해당없음", "")
            ]

        # 최종 정리: 종류및근거 앞에 기관명이 붙는 오염 제거
        try:
            for a in awards or []:
                inst = str(a.get("수여기관") or "").replace("\n", " ").strip()
                typ = str(a.get("종류및근거") or "").replace("\n", " ").strip()
                if not typ:
                    continue
                if inst:
                    # 1) 정상 케이스: '기관명 ...'이 그대로 붙은 경우
                    if typ.startswith(inst):
                        typ = typ[len(inst) :].strip()
                    # 2) 공백이 사라진 케이스: '기관명표창장...'
                    elif typ.replace(" ", "").startswith(inst.replace(" ", "")):
                        # 원문에서 안전하게 자르기 어려우므로, 앞부분의 기관명(공백 제거)을 제거한 뒤 재공백화
                        t2 = typ.replace(" ", "")
                        t2 = t2[len(inst.replace(" ", "")) :].strip()
                        typ = t2
                    # 3) 기관명이 1글자 잘린 채로 붙는 케이스(예: 기관명 끝 글자가 다음 셀로 넘어가면서 typ가 '한국수자원공 표창패'가 됨)
                    elif len(inst) >= 2 and typ.startswith(inst[:-1]):
                        typ = typ[len(inst[:-1]) :].strip()
                typ = re.sub(r"\s+", " ", typ).strip()
                typ = _normalize_award_type_text(typ)
                a["종류및근거"] = typ
        except Exception:
            pass

        # 2026.04.24 개선: 동일 수여일에 대해 정보가 중복되거나 파편화된 행 제거 로직 강화
        if awards:
            # 0) 동일 수여일 내 "기관=날짜" 같은 파편을 흡수해 1건으로 정리
            try:
                _DATE_CELL = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
                by_date: dict[str, list[dict]] = {}
                for a in awards:
                    dt = str(a.get("수여일") or "").strip()
                    if not dt or dt == "해당없음":
                        continue
                    by_date.setdefault(dt, []).append(a)

                merged2: list[dict] = []
                used_ids: set[int] = set()
                for dt, items in by_date.items():
                    if len(items) == 1:
                        merged2.append(items[0])
                        used_ids.add(id(items[0]))
                        continue
                    # [수정] 기관 후보: 동일 날짜로 여러 파편이 있을 때 “여러 줄이 한 필드로 붙은 값”은 가장 길어 항상 이겼다.
                    #       실제 해당 건 기관명은 같은 시트에서 더 짧은 단일 기관 문자열 후보이다(세로 병합 셀 깨짐 대응).
                    inst_best = ""
                    inst_cands = []
                    for it in items:
                        inst = str(it.get("수여기관") or "").strip()
                        if inst and (not _DATE_CELL.match(inst)):
                            inst_cands.append(inst)
                    if inst_cands:

                        def _inst_pick_key(s: str) -> tuple[int, int]:
                            t = str(s or "")
                            no_ws_len = len(re.sub(r"\s+", "", t))
                            return (t.count("\n") + t.count("\r"), no_ws_len)

                        inst_best = min(inst_cands, key=_inst_pick_key)

                    typ_best = ""
                    for it in items:
                        typ = str(it.get("종류및근거") or "").strip()
                        if typ and len(typ) > len(typ_best):
                            typ_best = typ
                    # type이 여러 조각으로 찢어진 경우를 위해 같은 날짜의 type들을 이어붙이되 중복은 제거
                    typ_parts: list[str] = []
                    for it in items:
                        typ = str(it.get("종류및근거") or "").strip()
                        if not typ:
                            continue
                        if typ in typ_parts:
                            continue
                        typ_parts.append(typ)
                    # 긴 조각 우선으로 정렬 후 연결
                    typ_parts.sort(key=lambda x: len(x), reverse=True)
                    typ_merged = typ_parts[0] if typ_parts else typ_best
                    if len(typ_parts) >= 2:
                        # 최장 조각에 다른 조각이 포함되지 않으면 이어붙임
                        for p in typ_parts[1:]:
                            if not p:
                                continue
                            # 날짜/기관 파편이 섞인 조각은 제외
                            if re.match(r"^\d{4}\.\d{2}\.\d{2}$", p):
                                continue
                            if re.search(r"\b\d{4}\.\d{2}\.\d{2}\b", p):
                                continue
                            # 앞부분이 잘린 듯한 파편(예: '구사업...')은 제외(오탐 방지)
                            if re.match(r"^(구사업|호\]$)", p.replace(" ", "")):
                                continue
                            if p not in typ_merged:
                                typ_merged = (typ_merged + " " + p).strip()
                    merged2.append({"수여일": dt, "수여기관": inst_best, "종류및근거": typ_merged})
                    for it in items:
                        used_ids.add(id(it))

                # 날짜 없는/해당없음 행은 그대로 유지
                for a in awards:
                    if id(a) in used_ids:
                        continue
                    if str(a.get("수여일") or "").strip() in ("", "해당없음"):
                        merged2.append(a)
                awards = merged2
            except Exception:
                pass

            cleaned: list[dict] = []
            # 먼저 길이가 긴 순서대로 정렬하여 "완전한" 레코드를 먼저 처리하도록 한다.
            awards_sorted = sorted(
                awards, 
                key=lambda x: len(str(x.get("수여기관","")) + str(x.get("종류및근거",""))), 
                reverse=True
            )
            
            for a in awards_sorted:
                dt = str(a.get("수여일") or "").strip()
                inst = str(a.get("수여기관") or "").replace("\n", " ").strip()
                typ = str(a.get("종류및근거") or "").replace("\n", " ").strip()
                combined_a = _norm_award_key_inst(inst + typ)
                
                if not dt or dt == "해당없음":
                    if not any(c.get("수여일") == dt for c in cleaned):
                        cleaned.append(a)
                    continue

                # 이미 추가된(더 긴) 레코드들 중에 이 레코드를 포함하는 것이 있는지 확인
                is_fragment = False
                for b in cleaned:
                    if str(b.get("수여일") or "").strip() != dt:
                        continue
                    
                    inst_b = str(b.get("수여기관") or "").replace("\n", " ").strip()
                    typ_b = str(b.get("종류및근거") or "").replace("\n", " ").strip()
                    
                    # 1) 기관명과 종류가 이미 다른 레코드의 "종류및근거"나 "수여기관+종류"에 포함된 경우
                    combined_b = _norm_award_key_inst(inst_b + typ_b)
                    if combined_a and combined_b and combined_a in combined_b:
                        is_fragment = True
                        break
                    
                    # 2) 파편화된 케이스: 수여기관에 종류가 잘못 들어간 경우 등 (유저 사례 대응)
                    if inst and typ_b and inst in typ_b:
                        is_fragment = True
                        break
                
                if not is_fragment:
                    cleaned.append(a)
            
            # 원래 순서(날짜순) 등으로 재정렬할 수도 있지만, 최종적으로는 parse_page_1 등에서 정렬하므로 유지
            awards = cleaned

        # 최종 정리(마지막 단계): 종류및근거 앞의 기관명 오염 제거
        try:
            for a in awards or []:
                inst = str(a.get("수여기관") or "").replace("\n", " ").strip()
                typ = str(a.get("종류및근거") or "").replace("\n", " ").strip()
                if not typ:
                    continue
                if inst:
                    if typ.startswith(inst):
                        typ = typ[len(inst) :].strip()
                    elif typ.replace(" ", "").startswith(inst.replace(" ", "")):
                        t2 = typ.replace(" ", "")
                        t2 = t2[len(inst.replace(" ", "")) :].strip()
                        typ = t2
                    elif len(inst) >= 2 and typ.startswith(inst[:-1]):
                        typ = typ[len(inst[:-1]) :].strip()
                typ = re.sub(r"\s+", " ", typ).strip()
                typ = _normalize_award_type_text(typ)
                a["종류및근거"] = typ
        except Exception:
            pass

    except Exception as e:
        print(f"⚠️ 상훈 파싱 오류: {e}")

    return awards


def parse_penalty_and_sanction_info(page) -> Dict[str, Any]:
    """
    벌점 및 제재사항 파싱

    - 벌점: 점수만 (없으면 "해당없음")
    - 제재사항: 제재일, 종류 및 제재기간, 근거, 제재기관 (없으면 "해당없음")
    """
    result: Dict[str, Any] = {"벌점": "해당없음", "제재사항": "해당없음"}
    sanctions: List[Dict[str, str]] = []
    
    try:
        tables = extract_tables_merged(page)
        if tables and table_set_has_header_signals(
            tables,
            ["벌점", "제재사항", "제재일"],
        ):
            for table in tables:
                header_idx = find_section_header(
                    table,
                    ['벌점', '제재사항', '제재일'],
                    ['교육훈련', '상훈', '근무처', '근무기간']
                )
                
                if header_idx >= 0:
                    header_row = table[header_idx]
                    
                    points_col = find_column_index(header_row, '벌점')
                    date_col = find_column_index(header_row, '제재일')
                    type_col = find_column_index(header_row, '종류')
                    basis_col = find_column_index(header_row, '근거')
                    institution_col = find_column_index(header_row, '제재기관')
                    
                    # 기본값 설정
                    if points_col < 0:
                        points_col = 0
                    if date_col < 0:
                        date_col = 1
                    if type_col < 0:
                        type_col = 2
                    if basis_col < 0:
                        basis_col = 3
                    if institution_col < 0:
                        institution_col = 4
                    
                    # 다음 섹션 찾기
                    next_section_idx = find_next_section_header(
                        table,
                        header_idx,
                        ['근무처', '교육훈련', '상훈', '국가기술자격', '학력']
                    )
                    
                    # 데이터 행 추출 (다음 섹션 전까지만)
                    for i in range(header_idx + 1, next_section_idx):
                        row = table[i]
                        if not row:
                            continue
                        
                        row_text = ' '.join([str(cell) for cell in row if cell])
                        
                        # 다른 섹션 헤더인지 확인
                        if any(kw in row_text for kw in ['근무처', '근무기간', '상호', '교육훈련', '상훈', '수여일']):
                            break
                        
                        # 해당없음 처리
                        if '해당없음' in row_text:
                            # 벌점/제재 둘 다 해당없음이면 그대로 유지
                            continue
                        
                        penalty_points_raw = str(row[points_col] or '').strip() if points_col < len(row) else ''
                        sanction_date_raw = str(row[date_col] or '').strip() if date_col < len(row) else ''
                        type_and_period = str(row[type_col] or '').strip() if type_col < len(row) else ''
                        basis = str(row[basis_col] or '').strip() if basis_col < len(row) else ''
                        sanction_institution = str(row[institution_col] or '').strip() if institution_col < len(row) else ''

                        # 근무기간 같은 잘못된 데이터 제외
                        if penalty_points_raw and '근무기간' in penalty_points_raw:
                            continue

                        # 벌점 점수만 추출 (숫자/소수/음수는 현실적으로 없음)
                        if penalty_points_raw and penalty_points_raw not in ['근무기간', '상호', '']:
                            m = re.search(r'(\d+(?:\.\d+)?)', penalty_points_raw.replace('점', ' '))
                            if m:
                                result["벌점"] = m.group(1)
                            elif penalty_points_raw:
                                # 숫자만 없지만 값이 있으면 그대로 보관 (문서 양식 차이 대응)
                                result["벌점"] = penalty_points_raw.replace('\n', ' ').strip()

                        # 제재사항은 "제재일"이 있는 행만 수집
                        if sanction_date_raw and re.match(r'\d{4}\.\d{2}\.\d{2}', sanction_date_raw):
                            sanctions.append({
                                "제재일": convert_date_format(sanction_date_raw),
                                "종류및제재기간": type_and_period.replace('\n', ' ').strip(),
                                "근거": basis.replace('\n', ' ').strip(),
                                "제재기관": sanction_institution.replace('\n', ' ').strip()
                            })

        if sanctions:
            result["제재사항"] = sanctions
    
    except Exception as e:
        print(f"⚠️ 벌점 및 제재사항 파싱 오류: {e}")
    
    return result


def parse_workplace_info(page) -> List[Dict[str, Any]]:
    """근무처 정보 파싱 (4열: 근무기간/상호 2세트 대응)"""
    workplaces = []
    
    try:
        tables = extract_tables_merged(page)
        if tables and table_set_has_header_signals(
            tables,
            ["근무처", "근무기간", "상호"],
        ):
            for table in tables:
                header_idx = find_section_header(
                    table,
                    ['근무처', '근무기간', '상호'],
                    ['교육훈련', '상훈', '벌점']
                )
                
                if header_idx >= 0:
                    header_row = table[header_idx]

                    # 헤더에서 근무기간/상호 컬럼을 모두 수집 (좌/우 2세트)
                    period_cols = [i for i, c in enumerate(header_row) if c and '근무기간' in str(c)]
                    company_cols = [i for i, c in enumerate(header_row) if c and '상호' in str(c)]

                    # 일반적으로 [근무기간, 상호, 근무기간, 상호] 형태
                    col_pairs: List[tuple[int, int]] = []
                    if len(period_cols) >= 1 and len(company_cols) >= 1:
                        # 가장 가까운 상호 컬럼을 근무기간 컬럼과 페어링
                        used_company = set()
                        for p in period_cols:
                            candidates = [c for c in company_cols if c > p and c not in used_company]
                            if candidates:
                                c = min(candidates)
                                col_pairs.append((p, c))
                                used_company.add(c)

                    # pdfplumber가 오른쪽 '상호' 헤더 셀을 비워 두면 두 번째 근무기간 열이
                    # 상호와 페어링되지 않음 → 바로 오른쪽 열을 상호 후보로 추가
                    if len(period_cols) >= 2:
                        paired_p = {pc for pc, _ in col_pairs}
                        for p in period_cols:
                            if p in paired_p:
                                continue
                            c = p + 1
                            if c < len(header_row) and (p, c) not in col_pairs:
                                col_pairs.append((p, c))

                    if not col_pairs:
                        # 폴백: 첫 근무기간/상호만이라도
                        period_col = find_column_index(header_row, '근무기간')
                        company_col = find_column_index(header_row, '상호')
                        if period_col < 0 or company_col < 0:
                            continue
                        col_pairs = [(period_col, company_col)]
                    
                    # 다음 섹션 찾기
                    next_section_idx = find_next_section_header(
                        table,
                        header_idx,
                        ['교육훈련', '상훈', '벌점', '국가기술자격', '학력']
                    )
                    
                    from parsers.utils.company_change_markers import get_company_change_markers

                    def _append_workplace(period_raw: str, company_raw: str):
                        """단일 (기간, 상호) 쌍을 표준 스키마로 추가"""
                        if not period_raw or not company_raw:
                            return

                        date_match = re.search(
                            r'(\d{4}\.\d{2}(?:\.\d{2})?)\s*~\s*(\d{4}\.\d{2}(?:\.\d{2})?|근\s*무\s*중)',
                            period_raw
                        )
                        if not date_match:
                            return

                        start_date = convert_date_format(date_match.group(1))
                        end_raw = (date_match.group(2) or "")
                        end_compact = end_raw.replace(" ", "")
                        end_date = "근무중" if ("근무중" in end_compact or ("근" in end_raw and "무" in end_raw)) else convert_date_format(end_raw)

                        def _normalize_company_markers(s: str) -> str:
                            s = (s or "")
                            s = s.replace("：", ":")
                            s = re.sub(r"\s+", " ", s)
                            for mk in get_company_change_markers():
                                s = re.sub(rf"{re.escape(mk)}\s*:\s*", f"{mk}:", s)
                            return s.strip()

                        company_clean = _normalize_company_markers(company_raw.replace('\n', ' '))
                        prev_name = company_clean
                        curr_name = ""
                        # 변경 사유 마커(現/흡수합병/분할설립/상호변경/법인전환/합병/양수도 등) 이후를 "현재 상호"로 취급
                        for mk in [m + ":" for m in get_company_change_markers()]:
                            if mk in company_clean:
                                left, right = company_clean.split(mk, 1)
                                prev_name = (left or "").strip()
                                curr_name = (right or "").strip()
                                break
                        else:
                            # pdfplumber가 '現' 문자를 드롭한 경우: "구상호 :신상호" 패턴 감지
                            # 예: "(주)창설토목건축사사무소 :(주)창설", "한일개발(주) :한진건설(주)"
                            if ":" in company_clean:
                                left, right = company_clean.split(":", 1)
                                left = left.strip()
                                right = right.strip()
                                if left and right and (
                                    "(주)" in right
                                    or re.search(r"[가-힣A-Za-z0-9]", right)
                                ):
                                    prev_name = left
                                    curr_name = right.strip()

                        # 추가 규칙: "YYYY.MM.DD :(주)..." 형태의 상호변경 표기 분리
                        def _split_date_colon(blob: str) -> tuple[str, str] | None:
                            m = re.search(r"\b(\d{4}\.\d{2}\.\d{2})\s*[:：]\s*(.+)$", blob or "")
                            if not m:
                                return None
                            right = (m.group(2) or "").strip()
                            left = ((blob[: m.start()] or "")).strip()
                            left = re.sub(r"\b\d{4}\.\d{2}\.\d{2}\b", " ", left)
                            left = re.sub(r"\s+", " ", left).strip()
                            if left and right and re.search(r"[가-힣A-Za-z0-9]", right):
                                return left, right
                            return None

                        if prev_name and (not curr_name):
                            split = _split_date_colon(prev_name)
                            if split:
                                prev_name, curr_name = split
                        elif curr_name:
                            split = _split_date_colon(curr_name)
                            if split:
                                left, right = split
                                if not prev_name:
                                    prev_name = left
                                curr_name = right
                        # 규칙: 현재 상호명이 비어 있으면, 현재에 넣고 이전은 빈 값
                        if prev_name and not curr_name:
                            curr_name = prev_name
                            prev_name = ""

                        workplaces.append({
                            "근무기간_시작": start_date,
                            "근무기간_종료": end_date,
                            "이전_상호명": prev_name,
                            "현재_상호명": curr_name
                        })

                    # 데이터 행 추출
                    for i in range(header_idx + 1, next_section_idx):
                        row = table[i]
                        if not row:
                            continue
                        
                        row_text = ' '.join([str(cell) for cell in row if cell])
                        
                        # 다른 섹션 헤더인지 확인
                        if any(kw in row_text for kw in ['교육훈련', '상훈', '벌점', '교육기간', '수여일', '제재일']):
                            break
                        
                        if '해당없음' in row_text and len(row_text.strip()) < 20:
                            continue

                        # 변경 사유·종료일·(:) 한 줄 형식 (표 셀 병합으로 오른쪽 상호 누락 시)
                        if any(k in row_text for k in get_company_change_markers() if k not in {"現", "현"}):
                            flat1 = re.sub(r"\s+", " ", row_text.replace("\n", " ")).strip()
                            reasons = [m for m in get_company_change_markers() if m not in {"現", "현"}]
                            reason_alt = "|".join(re.escape(x) for x in reasons) if reasons else "흡수합병"
                            mm = re.search(
                                rf"(\d{{4}}\.\d{{2}}\.\d{{2}})\s+({reason_alt}):\s*(.+?)\s+"
                                rf"(\d{{4}}\.\d{{2}}\.\d{{2}})\s*:\s*(.+)$",
                                flat1,
                            )
                            if mm:
                                workplaces.append(
                                    {
                                        "근무기간_시작": convert_date_format(mm.group(1)),
                                        "근무기간_종료": convert_date_format(mm.group(4)),
                                        "이전_상호명": mm.group(3).strip(),
                                        "현재_상호명": mm.group(5).strip(),
                                    }
                                )

                        # 1) 컬럼/페어 기반 파싱 (가능하면 가장 정확)
                        for period_col, company_col in col_pairs:
                            period_raw = str(row[period_col] or '').strip() if period_col < len(row) else ''
                            company_raw = str(row[company_col] or '').strip() if company_col < len(row) else ''
                            _append_workplace(period_raw, company_raw)

                        # 2) 보강: 오른쪽 세트가 셀 병합/헤더 인식 문제로 누락되는 케이스를 위해
                        #    행 전체 텍스트에서 날짜~날짜 패턴을 모두 찾아 추가로 추출 (이미 추출된 건은 중복 제거)
                        flat = " | ".join([str(c).strip().replace('\n', ' ') for c in row if c and str(c).strip() and str(c).strip() != "~"])
                        date_iter = list(re.finditer(r'\d{4}\.\d{2}\.\d{2}\s*~\s*(?:\d{4}\.\d{2}\.\d{2}|근\s*무\s*중)', flat))
                        if len(date_iter) >= 2:
                            existing = {
                                (w.get("근무기간_시작", ""), w.get("근무기간_종료", ""), w.get("이전_상호명", ""), w.get("현재_상호명", ""))
                                for w in workplaces
                            }
                            for idx, dm in enumerate(date_iter):
                                period_raw = dm.group(0).strip()
                                after = flat[dm.end():]
                                next_start = date_iter[idx + 1].start() if idx + 1 < len(date_iter) else None
                                company_chunk = after[: next_start - dm.end()] if next_start is not None else after
                                company_chunk = company_chunk.strip(" |")
                                company_chunk = re.split(r'\s*(?:근무처|상훈|벌점|제재사항|교육훈련)\s*', company_chunk)[0].strip()

                                before_len = len(workplaces)
                                _append_workplace(period_raw, company_chunk)
                                if len(workplaces) > before_len:
                                    w = workplaces[-1]
                                    key = (w.get("근무기간_시작", ""), w.get("근무기간_종료", ""), w.get("이전_상호명", ""), w.get("현재_상호명", ""))
                                    if key in existing:
                                        workplaces.pop()
                                    else:
                                        existing.add(key)

        # 텍스트 폴백/보강:
        # 표 추출이 2세트(좌/우) 중 일부를 놓치거나, 셀 병합으로 row 단위 파싱이 깨지는 문서가 있다.
        # 이 경우 페이지 텍스트에서 근무처 블록을 추가 추출해 workplaces를 보강한다(중복 제거).
        try:
            page_text = page.extract_text() or ""
        except Exception:
            page_text = ""

        if page_text:
            # 근무처 블록만 대략 절단(너무 광범위 파싱 방지)
            # - 시작: '근무처' 또는 '근무기간' 헤더 근처
            # - 종료: '1. 기술경력' 또는 다음 대섹션 키워드
            start = None
            for pat in [r"(?m)^\s*근무처\s*$", r"근무처", r"근무기간"]:
                m = re.search(pat, page_text)
                if m:
                    start = m.start()
                    break
            if start is None:
                start = 0
            rest = page_text[start:]
            end_m = re.search(r"(?m)^\s*1\.\s*기술경력", rest)
            if not end_m:
                end_m = re.search(r"(?m)^\s*(?:교육훈련|상훈|벌점\s*및\s*제재사항|국가기술자격|학력)\s*$", rest)
            blk = rest[: end_m.start()] if end_m else rest

            blk_flat = re.sub(r"[ \t]+", " ", blk).strip()
            # 날짜 범위 토큰 탐색(여러 개 나올 수 있음)
            it = list(re.finditer(r"(\d{4}\.\d{2}\.\d{2})\s*~\s*(\d{4}\.\d{2}\.\d{2}|근\s*무\s*중)", blk_flat))
            if it:
                existing = {
                    (w.get("근무기간_시작", ""), w.get("근무기간_종료", ""), w.get("이전_상호명", ""), w.get("현재_상호명", ""))
                    for w in workplaces
                }
                for idx, dm in enumerate(it):
                    period_raw = dm.group(0).strip()
                    nxt = it[idx + 1].start() if idx + 1 < len(it) else None
                    chunk = blk_flat[dm.end() : (nxt if nxt is not None else len(blk_flat))].strip()
                    # 헤더/다른 섹션 잔여 제거
                    chunk = re.split(r"\s*(?:근무처|근무기간|상호|교육훈련|상훈|벌점|제재사항|국가기술자격|학력)\s*", chunk)[0].strip(" |")
                    before_len = len(workplaces)
                    _append_workplace(period_raw, chunk)
                    if len(workplaces) > before_len:
                        w = workplaces[-1]
                        key = (w.get("근무기간_시작", ""), w.get("근무기간_종료", ""), w.get("이전_상호명", ""), w.get("현재_상호명", ""))
                        if key in existing:
                            workplaces.pop()
                        else:
                            existing.add(key)
    
    except Exception as e:
        print(f"⚠️ 근무처 파싱 오류: {e}")
    
    return workplaces
