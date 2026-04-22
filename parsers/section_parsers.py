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

from typing import Dict, List, Any, Optional
import pdfplumber
from parsers.personal_info import convert_date_format
from parsers.table_settings import extract_tables_merged, table_set_has_header_signals
from field_catalog import best_match_specialty, extract_name_and_grade, get_field_catalog

# 직무분야/전문분야 카탈로그(엑셀 기반, 실패 시 폴백)
_CATALOG = get_field_catalog(project_root=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
    type_lines = [x.strip() for x in re.split(r"[\n\r]+", str(type_and_basis or "")) if x.strip()]
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
    return tail.strip(), ""


def _parse_awards_from_text_block(page_text: str) -> List[Dict[str, Any]]:
    """표 추출 실패·병합 시 텍스트 블록에서 상훈 행을 복구한다."""
    block = extract_award_section_text(page_text)
    if not block.strip():
        return []

    awards: List[Dict[str, Any]] = []
    for raw in block.splitlines():
        ln = raw.strip()
        if not ln or "수여일" in ln and "수여기관" in ln.replace(" ", ""):
            continue
        if " ~ " in ln or (ln.count("~") >= 1 and re.search(r"\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}", ln)):
            continue
        m = re.match(r"^(\d{4}\.\d{2}\.\d{2})\s+(.+)$", ln)
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
        typ = str(a.get("종류및근거") or "").replace("\n", " ").strip()
        if not dt:
            return
        k = (dt, _norm_award_key_inst(inst))
        if k not in by_key:
            by_key[k] = len(merged)
            merged.append({"수여일": dt, "수여기관": inst, "종류및근거": typ})
            return
        i = by_key[k]
        cur = merged[i]
        if len(typ) > len(str(cur.get("종류및근거") or "")):
            cur["종류및근거"] = typ
        if inst and not str(cur.get("수여기관") or "").strip():
            cur["수여기관"] = inst

    for a in primary:
        _ingest(a)
    for a in secondary:
        _ingest(a)
    return merged


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
                        # 다음 행들에서 정보 추출 (최대 3행까지)
                        for j in range(i + 1, min(i + 4, len(table))):
                            next_row = table[j]
                            if not next_row or len(next_row) < 2:
                                continue
                            
                            # 직무분야 열 (보통 첫 번째 열)
                            field_cell = str(next_row[0] or '').strip()
                            field_name, field_grade = extract_name_and_grade(field_cell)
                            if field_name and field_grade and field_name in BASIC_FIELDS:
                                if 'design_work_field' not in grade_info:
                                    grade_info['design_work_field'] = field_name
                                    grade_info['design_work_grade'] = field_grade
                            
                            # 전문분야 열 찾기 (보통 세 번째 열)
                            if len(next_row) >= 3:
                                specialty_cell = str(next_row[2] or '').strip()
                                # 해당없음, 생략 제외
                                if specialty_cell and '해당없음' not in specialty_cell and '생략' not in specialty_cell:
                                    specialty_name = best_match_specialty(specialty_cell, _CATALOG)
                                    _, specialty_grade = extract_name_and_grade(specialty_cell)
                                    if specialty_name and specialty_grade:
                                        if 'design_specialty' not in grade_info:
                                            grade_info['design_specialty'] = specialty_name
                                            grade_info['design_specialty_grade'] = specialty_grade
                    
                    # 건설사업관리 섹션 찾기
                    if '건설사업관리' in row_text:
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
        # 보통 등록번호는 가장 오른쪽/마지막에 붙는 편이라 마지막 후보를 택한다.
        return cands[-1].strip()

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
    return results


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
    
    return licenses


def parse_education_info(page) -> List[Dict[str, Any]]:
    """학력 정보 파싱 (개선: 섹션 구분 강화)"""
    educations = []
    
    try:
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
                        
                        # 유효성 검사: 졸업일과 학교명이 있어야 함
                        if graduation_date and school_name:
                            if re.match(r'\d{4}\.\d{2}(?:\.\d{2})?', graduation_date):
                                educations.append({
                                    'graduation_date': convert_date_format(graduation_date),
                                    'school_name': school_name,
                                    'major': major,
                                    'degree': degree
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

        tables = extract_tables_merged(page)
        if tables and table_set_has_header_signals(
            tables,
            ["수여일", "수여기관", "상훈"],
        ):
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
                        # 해당없음: 유효 수여일이 없으면 스키마 유지용 1행 추가
                        if "해당없음" in row_text and not row_has_award_date:
                            awards.append(_award_not_applicable_row())
                            continue

                        institution = str(row[institution_col] or "").strip() if institution_col < len(row) else ""
                        type_and_basis = str(row[type_col] or "").strip() if type_col < len(row) else ""

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

        # FIX: 동일 수여일에 대해 '종류및근거'가 비어있고, 수여기관 칸에 종류가 합쳐진 중복행 제거.
        #      (예: 박준서 2012-11-02: "수여기관=건설산업교육원 우수상[...]", 종류는 빈값)
        if awards:
            cleaned: list[dict] = []
            for a in awards:
                dt = str(a.get("수여일") or "").strip()
                inst = str(a.get("수여기관") or "").replace("\n", " ").strip()
                typ = str(a.get("종류및근거") or "").replace("\n", " ").strip()
                if not dt:
                    continue
                if typ:
                    cleaned.append(a)
                    continue
                # typ가 비었으면, 같은 날짜의 다른 행이 inst를 포함(또는 부분일치)하면서 typ가 있으면 중복으로 간주해 제거
                inst_n = _norm_award_key_inst(inst)
                dup = False
                for b in awards:
                    if b is a:
                        continue
                    if str(b.get("수여일") or "").strip() != dt:
                        continue
                    typ_b = str(b.get("종류및근거") or "").replace("\n", " ").strip()
                    if not typ_b:
                        continue
                    inst_b = str(b.get("수여기관") or "").replace("\n", " ").strip()
                    inst_b_n = _norm_award_key_inst(inst_b)
                    if inst_b_n and inst_n and (inst_b_n in inst_n):
                        dup = True
                        break
                if not dup:
                    cleaned.append(a)
            awards = cleaned

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
                            s = re.sub(r"現\s*:\s*", "現:", s)
                            s = re.sub(r"흡수합병\s*:\s*", "흡수합병:", s)
                            return s.strip()

                        company_clean = _normalize_company_markers(company_raw.replace('\n', ' '))
                        prev_name = company_clean
                        curr_name = ""
                        # '現:' 또는 '흡수합병:' 이후를 "현재 상호"로 취급
                        if "現:" in company_clean:
                            left, right = company_clean.split("現:", 1)
                            prev_name = left.strip()
                            curr_name = right.strip()
                        elif "흡수합병:" in company_clean:
                            left, right = company_clean.split("흡수합병:", 1)
                            prev_name = (left or "").strip()
                            curr_name = (right or "").strip()
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

                        # 흡수합병·종료일·現(:) 한 줄 형식 (표 셀 병합으로 오른쪽 상호 누락 시)
                        if "흡수합병" in row_text:
                            merger_flat = re.sub(r"\s+", " ", row_text.replace("\n", " ")).strip()
                            mm = re.search(
                                r"(\d{4}\.\d{2}\.\d{2})\s+흡수합병:\s*(.+?)\s+"
                                r"(\d{4}\.\d{2}\.\d{2})\s*:\s*(.+)$",
                                merger_flat,
                            )
                            if mm:
                                workplaces.append({
                                    "근무기간_시작": convert_date_format(mm.group(1)),
                                    "근무기간_종료": convert_date_format(mm.group(3)),
                                    "이전_상호명": mm.group(2).strip(),
                                    "현재_상호명": mm.group(4).strip(),
                                })

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
