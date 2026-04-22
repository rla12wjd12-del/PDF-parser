#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
워드 bbox 기반 '위치(템플릿) 파싱' 코어.

목적
- pdfplumber.extract_tables()의 셀/선 인식 흔들림을 피하고,
  word bbox로 행/열을 직접 재구성해 사업명 오염(직무/담당업무/직위가 사업명에 합쳐짐) 등을 줄인다.

원칙
- 템플릿 결과가 의심스러우면(검증 실패) 즉시 기존 파서(텍스트/휴리스틱)로 폴백한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import re
from datetime import datetime


# layout_extractor.Word 와 호환되는 최소 인터페이스
@dataclass(frozen=True)
class BBoxWord:
    text: str
    x0: float
    x1: float
    top: float
    bottom: float


def _as_bbox_word(w: Any) -> Optional[BBoxWord]:
    try:
        t = str(getattr(w, "text", "") or "").strip()
        if not t:
            return None
        return BBoxWord(
            text=t,
            x0=float(getattr(w, "x0", 0.0)),
            x1=float(getattr(w, "x1", 0.0)),
            top=float(getattr(w, "top", 0.0)),
            bottom=float(getattr(w, "bottom", 0.0)),
        )
    except Exception:
        return None


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _compact(s: str) -> str:
    return re.sub(r"\s+", "", (s or "")).strip()


def _yyyy_mm_dd_to_iso(date_str: str) -> str:
    s = (date_str or "").strip()
    if not s:
        return ""
    m = re.fullmatch(r"(\d{4})\.(\d{2})\.(\d{2})", s)
    if not m:
        return ""
    yyyy, mm, dd = map(int, m.groups())
    try:
        return datetime(yyyy, mm, dd).strftime("%Y-%m-%d")
    except ValueError:
        return ""


_DATE_STRICT = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
_DAYS_TOKEN_RE = re.compile(r"\(\s*(\d[\d,]*)\s*일\s*\)")


@dataclass(frozen=True)
class XRange:
    x0: float
    x1: float

    def shifted(self, dx: float) -> "XRange":
        return XRange(self.x0 + dx, self.x1 + dx)

    def contains_word(self, w: BBoxWord) -> bool:
        cx = (w.x0 + w.x1) / 2.0
        return self.x0 <= cx < self.x1


@dataclass(frozen=True)
class TechTemplate:
    """
    기술경력(제2쪽)에서 관측된 고정 열 구간 템플릿.
    - 사업명 라인: [사업명 | 직무분야 | 담당업무]
    - ~ 라인: [발주자 | 공사종류 | 전문분야 | 직위]
    """

    # 앵커(보정) 기준: 시작일 날짜 라인(좌측) x0 대표값
    anchor_date_x0: float = 30.4

    # 사업명/직무/담당업무 라인 (y는 다르지만 같은 열 경계)
    col_project_name: XRange = XRange(70.0, 360.0)
    col_job_field: XRange = XRange(360.0, 440.0)
    col_job_duty: XRange = XRange(440.0, 600.0)

    # '~' 라인
    col_issuer: XRange = XRange(70.0, 220.0)
    col_work_type: XRange = XRange(220.0, 360.0)
    col_specialty: XRange = XRange(360.0, 440.0)
    col_position: XRange = XRange(440.0, 600.0)

    # 날짜(시작/종료) 라인 x 범위
    col_date: XRange = XRange(25.0, 90.0)

    def shifted(self, dx: float) -> "TechTemplate":
        return TechTemplate(
            anchor_date_x0=self.anchor_date_x0 + dx,
            col_project_name=self.col_project_name.shifted(dx),
            col_job_field=self.col_job_field.shifted(dx),
            col_job_duty=self.col_job_duty.shifted(dx),
            col_issuer=self.col_issuer.shifted(dx),
            col_work_type=self.col_work_type.shifted(dx),
            col_specialty=self.col_specialty.shifted(dx),
            col_position=self.col_position.shifted(dx),
            col_date=self.col_date.shifted(dx),
        )


@dataclass(frozen=True)
class CmTemplate:
    """
    건설사업관리/감리경력(제3쪽) 템플릿.
    - 관측상 열 경계는 기술경력과 매우 유사하므로 기본값을 공유한다.
    """

    anchor_date_x0: float = 30.4
    col_project_name: XRange = XRange(70.0, 360.0)
    col_job_field: XRange = XRange(360.0, 440.0)
    col_job_duty_or_specialty: XRange = XRange(440.0, 600.0)

    col_issuer: XRange = XRange(70.0, 220.0)
    col_work_type: XRange = XRange(220.0, 360.0)
    col_specialty: XRange = XRange(360.0, 440.0)
    col_position: XRange = XRange(440.0, 600.0)

    col_date: XRange = XRange(25.0, 90.0)

    def shifted(self, dx: float) -> "CmTemplate":
        return CmTemplate(
            anchor_date_x0=self.anchor_date_x0 + dx,
            col_project_name=self.col_project_name.shifted(dx),
            col_job_field=self.col_job_field.shifted(dx),
            col_job_duty_or_specialty=self.col_job_duty_or_specialty.shifted(dx),
            col_issuer=self.col_issuer.shifted(dx),
            col_work_type=self.col_work_type.shifted(dx),
            col_specialty=self.col_specialty.shifted(dx),
            col_position=self.col_position.shifted(dx),
            col_date=self.col_date.shifted(dx),
        )


@dataclass(frozen=True)
class ParsedBlock:
    start_iso: str
    end_iso: str
    인정일수: str
    참여일수: str
    y_start: float
    y_tilde: float | None
    i_start: int
    i_tilde: int | None
    i_end: int
    i_days_end: int | None


def group_words_to_lines(words: Sequence[Any], *, y_tolerance: float = 2.0) -> List[Tuple[float, List[BBoxWord]]]:
    ws = [_as_bbox_word(w) for w in words]
    ws2 = [w for w in ws if w is not None]
    if not ws2:
        return []
    ws2.sort(key=lambda w: (w.top, w.x0))

    lines: List[Tuple[float, List[BBoxWord]]] = []
    cur_top: float | None = None
    cur: List[BBoxWord] = []
    for w in ws2:
        if cur_top is None:
            cur_top = w.top
            cur = [w]
            continue
        if abs(w.top - cur_top) <= y_tolerance:
            cur.append(w)
        else:
            lines.append((cur_top, sorted(cur, key=lambda x: x.x0)))
            cur_top = w.top
            cur = [w]
    if cur_top is not None and cur:
        lines.append((cur_top, sorted(cur, key=lambda x: x.x0)))
    return lines


def join_words(words: Sequence[BBoxWord], *, join_gap: float = 1.2) -> str:
    if not words:
        return ""
    parts: List[str] = []
    prev_x1: float | None = None
    for w in sorted(words, key=lambda x: x.x0):
        t = str(w.text or "")
        if not t:
            continue
        if not parts:
            parts.append(t)
            prev_x1 = w.x1
            continue
        gap = (w.x0 - (prev_x1 or w.x0))
        if gap <= join_gap:
            parts[-1] = parts[-1] + t
        else:
            # 숫자/기호 연속은 무공백, 그 외는 공백 1칸
            if re.match(r"^[\)\],\.\-]$", t):
                parts[-1] = parts[-1] + t
            else:
                parts.append(t)
        prev_x1 = w.x1
    return _norm_space(" ".join(parts))


def join_words_in_xrange(line_words: Sequence[BBoxWord], xr: XRange) -> str:
    picked = [w for w in line_words if xr.contains_word(w)]
    # 한글 문서에서는 글자 단위로 분절되는 경우가 많아 무공백 결합이 기본
    s = join_words(picked, join_gap=1.2)
    return s


def estimate_dx_by_date_anchor(lines: Sequence[Tuple[float, List[BBoxWord]]], template: TechTemplate) -> float:
    """
    시작일/종료일 날짜 라인의 대표 x0를 관측해 템플릿 x 오프셋(dx)을 추정한다.
    - 정확도가 애매하면 0으로 둔다(검증 단계에서 폴백).
    """
    xs: List[float] = []
    for _top, ws in lines:
        date_txt = _compact(join_words_in_xrange(ws, template.col_date))
        if _DATE_STRICT.match(date_txt):
            # 날짜 라인에서 가장 왼쪽 글자의 x0를 앵커로 취한다.
            left = min((w.x0 for w in ws if template.col_date.contains_word(w)), default=None)
            if left is not None:
                xs.append(float(left))
    if len(xs) < 2:
        return 0.0
    xs.sort()
    median = xs[len(xs) // 2]
    dx = median - template.anchor_date_x0
    # 과도한 보정은 오히려 위험하므로 클램프
    if abs(dx) > 25.0:
        return 0.0
    return float(dx)


def extract_blocks_with_y(lines: Sequence[Tuple[float, List[BBoxWord]]], template: TechTemplate) -> List[ParsedBlock]:
    """
    bbox 라인 스트림에서 (시작일, '~' 라인, 종료일, (xx일)(xx일)) 블록을 추출한다.
    """
    blocks: List[ParsedBlock] = []
    i = 0
    while i < len(lines):
        top, ws = lines[i]
        start_txt = _compact(join_words_in_xrange(ws, template.col_date))
        if not _DATE_STRICT.match(start_txt):
            i += 1
            continue
        start_iso = _yyyy_mm_dd_to_iso(start_txt)
        if not start_iso:
            i += 1
            continue

        # 다음 N줄 내 '~' 라인 탐색
        tilde_idx: int | None = None
        # FIX: 일부 PDF는 시작일~'~' 사이에 줄바꿈/셀 병합으로 6줄보다 더 벌어질 수 있다.
        #      (이 경우 블록이 통째로 누락되어 페이지별 건수 불일치로 이어짐)
        for k in range(i + 1, min(len(lines), i + 13)):
            _t2, ws2 = lines[k]
            # '~' 토큰은 대체로 x~50 부근에 존재
            has_tilde = any((w.text or "").strip() == "~" for w in ws2)
            if has_tilde:
                tilde_idx = k
                break
        if tilde_idx is None:
            i += 1
            continue

        # '~' 다음 N줄 내 종료일(날짜) 라인 탐색
        end_idx: int | None = None
        end_iso = ""
        for k in range(tilde_idx + 1, min(len(lines), tilde_idx + 13)):
            _t3, ws3 = lines[k]
            end_txt = _compact(join_words_in_xrange(ws3, template.col_date))
            if _DATE_STRICT.match(end_txt):
                end_iso = _yyyy_mm_dd_to_iso(end_txt)
                end_idx = k
                break
        if end_idx is None:
            i = tilde_idx + 1
            continue

        # 일수 토큰은 종료일 이후 1~20줄 내에 등장하는 경우가 많다.
        window_txt: List[str] = []
        for k in range(end_idx + 1, min(len(lines), end_idx + 22)):
            _t4, ws4 = lines[k]
            # 전체 라인 텍스트(열 분리 없이)로 토큰 탐색
            line_full = _compact(join_words(ws4))
            if line_full:
                window_txt.append(line_full)
            toks = _DAYS_TOKEN_RE.findall("".join(window_txt))
            if len(toks) >= 2:
                인정 = (toks[0] or "").replace(",", "").strip()
                참여 = (toks[1] or "").replace(",", "").strip()
                blocks.append(
                    ParsedBlock(
                        start_iso=start_iso,
                        end_iso=end_iso,
                        인정일수=인정,
                        참여일수=참여,
                        y_start=float(top),
                        y_tilde=float(lines[tilde_idx][0]) if tilde_idx is not None else None,
                        i_start=int(i),
                        i_tilde=int(tilde_idx) if tilde_idx is not None else None,
                        i_end=int(end_idx),
                        i_days_end=int(k),
                    )
                )
                i = k + 1
                break
        else:
            # 일수 토큰이 없으면 빈 값으로라도 블록은 유지(후단 폴백 검증에서 걸러짐)
            blocks.append(
                ParsedBlock(
                    start_iso=start_iso,
                    end_iso=end_iso,
                    인정일수="",
                    참여일수="",
                    y_start=float(top),
                    y_tilde=float(lines[tilde_idx][0]) if tilde_idx is not None else None,
                    i_start=int(i),
                    i_tilde=int(tilde_idx) if tilde_idx is not None else None,
                    i_end=int(end_idx),
                    i_days_end=None,
                )
            )
            i = end_idx + 1
    return blocks


def _is_bonsa_like_name(s: str) -> bool:
    t = _compact(_norm_space(s or ""))
    if not t:
        return False
    # '본사' 자체 또는 '본사...' 파생(예: '본사대기', '본사(OO부서)' 등)을 허용
    return t == "본사" or t.startswith("본사")


def _looks_like_overview_sentence(s: str) -> bool:
    """
    사업명으로 보기 어려운 '개요형 문장'을 탐지한다.
    - 예: 관로조사및탐사(D100㎜ L13.0㎞...), 급수전..., 4,722전
    """
    raw = _norm_space(s or "")
    t = _compact(raw)
    if not t:
        return False
    # 규격/단위/길이/직경 토큰이 섞이면 개요일 확률이 높다.
    if re.search(r"(D\d+|L\d+(\.\d+)?|Ø\d+)", raw, flags=re.IGNORECASE):
        return True
    if re.search(r"[㎜㎞㎡㎥]", raw):
        return True
    # 콤마가 다수이고 숫자 비율이 높으면 수량 나열형 개요로 간주
    if raw.count(",") >= 2 and re.search(r"\d", raw):
        return True
    # '(...)' 정보가 길게 붙는 형태도 개요에 흔함
    if raw.count("(") >= 1 and re.search(r"\d", raw) and len(raw) >= 25:
        return True
    return False


def _looks_like_header_or_footer_boilerplate(s: str) -> bool:
    """
    텍스트 기반 파서(page_2/page_3)의 _is_footer_or_header_line()와 동일한 목적.
    템플릿(레이아웃) 파싱은 bbox 라인 스트림을 쓰므로, 여기서도 반복 헤더/푸터/안내문을 제거한다.
    """
    raw = _norm_space(s or "")
    if not raw:
        return True
    # 페이지 헤더/푸터/안내문
    if raw.startswith("Page :") or raw.startswith("성명 :"):
        return True
    # 섹션 헤더(텍스트 기반 파서와 동일 규칙)
    if "기술경력" in raw and ("1." in raw or raw.startswith("1")):
        return True
    if "본 증명서는 인터넷으로 발급" in raw:
        return True
    footer_keywords = [
        "발급증명서확인",
        "www.kocea.or.kr",
        "문서확인번호",
        "문서 하단",
        "문서하단",
        "바코드로",
        "위·변조",
        "위변조",
        "발급일로부터",
        "90일까지",
        "가능합니다",
        "확인해 주십시오",
        "변조 여부",
    ]
    if any(k in raw for k in footer_keywords):
        return True
    # 섹션/페이지 표식
    if raw.startswith("(") and "쪽" in raw:
        return True
    # 테이블 컬럼 헤더(짧은 문장 + 키워드 다수)
    header_keywords = [
        "사업명",
        "직무분야",
        "담당업무",
        "참여기간",
        "발주자",
        "공사종류",
        "전문분야",
        "직위",
        "공사(용역)개요",
        "책임정도",
        "공사(용역)금액",
        "적용 공법",
        "적용 융",
        "시설물 종류",
        "비고",
        "(인정일)",
        "(참여일)",
    ]
    if raw.startswith("사업명") and ("직무분야" in raw or "담당업무" in raw):
        return True
    if raw in {"참여기간", "비고", "(인정일)", "(참여일)"}:
        return True
    hit = sum(1 for k in header_keywords if k in raw)
    if hit >= 2 and len(raw) <= 90:
        return True
    return False


def _filter_lines_for_template(
    lines: Sequence[Tuple[float, List[BBoxWord]]],
    template: TechTemplate,
) -> List[Tuple[float, List[BBoxWord]]]:
    """
    템플릿 파싱 전 단계에서 불필요 라인(헤더/푸터/안내문/컬럼 헤더)을 제거한다.
    - 이 필터가 없으면, 페이지 상단 안내문이 '사업명/직무/담당업무'로 선택되는 치명 오염이 발생한다.
    """
    out: List[Tuple[float, List[BBoxWord]]] = []
    for top, ws in (lines or []):
        if not ws:
            continue
        full = join_words(ws)
        # 날짜 라인은 앵커/블록 탐지를 위해 유지(보일러플레이트 오인 방지)
        date_txt = _compact(join_words_in_xrange(ws, template.col_date))
        if _DATE_STRICT.match(date_txt):
            out.append((top, ws))
            continue
        if _looks_like_header_or_footer_boilerplate(full):
            continue
        out.append((top, ws))
    return out


def pick_best_project_line_in_range(
    lines: Sequence[Tuple[float, List[BBoxWord]]],
    i_from: int,
    i_to: int,
    template: TechTemplate,
) -> Optional[List[BBoxWord]]:
    """
    [i_from, i_to] 범위에서 사업명 열에 가장 그럴듯한 라인을 고른다.
    - '본사' 계열은 최우선.
    - 개요형 문장(규격/수량 나열)은 강하게 감점.
    """
    if not lines:
        return None
    n = len(lines)
    a = max(0, int(i_from))
    b = min(n - 1, int(i_to))
    if a > b:
        return None

    best_ws: Optional[List[BBoxWord]] = None
    best_score = -10_000

    def _score(name: str) -> int:
        s = _norm_space(name or "")
        t = _compact(s)
        if not t:
            return -10_000
        if _looks_like_header_or_footer_boilerplate(s):
            return -9_000
        if _is_bonsa_like_name(s):
            return 10_000
        if _looks_like_overview_sentence(s):
            return -500
        # 너무 일반적인 단어만 있는 경우(예: "용역", "공사")는 낮게
        if t in {"용역", "공사", "사업"}:
            return 1
        has_ko = bool(re.search(r"[가-힣]", s))
        return (10 if has_ko else 0) + min(60, len(s))

    for i in range(a, b + 1):
        _top, ws = lines[i]
        if not ws:
            continue
        nm = join_words_in_xrange(ws, template.col_project_name)
        sc = _score(nm)
        if sc > best_score:
            best_score = sc
            best_ws = ws
            if best_score >= 10_000:
                break
    return best_ws


def pick_best_project_line_in_range_with_index(
    lines: Sequence[Tuple[float, List[BBoxWord]]],
    i_from: int,
    i_to: int,
    template: TechTemplate,
) -> Tuple[int | None, Optional[List[BBoxWord]], str, int]:
    """
    pick_best_project_line_in_range의 확장 버전.
    반환: (best_index, best_ws, best_name, best_score)
    """
    if not lines:
        return None, None, "", -10_000
    n = len(lines)
    a = max(0, int(i_from))
    b = min(n - 1, int(i_to))
    if a > b:
        return None, None, "", -10_000

    best_i: int | None = None
    best_ws: Optional[List[BBoxWord]] = None
    best_name = ""
    best_score = -10_000

    for i in range(a, b + 1):
        _top, ws = lines[i]
        if not ws:
            continue
        nm = join_words_in_xrange(ws, template.col_project_name)
        sc = _project_name_score(nm)
        if sc > best_score:
            best_score = sc
            best_ws = ws
            best_i = i
            best_name = nm
            if best_score >= 10_000:
                break
    return best_i, best_ws, best_name, int(best_score)


def _project_name_score(name: str) -> int:
    s = _norm_space(name or "")
    t = _compact(s)
    if not t:
        return -10_000
    if _looks_like_header_or_footer_boilerplate(s):
        return -9_000
    if _is_bonsa_like_name(s):
        return 10_000
    if _looks_like_overview_sentence(s):
        return -500
    if t in {"용역", "공사", "사업"}:
        return 1
    has_ko = bool(re.search(r"[가-힣]", s))
    return (10 if has_ko else 0) + min(60, len(s))


def pick_nearest_line_above(lines: Sequence[Tuple[float, List[BBoxWord]]], y: float, *, max_back: int = 14) -> Optional[List[BBoxWord]]:
    # lines 는 top 오름차순이라고 가정
    idx = None
    for i, (top, _ws) in enumerate(lines):
        if top >= y:
            idx = i
            break
    if idx is None:
        idx = len(lines)
    for j in range(idx - 1, max(-1, idx - 1 - max_back), -1):
        _top, ws = lines[j]
        if ws:
            return ws
    return None


def pick_best_project_line_above(
    lines: Sequence[Tuple[float, List[BBoxWord]]],
    y: float,
    template: TechTemplate,
    *,
    max_back: int = 18,
) -> Optional[List[BBoxWord]]:
    """
    시작일 라인 위에서 '사업명' 열에 가장 그럴듯한 라인을 고른다.
    FIX: 일부 페이지에서 바로 위 라인이 '용역' 같은 꼬리만 남는 경우가 있어,
         단순 nearest 선택이 사업명 오염/누락을 만든다.
    """
    # candidates: (score, ws)
    best_ws: Optional[List[BBoxWord]] = None
    best_score = -1

    idx = None
    for i, (top, _ws) in enumerate(lines):
        if top >= y:
            idx = i
            break
    if idx is None:
        idx = len(lines)

    def _score(name: str) -> int:
        s = _norm_space(name or "")
        t = _compact(s)
        if not t:
            return 0
        # 본사는 최우선
        if _is_bonsa_like_name(s):
            return 10_000
        # 개요형 문장은 강하게 배제
        if _looks_like_overview_sentence(s):
            return -500
        # 너무 일반적인 단어만 있는 경우(예: "용역", "공사")는 낮게
        if t in {"용역", "공사", "사업"}:
            return 1
        # 최소 길이 + 한글 포함을 선호
        has_ko = bool(re.search(r"[가-힣]", s))
        return (10 if has_ko else 0) + min(40, len(s))

    for j in range(idx - 1, max(-1, idx - 1 - max_back), -1):
        _top, ws = lines[j]
        if not ws:
            continue
        nm = join_words_in_xrange(ws, template.col_project_name)
        sc = _score(nm)
        if sc > best_score:
            best_score = sc
            best_ws = ws
        # 충분히 좋은 후보를 찾았으면 조기 종료
        if best_score >= 28:
            break
    return best_ws


def parse_tech_page_by_template(words: Sequence[Any]) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    """
    기술경력 1페이지를 템플릿으로 파싱한다.
    반환: (rows, meta)  — meta에는 검증용 정보 포함
    """
    tpl0 = TechTemplate()
    lines0 = group_words_to_lines(words, y_tolerance=2.0)
    dx = estimate_dx_by_date_anchor(lines0, tpl0)
    tpl = tpl0.shifted(dx) if abs(dx) > 0.1 else tpl0
    lines = _filter_lines_for_template(lines0, tpl)

    blocks = extract_blocks_with_y(lines, tpl)
    out_rows: List[Dict[str, str]] = []

    # 다음 블록의 '사업명(pre-window)'이 현재 블록의 post-window로 끼어들어
    # 한 칸 밀림(중복/치환)을 만드는 케이스가 있어, post 후보 탐색에서 마지막 N줄을 제외한다.
    _POST_GUARD_BACK = 6

    for bi, b in enumerate(blocks):
        # 1) 사업명/직무/담당업무
        # FIX: 일부 문서는 사업명 라인이 날짜/일수 블록 '아래'에 위치한다.
        #      (예: 1989.03.29~1989.07.14 다음 줄에 '본사')
        # 핵심: 사업명 라인은 문서/엔진에 따라 블록 '위'(시작일 직전) 또는 '아래'(일수 직후)에 올 수 있으므로,
        #       이전/다음 블록 경계 안에서만 후보를 찾고(크로스-블록 오염 방지), 점수로 최종 선택한다.
        next_i_start = blocks[bi + 1].i_start if (bi + 1) < len(blocks) else len(lines)
        prev_end = None
        if bi > 0:
            pb = blocks[bi - 1]
            prev_end = (pb.i_days_end if pb.i_days_end is not None else pb.i_end)
        # pre-window: (이전 블록 끝 + 1) ~ (현재 시작일 - 1)
        if prev_end is None:
            pre_from = max(0, b.i_start - 18)
        else:
            pre_from = int(prev_end + 1)
        pre_to = int(b.i_start - 1)
        pre_i, ws_pre, nm_pre, pre_sc = pick_best_project_line_in_range_with_index(
            lines, pre_from, pre_to, tpl
        )

        # post-window: (현재 블록 끝 + 1) ~ (다음 시작일 - 1)
        post_from = int((b.i_days_end + 1) if b.i_days_end is not None else (b.i_end + 1))
        raw_post_to = int(max(post_from, next_i_start - 1))
        # 다음 블록 시작 직전 구간은 다음 사업명(pre 후보)이 위치하는 경우가 많으므로 제외
        post_to = int(min(raw_post_to, max(post_from - 1, next_i_start - 1 - _POST_GUARD_BACK)))
        post_i, ws_post, nm_post, post_sc = pick_best_project_line_in_range_with_index(
            lines, post_from, post_to, tpl
        )

        # 거리 기반 tie-break:
        # - pre 후보는 시작일 직전에 있을수록 신뢰
        # - post 후보는 post_from(일수 직후)에 가까울수록 신뢰
        pre_dist = abs(int(b.i_start) - int(pre_i)) if pre_i is not None else 10_000
        post_dist = abs(int(post_from) - int(post_i)) if post_i is not None else 10_000

        # 점수(내용 품질) 우선 + 거리로 미세 조정
        pre_final = (pre_sc * 100) - pre_dist
        post_final = (post_sc * 100) - post_dist

        if pre_final >= post_final:
            ws_name = ws_pre
            ws_name_i = pre_i
        else:
            ws_name = ws_post
            ws_name_i = post_i

        if ws_name is None:
            # 최후 백업(경계 계산이 실패한 경우만)
            ws_name = pick_best_project_line_above(lines, b.y_start, tpl, max_back=18)
            ws_name_i = None
        name = join_words_in_xrange(ws_name or [], tpl.col_project_name)
        jf = join_words_in_xrange(ws_name or [], tpl.col_job_field)
        duty = join_words_in_xrange(ws_name or [], tpl.col_job_duty)

        # FIX: 사업명이 줄바꿈/셀 래핑으로 다음 라인에 꼬리만 남는 경우(예: 마지막 '용역')
        # - 아래 후보 라인이 job_field/duty를 갖고 name은 비어 있는 경우가 흔하므로,
        #   name 꼬리 토큰을 인접 라인에서 보강한다.
        try:
            if name and ws_name_i is not None:
                for j in range(int(ws_name_i) + 1, min(len(lines), int(ws_name_i) + 4)):
                    _top2, ws2 = lines[j]
                    # 날짜/틸드 라인은 제외
                    if _DATE_STRICT.match(_compact(join_words_in_xrange(ws2, tpl.col_date))):
                        continue
                    if any((w.text or "").strip() == "~" for w in ws2):
                        continue
                    tail = _norm_space(join_words_in_xrange(ws2, tpl.col_project_name))
                    if not tail:
                        continue
                    if _looks_like_header_or_footer_boilerplate(tail):
                        continue
                    # 매우 짧은 꼬리(예: '용역')만 붙인다.
                    tt = _compact(tail)
                    if len(tt) <= 6 and ("용역" in tail) and (tt not in _compact(name)):
                        name = _norm_space(name + tail)
                        break
        except Exception:
            pass

        # FIX: name 라인과 job_field/duty 라인이 분리되는 경우가 있어,
        # job_field/duty가 비어 있으면 인접 라인에서 보강한다.
        try:
            if ws_name_i is not None and (not jf or not duty):
                best_jf = jf
                best_dt = duty
                best_sc = len(jf) + len(duty)
                for j in range(int(ws_name_i), min(len(lines), int(ws_name_i) + 4)):
                    _top2, ws2 = lines[j]
                    if _DATE_STRICT.match(_compact(join_words_in_xrange(ws2, tpl.col_date))):
                        continue
                    if any((w.text or "").strip() == "~" for w in ws2):
                        continue
                    jf2 = join_words_in_xrange(ws2, tpl.col_job_field)
                    dt2 = join_words_in_xrange(ws2, tpl.col_job_duty)
                    sc2 = len(jf2) + len(dt2)
                    if sc2 > best_sc:
                        best_sc = sc2
                        best_jf = jf2
                        best_dt = dt2
                jf = best_jf
                duty = best_dt
        except Exception:
            pass

        # 2) '~' 라인에서 발주자/공사종류/전문분야/직위
        issuer = ""
        wt = ""
        spec = ""
        pos = ""
        if b.y_tilde is not None:
            ws_tilde = None
            for top, ws in lines:
                if abs(top - b.y_tilde) <= 2.0:
                    ws_tilde = ws
                    break
            if ws_tilde is not None:
                issuer = join_words_in_xrange(ws_tilde, tpl.col_issuer)
                wt = join_words_in_xrange(ws_tilde, tpl.col_work_type)
                spec = join_words_in_xrange(ws_tilde, tpl.col_specialty)
                pos = join_words_in_xrange(ws_tilde, tpl.col_position)

        out_rows.append(
            {
                "사업명": name,
                "발주자": issuer,
                "공사종류": wt,
                "참여기간_시작일": b.start_iso,
                "참여기간_종료일": b.end_iso,
                "인정일수": b.인정일수,
                "참여일수": b.참여일수,
                "직무분야": jf,
                "전문분야": spec,
                "담당업무": duty,
                "책임정도": "",
                "직위": pos,
                "공사(용역)금액(백만원)": "",
                "공사(용역)개요": "",
                "적용 공법": "",
                "적용 융복합건설기술": "",
                "적용 신기술 등": "",
                "시설물 종류": "",
                "비고": "",
            }
        )

    # FIX: 일부 페이지에서 동일한 날짜 블록(시작/종료/일수)이 bbox 라인 스트림에 중복 등장해
    #      템플릿 결과가 '동일 기간 2건'으로 생성되는 경우가 있다.
    #      이 경우 다음/이전 사업명 치환(한 칸 밀림)로 이어질 수 있어, 동일 기간 블록은 1건으로 병합한다.
    def _row_quality(r: Dict[str, str]) -> int:
        score = 0
        for k in ("사업명", "발주자", "공사종류", "직무분야", "전문분야", "담당업무", "직위"):
            v = _norm_space(str((r or {}).get(k) or ""))
            if not v:
                continue
            if _looks_like_header_or_footer_boilerplate(v):
                score -= 200
                continue
            if k == "사업명" and _looks_like_overview_sentence(v):
                score -= 50
            # 길이 기반 가산(긴 텍스트가 더 정보량이 많음)
            score += min(30, len(v))
        # 공사종류는 구체적인 표현(예: '하천정비(지방)')을 약간 선호
        wt = _norm_space(str((r or {}).get("공사종류") or ""))
        if wt and ("(" in wt or ")" in wt):
            score += 5
        return int(score)

    merged: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}
    for r in out_rows:
        key = (
            str(r.get("참여기간_시작일") or ""),
            str(r.get("참여기간_종료일") or ""),
            str(r.get("인정일수") or ""),
            str(r.get("참여일수") or ""),
        )
        prev = merged.get(key)
        if prev is None:
            merged[key] = r
        else:
            # 품질 점수가 높은 쪽을 채택, 동일하면 기존 유지(안정성)
            if _row_quality(r) > _row_quality(prev):
                merged[key] = r
    out_rows = list(merged.values())

    meta = {"dx": dx, "n_blocks": len(blocks), "n_rows": len(out_rows)}
    return out_rows, meta


def parse_cm_page_by_template(words: Sequence[Any]) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    """
    CM/감리 1페이지를 템플릿으로 파싱한다.
    - 일부 문서에서 col4가 '담당업무'로 쓰이기도 하고 '전문분야'로 쓰이기도 하므로
      템플릿 단계에서는 '담당업무'에 우선 넣고, 필요 시 상위 파서에서 정규화한다.
    """
    tpl0 = CmTemplate()
    lines0 = group_words_to_lines(words, y_tolerance=2.0)
    # 날짜 앵커는 동일 방식으로 dx 추정
    dx = 0.0
    try:
        # TechTemplate용 함수 재사용: col_date/anchor_date_x0만 같으면 됨
        dx = estimate_dx_by_date_anchor(lines0, TechTemplate(anchor_date_x0=tpl0.anchor_date_x0, col_date=tpl0.col_date))
    except Exception:
        dx = 0.0
    tpl = tpl0.shifted(dx) if abs(dx) > 0.1 else tpl0
    # CM도 동일한 안내문/헤더가 반복되므로 동일 필터 적용(열 경계는 동일)
    lines = _filter_lines_for_template(
        lines0,
        TechTemplate(
            anchor_date_x0=tpl.anchor_date_x0,
            col_project_name=tpl.col_project_name,
            col_job_field=tpl.col_job_field,
            col_job_duty=tpl.col_job_duty_or_specialty,
            col_issuer=tpl.col_issuer,
            col_work_type=tpl.col_work_type,
            col_specialty=tpl.col_specialty,
            col_position=tpl.col_position,
            col_date=tpl.col_date,
        ),
    )

    # 날짜 블록 추출은 기술경력과 동일 패턴(시작일-~-종료일-일수)
    blocks = extract_blocks_with_y(
        lines,
        TechTemplate(
            anchor_date_x0=tpl.anchor_date_x0,
            col_project_name=tpl.col_project_name,
            col_job_field=tpl.col_job_field,
            col_job_duty=tpl.col_job_duty_or_specialty,
            col_issuer=tpl.col_issuer,
            col_work_type=tpl.col_work_type,
            col_specialty=tpl.col_specialty,
            col_position=tpl.col_position,
            col_date=tpl.col_date,
        ),
    )

    out_rows: List[Dict[str, str]] = []
    for b in blocks:
        ws_name = pick_best_project_line_above(
            lines,
            b.y_start,
            TechTemplate(
                anchor_date_x0=tpl.anchor_date_x0,
                col_project_name=tpl.col_project_name,
                col_job_field=tpl.col_job_field,
                col_job_duty=tpl.col_job_duty_or_specialty,
                col_issuer=tpl.col_issuer,
                col_work_type=tpl.col_work_type,
                col_specialty=tpl.col_specialty,
                col_position=tpl.col_position,
                col_date=tpl.col_date,
            ),
            max_back=18,
        )
        name = join_words_in_xrange(ws_name or [], tpl.col_project_name)
        jf = join_words_in_xrange(ws_name or [], tpl.col_job_field)
        duty = join_words_in_xrange(ws_name or [], tpl.col_job_duty_or_specialty)

        issuer = ""
        wt = ""
        spec = ""
        pos = ""
        if b.y_tilde is not None:
            ws_tilde = None
            for top, ws in lines:
                if abs(top - b.y_tilde) <= 2.0:
                    ws_tilde = ws
                    break
            if ws_tilde is not None:
                issuer = join_words_in_xrange(ws_tilde, tpl.col_issuer)
                wt = join_words_in_xrange(ws_tilde, tpl.col_work_type)
                spec = join_words_in_xrange(ws_tilde, tpl.col_specialty)
                pos = join_words_in_xrange(ws_tilde, tpl.col_position)

        out_rows.append(
            {
                "사업명": name,
                "발주자": issuer,
                "공사종류": wt,
                "참여기간_시작일": b.start_iso,
                "참여기간_종료일": b.end_iso,
                "인정일수": b.인정일수,
                "참여일수": b.참여일수,
                "직무분야": jf,
                "전문분야": spec,
                "담당업무": duty,
                "책임정도": "",
                "직위": pos,
                "공사(용역)금액(백만원)": "",
                "공사(용역)개요": "",
                "적용 공법": "",
                "적용 융복합건설기술": "",
                "적용 신기술 등": "",
                "시설물 종류": "",
                "비고": "",
            }
        )

    meta = {"dx": dx, "n_blocks": len(blocks), "n_rows": len(out_rows)}
    return out_rows, meta


def is_tech_template_result_trustworthy(rows: Sequence[Dict[str, str]], meta: Dict[str, Any]) -> bool:
    """
    템플릿 결과가 믿을만한지(=기존 파서로 폴백할지) 판정한다.
    """
    if not rows:
        return False
    # 필수: 사업명 + 시작/종료일이 일정 비율 이상
    good = 0
    for r in rows:
        nm = str(r.get("사업명") or "").strip()
        st = str(r.get("참여기간_시작일") or "").strip()
        en = str(r.get("참여기간_종료일") or "").strip()
        if nm and st and en:
            good += 1
    if good / max(1, len(rows)) < 0.7:
        return False

    # 오염 탐지(대표 케이스): 사업명에 '직무분야+담당업무'가 붙는 경우를 방지하는 게 목적
    # 템플릿은 x-구간 분리이므로 이 검증이 실패하면 추출 품질이 낮다는 뜻.
    bad = 0
    for r in rows:
        nm = _compact(str(r.get("사업명") or ""))
        jf = _compact(str(r.get("직무분야") or ""))
        dt = _compact(str(r.get("담당업무") or ""))
        if jf and dt and (jf + dt) and ((jf + dt) in nm):
            bad += 1
    if bad > max(1, len(rows) // 6):
        return False
    return True

