# PDF Parser 종합 진단 (운영 안정성/확장성, 정확도 최우선)

대상: 건설기술인 경력 PDF → JSON 파서 전체 프로젝트  
환경: 로컬 PC(추후 확장 가능)  
입력 특성(확정): **암호화/권한제한 PDF 없음, 스캔 이미지 PDF 없음(텍스트 추출 가능 전제)**  
목표(확정): **SLA 없음, 정확도/누락없는 파싱 우선**  
“누락 없음” 정의(확정): **필드 단위 완전성(field-level completeness)**  

---

## 1차 답변: 아키텍처 관점 종합 진단 + 로드맵

### 1) 전체 진단(현재 단계/총평/리스크 3)

- **현재 단계**: 기능은 상당히 진척(페이지/섹션 파서 분리, `DocumentContext` 캐시, 배치/검증 스크립트 존재)했지만, 운영 기준(재현성/관측성/품질 게이트)으로는 **프로토타입~초기 운영** 단계.
- **총평**: “정확도 최우선”을 위해 휴리스틱/후처리/검증을 적극 적용한 점은 강점. 다만 현재는 정확도 로직과 실행/IO/검증/리포팅이 `main.py` 중심으로 결합되어 있어, **확장/개선 시 회귀 리스크**가 큼.

**Top 리스크 3가지**
1) **의존성 재현성(P0)**: `excel_export.py`는 `openpyxl`을 사용하나 `requirements.txt`에 누락되어 있어, 새 환경에서 **즉시 런타임 실패** 가능.
2) **조용한 누락(정확도 목표와 충돌)(P0/P1)**: 예외를 삼키고 빈 문자열/빈 리스트로 폴백하는 패턴이 많아(특히 추출 레이어), 실제 운영에서 **“실패가 누락으로 변환”**될 수 있음.
3) **성능/리소스 병목(P1)**: PyMuPDF word 추출이 페이지 호출마다 `fitz.open()`을 수행하는 구조(문서 open/close 반복)로 보이며, 다중 PDF 처리 시 IO/CPU 병목이 커질 수 있음.

---

### 2) 기능/진행도(구현 상태 및 한계)

**구현 상태(강점)**
- 페이지/섹션별 파서 분리: `parsers/page_1_parser.py`, `page_2_parser.py`, `page_3_parser.py`
- 컨텍스트 캐시: `DocumentContext`가 `extract_text()`/`extract_tables()`/word 추출 결과를 캐시
- 검증 로직 존재: 섹션 건수 검증 + 기술경력 페이지별 `~` 기반 교차검증 등
- 배치/검증 스크립트 존재: `batch_parse.py`, `verify_parsing.py`

**한계**
- 검증이 “콘솔 출력” 중심이며, 실패를 **명확한 상태로 격상(Exit code/품질 게이트)**하지 않아 자동 판정이 약함.
- “누락 없음”이 **필드 단위 완전성**인데, 현재 검증은 “건수/일부 필드(사업명)” 중심이어서 목표 정의와 불일치.

---

### 3) 아키텍처 및 구조(책임 분리/유지보수성)

**현재 구조 요약**
- `main.py`: 오케스트레이션 + 후처리 + 검증 + 저장 + CLI까지 혼재(대형 파일)
- `parsers/`: 페이지/섹션별 파서 + 레이아웃 추출 유틸
- `excel_export.py`: 결과를 xlsx로 내보내는 후처리

**핵심 구조 리스크**
- `main.py` 결합도가 높아 변경 시 회귀 범위가 큼(정확도 우선 시스템에서 치명적).
- `page_2_parser.py`가 매우 큰 파일로 보이며, 장기적으로 룰/휴리스틱이 누적될수록 복잡도가 폭증할 가능성이 큼.

---

### 4) 기술적 리스크(중요)

#### A. 오류 발생 지점(정확도 우선 관점)
- **P0: 의존성 누락**: `openpyxl`이 `requirements.txt`에 없음 → 엑셀 저장 기능은 즉시 실패할 수 있음.
- **P0/P1: 예외 삼킴(조용한 누락)**:
  - `DocumentContext.get_text/get_tables/get_word_lines/get_words` 계열에서 실패 시 `""`/`[]`로 폴백하는 방식은 “크래시 방지”에는 유리하나, 정확도 목표에서는 **결함을 숨길 위험**이 큼.
  - 권장: 폴백하되 **반드시 structured error를 누적**하고 최종 결과의 `_파싱오류`에 기록.

#### B. 리소스/파일핸들/메모리
- `DocumentContext`는 pdfplumber open을 문서당 1회로 묶는 점은 좋음.
- PyMuPDF 추출은 문서 핸들을 재사용하지 않으면, 다중 PDF/다페이지에서 open/close 반복으로 병목 가능.

#### C. 엣지 케이스(확정 전제 반영)
- 암호화/권한제한/스캔 이미지는 **없음(확정)** → OCR/암호 해제는 범위 밖.
- 대신 현실적인 리스크는 다음 쪽:
  - 특정 페이지에서 `extract_text()`가 부분 실패(빈 문자열)하거나 표 추출이 깨지는 케이스
  - 동일 템플릿이더라도 폰트/줄바꿈/좌표가 달라 휴리스틱이 흔들리는 케이스

---

### 5) 린트/정적 분석(PEP8/타입/복잡도/Dead Code)
- 타입 힌트는 일부 존재하나, 전체적으로 dict/list 중심이라 정적 분석의 이점이 제한됨.
- `sys.path.insert(...)`가 여러 파일에 반복되어 패키징/실행 위치에 민감(운영/확장 시 취약).
- 대형 파서 파일(`page_2_parser.py`)은 함수 복잡도/사이드이펙트가 커지기 쉬워, 룰 추가 시 회귀 위험이 큼.

---

### 6) 성능 및 병목(정확도 우선 + 현실적인 최적화)
SLA가 없으므로 “극단적 최적화”보다 “재현성/정확도/관측성” 우선이 타당합니다. 다만 정확도 개선을 위한 반복 실행/배치 검증을 위해 아래 병목은 제거 가치가 큽니다.

- **문서 단위 PyMuPDF 핸들 재사용(P1)**: word 추출이 여러 번 호출되면 open/close 비용이 커짐.
- **섹션 시작 페이지 탐색 캐시(P1)**: 전 페이지 텍스트 스캔은 1회만 수행하고 결과를 재사용.
- **캐시 전략**:
  - text/tables/words/lines 캐시는 유지하되, 캐시 미스/예외를 `_파싱오류`에 반드시 기록
  - 배치 처리 시 문서 단위 캐시 해제(컨텍스트 종료)로 메모리 상한을 제어

---

### 7) 테스트 전략(정확도/필드 완전성 중심)

#### A. 품질 게이트 정의(권장)
“누락 없음 = 필드 단위 완전성”을 운영적으로 측정 가능하게 만들기 위해, 다음 3단계를 권장합니다.

1) **스키마 키 존재**: 최상위 키/섹션 키가 모두 존재 (`validate_output`의 required_keys 확장/고도화)
2) **레코드 필수 필드 non-empty**: 섹션별 레코드의 필수 필드가 비어 있지 않음  
   - 예: `기술경력`의 `사업명`, `참여기간_시작일`, `참여기간_종료일` 등  
   - 예: `학력`의 `학교명`, `졸업일` 등(정의 필요)
3) **추출 결함(예외/폴백) 0**: `_파싱오류`에 “추출 실패/예외”가 남지 않음(또는 허용 목록만 통과)

#### B. 단위 테스트(추천 리스트)
- `DocumentContext` 캐시 동작 및 예외 수집(추출 실패 시 errors 누적 여부)
- `layout_extractor.words_to_lines`: 경계값(y_tolerance/join_gap) 회귀
- `count_career_rows_by_tilde`: 플레이스홀더 날짜(`****.**.**`) 케이스 회귀

#### C. 통합 테스트(추천 리스트)
- 고정 샘플 PDF 20~50개에 대해:
  - `_검증` 불일치 0
  - `_파싱오류` 0(또는 허용된 경미 경고만)
  - 섹션별 필수 필드 공백률 0%
- 회귀 테스트는 “룰 추가 시 기존 샘플에서 완전성 저하가 없는지”를 자동 감지

---

### 8) 개선 로드맵(P0~P3)

#### P0 (즉시)
- **의존성 고정/재현성**: `requirements.txt`에 `openpyxl` 추가(또는 잠금 도입).
- **조용한 누락 제거**: 추출 단계 예외/폴백을 **반드시 structured error로 기록**하고 `_파싱오류`에 포함.
- **필드 완전성 검증 도입**: 섹션별 “필수 필드” 정의 및 자동 검증(실패 시 exit code / 상태 표시).

#### P1 (단기)
- PyMuPDF 문서 핸들 **문서당 1회 open 재사용**(컨텍스트 소유/close).
- `main.py` 분리: 파싱(추출) / 후처리 / 검증(품질 게이트) / 출력(저장) 레이어 분할.
- `print` 중심 로그를 `logging`으로 전환(파일/JSON 로그 옵션).

#### P2 (중기)
- 결과 스키마를 `TypedDict` 또는 `pydantic`로 명시하고 필드 완전성/타입 검증 자동화.
- 휴리스틱 룰을 버전 관리(룰 세트 버전 + 샘플 평가 리포트)하여 회귀를 통제.

#### P3 (장기)
- 로컬→서버 확장 시: 문서 단위 작업 격리, 재시도/타임아웃, 실패 문서 격리(DLQ) 등 파이프라인화.

---

## 2차 답변: P0/P1 이슈 상세 기술 분석 + 리팩터링 포인트

### (P0) `openpyxl` 의존성 누락
- 현 상태에서 `--excel` 기본이 켜져 있고, `excel_export.py`는 `openpyxl`을 import함.
- `requirements.txt`에 없으면 새 환경에서 엑셀 저장이 즉시 실패 → 운영/배포 재현성 붕괴.
- **조치**: `requirements.txt`에 `openpyxl` 추가 + 버전 고정 전략 도입 권장.

### (P0/P1) “조용한 누락” 패턴(정확도 목표와 정면 충돌)
- 추출 실패를 `""`/`[]`로 바꾸면, 결과는 “그럴듯한 빈값”으로 저장되며 문제를 늦게 발견하게 됨.
- **조치**:
  - 폴백은 유지하되, 반드시 다음 메타를 함께 기록:
    - stage(text/tables/words/lines), page(1-based), engine, error(repr), (가능하면 traceback 요약)
  - 최종 JSON의 `_파싱오류`에 결합해 배치에서 자동 판정 가능하게.

### (P1) PyMuPDF open/close 반복 병목
- word/line 기반 보정이 많을수록 PyMuPDF 호출이 잦아질 가능성이 큼.
- **조치**: `DocumentContext`가 `fitz.Document`를 소유하고 문서당 1회만 open.

### (P1) “필드 단위 완전성”을 코드 레벨로 고정(품질 게이트)
- 현재 검증은 건수/사업명 중심.
- **조치**:
  - 섹션별 필수 필드를 명시하고, 공백/타입 오류/형식 오류를 `_파싱오류`로 수집.
  - 배치/CLI에서 “완전성 실패 시 exit code != 0” 옵션 제공(정확도 우선 운영에 적합).

---

## 3차 답변: 개선 코드 예시(Refactoring Sample)

아래는 “문서당 PyMuPDF 1회 open + 추출 예외 구조화” 아이디어 샘플입니다.  
프로젝트 적용 시에는 기존 `layout_extractor.Word` 정규화 함수와 결합하는 형태를 권장합니다.

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

import pdfplumber


@dataclass
class DocumentContext:
    pdf_path: str
    pdf: pdfplumber.PDF
    pages: List[Any]

    # NEW: 문서 단위 PyMuPDF 핸들(옵션)
    _fitz_doc: Any | None = None

    # NEW: 추출 단계 오류 누적(조용한 누락 방지)
    errors: List[dict] = field(default_factory=list)

    _text_cache: Dict[int, str] = field(default_factory=dict)
    _words_cache: Dict[Tuple[int, str], List[Any]] = field(default_factory=dict)

    @classmethod
    def open(cls, pdf_path: str, *, enable_fitz: bool = True) -> "DocumentContext":
        pdf = pdfplumber.open(pdf_path)
        ctx = cls(pdf_path=pdf_path, pdf=pdf, pages=list(pdf.pages))
        if enable_fitz:
            try:
                import fitz  # PyMuPDF
                ctx._fitz_doc = fitz.open(pdf_path)  # 문서당 1회
            except Exception as e:
                ctx.errors.append({"stage": "fitz.open", "page": -1, "error": repr(e)})
                ctx._fitz_doc = None
        return ctx

    def close(self) -> None:
        try:
            if self._fitz_doc is not None:
                self._fitz_doc.close()
        except Exception:
            pass
        try:
            self.pdf.close()
        except Exception:
            pass

    def __enter__(self) -> "DocumentContext":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @property
    def total_pages(self) -> int:
        return len(self.pages)

    def get_text(self, page_idx: int) -> str:
        if page_idx in self._text_cache:
            return self._text_cache[page_idx]
        if page_idx < 0 or page_idx >= self.total_pages:
            self._text_cache[page_idx] = ""
            return ""
        page = self.pages[page_idx]
        try:
            txt = page.extract_text() or ""
        except Exception as e:
            self.errors.append({"stage": "pdfplumber.extract_text", "page": page_idx + 1, "error": repr(e)})
            txt = ""
        self._text_cache[page_idx] = txt
        return txt

    def get_words(self, page_idx: int, *, engine: str = "auto") -> List[Any]:
        key = (page_idx, (engine or "auto"))
        if key in self._words_cache:
            return self._words_cache[key]

        eng = (engine or "auto").lower().strip()
        out: List[Any] = []

        # 1) PyMuPDF 우선(가능할 때만)
        if eng in {"auto", "pymupdf"} and self._fitz_doc is not None:
            try:
                if 0 <= page_idx < self._fitz_doc.page_count:
                    page = self._fitz_doc.load_page(page_idx)
                    out = page.get_text("words") or []
            except Exception as e:
                self.errors.append({"stage": "fitz.get_text(words)", "page": page_idx + 1, "error": repr(e)})
                if eng == "pymupdf":
                    out = []

        # 2) pdfplumber 폴백
        if not out and 0 <= page_idx < self.total_pages:
            try:
                out = self.pages[page_idx].extract_words() or []
            except Exception as e:
                self.errors.append({"stage": "pdfplumber.extract_words", "page": page_idx + 1, "error": repr(e)})
                out = []

        self._words_cache[key] = out
        return out
```

### 이 샘플을 프로젝트에 넣을 때의 “정확도 우선” 적용 규칙
- **절대 금지**: 예외를 삼키고 결과만 저장(= 조용한 누락)
- **권장 정책**:
  - `ctx.errors`가 1건이라도 있으면 최종 `result["_파싱오류"]`에 추가
  - “필드 완전성 실패”도 `_파싱오류`에 추가(섹션/레코드 인덱스/필드명/원인)

