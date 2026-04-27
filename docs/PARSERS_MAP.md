## parsers/ 정리 맵 (런타임 기준)

이 문서는 `main.py:parse_full_document()`의 **실제 실행(import/호출) 흐름**을 기준으로,
`parsers/` 안 파일들이 “코어/의존/실험” 중 어디에 속하는지 정리합니다.

### 폴더 구조(현행)

- `parsers/core/`: 런타임 필수 코어(페이지 파서/오케스트레이션)
- `parsers/utils/`: 코어가 공유하는 유틸(표/레이아웃/휴리스틱/공용 헬퍼)
- `parsers/experimental/`: 레거시/실험 코드(런타임 기본 경로에서는 비필수)
- `parsers/*.py`: **호환용 re-export(프록시)** 모듈(기존 import 경로 유지)

### 실행 콜 그래프(핵심)

- **엔트리**
  - `main.py: main()` → `_parse_one_pdf()` → `parse_full_document(pdf_path)`

- **컨텍스트**
  - `parsers/core/document_context.py: DocumentContext.open()`  
    - pdfplumber(필수) + fitz/PyMuPDF(가능하면) 핸들을 열고 텍스트/표/words 캐시 제공

- **제1쪽(1/4)**
  - `parsers/core/page_1_parser.py: parse_page_1(ctx, page_num=0)`
    - 내부에서 `parsers/core/section_parsers.py`의 섹션 파서 호출

- **제2쪽 기술경력(2/4)**
  - 현재 메인 경로: `parsers/core/page_2_parser.py: parse_page_2(ctx, page_idx)`
    - **표 기반 단독 파서(현행)**
    - 표 기반 코어는 `parsers/utils/tech_career_table_only_core.py`에 있음
  - 공용 유틸(2~3쪽 공용):
    - `parsers/utils/tech_career_common.py: merge_cross_page_tech_overviews`
    - `parsers/utils/tech_career_common.py: _is_footer_or_header_line`
    - `parsers/utils/tech_career_common.py: _strip_tail_job_duty`

- **제3쪽 CM/감리경력(3/4)**
  - `parsers/core/page_3_parser.py: parse_page_3(ctx, page_idx)`
    - 내부에서 2~3쪽 공용 유틸(`parsers/utils/tech_career_common.py`)을 재사용
    - 과거 레거시 page2 유틸 의존은 제거했고, 필요한 로직은 `parsers/utils/page_2_flow_utils.py`로 재구현

- **요약(4/4)**
  - `parsers/core/page_summary_parser.py: parse_page_summary(ctx, summary_start)`

- **품질 게이트**
  - `parsers/core/quality_gate.py: check_field_completeness(result)`

---

### 분류: 코어 / 의존 / 실험(또는 조건부)

#### 코어(삭제/이동 금지)

- `parsers/core/document_context.py`
- `parsers/core/page_1_parser.py`
- `parsers/core/page_2_parser.py`
- `parsers/core/page_3_parser.py`
- `parsers/core/page_summary_parser.py`
- `parsers/core/section_parsers.py`
- `parsers/core/quality_gate.py`

#### 코어 의존(공용 유틸)

- `parsers/utils/table_settings.py`
- `parsers/utils/table_career_parser.py`
- `parsers/utils/layout_extractor.py` (조건부로 사용될 수 있음)
- `parsers/utils/personal_info.py`
- `parsers/utils/issuer_reference.py`
- `parsers/utils/worktype_classifier.py`
- `parsers/utils/tech_career_heuristics.py`
- `parsers/utils/tech_career_table_only_core.py`
- `parsers/utils/tech_career_common.py`
- `parsers/utils/page_2_flow_utils.py` (page3에서 쓰는 “흐름 사업명/표 매칭” 유틸)

#### 혼합/허브(현재도 런타임 필수)

- (해당 없음 — 공용 유틸은 `parsers/utils/`로 이동)

#### 실험/조건부 경로

- `parsers/experimental/template_table_parser.py`
  - 특정 조건에서 지연 import로 호출될 수 있는 경로가 있어, 즉시 삭제는 비추천
- `parsers/experimental/page_2_parser_legacy_impl.py`
  - 과거 레거시 2쪽 파서 구현(비교/검증/디버그 용도)
- `parsers/experimental/page_2_parser_legacy.py`
  - 위 impl을 가리키는 얇은 호환 래퍼(파일 자체는 작게 유지)

---

### 리팩터링 메모(B)

- 2쪽(기술경력) 표 기반 추출/후처리는 `parsers/utils/tech_career_table_only_core.py`로 분리되어 있고,
  런타임 파서는 `parsers/core/page_2_parser.py`가 담당합니다.
- 레거시 2쪽 파서는 `parsers/experimental/page_2_parser_legacy_impl.py`에 보관하되,
  `parsers/experimental/page_2_parser_legacy.py`는 얇은 래퍼로 유지합니다.

