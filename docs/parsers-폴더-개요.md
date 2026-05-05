# `parsers` 패키지 개요

건설기술인 경력증명서 PDF를 JSON 등으로 변환할 때, 페이지·섹션·표 단위로 동작하는 파싱 코드가 `external/PDF-parser/parsers` 아래에 모여 있다. 이 문서는 **`parsers` 폴더 구조**, **import 경로(호환 심)**, 그리고 **PDF가 들어온 뒤 코드가 실행되는 순서**를 한곳에 정리한다.

---

## 파서에서 쓰는 외부 라이브러리 — PDF 추출(pdfplumber 계열)과 유사 기능

「pdfplumber와 같은 기능」은 보통 **(1) PDF 열기**, **(2) 페이지 텍스트 추출**, **(3) 표(테이블) 추출**, **(4) 단어·좌표(bbox) 기반 레이아웃** 정도로 묶인다. 아래는 **현재 이 레포 파서가 실제로 쓰는 것**과, **같은 축에서 흔히 쓰이지만 여기에는 없는 것**을 나란히 정리한 것이다.

### 이 레포에서 PDF 파싱에 실제 사용하는 패키지

| 패키지(PyPI) | import / 비고 | 역할( pdfplumber가 담당하는 범위와 비교 ) | 코드에서 사용되는 곳(대표) |
|--------------|---------------|-------------------------------------------|-----------------------------|
| **pdfplumber** | `import pdfplumber` | 문서·페이지 열기, `extract_text`, `extract_tables`, `extract_words`, 선/좌표 기반 표 설정 등 **주력 PDF 엔진** | `DocumentContext`(문서·페이지·텍스트·표), `table_settings`·`extract_tables_merged`, `section_parsers` 등 |
| **PyMuPDF** | `import fitz` (패키지 이름은 PyMuPDF) | 페이지 단위 `get_text("words")` 등 **단어+좌표** 추출이 강함. 일부 경로에서 pdfplumber보다 안정적인 bbox | `DocumentContext.open`에서 문서를 한 번 더 연 뒤 `layout_extractor`에 전달, `get_words` / `get_word_lines`의 **auto 엔진 시 PyMuPDF 우선** |
| **pdfminer.six** | (직접 import 없음) | PDF 바이트를 해석하는 **저수준** 레이어. pdfplumber가 내부적으로 사용 | pip로는 `pdfplumber` 설치 시 **전이 의존성**으로 따라옴 |

정리하면, **표·페이지 단위 텍스트·테이블 추출의 축은 pdfplumber**, **단어 bbox·라인 재구성이 필요할 때 보조로 PyMuPDF**를 함께 쓴다. 둘 다 없을 때를 가정한 **제3의 PDF 추출 라이브러리**는 파서 코어에 두지 않았다.

### PDF 처리 측면에서 pdfplumber·PyMuPDF와 **비슷한 위치**에 있는 대표 대안들 (현재 파서에서는 미사용)

같은 목적(열기·텍스트·표·좌표)을 위해 생태계에서 자주 선택되나, **`external/PDF-parser`의 `main.parse_full_document` 경로 코드에는 포함되어 있지 않다.**

| 종류 | 예시 | 한 줄 비고 |
|------|------|------------|
| 순수 Python PDF I/O·텍스트 | **pypdf**(구 PyPDF2), **pypdfium2** | 병합·분할·일부 텍스트 추출 등. 표/복잡 레이아웃은 파서가 직접 짜야 하는 경우가 많음 |
| 저수준 텍스트 | **pdfminer.six** 단독 사용 | 레이아웃까지 직접 조립할 때. 이 레포는 pdfplumber 경유로만 간접 사용 |
| 다른 고수준 래퍼 | **PyPDF2**(레거시 명칭) | pypdf 계열과 통합·이전되는 경우가 많음 |
| 표 추출 특화 | **camelot-py**, **tabula-py** | 표에 특화(그리드/스트림). 이 레포는 pdfplumber `extract_tables` + 자체 휴리스틱 |
| OCR·스캔 PDF | **Tesseract** + 전처리, **ocrmypdf** 등 | 텍스트 레이어가 없는 스캔본용. 현재 경력증명서 파서 전제와는 별층 |
| 시스템 유틸 | **Poppler** `pdftotext`, **mutool** | CLI·서브프로세스로 텍스트 덤프. 이 레포는 Python 패키지만 사용 |
| 통합 추상화 | LangChain/Unstructured 등의 **문서 로더** | 여러 형식 통합 시 사용. 여기서는 미사용 |

원하면 위 표를 기준으로 “pdfplumber만 쓰는 파이프라인” 또는 “PyMuPDF만 쓰는 파이프라인”처럼 **엔진을 바꾸는 설계**를 검토할 수 있으나, 현재 구현은 **pdfplumber + PyMuPDF 이중층**에 맞춰져 있다.

### `requirements.txt`에 있으나 PDF 파싱 코어와 거리가 있는 것(참고)

| 패키지 | 이 문서 맥락에서의 위치 |
|--------|-------------------------|
| **openpyxl** | 파싱 **결과를 Excel(.xlsx)로 저장**할 때 (`excel_export` 등). PDF 바이너리 해석과는 무관 |
| **fastapi**, **uvicorn**, **python-multipart** | `service/app.py`로 PDF를 HTTP 업로드받아 파서에 넘김 |
| **Pillow**(PIL), **pandas**, **streamlit**, **streamlit-image-coordinates** | `requirements.txt`에 포함되어 있으나, **`parsers`/ `main`/ `layout_extractor`/ `excel_export`(openpyxl)** 등 핵심 파서 코드 경로에서는 **직접 import되지 않는다**(다른 스크립트·실험 UI· 또는 pdfplumber 등의 간접 의존 가능성만 있음) |

실제 배포 환경에서 무엇이 설치되는지는 `external/PDF-parser/requirements.txt`를 기준으로 하면 된다.

---

## PDF 업로드 후 파싱되는 과정 (순서)

앱·스크립트가 PDF **파일 경로**를 넘기거나, 웹에서 멀티파트로 올리면 결국 동일한 함수 `main.parse_full_document(경로)`가 호출된다.

### 1) 파일이 들어오는 진입점

| 경로 | 동작 |
|------|------|
| **FastAPI** `service/app.py`의 `POST /parse` | 업로드된 PDF 바이트를 **임시 `.pdf` 파일**로 저장한 뒤 `parse_full_document(tmp_path)` 호출. 처리 후(기본) 임시 파일 삭제. |
| **CLI** `main.py` | 인자로 받은 경로, `--pick` / `--pick-multi`로 고른 파일, `--dir`로 모은 PDF들이 각각 `_parse_one_pdf` → `parse_full_document`로 이어진다. |

즉 “업로드”는 브라우저·다이얼로그·배치 처리와 무관하게, 마지막에는 **디스크상의 PDF 경로 하나**가 `parse_full_document`에 전달된다.

### 2) `DocumentContext`로 PDF 열기 (`core/document_context.py`)

`parse_full_document`는 곧바로 `with DocumentContext.open(pdf_path) as ctx:` 블록을 연다.

- **pdfplumber**로 문서를 열고 `pages` 목록을 유지한다.
- 가능하면 **PyMuPDF(fitz)** 도 같은 경로로 연다(좌표·보조 추출용). 실패 시 `ctx.errors`에만 기록하고 계속 진행한다.
- 이후 모든 페이지 파서는 `ctx.get_text(i)`, `ctx.get_tables(i)` 등을 쓰므로, **같은 PDF를 반복해서 열지 않고** 텍스트·표 추출 결과가 캐시된다.
- `extract_text` / `extract_tables` 등에서 예외가 나면 빈 값으로 넘기되, **`ctx.errors`에 누적**해 나중에 `_파싱오류`에 합칠 수 있게 한다.

### 3) 제1쪽 통합 파싱 — `parse_page_1` (`core/page_1_parser.py`)

- **항상 0번 페이지**(`page_num=0`)를 대상으로 인적사항·등급·국가기술자격·학력·교육훈련·상훈·벌점/제재·근무처 등 **제1쪽 전체**를 한 번에 구조화한다.
- 내부적으로 **`core/section_parsers.py`** 의 섹션별 함수(표·텍스트 조합)를 많이 사용한다.  
  (등급, 자격, 학력, 교육훈련, 상훈, 벌점/제재, 근무처 등)

### 4) 섹션이 어느 페이지에 있는지 찾기 (`main.py` 내부)

문서마다 “기술경력”“건설사업관리 및 감리경력”“분야별 참여기간 인정일”이 시작하는 페이지가 다를 수 있어, **전체 페이지 텍스트에서 키워드**로 구간을 잡는다.

- **`tech_start`**: `"1. 기술경력"` 또는 `"기술경력"` 이 포함된 첫 페이지.
- **`cm_start`**: `"2. 건설사업관리 및 감리경력"` 또는 `"건설사업관리"`·`"감리경력"` 조합.
- **`summary_start`**: `"분야별 참여기간 인정일"`, `"공사종류별 인정일수"`, `"직무/전문분야별 인정일수"` 등 요약 블록 키워드.

찾지 못하면 예전 가정(고정 페이지 번호)으로 **폴백**한다.

### 5) 기술경력 — `parse_page_2` (`core/page_2_parser.py`)

- `tech_start`부터 `tech_end`(CM 구간 시작 직전 또는 요약 직전)까지 **페이지마다** `parse_page_2(ctx, page_idx)`를 호출해 행 리스트를 이어 붙인다.
- **`utils/tech_career_common.py`** 의 `merge_cross_page_tech_overviews`로 페이지를 넘나드는 **공사(용역)개요**(`┖→` 등)를 합친다.
- 같은 사업명·기간 중복 표시용 메타(`_dup_name_period*` 등), 페이지 스냅샷 기반 검증(`_verify_tech_career_per_page_against_pdf`)이 이 단계에서 붙는다.
- 표 추출·행 파싱 로직은 **`utils/table_settings.py`**, **`utils/table_career_parser.py`**, **`utils/page_2_flow_utils.py`**, **`utils/tech_career_table_only_core.py`** 등과 연결된다.

### 6) 건설사업관리·감리 경력 — `parse_page_3` (`core/page_3_parser.py`)

- `cm_start`부터 `cm_end`(요약 페이지 직전)까지 페이지별로 `parse_page_3`를 호출해 `건설사업관리및감리경력` 리스트를 만든다.
- CM 구간 **마지막 페이지 텍스트**에서 `main`이 `_parse_cm_work_periods`, `_parse_recent_1y_service_stats`를 호출해 **`업무수행기간`**, **`용역완성비율`** 최상위 키를 채운다(행 단위 필드가 아님).

### 7) 기술경력·CM 공통 후처리 (`main.py` + `utils/tech_career_common.py`)

- 사업명에 붙은 **직무/담당업무 꼬리** 제거 등 `_strip_tail_job_duty` 등.
- **공사종류** 보정(사업명 키워드·발주자 칸 밀림 등).
- **`normalize_specialty_field` / `normalize_duty_field` / `normalize_worktype_field`** 로 전문분야·담당업무·공사종류 문자열 정리(카탈로그·휴리스틱과 연동).

### 8) 요약 페이지 — `parse_page_summary` (`core/page_summary_parser.py`)

- `summary_start`가 잡힌 경우에만 `parse_page_summary(ctx, summary_start)`를 호출한다.
- `공사종류별인정일수`, `직무전문분야별인정일수` 배열을 채운다.

### 9) 검증·오류 수집 (`main.py` + `core/quality_gate.py`)

콘솔 로그상 “[4/4]” 다음에 **항목 수 검증·기타 검증**이 이어진다.

- PDF 쪽에서 독립적으로 잡은 건수(`count_pdf_items` 등)와 파싱 결과 건수를 비교해 **`_검증`** 맵을 채우고, 불일치 시 **`_파싱오류`** 후보에 넣는다. (상훈은 `수여일`이 `YYYY-MM-DD`인 행만 유효 건수로 센다.)
- 기술경력·CM의 **사업명 누락/타입**은 `log_technical_career_field_issues`로 점검한다.
- **`ctx.errors`**(추출 단계 예외)를 그대로 병합한다.
- **`quality_gate.check_field_completeness`** 로 필수 필드 누락을 구조화해 오류 목록에 추가한다.
- 문제가 하나라도 있으면 최종 JSON에 **`_파싱오류`** 배열이 붙는다.

### 10) CLI에서만 이어지는 저장 단계

API `/parse`는 위 결과 **dict를 JSON으로 반환**하고 끝난다.  
`main.py` CLI 모드에서는 같은 `result`에 대해 **기술경력·CM 문자열 내 줄바꿈 정리** 후 `json_output` 등에 저장하고, 옵션에 따라 **엑셀 내보내기**(`excel_export`)까지 수행한다.

---

## 디렉터리 구조

| 경로 | 역할 |
|------|------|
| `parsers/*.py` (루트) | **호환용 re-export(심)**. 예전 경로(`parsers.page_1_parser` 등)를 유지하고, 실제 구현은 대부분 `core/` 또는 `utils/`로 위임한다. (`__getattr__` 심 또는 `from parsers.core... import *` 패턴.) |
| `parsers/core/` | **핵심 파서**: `DocumentContext`, 페이지 오케스트레이션(`page_*_parser`), 섹션 파서(`section_parsers`), 요약(`page_summary_parser`), 품질 게이트(`quality_gate`) 등. |
| `parsers/utils/` | **공용 유틸**: 표 추출(`table_settings`, `extract_tables_merged`), 표 기반 경력 파싱(`table_career_parser`), 레이아웃, 직무/공사종류 휴리스틱, 기술경력 공통 정규화(`tech_career_common`) 등. |
| `parsers/experimental/` | 실험·레거시 비교용(`page_2_parser_legacy*`, 테이블 탐색 스크립트 등). **기본 `main` 파이프라인에서는 쓰이지 않는 것이 일반적**이다. |

루트 심 예시:

- `parsers/section_parsers.py` → `parsers.core.section_parsers`
- `parsers/tech_career_common.py` → `parsers.utils.tech_career_common`
- `parsers/page_2_parser.py` → `parsers.core.page_2_parser`에 `__getattr__`로 위임

새 코드를 추가할 때는 **구현은 `core/` 또는 `utils/`에 두고**, 기존 import를 깨지 않아야 하면 루트에 동일 이름의 심을 두는 패턴을 따른다.

---

## `DocumentContext` (`core/document_context.py`)

PDF 한 건에 대해 다음을 한 객체로 묶는다.

- `pdfplumber`로 연 `PDF`와 `pages`
- (가능하면) PyMuPDF(`fitz`) 문서 핸들 — 좌표·이미지 등 보조 추출용
- 페이지별 **텍스트·표·단어** 캐시
- 추출 단계에서 발생한 예외를 `errors`에 누적해, 이후 `_파싱오류` 등에 반영할 수 있게 함  
  표 추출 설정은 **`utils/table_settings.py`** 의 `TABLE_SETTINGS_VERSION`, `extract_tables_merged`를 사용한다.

하위 파서는 `ctx.get_text(i)`, `ctx.get_page(i)` 등으로 동일 PDF를 반복 열지 않고 재사용한다.

---

## 페이지 단위 파서 (`core/`)

### `page_1_parser.py`

제1쪽(개인·등급·자격·학력·교육·상훈·벌점·근무처 등) 통합 파싱.

- 공개 진입점: `parse_page_1(ctx, page_num=0)`, 텍스트만으로 시험할 때 `parse_page_1_from_text(combined_text)`
- `section_parsers`의 섹션별 함수(등급, 자격, 학력, 교육훈련, 상훈 등)와 텍스트 기반 보조(근무처 등)를 조합한다.

### `page_2_parser.py`

기술경력 구간 파싱. `parse_page_2(ctx, page_num)`가 경력 행 리스트를 반환한다. 표 전용 후처리 플래그 등은 모듈 내부 상수/함수로 제어된다.

### `page_3_parser.py`

건설사업관리·감리 경력 구간 파싱.

- 공개: `parse_page_3(ctx, page_num)`
- `main` 등에서 직접 쓰는 보조: `_parse_recent_1y_service_stats`, `_parse_cm_work_periods` 등

### `page_summary_parser.py`

「분야별 참여기간 인정일」 요약 블록(공사종류별·직무/전문분야별 인정일수 등)을 텍스트 기반으로 파싱한다.

- `parse_page_summary(ctx, page_num)` → `공사종류별인정일수`, `직무전문분야별인정일수` 키를 가진 dict
- 긴 라벨이 줄바꿈으로 쪼개지는 경우를 `_stitch_wrapped_left_labels_in_section` 등으로 보정한다.

---

## 섹션 파서 (`core/section_parsers.py`)

모듈 docstring 기준으로 **제1쪽 섹션별** 로직이 한 파일에 집중되어 있다. `pdfplumber` 페이지 객체와 `extract_tables_merged` 등을 사용한다.

### 표·섹션 탐색 헬퍼

- `find_column_index`, `find_section_header`, `find_next_section_header` — 헤더 키워드로 열·블록 경계를 찾는다.

### 주요 공개 함수(요약)

| 함수 | 설명 |
|------|------|
| `parse_grade_info` | 설계·시공 / 건설사업관리 / 품질관리 등급, 직무분야·전문분야. `field_catalog`로 전문분야 정규화. |
| `parse_license_info` | 국가기술자격(종목·합격일·등록번호). 중복 표 순회 시 `merge_duplicate_license_records_by_qualification`로 정리. |
| `parse_education_info` | 학력(학위·상태, 학교명 이전/현재 분리, 학과 이어붙임 등). |
| `parse_training_info` | 교육훈련 — **표에서 과정명**, **텍스트에서 기관·교육인정여부**를 결합하는 하이브리드 방식. |
| `parse_award_info` | 상훈. 표 추출 + 텍스트 폴백, 병합셀·멀티라인·수여기관/종류및근거 경계 등 다수의 후처리 함수로 안정화. |
| `parse_penalty_and_sanction_info` | 벌점 및 제재사항(dict: `벌점`, `제재사항`). |
| `parse_workplace_info` | 근무처(근무기간·상호, 2세트 컬럼 대응). |

### 상훈 관련(품질·진단용)

`main.py`의 상훈 기대 건수 추정 등에서 사용:

- `extract_award_section_text`, `count_award_data_lines_in_section_text`
- `_find_award_table_header_idx`, `_is_award_table_boundary_row`

상훈 블록은 정규식·토큰 목록(`_AWARD_TYPE_TOKENS` 등)이 많아, PDF 레이아웃 편차에 맞춘 점진적 개선이 이 파일에 누적되어 있다.

---

## 기술경력 공통 (`utils/tech_career_common.py`)

원래 `page_2_parser`에 있던 **`main`·`page_3`이 공유하는 유틸**을 분리한 모듈이다.

- **필드 정규화**: `normalize_specialty_field`, `normalize_duty_field`, `normalize_worktype_field`  
  - 전문분야는 `field_catalog.best_match_specialty`, 특허/신기술 문구 보존, `*`로 이어진 직무 꼬리 복원 등.
- **라인 분류**: `_is_footer_or_header_line`, `_strip_tail_job_duty` — 사업명과 푸터/헤더, 직무분야 꼬리 분리.
- **페이지 경계**: `extract_tech_overview_continuation_from_page_text`, `merge_cross_page_tech_overviews` — `┖→` 등으로 이어지는 **공사(용역)개요**를 다음 페이지 텍스트에서 이어 붙인다.

의존성: `DocumentContext`, `tech_career_heuristics`, 프로젝트 루트의 `field_catalog` 등.

---

## 기타 `utils/` 모듈(역할만 간단히)

| 모듈 | 역할 |
|------|------|
| `table_settings.py` | `pdfplumber` 표 추출 전략(가상 세로선, lines/text 전략), `extract_tables_merged`, `table_set_has_header_signals`. |
| `table_career_parser.py` | 기술경력 표 파싱 코어. |
| `layout_extractor.py` | 좌표·레이아웃 기반 보조 추출. |
| `tech_career_heuristics.py` | 직무분야 힌트 등 YAML/데이터 기반 휴리스틱 로드. |
| `tech_career_table_only_core.py` | 표 전용 경로 보조. |
| `page_2_flow_utils.py` | 2쪽 파싱 흐름 보조. |
| `personal_info.py` | 날짜 형식 변환 등 개인/공통 포맷. |
| `worktype_classifier.py` | 공사종류 분류. |
| `company_change_markers.py` | 근무처 상호 변경 마커 등. |
| `issuer_reference.py` | 발급 기관 참조 데이터. |
| `logger.py` | 로깅. |

(`core/`에 동명 파일이 있으면, 루트 심이 `utils` 또는 `core` 중 어느 쪽을 가리키는지 해당 심 파일을 확인하면 된다.)

---

## 품질 게이트 (`core/quality_gate.py`)

`check_field_completeness(result)` — 파싱 결과 dict에 대해 섹션별 **필수 필드**가 비어 있는지 검사한다. `REQUIRED_FIELDS`에 정의된 키(기술경력, CM/감리 경력, 학력, 국가기술자격, 근무처 등)를 기준으로 구조화된 오류 목록을 반환한다.

---

## `experimental/`

`page_2_parser_legacy`, `page_2_parser_legacy_impl`, `page1_border_explorer` 등 대체·실험 코드가 들어 있다. 기본 `main` 파이프라인과 분리해 두고, 비교·회귀·단계적 교체용으로 쓰는 것을 전제로 한다.

---

## 데이터·카탈로그와의 관계

- `section_parsers`, `tech_career_common` 등은 **`field_catalog`**(직무분야·전문분야 카탈로그)와 연동한다.
- 저장 위치는 보통 프로젝트 루트의 `data/`(예: `field_catalog.json`)이며, 모듈마다 `Path`/`os.path`로 루트를 잡는 주석이 있다. **서브모듈만 클론한 경우** `data/` 동기화 여부를 확인해야 한다.

---

## 상위 진입점과의 연결

| 모듈 | 역할 |
|------|------|
| `PDF-parser/main.py` | `parse_full_document`: `DocumentContext`, `parse_page_1`~`3`, `parse_page_summary`, 구간 탐색, 후처리, 건수/필드 검증, `_파싱오류` 정리. |
| `PDF-parser/service/app.py` | HTTP 업로드 → 임시 파일 → `parse_full_document` → JSON 응답. |

import 측면에서 `main.py`는 대표적으로 다음을 쓴다.

- `parse_page_1`, `parse_page_2`, `parse_page_3`, `parse_page_summary`
- `DocumentContext`
- `tech_career_common`의 병합·정규화 함수
- `section_parsers`의 상훈 관련 일부 심볼
- `quality_gate.check_field_completeness`

새 기능을 넣을 때는 **어느 페이지/섹션에 속하는지**를 먼저 정한 뒤, `core`의 해당 파서 또는 `utils` 공용 모듈에 넣고, 필요 시 루트 심만 추가하는 방식이 기존 구조와 맞는다.
