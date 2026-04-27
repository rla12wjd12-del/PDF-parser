# 프로젝트 보고서: 건설기술인 PDF 경력증명서 파서

작성일: 2026-04-20  
대상 워크스페이스: `d:\PDF parser`

---

## 파트 1: [관리자용] 프로젝트 현황 및 요약 (비개발자용)

### 프로젝트 목표(요청사항 기준)
- **건설기술인 PDF 경력증명서**를 읽어서 **구조화된 JSON으로 파싱**하고, 이를 **엑셀(.xlsx)로 변환**해 **데이터를 관리/검증**할 수 있게 만드는 프로젝트입니다.

### 한 줄 비유로 설명
- 이 프로젝트는 **“종이로 된 경력증명서를 스캐너로 읽고, 표와 문장을 ‘엑셀에 바로 붙일 수 있는 칸칸이 데이터’로 정리해주는 자동 분류기”**에 가깝습니다.  
  PDF가 조금 삐뚤거나 줄바꿈이 깨져도 최대한 복원하려고 **보정 규칙(휴리스틱)**이 많이 들어가 있습니다.

### 프로젝트 진행도(추정)
- **약 80% 구현 완료**로 판단됩니다.
  - **완료에 가까운 것**
    - PDF→JSON 엔드투엔드 파싱
    - JSON→엑셀 변환(섹션별 시트)
    - 다중 PDF 배치 파싱
    - 자동 검증 리포트/오류 기록(`_검증`, `_파싱오류`)
    - 다양한 문서 레이아웃 편차 보정(표+텍스트+좌표 기반 추출 폴백)
  - **남은/불확실한 것**
    - 입력 포맷(ZIP/OCR 텍스트) 처리 불일치(코드는 있으나 실행 경로에서 차단)
    - 성능(배치 대량 처리) 최적화
    - 문서 양식 변경 내성(요약 섹션 범위, 레이아웃 편차 확대 대응)
    - 배포/설치 재현성(의존성 파일 부재)

### 현재 제공 가능한 주요 기능 요약(쉬운 표현)
- **이 프로젝트는 현재**
  - PDF에서 아래 데이터를 뽑아
    - 인적사항/등급/자격/학력/교육훈련/상훈/벌점·제재/근무처/기술경력/감리경력/요약 인정일수
  - **JSON 파일로 저장**하고(`json_output/`)
  - 같은 내용을 **엑셀 파일(시트=섹션)로 자동 생성**하며(`excel_output/`)
  - 원본 PDF와 파싱 결과의 **“건수 불일치/누락 의심”을 자동 검출**해서 **오류 목록과 검증 리포트**로 남깁니다.

### 현 상황의 리스크(무엇이 잘 되고, 무엇이 우려되는지)
- **잘 되고 있는 점(강점)**
  - **실무형 파이프라인**: 결과물(JSON/Excel) 생성 + 검증/리포팅까지 포함된 흐름이 거의 완성돼 있습니다.
  - **문서 편차 대응**: 표 추출 실패 시 텍스트/좌표 기반 라인 스트림으로 폴백하는 구조가 있어 단순 정규식 파서보다 강합니다.
  - **운영 관점 품질경보 체계**: `_검증`, `_파싱오류`, `scripts/verify/test_validator.py`로 이상징후를 조기에 발견할 수 있습니다.
- **우려되는 점(리스크)**
  - **문서 양식 변경 리스크**: 줄바꿈/표 구조/요약 페이지 구성 변경 시 휴리스틱이 깨질 수 있습니다. 특히 요약 파서가 “요약 시작 페이지부터 문서 끝까지” 합쳐 읽어 **부록/추가 페이지 혼입 오탐** 가능성이 있습니다.
  - **대량 배치 성능/안정성**: 페이지별 파서 호출 과정에서 PDF를 여러 번 여는 경로가 있어 처리 시간이 늘고 실패 지점도 증가할 수 있습니다.
  - **유지보수 난이도**: 기술경력 파서(`page_2_parser.py`)가 매우 크고 복잡해 변경 비용이 커질 수 있습니다.
  - **입력 정책(PDF-only) 위반 입력**: ZIP 등 비-PDF 입력이 들어오면 즉시 실패합니다. 운영에서는 입력 전 단계에서 확장자/포맷을 강제해 불필요한 실패를 줄이는 편이 안전합니다.
  - **배포/설치 리스크**: `requirements.txt`/`pyproject.toml` 부재로 환경이 바뀌면 재현성이 떨어질 수 있습니다.

---

## 파트 2: [개발자용] 기술 진단 및 수정 가이드 (기술적 전문 용어)

### 코드 구조(디렉토리/역할 요약)

#### 루트(엔트리/오케스트레이션/산출)
- `main.py`
  - 섹션 시작 페이지 탐지(키워드 기반)
  - `page_1/2/3/summary` 파서 호출 및 결과 병합
  - JSON 저장(`json_output/`) 및 Excel 저장(`excel_export.py`)
  - 원본 대비 건수 검증(`count_pdf_items`, `_verify_tech_career_per_page_against_pdf`)
  - 필드 품질 검증(`log_technical_career_field_issues`) 및 `_파싱오류` 기록
- `excel_export.py`: dict/JSON → 단일 xlsx 워크북 변환(최상위 키별 시트, `_파싱오류` 강조 시트)
- `batch_parse.py`: 특정 파일 목록 기반 배치 파싱(샘플/내부용 성격)
- `verify_parsing.py`: 파싱 결과 간단 품질 검사(교육훈련 오염/학력 괄호/사업명 타입 등)
- `run_parser.bat`: Windows 실행 런처(더블클릭/드래그앤드롭)
- `field_catalog.py`: 직무/전문분야 카탈로그(JSON 우선, xlsx 폴백) + 매칭 유틸
- `PARSING_ANALYSIS.md`: 파싱 플로우/리스크 분석 문서(현재 코드와 높은 일치)

#### `parsers/` (파싱 코어)
- `page_1_parser.py`: 1~(최대 8)페이지 통합 텍스트 기반 + 표 기반 보강으로 인적/등급/자격/학력/교육훈련/상훈/벌점·제재/근무처 추출
- `page_2_parser.py`: 기술경력 핵심 파서(초대형). `~` 라인 파싱, date_blocks 우선 확보, 표 기반 보강, `┖→` 페이지경계 병합, 필드 시프트/노이즈 제거 등
- `page_3_parser.py`: 건설사업관리·감리경력 파서. 표 기반 필드 + 텍스트/좌표기반 date_blocks 병합 + 최근 1년 통계 파싱
- `page_summary_parser.py`: 요약 인정일수(공사종류별/직무전문분야별) 파서
- `section_parsers.py`: 1쪽 계열 세부 섹션 파서(등급/자격/상훈/근무처/벌점 등) + 카탈로그 기반 보강
- `layout_extractor.py`: PyMuPDF(fitz) 우선 / pdfplumber 폴백 word bbox 기반 라인 스트림 재구성(레이아웃 의존성 완화)
- `tech_career_heuristics.py`: 기술경력 휴리스틱 토큰/정규식 모음 + `data/tech_career_heuristics.json` 오버라이드
- `worktype_classifier.py`: 공사종류 카탈로그 기반 판별/분리(`data/*categories_tree.json`)
- `issuer_reference.py`: 행정구역/공공기관 카탈로그 기반 발주자 접두 매칭(`data/*regions*`, `data/*institutions*`)

#### `data/` (룰/카탈로그)
- 공사종류/건축분류 트리, 행정구역/공공기관 트리, `field_catalog.json`, `tech_career_heuristics.json` 등

#### `tests/` / `scripts/verify/`
- `tests/`: 핵심 휴리스틱(틸드 라인, date_blocks 병합, 페이지 메트릭 등) 회귀 테스트 존재
- `scripts/verify/test_validator.py`: 다중 PDF 자동 파싱/검증 리포팅(품질지표: fallback 비율, 날짜 형식, _검증 불일치, 필드 시프트 의심 등)

---

### 기능별 코드 현황

| 기능명 | 담당 파일 | 구현 상태 | 개선 필요성 |
|---|---|---:|---|
| PDF→JSON 전체 파이프라인(섹션 오케스트레이션/저장) | `main.py` | 구현됨 | 성능(중복 open), 예외/로그 구조화 |
| 1쪽 계열(인적/등급/자격/학력/교육훈련/상훈/벌점/근무처) | `parsers/page_1_parser.py`, `parsers/section_parsers.py` | 구현됨 | 중복 정규화 로직 정리, 테스트 확장 |
| 기술경력 파싱(핵심) | `parsers/page_2_parser.py`, `parsers/tech_career_heuristics.py` | 구현됨(매우 복잡) | 모듈 분리/리팩토링, 성능/가독성/테스트 커버리지 강화 |
| 페이지 경계 개요 이어붙이기(┖→) | `parsers/page_2_parser.py: merge_cross_page_tech_overviews` | 구현됨 | 안전가드(짧은 cont 제거 방지 등) 보강 |
| 건설사업관리·감리경력 파싱 | `parsers/page_3_parser.py` | 구현됨 | date_blocks↔table 정합성 강화, 통계 파싱 안정화 |
| 요약 인정일수(공사종류별/직무전문분야별) | `parsers/page_summary_parser.py` | 구현됨 | 섹션 종료 앵커/페이지 합치기 상한 필요 |
| JSON→Excel(시트별) + 오류 시트 | `excel_export.py` | 구현됨 | 대용량 시 성능(autofit 비용) 옵션화 고려 |
| 자동 검증/리포팅(배치 품질지표) | `scripts/verify/test_validator.py`, `verify_parsing.py` | 구현됨 | CI 연동, 기준치(FAIL/WARN) 튜닝 |
| 배치 파싱(샘플 세트) | `batch_parse.py` | 구현됨(하드코딩) | 디렉토리 기반 일반화(validator의 `--dir` 활용 권장) |
| 의존성/배포 재현성 | (없음) | 미흡 | `requirements.txt` 또는 `pyproject.toml` 추가 권장 |

---

### 기술적 개선 제안(리팩토링/안정성/의존성)

#### 1) 성능/안정성: `pdfplumber.open()` 반복 제거
- 문제: 배치 파싱에서 IO 비용↑, 실패 지점↑
- 제안: 상위에서 PDF 핸들 1회 열고 하위 파서에 `pages`/텍스트/워드 스트림을 주입하는 `DocumentContext`(또는 캐시) 도입

#### 2) 유지보수성: `page_2_parser.py` 분할
- 분할 후보
  - `tilde_parser.py` (`_parse_tilde_line` 및 issuer/worktype/specialty 정리)
  - `date_blocks.py` (`_extract_date_blocks_from_text` 및 병합/검증)
  - `overview_merge.py` (`┖→`/cross-page merge)
  - `field_shift_fixes.py` (시프트/노이즈 제거 규칙)

#### 3) 신뢰성: 요약 파서 범위 제한
- 문제: `summary_start..EOF` 합치기 → 부록 혼입 시 오탐 위험
- 제안: 종료 앵커(다음 섹션 타이틀/고정 문구) + 최대 합치기 페이지 수 상한

#### 4) 입력 정책: PDF만 지원(비-PDF 입력은 명시적 에러)
- 결정: **ZIP(OCR 텍스트) 입력은 지원하지 않음**. 입력값은 **PDF 파일(.pdf)** 로 고정.
- 조치: ZIP 로더/분기 제거(또는 격리) + 문서에서 ZIP 언급 제거로 혼선 방지

#### 5) 배포/재현성: 의존성 명시
- 필수: `pdfplumber`, `openpyxl`
- 옵션: `PyMuPDF`(= `fitz`) (레이아웃 기반 추출 성능/정확도 개선)
- 테스트: `pytest` 또는 `unittest` (현재 혼재)

#### 6) 로깅/관측성 개선
- `print` 중심 → 운영/배치에서 노이즈 및 분석 난이도↑
- `logging` 도입 + 결과 JSON에 메타(`_schema_version`, `_parser_version`, 엔진 사용 여부 등) 추가 권장

---

### Action Item: 오늘 바로 처리 우선순위

#### P0 (즉시 리스크 저감)
- 의존성 고정 파일 추가: `requirements.txt` 또는 `pyproject.toml`
- `page_summary_parser.py` 안전화: EOF 합치기 제거/완화(종료 앵커 + 페이지 상한)
- 입력 정책 고정: **PDF만 지원**(ZIP 등 비-PDF 입력 경로/문서 정리)

#### P1 (성능/확장)
- PDF open 1회화 + 컨텍스트/캐시 주입
- 검증 신뢰도 레벨링: `_검증` 불일치를 섹션별로 WARN/FAIL 정책화(운영 노이즈 감소)

#### P2 (유지보수성)
- `page_2_parser.py` 모듈 분리 + 단위 테스트 확장
- 중복 정규화 로직(회사명/근무처 파싱 등) 공통 유틸로 추출

