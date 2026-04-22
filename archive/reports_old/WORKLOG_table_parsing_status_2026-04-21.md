# 진행상황 정리 (표 기반 page2/page3 → JSON 파싱)

작성일: 2026-04-21  
작업자: Cursor Agent

## 1) 목표(요약)
- **page2(기술경력), page3(건설사업관리 및 감리경력)**: pdfplumber **원시 표(6열) 기반**으로 파싱하여 기존 **JSON 스키마**에 맞게 채움.
- **page1/summary/personal_info/section**: 기존 **텍스트 기반 파싱 유지**.
- 참여기간 셀 예시 포맷:
  - `1996.03.14 ~ 1996.04.23 (31일) (41일)` 형태를
  - `참여기간_시작일 / 참여기간_종료일 / 인정일수 / 참여일수`로 분리
- 참여기간 셀에 **`┖→` 포함 시 이전 경력에 이어붙임**
- 최종 산출: **JSON + Excel**

## 2) 구현 완료된 변경 사항

### 2-1. 공통 유틸 신규 추가
- 파일: `parsers/table_career_parser.py`
- 기능:
  - `normalize_table_to_6cols(table)`: 빈 컬럼 끼임(10열처럼 보이는 현상) → **6열 정규화**
  - `find_header_start_row(table6)`: 헤더 시작 위치 추정
  - `iter_records_4rows(table6, header_start)`: 헤더(4행) 이후를 **4행=1레코드**로 반복
  - `parse_period_cell(text, yyyy_mm_dd_to_iso=...)`: 참여기간 칸에서 날짜/일수 분리 + `┖→` 감지
  - `merge_into_previous(prev, cur, keys=...)`: `┖→` 케이스에서 이전 레코드로 텍스트 누적

### 2-2. page2(기술경력) 표 기반 파싱 주경로로 전환
- 파일: `parsers/page_2_parser.py`
- 추가 함수: `_parse_tech_careers_from_raw_table(page, page_num_1based=...)`
  - `safe_extract_tables(page, LINE_TABLE_SETTINGS)` 우선 시도
  - 실패 시 `extract_tables_merged(page)` 폴백
  - best table 선택 후 `normalize_table_to_6cols()` 적용
  - `find_header_start_row()` → `iter_records_4rows()`로 4행 블록 파싱
  - `parse_period_cell()`로 `참여기간_시작일/종료일/인정일수/참여일수` 채움
  - `┖→` 감지 시 `merge_into_previous()`로 이전 경력에 이어붙임
- `parse_page_2()`에서 위 표 기반 파싱이 **성공하면 즉시 return**하도록 연결됨.

### 2-3. page3(CM) 표 기반 파싱 주경로로 전환(+ 경력 외 블록 차단)
- 파일: `parsers/page_3_parser.py`
- 추가 함수: `_parse_cm_careers_from_raw_table(page, page_num_1based=...)`
  - page2와 동일한 6열/4행 파싱 전략
  - **경력 외 블록 차단**: `업무수행기간`, `용역완성비율`, `인정일수현황` 등 키워드가 등장하면 그 지점에서 테이블을 잘라 경력 파싱 중단
- `parse_page_3()`에서 위 표 기반 파싱이 **성공하면 즉시 return**하도록 연결됨.

### 2-4. Excel 원시표 헤더 복원(이전 작업)
- 사용자 확인 완료 파일: `excel_output/손인호_L27_R560_lines_recovered_headers_v5.xlsx`
- 목적: 원시 표에서 깨졌던 헤더를 엑셀에서 “참여기간/비고” 등으로 보이게 함(표 모양 검증용).
- 이 작업은 “원시표 검증용”이며, JSON 파싱용 로직과는 별개로 유지.

## 3) 실행/테스트 로그(손인호 PDF)

### 3-1. 실행 명령
```bash
python main.py "originalPDF\조경 손인호 경력증명서(2025.09.22).pdf" --excel --validate
```

### 3-2. 결과(핵심)
- 프로세스는 **exit code 0**으로 정상 종료(예외 없이 JSON/Excel 생성).
- 기술경력: 최종 **총 419건** 출력으로 표시됨.
- CM(건설사업관리 및 감리경력): **총 1건** 출력으로 표시됨.
- 기술경력 페이지별 검증에서 다수 페이지에 대해 `PDF 유효 ~ 개수` 대비 `파싱 건수`가 적다는 경고가 발생.

> 참고: Windows 콘솔(cp949)에서 한글 출력이 깨져 보이는 로그가 있으나, 파싱 자체가 실패한 것은 아님.

## 4) 현재 남아있는 이슈 / 다음 작업(중요)

### 4-1. (중요) page3(CM)가 1건만 잡히는 문제
현상:
- 손인호 PDF에서 CM 파싱 결과가 1건으로만 나옴.
가능 원인(추정):
- `_parse_cm_careers_from_raw_table()`의 **stop 키워드(경력 외 블록 차단)**가 너무 공격적이라, 데이터 영역 초반에서 잘려나갈 수 있음.
- 혹은 `normalize_table_to_6cols()` 이후 헤더 탐지가 잘못되어 데이터 블록이 적게 잡힐 수 있음.
다음 액션:
- CM 표의 실제 데이터 영역에서 stop 키워드가 **어떤 행/문구**에서 매칭되는지 디버그 출력(페이지 113 등) 추가
- stop 키워드를 “업무수행기간/용역완성비율” 중심으로 줄이고, “인정일수/현황/최근” 같은 범용 키워드는 제거/조건 강화
- `iter_records_4rows()`에 들어가기 전 **데이터 행 개수/4의 배수 여부** 로깅

### 4-2. 기술경력 페이지별 검증 경고(유효 ~ 개수 vs 파싱 건수)
현상:
- 많은 페이지에서 `~` 라인 기반 PDF 카운트와, 표 기반 파싱 결과 건수가 다름.
가능 원인(추정):
- 표 기반 파싱은 **4행 블록**을 기준으로 하므로, 표 추출이 일부 행을 합치거나 공백행/헤더 반복을 다르게 반환하면 건수 차이가 날 수 있음.
- 기존 검증 로직이 `~` 라인 수를 “경력 하한”으로 보는데, 표 기반에서는 1건이 4행이므로 `~` 라인과 1:1이 아닐 수 있음.
다음 액션:
- 검증 함수가 사용하는 “PDF 유효 ~ 개수”의 의미를 표 기반에 맞게 재정의하거나,
- 표 기반 파싱에서는 검증 기준을 “4행 블록 수” 또는 “기간 셀에서 날짜 2개 추출 성공한 건수”로 바꾸는 방안 검토

### 4-3. 10열로 보이는 케이스(빈 컬럼 끼임) 안정화
현 상태:
- `normalize_table_to_6cols()`에서 상단 스캔 기준으로 빈 컬럼을 제거하도록 구현됨.
다음 액션:
- 특정 PDF에서 6열로 안 줄어드는 케이스가 생기면, (1) 스캔 범위 조정, (2) “필수 컬럼(기간/비고) 고정” 기반 선택 로직 보강

## 5) 다음 세션 빠른 재개 체크리스트
- 손인호 PDF의 CM 시작 페이지(로그상 113)에서 원시 표를 엑셀로 뽑아 데이터 영역을 육안 확인:
  - `scripts/export_tables_with_custom_borders.py` 또는 `scripts/export_recognized_tables_to_excel.py` 사용
- `parsers/page_3_parser.py`의 `_CM_STOP_KEYWORDS`를 보수적으로 줄인 뒤 재실행:
  - `python main.py "...손인호..." --excel --validate`
- 결과 JSON에서:
  - `건설사업관리및감리경력` 리스트가 적절한 건수로 증가했는지
  - `업무수행기간`, `용역완성비율`이 여전히 최상위에서 파싱되는지(경력 리스트에 섞이지 않는지) 확인

## 6) 이번 세션에서 수정/추가된 파일 목록
- `parsers/table_career_parser.py` (신규)
- `parsers/page_2_parser.py` (표 기반 6열/4행 파싱 주경로 추가)
- `parsers/page_3_parser.py` (표 기반 6열/4행 파싱 주경로 추가 + 경력 외 블록 차단)

