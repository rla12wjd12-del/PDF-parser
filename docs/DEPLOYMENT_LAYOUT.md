# 운영(배포) 최소 구성 vs 개발 도구 구분

이 문서는 `main.py`(PDF → JSON → Excel) 실행에 필요한 **운영 최소 구성**과,
테스트/검증/디버그용 **개발 도구**를 분리한 폴더 구조를 설명합니다.

## 운영(배포) 최소 구성 (Keep)

아래만 있으면 `python main.py ...` 실행이 가능합니다.

- `main.py`
- `parsers/`
- `excel_export.py`
- `field_catalog.py`
- `data/`
  - `field_catalog.json`
  - `tech_career_heuristics.json`
  - `Construction_categories_tree.json`
  - `building_categories_tree.json`
  - `public_institutions_tree.json`
  - `public_institutions_flat.json`
  - (기타 파서에서 참조하는 데이터 파일)
- `requirements.txt`
- (선택) `run_parser.bat`

### 로컬에 있어도 git에는 커밋되지 않는 것
- `json_output/`, `excel_output/` (실행 산출물)
- `__pycache__/`, `*.pyc` (파이썬 캐시)

## 개발 도구 (Dev)

아래는 운영 경로에 필수는 아니지만, 품질 검증/디버깅/실험에 유용한 파일들입니다.

- `dev/tests/`: 단위/회귀 테스트
- `dev/scripts/`: 점검/덤프/탐색 스크립트
- `dev/scripts/verify/`: 파싱 검증 유틸
- `dev/tools/`: GUI/시각화/카탈로그 생성 등 개발 도구
- `dev/archive/`: 과거 리포트/검증 로그 보관
- `dev/docs/`: 분석/리포트 문서
- `dev/batch_parse.py`, `dev/verify_parsing.py`, `dev/test_validator.py` 등: 배치/검증 엔트리

