# 파싱 보강 진행 상황

계획: [PLAN.md](PLAN.md) · 기준점: `baseline/pre-hardening-20261012` (`45346dd`)

| 단계 | 내용 | 상태 | 검증 결과 | 커밋 / 태그 |
|---|---|---|---|---|
| 0 | 준비: 기준점 태그, worktree, 회귀 테스트 도구, 기준 결과 | ✅ 완료 | 기준 결과 재생성 = 운영 코드 결과와 변경 0건(결정성 확인). 단위테스트 15 통과 / 3 파일은 기존부터 import 오류 | `stage0` / `hardening/stage0-pass` |
| 1 | 경력 산정 필드 칸 섞임·누락 (5건) | ⬜ | | |
| 2 | 사업명 훼손 (17건) | ⬜ | | |
| 3 | 페이지 넘김 연장행 오첨부 (8건) | ⬜ | | |
| 4 | 검증 오경보 + debug_page 정리 | ⬜ | | |
| 5 | 요약표 이름 잘림·붙음 (9건) | ⬜ | | |
| 6 | 한자 누락 (3건) | ⬜ | | |
| 7 | 처리 시간 단축 | ⬜ | | |
| 8 | 최종 통합 검증 | ⬜ | | |
| 9 | 운영 적용·롤백 가이드 | ⬜ | | |

## 기록
- 2026-10-12: 기준점 태그 `baseline/pre-hardening-20261012` 생성, worktree `C:\PythonPractice\PDF-parser-hardening` (브랜치 `hardening/parse-accuracy`) 생성. 운영 폴더 무변경.
- 2026-10-12: 0단계 완료. 회귀 도구 `dev/regression/run_regression.py`(run/compare). 50개 기준 결과 생성(8병렬, 벽시계 144.5s). 운영 폴더 코드로 만든 결과와 비교해 변경 0건 → 결과가 결정적임을 확인.
  - 기존 단위테스트: 15 passed, 3개 파일(`test_duplicate_tuple_date_blocks`, `test_tech_career_page_metrics`, `test_tilde_training_parsers`)은 수정 전부터 삭제된 함수를 import해 수집 오류. 회귀 기준 = 15 passed 유지.
