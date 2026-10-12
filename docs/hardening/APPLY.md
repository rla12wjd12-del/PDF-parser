# 파싱 보강 운영 적용·롤백 가이드

- 기준점(현재 운영 코드): 태그 `baseline/pre-hardening-20261012` = `45346dd`
- 적용 대상: 브랜치 `hardening/parse-accuracy` (최종 태그 `hardening/stage7-pass`)
- 단계별 태그: `hardening/stage0-pass` … `hardening/stage7-pass` (각 단계 검증 통과 시점)
- 상세 기록: [PROGRESS.md](PROGRESS.md), 계획: [PLAN.md](PLAN.md)

## 1. 무엇이 바뀌나

| 단계 | 바뀌는 결과 | 50개 시험 PDF 기준 |
|---|---|---|
| 1 | 빠졌던 연·월(YYYY.MM) 경력 인식, CM 시작 판정, 연장 행 칸 배정 | 누락 경력 26건 복구, 칸 오염 140건 수정 |
| 2 | 직무·담당이 있는 경력의 사업명을 고치지 않음 | 사업명 17건 복구 |
| 3 | 다음 쪽 첫 경력 아래의 `┖→`를 앞 쪽 개요로 붙이지 않음 | 14건 수정(내용 손실 0) |
| 4 | 새 필드 `_검증_좌표`(좌표 기반 경력 수 검증), 불일치 시 `_파싱오류`에 `좌표기준_경력수불일치` | 50개 모두 일치 |
| 5 | 요약표 공사종류별 인정일수를 좌표로 읽음(합계 검산 통과 시만) | 1건 수정 |
| 6 | pdfplumber가 빠뜨린 한자 복원 | 3건 복원 |
| 7 | 처리 시간 단축(결과 변경 없음) | 6개 문서 합계 209.9초 → 68.2초 |

- 최종 비교(기준점 대비): 50개 파일, 변경 470항목, 예외 0. 모든 변경은 단계별로 분류·원문 확인 완료.
- JSON 구조: 기존 키는 그대로, `_검증_좌표` 키만 추가.
- 웹앱 참고: 웹앱 `PersonnelDetailsSchema`(zod)가 `_파싱오류`, `_검증_좌표`를 지워 화면에 보이지 않는다. 표시가 필요하면 웹앱 쪽 별도 작업.

## 2. 적용 전 확인

PowerShell, 운영 폴더 `C:\PythonPractice\CM-project\external\PDF-parser`:

```powershell
git status --short          # .venv/ 외 변경이 없어야 함
git rev-parse --short HEAD  # 45346dd 이어야 함(다르면 적용 전 다시 검토)
```

- 운영 venv 에 새 패키지는 필요 없다(requirements.txt 변경 없음).

## 3. 적용

```powershell
cd C:\PythonPractice\CM-project\external\PDF-parser
git merge --ff-only hardening/parse-accuracy
```

- 파서 API 는 `uvicorn --reload` 로 실행 중이라 .py 변경을 감지해 자동 재기동된다. 자동 재기동이 안 되면 파서 API 를 다시 시작한다.
- 상위 저장소(CM-project)에서는 서브모듈 `external/PDF-parser` 가 변경으로 표시된다. 상위 저장소 커밋 여부는 별도로 결정한다.

## 4. 적용 후 확인

1. `http://127.0.0.1:8001/health` → `{"status":"ok"}`
2. 웹앱에서 경력증명서 1개(20쪽 이상 권장) 업로드 → 정상 결과, 이전보다 빨라짐
3. 파서 콘솔에 `[WARN] 병렬 처리 실패` 가 없는지 확인(있어도 결과는 정상이며 순차 방식으로 처리된 것)

## 5. 환경변수 (선택)

| 변수 | 기본값 | 의미 |
|---|---|---|
| `PDFPARSER_WORKERS` | min(8, CPU-1) | 병렬 작업 프로세스 수. `1` 이면 병렬 끔(기존과 같은 순차 방식, 결과 동일) |
| `PDFPARSER_PARALLEL_MIN_PAGES` | 20 | 이 쪽수 미만 문서는 순차 처리 |
| `PDFPARSER_PARALLEL_TIMEOUT` | 600 | 병렬 처리 제한 시간(초). 넘으면 순차 처리로 전환 |

- 서버 CPU 가 적거나 다른 서비스와 함께 돌 때는 `PDFPARSER_WORKERS` 를 낮춘다(측정 PC: 물리 8코어에서 8이 최적, 6과 거의 같음, 4는 느림).
- 병렬 처리 중에는 작업 프로세스가 문서당 최대 `PDFPARSER_WORKERS` 개 생겼다가 문서 처리 후 정리된다. 동시에 병렬 처리하는 문서는 1개뿐이다.

## 6. 롤백

### 6-1. 속도 개선(7단계)만 끄기 — 코드 변경 없음
파서 API 실행 환경에 `PDFPARSER_WORKERS=1` 을 설정하고 파서 API 를 다시 시작한다.
(`dev-cmd/scripts/start-parser-api.cmd` 의 uvicorn 실행 줄 앞에 `set PDFPARSER_WORKERS=1`)

### 6-2. 전체를 운영 이전 상태로
```powershell
cd C:\PythonPractice\CM-project\external\PDF-parser
git reset --hard baseline/pre-hardening-20261012
```
- 적용이 fast-forward 였으므로 이 명령으로 정확히 적용 전 코드(`45346dd`)로 돌아간다. 보강 커밋은 브랜치 `hardening/parse-accuracy` 에 그대로 남는다.
- 그 사이 운영 폴더에서 다른 커밋을 했다면 reset 대신 아래 6-3 을 쓴다.
- `--reload` 로 자동 재기동되며, 안 되면 파서 API 를 다시 시작한다.

### 6-3. 특정 단계만 되돌리기
되돌릴 단계의 커밋을 `git revert` 한다(최근 단계부터 되돌리는 것이 안전).

| 단계 | 코드 커밋 |
|---|---|
| 7 | `feea547` |
| 6 | `cfae06c` |
| 5 | `fa17745` |
| 4 | `718403c` `ad472e1` |
| 3 | `c3b0b1a` |
| 2 | `e3abbb4` |
| 1 | `a76a5c6` `c4c7521` `d01f3d0` |

예: `git revert --no-edit feea547` (7단계 되돌리기). 문서(`docs:`)·도구(`stage0`) 커밋은 결과에 영향이 없다.

## 7. 회귀 테스트 다시 돌리기 (선택)

개인정보가 든 PDF·결과는 저장소에 넣지 않는다. 결과 폴더는 저장소 밖에 둔다.

```powershell
$py = "C:\PythonPractice\CM-project\external\PDF-parser\.venv\Scripts\python.exe"
& $py dev/regression/run_regression.py run --root . --pdf-dir <PDF폴더> --out-dir <결과폴더> --jobs 4
& $py dev/regression/run_regression.py compare --baseline <기준결과> --candidate <결과폴더> --report <보고서.txt>
& $py -m pytest dev/tests -q   # 36 passed (3개 파일은 보강 전부터 import 오류)
```
