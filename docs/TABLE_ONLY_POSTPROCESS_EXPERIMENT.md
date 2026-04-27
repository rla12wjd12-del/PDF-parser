## Table-only 후처리 ON/OFF 실증 결과

### 목적

`parsers/page_2_parser_table_only.py`(기술경력 표 기반 단독 파서)에서 적용하는 후처리가
실제로 결과(JSON)에 영향을 주는지 확인한다.

### 스위치

- **ON(기본값)**: 환경변수 미설정 또는 `PDFPARSER_TABLE_ONLY_POSTPROCESS=1`
- **OFF**: `PDFPARSER_TABLE_ONLY_POSTPROCESS=0` (또는 `false/no/off`)

### 테스트 입력 PDF

- `d:\경력증명서\대기자 경력증명서\곽진순 경력증명서(2025.11.05).pdf`
- `d:\경력증명서\대기자 경력증명서\김병수 경력증명서(2025.10.21).pdf`
- `d:\경력증명서\대기자 경력증명서\조성택 경력증명서(2025.11.04).pdf`

### 실행 방법(예시)

PowerShell 기준:

```powershell
# ON
$env:PDFPARSER_TABLE_ONLY_POSTPROCESS='1'
python .\main.py "d:\...\곽진순 경력증명서(2025.11.05).pdf" --out "json_output\곽진순_postON.json" --no-excel

# OFF
$env:PDFPARSER_TABLE_ONLY_POSTPROCESS='0'
python .\main.py "d:\...\곽진순 경력증명서(2025.11.05).pdf" --out "json_output\곽진순_postOFF.json" --no-excel
```

### 결과 요약

세 PDF 모두에서 **후처리 ON/OFF 결과 JSON이 완전히 동일(최상위 dict equality == True)** 했다.

- 곽진순(2025.11.05): 동일
- 김병수(2025.10.21): 동일
- 조성택(2025.11.04): 동일

즉, **이번 3개 샘플에 한해서는 table-only 후처리가 결과를 변화시키지 않았다.**

### 해석/주의

- 이 결과는 “후처리가 절대 불필요”를 증명하는 것은 아니고,
  **이번 샘플 3개에서는 표 추출 품질이 충분히 좋아 후처리로 수정될 부분이 없었다**는 의미다.
- 다른 PDF에서 표 추출이 흔들리는 경우(헤더 혼입, 칸 밀림, 발주자↔직위 swap 등)에는
  ON/OFF 차이가 다시 발생할 수 있다.

