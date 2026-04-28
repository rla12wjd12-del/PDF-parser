from __future__ import annotations

import json
import os
import tempfile
from io import BytesIO
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

# Import project functions (repo root is already in sys.path via main.py logic,
# but when serving via uvicorn we ensure relative imports work by using absolute modules).
from excel_export import export_dict_to_excel_bytes
from main import parse_full_document


app = FastAPI(title="PDF-parser API", version="0.1.0")


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/parse")
async def parse_pdf(file: UploadFile = File(...)) -> JSONResponse:
    """
    Accepts a PDF file and returns parsed JSON (dict).
    """
    filename = file.filename or "upload.pdf"
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF 파일만 업로드할 수 있습니다.")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="빈 파일입니다.")

    keep_temp = _env_bool("PDF_PARSER_KEEP_TEMP", False)
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(data)
            tmp.flush()
            tmp_path = tmp.name

        result = parse_full_document(tmp_path)
        if not isinstance(result, dict):
            raise HTTPException(status_code=500, detail="파서가 dict 결과를 반환하지 않았습니다.")

        # FastAPI JSON encoding should handle Korean keys/values properly (UTF-8).
        return JSONResponse(content=result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파싱 실패: {e}") from e
    finally:
        if tmp_path and (not keep_temp):
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass


@app.post("/excel")
async def json_to_excel(payload: dict[str, Any]) -> StreamingResponse:
    """
    Accepts JSON payload and returns an Excel workbook (.xlsx) as bytes.

    Expected payload:
      - data: dict (required)
      - filename: str (optional, for Content-Disposition)
    """
    data = payload.get("data")
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="payload.data 는 dict(객체)여야 합니다.")

    filename = payload.get("filename")
    if not isinstance(filename, str) or not filename.strip():
        filename = "export.xlsx"
    if not filename.lower().endswith(".xlsx"):
        filename = filename + ".xlsx"

    try:
        xlsx_bytes = export_dict_to_excel_bytes(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"엑셀 생성 실패: {e}") from e

    bio = BytesIO(xlsx_bytes)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )

