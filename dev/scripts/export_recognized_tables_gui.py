#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
표 인식 결과 Excel 덤프 — PDF/저장 경로를 GUI로 선택한다.

실행:
  python scripts/export_recognized_tables_gui.py

(프로젝트 루트에서 실행 권장)
"""

from __future__ import annotations

import importlib.util
import sys
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_SPEC = importlib.util.spec_from_file_location(
    "export_recognized_tables_to_excel",
    Path(__file__).resolve().parent / "export_recognized_tables_to_excel.py",
)
assert _SPEC and _SPEC.loader
_mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_mod)
run_export_job = _mod.run_export_job


def main() -> None:
    root = tk.Tk()
    root.title("표 인식 → Excel 덤프")
    root.geometry("640x280")
    root.minsize(560, 240)

    pdf_var = tk.StringVar(value="")
    out_var = tk.StringVar(value="")
    pages_var = tk.StringVar(value="")
    strategy_var = tk.BooleanVar(value=False)
    clean_var = tk.BooleanVar(value=True)
    recover_headers_var = tk.BooleanVar(value=False)

    pad = {"padx": 8, "pady": 4}

    frm = ttk.Frame(root, padding=10)
    frm.pack(fill=tk.BOTH, expand=True)

    def row_file(row: int, label: str, var: tk.StringVar, browse_cmd) -> None:
        ttk.Label(frm, text=label, width=14).grid(row=row, column=0, sticky=tk.W, **pad)
        ent = ttk.Entry(frm, textvariable=var, width=72)
        ent.grid(row=row, column=1, sticky=tk.EW, **pad)
        ttk.Button(frm, text="찾아보기…", command=browse_cmd).grid(row=row, column=2, **pad)

    def browse_pdf() -> None:
        p = filedialog.askopenfilename(
            title="PDF 선택",
            filetypes=[("PDF", "*.pdf"), ("모든 파일", "*.*")],
        )
        if p:
            pdf_var.set(p)
            pdf_path = Path(p)
            if not out_var.get().strip():
                out_var.set(str(pdf_path.parent / f"{pdf_path.stem}_recognized_tables.xlsx"))

    def browse_out() -> None:
        initial = out_var.get().strip() or str(Path.home() / "recognized_tables.xlsx")
        p = filedialog.asksaveasfilename(
            title="Excel 저장 위치",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx"), ("모든 파일", "*.*")],
            initialfile=Path(initial).name,
            initialdir=str(Path(initial).parent) if Path(initial).parent.is_dir() else None,
        )
        if p:
            out_var.set(p)

    row_file(0, "PDF 파일", pdf_var, browse_pdf)
    row_file(1, "Excel 저장", out_var, browse_out)

    ttk.Label(frm, text="페이지 (선택)").grid(row=2, column=0, sticky=tk.W, **pad)
    ttk.Entry(frm, textvariable=pages_var, width=72).grid(row=2, column=1, sticky=tk.EW, **pad)
    ttk.Label(frm, text="").grid(row=2, column=2)

    hint = (
        "비우면 전체 페이지. 예: 0-5 또는 0 1 2 (0부터 시작하는 인덱스)"
    )
    ttk.Label(frm, text=hint, foreground="gray").grid(row=3, column=1, sticky=tk.W, padx=8)

    ttk.Checkbutton(
        frm,
        text="전략별 덤프 (LINE / TEXT / DEFAULT 구분)",
        variable=strategy_var,
    ).grid(row=4, column=1, sticky=tk.W, **pad)

    ttk.Checkbutton(
        frm,
        text="헤더/푸터/기본문구 제거(정제 출력)",
        variable=clean_var,
    ).grid(row=5, column=1, sticky=tk.W, **pad)

    ttk.Checkbutton(
        frm,
        text="헤더 라벨 복원(참여기간/비고 등 한글로 표시)",
        variable=recover_headers_var,
    ).grid(row=6, column=1, sticky=tk.W, **pad)

    frm.columnconfigure(1, weight=1)

    def run() -> None:
        pdf_s = pdf_var.get().strip()
        out_s = out_var.get().strip()
        if not pdf_s:
            messagebox.showwarning("입력", "PDF 파일을 선택하세요.")
            return
        if not out_s:
            messagebox.showwarning("입력", "Excel 저장 경로를 지정하세요.")
            return
        pdf_path = Path(pdf_s)
        out_path = Path(out_s)
        pages_spec = pages_var.get().strip() or None

        path, err = run_export_job(
            pdf_path,
            out_path,
            pages_spec,
            bool(strategy_var.get()),
            bool(clean_var.get()),
            recover_headers=bool(recover_headers_var.get()),
        )
        if err or path is None:
            messagebox.showerror("실패", err or "알 수 없는 오류")
            return
        messagebox.showinfo("완료", f"저장했습니다.\n\n{path.resolve()}")

    btn_fr = ttk.Frame(frm)
    btn_fr.grid(row=7, column=0, columnspan=3, pady=16)
    ttk.Button(btn_fr, text="Excel 생성", command=run).pack(side=tk.LEFT, padx=4)
    ttk.Button(btn_fr, text="종료", command=root.destroy).pack(side=tk.LEFT, padx=4)

    root.mainloop()


if __name__ == "__main__":
    main()
