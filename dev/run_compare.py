#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
page_2_parser 비교 실행 스크립트

사용법:
    python run_compare.py "경로/파일.pdf"
    python run_compare.py "경로/폴더"
    (인자가 없으면 파일 선택창이 뜹니다.)
"""

import sys
import os
import argparse
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))

from compare_page_2_parsers import main as compare_main

def pick_path_via_dialog():
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.askopenfilename(
            title="비교할 PDF 파일을 선택하세요 (폴더를 선택하려면 취소 후 명령행 인자 사용)",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        root.destroy()
        return path
    except Exception:
        return None

if __name__ == "__main__":
    # 인자가 없으면 파일 선택창 시도
    if len(sys.argv) == 1:
        path = pick_path_via_dialog()
        if path:
            sys.argv.append(path)
        else:
            print("사용법: python run_compare.py <PDF파일 또는 폴더>")
            print("팁: 파일을 인자로 주지 않으면 탐색기 창이 열립니다.")
            sys.exit(0)
    
    # compare_page_2_parsers 의 main 호출
    compare_main()
