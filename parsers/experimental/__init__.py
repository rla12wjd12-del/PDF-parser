# -*- coding: utf-8 -*-
"""
제1쪽 표 기반 파싱 실험 모듈.

core `page_1_parser`와 병행하며, 좌·우 가상 세로선 + pdfplumber 표 추출 파이프라인만
모듈 단위로 분리해 두었다. 안정화 후 core로 옮길 예정.
"""

from parsers.experimental.page1_virtual_lines import (
    PAGE1_VIRTUAL_LEFT_X,
    PAGE1_VIRTUAL_RIGHT_X,
    PAGE1_VIRTUAL_RIGHT_X_ALT,
    page1_explicit_vertical_lines,
    page1_line_table_settings,
    page1_line_table_settings_alt,
)
from parsers.experimental.page1_table_extract import (
    collect_page1_flat_table_rows,
    extract_page1_normalized_rows_for_page,
    normalize_cell_text,
    normalize_table_rows,
    merge_broken_rows,
)
from parsers.experimental.page1_table_sections import (
    detect_section_ranges,
    classify_unassigned_rows,
    rows_to_multiline_text,
)
from parsers.experimental.page1_table_schema import (
    map_page1_table_rows_to_schema,
    map_rows_to_existing_schema,
)
from parsers.experimental.page1_table_preview import build_page1_table_preview_payload

__all__ = [
    "PAGE1_VIRTUAL_LEFT_X",
    "PAGE1_VIRTUAL_RIGHT_X",
    "PAGE1_VIRTUAL_RIGHT_X_ALT",
    "page1_explicit_vertical_lines",
    "page1_line_table_settings",
    "page1_line_table_settings_alt",
    "collect_page1_flat_table_rows",
    "extract_page1_normalized_rows_for_page",
    "normalize_cell_text",
    "normalize_table_rows",
    "merge_broken_rows",
    "detect_section_ranges",
    "classify_unassigned_rows",
    "rows_to_multiline_text",
    "map_page1_table_rows_to_schema",
    "map_rows_to_existing_schema",
    "build_page1_table_preview_payload",
]
