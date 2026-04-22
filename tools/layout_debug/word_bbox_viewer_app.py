#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import binascii
import hashlib
import io
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

import pdfplumber
import streamlit as st

# 어디서 실행해도 프로젝트 루트 import가 되도록 보정
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../PDF parser
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _extract_words_pdfplumber(page: Any) -> list[dict[str, float | str]]:
    words = (
        page.extract_words(
            keep_blank_chars=False,
            use_text_flow=True,
            extra_attrs=["top", "bottom", "x0", "x1"],
        )
        or []
    )
    out: list[dict[str, float | str]] = []
    for w in words:
        if not isinstance(w, dict):
            continue
        t = str(w.get("text") or "").strip()
        if not t:
            continue
        out.append(
            {
                "text": t,
                "x0": float(w.get("x0") or 0.0),
                "x1": float(w.get("x1") or 0.0),
                "top": float(w.get("top") or 0.0),
                "bottom": float(w.get("bottom") or 0.0),
            }
        )
    return out


def _default_vertical_borders(page: Any, words: list[dict[str, float | str]]) -> tuple[float, float]:
    """페이지 너비 및 단어 bbox로 좌/우 가상 외곽선 기본값."""
    pw = float(getattr(page, "width", 0.0) or 0.0)
    if pw <= 0:
        return 5.0, 590.0
    if not words:
        return min(5.0, pw * 0.02), max(pw - 5.0, pw * 0.98)
    xs0 = min(float(w["x0"]) for w in words)
    xs1 = max(float(w["x1"]) for w in words)
    pad = 2.0
    left = max(0.0, xs0 - pad)
    right = min(pw, xs1 + pad)
    if left >= right:
        return min(5.0, pw * 0.02), max(pw - 5.0, pw * 0.98)
    return left, right


def _build_table_settings(
    *,
    t_v: str,
    t_h: str,
    snap: int,
    join: int,
    edge_min: int,
    min_words_v: int,
    min_words_h: int,
    use_virtual_border: bool,
    v_left: float,
    v_right: float,
) -> dict[str, Any]:
    settings: dict[str, Any] = {
        "vertical_strategy": t_v,
        "horizontal_strategy": t_h,
        "snap_tolerance": int(snap),
        "join_tolerance": int(join),
        "edge_min_length": int(edge_min),
        "min_words_vertical": int(min_words_v),
        "min_words_horizontal": int(min_words_h),
    }
    # 가상 좌/우선만 넣고 vertical_strategy=explicit 로 고정하면,
    # explicit 은 "지정한 x만" 세로 구획으로 쓰므로 내부 세로선이 전부 사라진다.
    # pdfplumber 에서는 lines/text 등과 함께 explicit_vertical_lines 를 주면
    # PDF 에서 잡힌 세로선 + 가상 외곽선이 병합된다(issue #820 설명 참고).
    if use_virtual_border and float(v_right) > float(v_left) > 0:
        settings["explicit_vertical_lines"] = [float(v_left), float(v_right)]
    return settings


def main() -> None:
    st.set_page_config(page_title="PDF Layout Debugger", layout="wide")
    st.title("PDF 레이아웃 디버거: word bbox / 라인 스트림 / 표(선) 기반 셀")

    uploaded = st.file_uploader("PDF 업로드", type=["pdf"])
    if uploaded is None:
        st.info("PDF를 업로드하면 word bbox(빨강), 테이블/셀(파랑/초록), 가상 외곽선(주황), 라인 스트림 결과를 비교할 수 있습니다.")
        return

    data = uploaded.getvalue()
    if not data:
        st.error("업로드된 파일이 비어있습니다.")
        return

    head = data[:1024].lstrip()
    if not head.startswith(b"%PDF-"):
        sig = binascii.hexlify(head[:32]).decode("ascii", errors="ignore")
        st.error(
            "업로드된 파일이 PDF로 인식되지 않습니다. (헤더에 %PDF- 없음)\n\n"
            "가능한 원인: 확장자는 .pdf지만 실제로는 다른 파일(HTML/이미지/다운로드 응답) 또는 손상된 파일입니다."
        )
        st.caption(f"업로드 바이트(앞 32바이트, hex): {sig}")
        return

    upload_sig = hashlib.sha256(data).hexdigest()[:16]

    with st.sidebar:
        st.header("옵션")
        password = st.text_input("PDF 비밀번호(필요 시)", value="", type="password")

    tmp_pdf_path: Optional[str] = None
    try:
        pdf = pdfplumber.open(io.BytesIO(data), password=(password or None))
    except Exception as e:
        st.error("PDF를 열지 못했습니다. (손상/보안/비정상 PDF일 가능성)")
        st.exception(e)
        st.info(
            "대응: 다른 PDF 뷰어에서 정상 열림 여부 확인 → 다시 저장(프린트 to PDF) 후 업로드, "
            "또는 비밀번호 입력을 시도해보세요."
        )
        return

    with pdf:
        total_pages = len(pdf.pages)
        if total_pages <= 0:
            st.error("PDF 페이지를 읽지 못했습니다.")
            return

        # 1) 페이지/해상도 먼저 (가상 경계는 페이지별 키 필요)
        with st.sidebar:
            st.divider()
            st.subheader("페이지")
            page_idx = st.number_input(
                "페이지(0부터)", min_value=0, max_value=max(0, total_pages - 1), value=0, step=1
            )
            resolution = st.slider("렌더링 해상도(resolution)", min_value=72, max_value=400, value=200, step=10)

        page = pdf.pages[int(page_idx)]
        words = _extract_words_pdfplumber(page)
        l_def, r_def = _default_vertical_borders(page, words)

        vk_l = f"virt_l_{upload_sig}_{int(page_idx)}"
        vk_r = f"virt_r_{upload_sig}_{int(page_idx)}"
        if vk_l not in st.session_state:
            st.session_state[vk_l] = float(l_def)
        if vk_r not in st.session_state:
            st.session_state[vk_r] = float(r_def)

        # 2) 나머지 사이드바 (가상 경계 number_input은 key로 session_state와 동기화)
        with st.sidebar:
            st.divider()
            st.subheader("라인 스트림")
            engine = st.selectbox("word 엔진", options=["pdfplumber", "pymupdf", "auto"], index=0)
            y_tolerance = st.slider("y_tolerance(같은 줄 판정)", min_value=0.0, max_value=10.0, value=2.0, step=0.5)
            join_gap = st.slider("join_gap(붙여쓰기 간격)", min_value=0.0, max_value=20.0, value=1.0, step=0.5)

            st.divider()
            st.subheader("표(선) 기반 감지")
            show_table_viz = st.checkbox("테이블/셀 bbox 오버레이 보기", value=True)
            st.caption(
                "가상 좌/우는 explicit_vertical_lines 로만 추가됩니다. "
                "세로는 보통 **lines**(PDF 선+가상선 병합)를 권장합니다. "
                "**explicit** 만 쓰면 지정한 x만 열로 잡혀 내부 셀이 사라질 수 있습니다. "
                "가로는 기본 text 전략을 권장합니다."
            )
            use_virtual_border = st.checkbox("가상 좌/우 외곽선 사용(표 인식용)", value=True)
            t_v = st.selectbox("vertical_strategy", options=["lines", "text", "explicit"], index=0)
            if use_virtual_border and t_v == "explicit":
                st.warning(
                    "vertical_strategy가 **explicit**이면 `explicit_vertical_lines`에 있는 x만 세로 구획으로 쓰입니다. "
                    "가상선이 좌·우 두 개뿐이면 **내부 열이 한 덩어리**로 나옵니다. "
                    "PDF 안쪽 세로선까지 살리려면 **lines**로 바꾸세요."
                )
            t_h = st.selectbox("horizontal_strategy", options=["lines", "text", "explicit"], index=1)
            snap = st.slider("snap_tolerance", 0, 10, 3, 1)
            join = st.slider("join_tolerance", 0, 10, 3, 1)
            edge_min = st.slider("edge_min_length", 0, 200, 20, 5)
            min_words_v = st.slider("min_words_vertical", 0, 10, 3, 1)
            min_words_h = st.slider("min_words_horizontal", 0, 10, 1, 1)

            st.divider()
            st.subheader("가상 외곽선 좌표 (PDF x)")
            v_left = st.number_input("virtual_left_x", min_value=0.0, step=1.0, key=vk_l, disabled=not use_virtual_border)
            v_right = st.number_input("virtual_right_x", min_value=0.0, step=1.0, key=vk_r, disabled=not use_virtual_border)
            c0, c1, c2 = st.columns(3)
            with c0:
                if st.button("기본값(단어 bbox 기준)", disabled=not use_virtual_border):
                    st.session_state[vk_l] = float(l_def)
                    st.session_state[vk_r] = float(r_def)
                    st.rerun()
            with c1:
                if st.button("표 재인식", type="primary"):
                    st.rerun()
            with c2:
                st.caption("좌표 수정 후 **표 재인식**을 누르면 감지를 다시 실행합니다.")

            st.divider()
            st.subheader("표시")
            word_line_width = st.slider("word bbox 선 두께", min_value=1, max_value=6, value=2)
            cell_line_width = st.slider("셀 bbox 선 두께", min_value=1, max_value=6, value=2)
            table_line_width = st.slider("테이블 bbox 선 두께", min_value=1, max_value=8, value=3)
            virtual_line_width = st.slider("가상 외곽선 두께", min_value=1, max_value=8, value=3)
            show_words = st.checkbox("word bbox(빨강) 보기", value=True)
            show_word_table = st.checkbox("단어 리스트(좌표)도 보기", value=False)
            show_lines = st.checkbox("라인 스트림(재구성 결과) 보기", value=True)
            max_lines = st.slider("라인 출력 최대 개수", min_value=20, max_value=500, value=120, step=20)

        # PyMuPDF 엔진용 임시 파일
        if engine in {"pymupdf", "auto"}:
            fd, tmp_pdf_path = tempfile.mkstemp(prefix="uploaded_", suffix=".pdf")
            with os.fdopen(fd, "wb") as f:
                f.write(data)

        im = page.to_image(resolution=int(resolution))

        if show_words and words:
            im = im.draw_rects(words, stroke="red", stroke_width=int(word_line_width))

        table_rects: list[dict[str, float]] = []
        cell_rects: list[dict[str, float]] = []
        table_settings = _build_table_settings(
            t_v=t_v,
            t_h=t_h,
            snap=snap,
            join=join,
            edge_min=edge_min,
            min_words_v=min_words_v,
            min_words_h=min_words_h,
            use_virtual_border=use_virtual_border,
            v_left=float(v_left),
            v_right=float(v_right),
        )

        if show_table_viz:
            try:
                from tools.layout_debug.pdfplumber_table_viz import find_tables_and_cells

                table_rects, cell_rects = find_tables_and_cells(page, table_settings=table_settings)
            except Exception as e:
                st.warning("테이블/셀 감지에 실패했습니다. (설정값을 조정해보세요)")
                st.exception(e)

            if table_rects:
                im = im.draw_rects(table_rects, stroke="blue", stroke_width=int(table_line_width))
            if cell_rects:
                im = im.draw_rects(cell_rects, stroke="green", stroke_width=int(cell_line_width))

        # 가상 외곽선: 항상 시각화 (인식용으로 켜진 경우)
        if use_virtual_border and float(v_right) > float(v_left) > 0:
            im = im.draw_vlines(
                [float(v_left), float(v_right)],
                stroke="orange",
                stroke_width=int(virtual_line_width),
            )

        annotated = im.annotated

        col_img, col_meta = st.columns([3, 2], gap="large")
        with col_img:
            st.subheader("오버레이 결과")
            st.image(annotated, caption=f"page={page_idx} / total={total_pages}", use_container_width=True)
            st.caption("색상: word=빨강, table=파랑, cell=초록, 가상 외곽선=주황")

            with st.expander("마우스로 좌/우 경계 x 찍기", expanded=True):
                st.write(
                    "이미지를 클릭한 뒤 **left/right 설정** 버튼을 누르면 좌표가 반영되고 자동으로 다시 그려집니다."
                )
                try:
                    from streamlit_image_coordinates import streamlit_image_coordinates  # type: ignore

                    scale = float(getattr(im, "scale", 1.0) or 1.0)
                    click_key = f"coords_{upload_sig}_{page_idx}_{resolution}_{use_virtual_border}"
                    clicked = streamlit_image_coordinates(annotated, key=click_key)
                    if clicked and isinstance(clicked, dict) and "x" in clicked and "y" in clicked:
                        px_x = float(clicked["x"])
                        pdf_x = px_x / scale
                        st.code(f"clicked(px x)={px_x:.1f}  ->  pdf x={pdf_x:.2f}")
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            if st.button("이 x를 left로", key=f"set_l_{click_key}", disabled=not use_virtual_border):
                                st.session_state[vk_l] = float(pdf_x)
                                st.rerun()
                        with c2:
                            if st.button("이 x를 right로", key=f"set_r_{click_key}", disabled=not use_virtual_border):
                                st.session_state[vk_r] = float(pdf_x)
                                st.rerun()
                        with c3:
                            if st.button("클릭 반영 후 재인식", key=f"rerun_{click_key}"):
                                st.rerun()
                    else:
                        st.caption("이미지를 클릭하면 x 좌표가 표시됩니다.")
                except Exception:
                    st.warning(
                        "클릭 좌표 기능을 쓰려면 `streamlit-image-coordinates`가 필요합니다.\n\n"
                        "설치: `python -m pip install -r requirements.txt`"
                    )

        with col_meta:
            st.subheader("요약")
            st.write(
                {
                    "pages": total_pages,
                    "page_idx": int(page_idx),
                    "resolution": int(resolution),
                    "words": len(words),
                    "tables": len(table_rects),
                    "cells": len(cell_rects),
                    "use_virtual_border": use_virtual_border,
                    "virtual_left_x": float(v_left),
                    "virtual_right_x": float(v_right),
                    "defaults_left_right": (float(l_def), float(r_def)),
                    "engine": engine,
                    "y_tolerance": float(y_tolerance),
                    "join_gap": float(join_gap),
                    "table_settings": table_settings,
                }
            )

            if show_word_table:
                st.subheader("단어 좌표 샘플")
                st.dataframe(words[:300], use_container_width=True, height=360)

            if show_lines:
                st.subheader("라인 스트림(재구성)")
                try:
                    from parsers.layout_extractor import extract_lines as _extract_lines

                    lines = _extract_lines(
                        pdf_path=tmp_pdf_path,
                        page_num=int(page_idx),
                        pdfplumber_page=page,
                        engine=engine,
                        y_tolerance=float(y_tolerance),
                        join_gap=float(join_gap),
                    )
                except Exception as e:
                    st.error("라인 스트림 재구성에 실패했습니다.")
                    st.exception(e)
                    lines = []

                st.caption(f"lines={len(lines)} (표시는 상위 {min(len(lines), int(max_lines))}개)")
                st.text_area(
                    "lines",
                    value="\n".join(lines[: int(max_lines)]),
                    height=360,
                    label_visibility="collapsed",
                )

            if show_table_viz:
                st.subheader("표 추출(extract_tables) 샘플")
                try:
                    sample_tables = page.extract_tables(table_settings=table_settings) or []
                except Exception:
                    sample_tables = []
                st.caption(f"tables_extracted={len(sample_tables)} (상위 1개만 표시)")
                if sample_tables:
                    st.dataframe(sample_tables[0], use_container_width=True, height=260)
                else:
                    st.write(
                        "추출된 테이블이 없습니다. horizontal_strategy를 lines로 바꾸거나, "
                        "snap/join tolerance를 조정해보세요."
                    )

    if tmp_pdf_path:
        try:
            os.remove(tmp_pdf_path)
        except Exception:
            pass


if __name__ == "__main__":
    main()
