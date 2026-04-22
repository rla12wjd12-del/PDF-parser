# -*- coding: utf-8 -*-
"""table_settings 헬퍼 단위 테스트."""

from parsers.table_settings import (
    pick_best_table,
    table_set_has_header_signals,
)


def test_table_set_has_header_signals_finds_keyword() -> None:
    tables = [
        [["a", "b"], ["국가기술자격", "종목", "합격일"]],
    ]
    assert table_set_has_header_signals(tables, ["종목", "합격"]) is True


def test_table_set_has_header_signals_empty() -> None:
    assert table_set_has_header_signals([], ["x"]) is False
    assert table_set_has_header_signals(None, ["x"]) is False  # type: ignore[arg-type]


def test_pick_best_table() -> None:
    def score(tbl: list) -> tuple[int, int]:
        return (len(tbl), max((len(r) for r in tbl), default=0))

    best = pick_best_table([["a"], ["a", "b", "c"], ["x"]], score)
    assert best == ["a", "b", "c"]
