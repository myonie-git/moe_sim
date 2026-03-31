"""Logical rank ordering helpers."""

from __future__ import annotations

from enum import Enum


class RankMapping(str, Enum):
    """Logical rank assignment strategies."""

    NATURAL = "natural"
    ROW_MAJOR = "row_major"
    COLUMN_MAJOR = "column_major"
    SNAKE = "snake"
    HILBERT = "hilbert"
    CUSTOM = "custom"
