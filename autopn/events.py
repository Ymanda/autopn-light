from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List

EVENT_FIELDS: List[str] = [
    "email_id",
    "date_iso",
    "speaker_name",
    "speaker_email",
    "theme",
    "topic_name",
    "topic_side",
    "topic_visibility",
    "hidden_topic_hint",
    "hidden_topic_confidence",
    "argument_ref_name",
    "argument_text",
    "reasoning_name",
    "sophism_name",
    "sophism_category",
    "fact_texts",
    "statement_type",
    "will_texts",
    "complaint_object",
    "complaint_strength",
    "email_excerpt",
    "relevance",
    "reasoning_credibility",
    "impact_score",
    "impact_direction",
]


def _ensure_directory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


class EventsSink:
    """A tiny append-only CSV writer for the HTML report pipeline."""

    def __init__(self, path: Path):
        self.path = Path(path)
        _ensure_directory(self.path)
        self._ensure_header()

    def _ensure_header(self) -> None:
        if not self.path.exists():
            with self.path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=EVENT_FIELDS)
                writer.writeheader()

    def write(self, **kwargs) -> None:
        row = {key: kwargs.get(key, "") for key in EVENT_FIELDS}
        for score_key in ("relevance", "reasoning_credibility", "impact_score"):
            value = row.get(score_key, "")
            try:
                row[score_key] = float(value) if value != "" else ""
            except Exception:
                row[score_key] = ""
        with self.path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=EVENT_FIELDS)
            writer.writerow(row)


def guess_theme(text: str, theme_keywords: Dict[str, Iterable[str]] | None = None) -> str:
    """Return the best theme label for the supplied text."""
    keywords = theme_keywords or {}
    normalized = (text or "").lower()
    normalized = re.sub(r"\s+", " ", normalized)
    best_label, best_hits = "", 0
    for label, terms in keywords.items():
        hits = sum(1 for term in terms if term and term.lower() in normalized)
        if hits > best_hits:
            best_label, best_hits = label, hits
    return best_label or "General"

