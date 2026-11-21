"""
Modular metadata lookup service for adult/OTT catalog items (/batch18).

Provides pure, unit-testable functions for:
  - IMDb Suggestion API fuzzy matching (free, fast, no API key)
  - OMDb exact ID / title lookup
  - TVMaze series lookup
  - Wikipedia OpenSearch + summary lookup
  - Groq fallback prompt construction when Gemini keys hit quota
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote


def normalize_title_tokens(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def pick_best_imdb_suggestion(
    items: List[Dict[str, Any]],
    movie_name: str,
    movie_year: str = "",
) -> Optional[Dict[str, Any]]:
    """Score IMDb suggestion items by title similarity, year agreement, and artwork."""
    target_norm = normalize_title_tokens(movie_name)
    if not target_norm:
        return None
    target_words = set(target_norm.split())

    best_item: Optional[Dict[str, Any]] = None
    best_score = -1

    for item in items or []:
        imdb_id = str(item.get("id") or "")
        if not imdb_id.startswith("tt"):
            continue

        label_norm = normalize_title_tokens(item.get("l") or "")
        score = 0
        if label_norm == target_norm:
            score += 60
        elif target_norm in label_norm or (label_norm and label_norm in target_norm):
            score += 35
        elif target_words:
            label_words = set(label_norm.split())
            if label_words:
                score += int(35 * len(target_words & label_words) / len(target_words))

        y_val = item.get("y")
        if movie_year and str(movie_year).isdigit() and y_val:
            diff = abs(int(movie_year) - int(y_val))
            if diff == 0:
                score += 30
            elif diff <= 2:
                score += 12
            elif diff > 3:
                score -= 60

        if isinstance(item.get("i"), dict) and item["i"].get("imageUrl"):
            score += 10

        if score > best_score:
            best_score = score
            best_item = item

    return best_item if best_score >= 30 else None


def build_imdb_suggestion_url(movie_name: str) -> str:
    clean_first = re.sub(r"[^a-z0-9]", "", str(movie_name or "").lower())[:1] or "x"
    return f"https://v3.sg.media-imdb.com/suggestion/{clean_first}/{quote(movie_name.strip())}.json"


def detect_ott_platform(*sources: str) -> str:
    combined = " ".join(str(s or "") for s in sources).lower()
    for platform in (
        "ullu",
        "altbalaji",
        "altt",
        "atrangii",
        "woow",
        "akkuott",
        "primeshots",
        "kooku",
        "neonx",
        "hotx",
        "voovi",
        "bigmoviezoo",
    ):
        if platform in combined:
            return platform.capitalize()
    return "Indian OTT (Ullu / AltBalaji / Atrangii / PrimeShots / Kooku / Woow)"
