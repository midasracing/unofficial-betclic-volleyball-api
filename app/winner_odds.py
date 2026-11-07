"""Select match-winner odds without publishing exact-score or set markets.

The upstream default stream can contain many markets; this module deliberately
exposes only the match winner (two teams), not set winner or exact score.
"""
import math
import unicodedata
from functools import lru_cache

@lru_cache(maxsize=256)
def _normalize(name):
    text = unicodedata.normalize("NFKD", name or "").casefold()
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).split())

# Exact matches after accent/case normalization, to avoid matching sets or
# 'winner of the Xth set' markets. More labels should be added only from logs.
MATCH_WINNER_LABELS = frozenset({
    "vainqueur du match", "vainqueur", "resultat du match",
    "match winner", "winner of the match", "match result",
    "zwyciezca meczu", "zwyciezca spotkania", "zwyciezca",
    "vincente incontro", "vincente partita",
    "sieger des spiels", "spiel sieger",
})

def is_match_winner(name):
    return _normalize(name) in MATCH_WINNER_LABELS


def winner_market(markets):
    """Return a normalized two-choice match-winner market, or None."""
    for market in markets or []:
        name = market.get("name") if isinstance(market, dict) else getattr(market, "name", None)
        if not is_match_winner(name):
            continue
        selections = market.get("selections") if isinstance(market, dict) else getattr(market, "selections", [])
        if not selections or len(selections) != 2:
            continue
        suspended = market.get("suspended", False) if isinstance(market, dict) else getattr(market, "suspended", False)
        pairs = []
        for selection in selections:
            label = selection.get("name") if isinstance(selection, dict) else getattr(selection, "name", None)
            odd = selection.get("odds") if isinstance(selection, dict) else getattr(selection, "odds", None)
            if (label is not None and isinstance(odd, (int, float)) and not isinstance(odd, bool)
                    and math.isfinite(odd) and 1 < odd < 10000):
                pairs.append({"name": label, "odds": odd})
        # Volleyball match winner is a two-way market; avoid unrelated markets.
        if len(pairs) == 2:
            return {"name": name, "suspended": bool(suspended), "selections": pairs}
    return None
