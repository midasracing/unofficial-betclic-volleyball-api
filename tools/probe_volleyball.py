"""Single-shot Betclic volleyball probe (no infinite polling)."""
import json
from betclic_api import BetclicClient
from app.filters import category

def main():
    client = BetclicClient(timeout=(5, 12))
    all_matches = []
    for offset in (0, 40, 80):
        page = client.get_matches("volleyball", offset=offset)
        all_matches.extend(page.get("matches", []))
        if len(page.get("matches", [])) < 40:
            break
    valid = [m for m in all_matches if category(m.competition, m.name)]
    live = [m for m in valid if m.is_live]
    print(json.dumps({
        "all_volleyball_count": len(all_matches),
        "eligible_mens_count": len(valid),
        "eligible_live_count": len(live),
        "eligible_matches": [{"id": m.id, "name": m.name, "competition": m.competition,
                              "is_live": m.is_live} for m in valid[:25]],
    }, indent=2, ensure_ascii=False))
    if live:
        match = client.get_match(live[0].id)
        print(f"LIVE detail: id={match.id}, markets={len(match.markets)}")
        print("Score support in source model: NO (requires separate decoder)")

if __name__ == "__main__":
    main()
