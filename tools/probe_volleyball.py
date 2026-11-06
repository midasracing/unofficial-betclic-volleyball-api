"""Single-shot Betclic volleyball probe (no infinite polling)."""
import json
from app.client import BetclicSnapshotClient as BetclicClient
from app.filters import category

def main():
    client = BetclicClient(timeout=(5, 12))
    all_matches = []
    offset = 0
    seen = set()
    for _ in range(4):
        page = client.get_matches("volleyball", offset=offset)
        batch = page.get("matches", [])
        fresh = [m for m in batch if m.id is not None and m.id not in seen]
        if not fresh:
            break
        seen.update(m.id for m in fresh)
        all_matches.extend(fresh)
        offset += len(batch)
    valid = [m for m in all_matches if len(m.teams) == 2 and category(m.competition, m.name)]
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
    client.close()

if __name__ == "__main__":
    main()
