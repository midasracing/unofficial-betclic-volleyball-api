from types import SimpleNamespace
from app.winner_odds import winner_market

def market(name, a=1.2, b=3.1):
    return SimpleNamespace(name=name, suspended=False, selections=[
        SimpleNamespace(name="Home", odds=a),
        SimpleNamespace(name="Away", odds=b),
    ])

def test_only_match_winner_from_many_markets():
    markets = [
        market("Score Final Sets"), market("2ème Set - Vainqueur du Set"),
        market("Vainqueur du match", 1.52, 2.48),
        market("3ème Set - Vainqueur du Set"),
    ]
    actual = winner_market(markets)
    assert actual == {"name": "Vainqueur du match", "suspended": False,
                      "selections": [{"name": "Home", "odds": 1.52},
                                     {"name": "Away", "odds": 2.48}]}

def test_no_winner_does_not_infer_from_set_or_score():
    assert winner_market([market("Score Final Sets"), market("1er Set - Vainqueur du Set")]) is None

def test_accept_normalized_polish_and_dict():
    result = winner_market([{"name": "Zwycięzca meczu", "suspended": True,
                             "selections": [{"name": "A", "odds": 1.8},
                                            {"name": "B", "odds": 2.1}]}])
    assert result["suspended"] is True
    assert result["selections"][1]["odds"] == 2.1

def test_reject_three_way_market_even_if_named_winner():
    bad = {"name": "Match Winner", "selections": [
        {"name": "1", "odds": 1.2}, {"name": "X", "odds": 4},
        {"name": "2", "odds": 6}]}
    assert winner_market([bad]) is None


def test_invalid_third_selection_does_not_turn_a_three_way_market_into_two_way():
    bad = {"name": "Match Winner", "selections": [
        {"name": "1", "odds": 1.2}, {"name": "X", "odds": float('nan')},
        {"name": "2", "odds": 6}]}
    assert winner_market([bad]) is None
