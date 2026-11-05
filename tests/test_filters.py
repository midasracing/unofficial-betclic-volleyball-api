from app.filters import category

def test_mens_leagues():
    assert category("PlusLiga") == "league:poland"
    assert category("SuperLega") == "league:italy"
    assert category("SV.League Men") == "league:japan"

def test_international():
    assert category("CEV Champions League") == "international:club"
    assert category("Volleyball Nations League") == "international:national"

def test_women_and_unknown_excluded():
    assert category("CEV Champions League Women") is None
    assert category("PlusLiga Women") is None
    assert category("Serie A1") is None
    assert category("Unknown Cup") is None
