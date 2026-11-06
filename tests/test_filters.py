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

def test_french_gender_suffix():
    assert category("Ligue des Champions F.") is None
    assert category("Ligue des Champions H.") == "international:club"


def test_french_domestic_league_aliases():
    assert category("Pologne Tauron Liga H.", "Projekt Warszawa - Slepsk Suwalki") == "league:poland"
    assert category("Italie Serie A1 H.") == "league:italy"
    assert category("Japon SV.League H.") == "league:japan"
    assert category("Pologne Tauron Liga F.") is None
    assert category("Italie Serie A1 F.") is None
    assert category("Japon SV.League F.") is None
    assert category("Pologne Tauron Liga H.", "Team A F. - Team B F.") is None


def test_verified_mens_japan_competition_id():
    assert category("Japon SV Ligue", "Voreas Hokkaido - Fragolad University", 36538) == "league:japan"
    assert category("Japon SV Ligue", "Unknown A - Unknown B") is None
    assert category("Japon SV Ligue F.", "Team A F. - Team B F.", 36538) is None
