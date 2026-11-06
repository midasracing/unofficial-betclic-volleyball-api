"""Strict men's competition allowlist. Unknown and ambiguous leagues stay excluded."""
import re
import unicodedata

def norm(text):
    return "".join(c for c in unicodedata.normalize("NFKD", text or "").casefold() if not unicodedata.combining(c))

WOMEN = re.compile(r"\b(women|woman|womens|female|feminin|femminile|femmes|kobiet|kobiety|zenska|zenske|dames|ladies)\b|女子|女性|女排")
LEAGUES = {
    "poland": ("plusliga", "plus liga"),
    "italy": ("superlega", "super lega", "serie a1 maschile"),
    "japan": ("sv.league men", "sv league men", "svleague men", "sv league m", "v league men"),
}
FRENCH_LEAGUES = {
    "poland": re.compile(r"^pologne\s+(?:tauron\s+liga|plus\s*liga)\s+h\.?$"),
    "italy": re.compile(r"^italie\s+(?:superlega|serie\s+a1)\s+h\.?$"),
    "japan": re.compile(r"^japon\s+(?:sv[.\s]*league|v[.\s]*league)\s+h\.?$"),
}
CLUB = ("champions league", "liga mistrzow", "ligue des champions", "cev champions", "cev cup", "puchar cev", "challenge cup", "club world championship", "club world championships", "klubowe mistrzostwa swiata")
NATIONAL = ("volleyball nations league", "nations league", "liga narodow", "world championship", "mistrzostwa swiata", "european championship", "mistrzostwa europy", "olympic", "olimpij", "world cup", "puchar swiata")

VERIFIED_MENS_LEAGUE_IDS = {36538: "league:japan"}

def category(competition, match_name=None, competition_id=None):
    competition_name = norm(competition)
    if not competition_name or WOMEN.search(competition_name + " " + norm(match_name)) or re.search(r"(?:^|\s)f\.(?:\s|$)", competition_name + " " + norm(match_name)):
        return None
    try:
        verified = VERIFIED_MENS_LEAGUE_IDS.get(int(competition_id))
    except (TypeError, ValueError):
        verified = None
    if verified:
        return verified
    for country, pattern in FRENCH_LEAGUES.items():
        if pattern.fullmatch(competition_name.strip()):
            return "league:" + country
    for country, names in LEAGUES.items():
        if any(alias in competition_name for alias in names):
            return "league:" + country
    if any(alias in competition_name for alias in CLUB):
        return "international:club"
    if any(alias in competition_name for alias in NATIONAL):
        return "international:national"
    return None
