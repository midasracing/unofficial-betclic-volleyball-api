# Dłuższy test strumienia — 6 listopada 2026

Główne okno obserwacji: **2026-11-06T11:56:08.553377+00:00 → 2026-11-06T12:06:08.555784+00:00**, 600 sekund.

Obserwowano dwa rozpoczęte mecze tenisowe. Każdy miał limit czterech połączeń, z co najmniej 90 sekundami przerwy po zamknięciu lub timeoutcie. Okno dziesięciu minut nie oznacza jednego nieprzerwanego połączenia. Dodatkowo sprawdzono mecz tenisa stołowego po jego planowanej godzinie rozpoczęcia.

| Mecz | Paczki | Połączenia | Zmiany danych punktowych na tym samym połączeniu | Zmiany surowych kursów na zwycięzcę na tym samym połączeniu | Zmiany z dwiema wartościami > 1,00 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cheonhui Joo - Miwa Harimoto | 4 | 2 | 0 | 0 | 0 |
| Aliaksandra Sasnovich - Maria Sakkari | 58 | 4 | 6 | 7 | 4 |
| Janice Tjen - Polina Kudermetova | 37 | 4 | 4 | 4 | 0 |

## Potwierdzone aktualizacje

Na trzecim połączeniu dla **Aliaksandra Sasnovich – Maria Sakkari** odebrano:

- punkty **15:0 → 15:15** przy stanie gemów 3:4 w drugim secie;
- parę kursów na zwycięzcę **5,90 / 1,06 → 6,90 / 1,03**, następnie **6,10 / 1,05**.

Powyższe zmiany przyszły w ramach jednego już otwartego połączenia HTTP. Nie były wynikiem ponownego pobierania szczegółów przez klienta. Zapisano też punkty 40:40, przewagę A:40 oraz wyniki ukończonych setów. Punkty pochodzą z danych wynikowych, nie z nazw rynków ani z kursów.

## Co wykryła analiza

- Dane wynikowe są obecne zarówno w początkowym obrazie meczu (`root.1.1.13`), jak i w osobnych powiadomieniach (`root.2.1.2.1`).
- W tenisie trzeba zachować tekstową wartość przewagi `A`; nie wolno sprowadzać wszystkich punktów do liczb całkowitych.
- Źródło przesyła także wartość kursu `1.00`. Parser upstream odrzuca wartości niewiększe od 1, przez co może pozostawić tylko jedną stronę rynku. Diagnostyka zachowuje surowe wartości i nie interpretuje niezweryfikowanych kodów źródła jako dostępności do gry.
- Dla meczu Cheonhui Joo – Miwa Harimoto nie odebrano danych punktowych ani rozpoznanego rynku na zwycięzcę. Sama flaga live i miniona godzina startu nie potwierdzają obecności tych danych.
- Występowały zamknięcia połączeń i timeouty. Potwierdzono aktualizacje, ale nie ciągłość połączenia przez całe dziesięć minut.

## Powtórzenie i testy offline

```bash
python -m tools.probe_long_streaming --seconds 600
# Narzędzie wypisze nowy katalog przechwytywania:
python -m tools.analyze_long_capture captures/long/IDENTYFIKATOR_TESTU
python -m pytest -q tests
```

Surowe wiadomości i pełne historie tego testu znajdują się lokalnie w `captures/long/` i są ignorowane przez Git. Minimalne autentyczne wiadomości punktowe stanowią fixtures w `tests/test_stream_evidence.py`. Nowe narzędzia dotyczą diagnostyki; zakres produkcyjnego API pozostaje bez zmian.
