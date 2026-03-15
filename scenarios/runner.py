"""
Benchmark runner – uruchamia 24 scenariusze CRUD na wybranej bazie danych.
Każdy scenariusz wykonywany jest <repetitions> razy i wyniki uśredniane.

Scenariusze:
  C1–C6  CREATE
  R1–R6  READ
  U1–U6  UPDATE
  D1–D6  DELETE
"""
import random
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from faker import Faker

fake = Faker("pl_PL")

# ────────────────────────────────────────────────────────────────
# Struktury wyników
# ────────────────────────────────────────────────────────────────


@dataclass
class ScenarioResult:
    scenario_id: str
    description: str
    times: List[float] = field(default_factory=list)

    @property
    def avg(self) -> float:
        valid = [t for t in self.times if t >= 0]
        return sum(valid) / len(valid) if valid else -1.0

    @property
    def min_t(self) -> float:
        valid = [t for t in self.times if t >= 0]
        return min(valid) if valid else -1.0

    @property
    def max_t(self) -> float:
        valid = [t for t in self.times if t >= 0]
        return max(valid) if valid else -1.0


# ────────────────────────────────────────────────────────────────
# Opisy scenariuszy (24 szt.)
# ────────────────────────────────────────────────────────────────

SCENARIO_DESCRIPTIONS: Dict[str, str] = {
    "C1": "Dodanie pojedynczego produktu alkoholowego",
    "C2": "Dodanie hurtowe 1000 produktów alkoholowych",
    "C3": "Dodanie nowego klienta",
    "C4": "Dodanie nowego paragonu",
    "C5": "Dodanie pozycji do paragonu",
    "C6": "Dodanie nowej faktury",
    "R1": "Pobranie produktu alkoholowego po ID",
    "R2": "Lista produktów z danej kategorii",
    "R3": "Historia paragonów klienta",
    "R4": "Szczegóły paragonu z pozycjami (JOIN)",
    "R5": "Top 10 najdroższych produktów alkoholowych",
    "R6": "Wyszukanie faktur z danego okresu",
    "U1": "Aktualizacja ceny produktu alkoholowego",
    "U2": "Aktualizacja danych klienta",
    "U3": "Zmiana statusu transakcji paragonu",
    "U4": "Masowa aktualizacja cen w kategorii (promocja)",
    "U5": "Aktualizacja statusu dostawy",
    "U6": "Aktualizacja danych producenta",
    "D1": "Usunięcie pojedynczego produktu alkoholowego",
    "D2": "Usunięcie klienta",
    "D3": "Usunięcie paragonu (kaskadowe z pozycjami)",
    "D4": "Usunięcie pozycji paragonu",
    "D5": "Masowe usunięcie 100 produktów tytoniowych",
    "D6": "Usunięcie dostawy",
}



def _measure(fn: Callable, *args, **kwargs) -> float:
    """Mierzy czas wykonania fn w sekundach."""
    start = time.perf_counter()
    fn(*args, **kwargs)
    return time.perf_counter() - start


# ────────────────────────────────────────────────────────────────
# Fabryka danych testowych
# ────────────────────────────────────────────────────────────────

_counter = {"alk": 9_000_000, "kli": 9_000_000, "par": 9_000_000,
            "poz": 9_000_000, "fak": 9_000_000, "tyt": 9_000_000,
            "dos": 9_000_000}


def _next(key: str) -> int:
    _counter[key] += 1
    return _counter[key]


def _make_alkohol():
    pid = _next("alk")
    return {
        "produkt_id": pid,
        "nazwa": f"Benchmark Vodka #{pid}",
        "marka": "Benchmark",
        "kategoria_id": random.randint(1, 20),
        "producent_id": random.randint(1, 50),
        "kraj_pochodzenia": "Polska",
        "rocznik": None,
        "procent_alkoholu": 40.0,
        "pojemnosc_ml": 700,
        "typ_opakowania": "butelka",
        "data_wprowadzenia_do_sprzedazy": "2025-01-01",
        "data_wycofania": None,
        "kod_kreskowy": str(random.randint(1_000_000_000_000, 9_999_999_999_999)),
        "cena_producenta": round(random.uniform(10, 200), 2),
        "stawka_vat": 23,
    }


def _make_klient():
    kid = _next("kli")
    return {
        "klient_id": kid,
        "imie": fake.first_name(),
        "nazwisko": fake.last_name(),
        "data_urodzenia": "1990-05-15",
        "plec": random.choice(["M", "K"]),
        "miasto": "Warszawa",
        "telefon": "600-000-000",
        "email": f"bench.{kid}@example.com",
        "data_rejestracji": "2025-01-01",
        "status_klienta": "aktywny",
        "punkty_lojalnosciowe": 0,
    }


def _make_paragon():
    parid = _next("par")
    return {
        "paragon_id": parid,
        "numer_paragonu": f"PAR/2025/{parid:08d}",
        "data_sprzedazy": "2025-01-15",
        "godzina_sprzedazy": "12:00:00",
        "pracownik_id": random.randint(1, 20),
        "klient_id": random.randint(1, 100),
        "forma_platnosci": "gotówka",
        "wartosc_netto": 100.0,
        "wartosc_brutto": 123.0,
        "status_transakcji": "zakonczona",
    }


def _make_pozycje(paragon_id: int, count: int = 5):
    poz = []
    for _ in range(count):
        pid = _next("poz")
        poz.append({
            "pozycja_id": pid,
            "paragon_id": paragon_id,
            "produkt_id": random.randint(1, 5000),
            "typ_produktu": random.choice(["A", "T"]),
            "wartosc_netto": round(random.uniform(5.0, 100.0), 2),
            "wartosc_brutto": round(random.uniform(6.0, 123.0), 2),
        })
    return poz


def _make_faktura():
    fid = _next("fak")
    return {
        "faktura_id": fid,
        "numer_faktury": f"FV/2025/{fid:08d}",
        "sprzedaz_id": random.randint(1, 100),
        "data_wystawienia": "2025-01-15",
        "data_sprzedazy": "2025-01-15",
        "nabywca_nazwa": "Firma Testowa Sp. z o.o.",
        "nabywca_nip": "123-456-78-90",
        "nabywca_adres": "Warszawa, ul. Testowa 1",
        "wartosc_netto": 100.0,
        "wartosc_vat": 23.0,
        "wartosc_brutto": 123.0,
        "status_faktury": "wystawiona",
    }


# ────────────────────────────────────────────────────────────────
# Główna funkcja benchmarkowa
# ────────────────────────────────────────────────────────────────

def run_benchmarks(db, repetitions: int = 3,
                   scenarios: Optional[List[str]] = None,
                   progress_callback=None) -> List[ScenarioResult]:
    """
    Uruchamia scenariusze CRUD na obiekcie db (BaseDB).

    :param db:                instancja bazy danych (połączona, z danymi)
    :param repetitions:       liczba powtórzeń każdego scenariusza
    :param scenarios:         lista ID do uruchomienia (None = wszystkie 24)
    :param progress_callback: fn(scenario_id, rep, total_reps)
    :return:                  lista ScenarioResult
    """
    cb = progress_callback or (lambda sid, r, t: None)

    all_scenarios = {
        "C1": lambda: db.C1_add_single_alkohol(_make_alkohol()),
        "C2": lambda: db.C2_add_bulk_alkohol([_make_alkohol() for _ in range(1000)]),
        "C3": lambda: db.C3_add_klient(_make_klient()),
        "C4": lambda: db.C4_add_paragon(_make_paragon()),
        "C5": lambda: db.C5_add_pozycje_paragonu(
            _make_pozycje(random.randint(1, 100))),
        "C6": lambda: db.C6_add_faktura(_make_faktura()),
        "R1": lambda: db.R1_get_alkohol_by_id(random.randint(1, 5000)),
        "R2": lambda: db.R2_get_products_by_kategoria(random.randint(1, 20)),
        "R3": lambda: db.R3_get_paragony_klienta(random.randint(1, 100)),
        "R4": lambda: db.R4_get_paragon_details(random.randint(1, 100)),
        "R5": lambda: db.R5_get_top_expensive_alkohol(10),
        "R6": lambda: db.R6_get_faktury_by_okres("2024-01-01", "2024-06-30"),
        "U1": lambda: db.U1_update_cena_alkohol(
            random.randint(1, 5000), round(random.uniform(10, 500), 2)),
        "U2": lambda: db.U2_update_klient(
            random.randint(1, 100),
            {"email": "updated@example.com", "telefon": "999-999-999"}),
        "U3": lambda: db.U3_update_status_paragonu(
            random.randint(1, 100), "zakonczona"),
        "U4": lambda: db.U4_bulk_update_ceny_kategoria(
            random.randint(1, 20), 10.0),
        "U5": lambda: db.U5_update_status_dostawy(
            random.randint(1, 100), "dostarczona"),
        "U6": lambda: db.U6_update_producent(
            random.randint(1, 50),
            {"nazwa": "Updated Producent", "strona_www": "https://updated.pl"}),
        "D1": lambda: db.D1_delete_alkohol(_next("alk")),
        "D2": lambda: db.D2_delete_klient(_next("kli")),
        "D3": lambda: db.D3_delete_paragon(_next("par")),
        "D4": lambda: db.D4_delete_pozycje_paragonu(_next("par")),
        "D5": lambda: db.D5_bulk_delete_tyton(
            [_next("tyt") for _ in range(100)]),
        "D6": lambda: db.D6_delete_dostawa(_next("dos")),
    }

    selected = scenarios or list(all_scenarios.keys())
    results: List[ScenarioResult] = []

    for sid in selected:
        if sid not in all_scenarios:
            continue
        fn = all_scenarios[sid]
        desc = SCENARIO_DESCRIPTIONS.get(sid, sid)
        result = ScenarioResult(scenario_id=sid, description=desc)
        for rep in range(1, repetitions + 1):
            cb(sid, rep, repetitions)
            try:
                elapsed = _measure(fn)
                result.times.append(elapsed)
            except Exception:
                result.times.append(-1.0)
        results.append(result)

    return results
