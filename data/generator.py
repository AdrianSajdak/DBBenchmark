"""
Generator danych testowych – model danych zgodny z TABELE.txt.
10 tabel: alkohol, tyton, kategorie_produktow, producenci, klienci,
pracownicy, paragony, pozycje_paragonu, faktury, dostawy.

Obsługa przyrostowego seedowania przez generate_delta():
  - generuje tylko NOWE rekordy między fazami (np. small → medium)
  - ID zaczyna się od poprzedniego maksimum + 1
"""
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from faker import Faker

fake = Faker("pl_PL")
random.seed(42)

# ────────────────────────────────────────────────────────────────
# Stałe słowniki dziedzinowe
# ────────────────────────────────────────────────────────────────

KATEGORIE_RAW: List[Dict] = [
    {"nazwa": "Piwo", "opis": "Piwa jasne, ciemne, kraftowe",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Wino czerwone", "opis": "Wina czerwone wytrawne i półsłodkie",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Wino białe", "opis": "Wina białe wytrawne i półsłodkie",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Wino różowe", "opis": "Wina różowe",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Wino musujące", "opis": "Szampan, prosecco, cava",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Wódka", "opis": "Wódki czyste",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Wódka smakowa", "opis": "Wódki smakowe i kolorowe",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Whisky", "opis": "Whisky szkocka, irlandzka",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Whisky bourbon", "opis": "Bourbon i Tennessee whiskey",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Rum", "opis": "Rumy jasne i ciemne",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Gin", "opis": "Giny London Dry i inne",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Koniak", "opis": "Koniaki i brandy",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Likier", "opis": "Likiery smakowe",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Nalewka", "opis": "Nalewki tradycyjne",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Tequila", "opis": "Tequila i mezcal",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Piwo bezalkoholowe", "opis": "Piwa 0% i niskoalkoholowe",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Wino bezalkoholowe", "opis": "Wina bezalkoholowe",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Absynt", "opis": "Absynt i piołunówka",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Cydr", "opis": "Cydry jabłkowe i gruszkowe",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Miód pitny", "opis": "Miody pitne polskie",
     "czy_alkohol": True, "czy_tyton": False},
    {"nazwa": "Papierosy", "opis": "Papierosy tradycyjne",
     "czy_alkohol": False, "czy_tyton": True},
    {"nazwa": "Cygara", "opis": "Cygara kubańskie i inne",
     "czy_alkohol": False, "czy_tyton": True},
    {"nazwa": "Cygaretki", "opis": "Cygaretki i mini cygara",
     "czy_alkohol": False, "czy_tyton": True},
    {"nazwa": "Tytoń do palenia", "opis": "Tytoń fajkowy i do skręcania",
     "czy_alkohol": False, "czy_tyton": True},
    {"nazwa": "Tytoń do żucia", "opis": "Snus i tytoń do żucia",
     "czy_alkohol": False, "czy_tyton": True},
    {"nazwa": "E-papierosy", "opis": "Elektroniczne papierosy",
     "czy_alkohol": False, "czy_tyton": True},
    {"nazwa": "Wkłady e-papierosów", "opis": "Wkłady, liquidy, pody",
     "czy_alkohol": False, "czy_tyton": True},
    {"nazwa": "Bibułki i filtry", "opis": "Bibułki, filtry, gilzy",
     "czy_alkohol": False, "czy_tyton": True},
    {"nazwa": "Fajki", "opis": "Fajki tradycyjne",
     "czy_alkohol": False, "czy_tyton": True},
    {"nazwa": "Szisza", "opis": "Tytoń do fajki wodnej",
     "czy_alkohol": False, "czy_tyton": True},
]

ALKOHOL_MARKI = [
    "Żubrówka", "Wyborowa", "Soplica", "Lubelska", "Absolut", "Finlandia",
    "Grey Goose", "Smirnoff", "Belvedere", "Chopin", "Pan Tadeusz",
    "Jack Daniel's", "Johnnie Walker", "Jim Beam", "Jameson", "Glenfiddich",
    "Hennessy", "Rémy Martin", "Courvoisier", "Martell",
    "Żywiec", "Tyskie", "Lech", "Okocim", "Heineken", "Carlsberg",
    "Corona", "Budweiser", "Desperados", "Peroni", "Guinness",
    "Carlo Rossi", "Freixenet", "Moët & Chandon", "Prosecco DOC",
    "Aperol", "Campari", "Martini", "Cinzano", "Jägermeister",
    "Ballantine's", "Grant's", "Famous Grouse", "Beefeater", "Gordon's",
    "Bacardi", "Havana Club", "Captain Morgan", "Malibu", "Diplomatico",
    "Soplica Wiśniowa", "Żołądkowa Gorzka", "Krupnik", "Goldwasser",
    "Strongbow", "Somersby", "Warka Radler",
]

ALKOHOL_NAZWY_PREFIXES = [
    "Premium", "Reserve", "Gold", "Black Label", "Silver",
    "Red Label", "Blue", "Classic", "Original", "Vintage",
    "Special", "Extra Dry", "Double Barrel", "Single Malt",
    "Blended", "Aged", "Smooth", "Craft", "Limited Edition",
]

TYTON_MARKI = [
    "Marlboro", "Camel", "Winston", "L&M", "Philip Morris",
    "Lucky Strike", "Pall Mall", "Davidoff", "Dunhill", "Rothmans",
    "Vogue", "Kent", "Parliament", "West", "Chesterfield",
    "American Spirit", "Newport", "IQOS", "Glo", "Vuse",
]

TYTON_NAZWY = [
    "Red", "Blue", "Gold", "Silver", "Menthol", "Ice",
    "Classic", "Lights", "Ultra Lights", "100s", "Slims",
    "Original", "Fresh", "Click", "Double", "Compact",
]

KRAJE = [
    "Polska", "Francja", "Włochy", "Hiszpania", "Niemcy",
    "Wielka Brytania", "Szkocja", "Irlandia", "USA", "Meksyk",
    "Kuba", "Brazylia", "Argentyna", "Chile", "Australia",
    "Japonia", "Rosja", "Szwecja", "Finlandia", "Holandia",
]

FORMY_PLATNOSCI = ["gotówka", "karta_debetowa", "karta_kredytowa", "BLIK", "przelew"]
STATUSY_TRANSAKCJI = ["zakonczona", "anulowana", "zwrot", "w_trakcie"]
STATUSY_FAKTURY = ["wystawiona", "oplacona", "anulowana", "korekta"]
STATUSY_DOSTAWY = ["zamowiona", "w_transporcie", "dostarczona", "anulowana", "reklamacja"]
STANOWISKA = ["kasjer", "kierownik_zmiany", "magazynier", "sprzedawca",
              "kierownik_sklepu", "zastepca_kierownika", "specjalista_IT"]
STATUSY_KLIENTA = ["aktywny", "nieaktywny", "zablokowany", "VIP"]
STATUSY_PRACOWNIKA = ["aktywny", "na_urlopie", "zwolniony", "zawieszony"]
TYP_OPAKOWANIA_ALKOHOL = ["butelka", "puszka", "karton", "bag-in-box", "beczka"]
TYP_OPAKOWANIA_TYTON = ["paczka", "karton", "folia", "puszka", "worek"]


# ────────────────────────────────────────────────────────────────
# Generator poszczególnych tabel
# ────────────────────────────────────────────────────────────────

def gen_kategorie_produktow(n: int = 30) -> List[Dict]:
    cats = []
    for i, raw in enumerate(KATEGORIE_RAW[:n], start=1):
        cats.append({
            "kategoria_id": i,
            "nazwa": raw["nazwa"],
            "opis": raw["opis"],
            "czy_alkohol": raw["czy_alkohol"],
            "czy_tyton": raw["czy_tyton"],
        })
    return cats


def gen_producenci(n: int, id_offset: int = 0) -> List[Dict]:
    """id_offset: generuj IDs od (id_offset+1) do (id_offset+n)."""
    prods = []
    for i in range(id_offset + 1, id_offset + n + 1):
        prods.append({
            "producent_id": i,
            "nazwa": fake.company(),
            "kraj": random.choice(KRAJE),
            "miasto": fake.city(),
            "rok_zalozenia": random.randint(1850, 2023),
            "strona_www": fake.url(),
            "czy_aktywny": random.choice([True, True, True, False]),
        })
    return prods


def gen_pracownicy(n: int, id_offset: int = 0) -> List[Dict]:
    emps = []
    for i in range(id_offset + 1, id_offset + n + 1):
        hire = fake.date_between(start_date="-15y", end_date="today")
        fired = None
        status = random.choice(STATUSY_PRACOWNIKA)
        if status == "zwolniony":
            fired = str(fake.date_between(start_date=hire, end_date="today"))
        emps.append({
            "pracownik_id": i,
            "imie": fake.first_name(),
            "nazwisko": fake.last_name(),
            "stanowisko": random.choice(STANOWISKA),
            "data_zatrudnienia": str(hire),
            "data_zwolnienia": fired,
            "pensja": round(random.uniform(3500.0, 12000.0), 2),
            "telefon": fake.phone_number(),
            "email": fake.email(),
            "status_pracownika": status,
        })
    return emps


def gen_klienci(n: int, id_offset: int = 0) -> List[Dict]:
    imiona = [fake.first_name() for _ in range(min(n, 5000))]
    nazwiska = [fake.last_name() for _ in range(min(n, 5000))]
    miasta = [fake.city() for _ in range(min(n, 2000))]
    telefony = [fake.phone_number() for _ in range(min(n, 5000))]

    klienci = []
    for i in range(id_offset + 1, id_offset + n + 1):
        bday = fake.date_of_birth(minimum_age=18, maximum_age=85)
        reg = fake.date_between(start_date="-5y", end_date="today")
        imie = random.choice(imiona)
        nazw = random.choice(nazwiska)
        klienci.append({
            "klient_id": i,
            "imie": imie,
            "nazwisko": nazw,
            "data_urodzenia": str(bday),
            "plec": random.choice(["M", "K"]),
            "miasto": random.choice(miasta),
            "telefon": random.choice(telefony),
            "email": f"{imie.lower()}.{nazw.lower()}.{i}@example.com",
            "data_rejestracji": str(reg),
            "status_klienta": random.choice(STATUSY_KLIENTA),
            "punkty_lojalnosciowe": random.randint(0, 50000),
        })
    return klienci


def gen_alkohol(n: int, kat_alkohol_ids: List[int],
                producent_ids: List[int], id_offset: int = 0) -> List[Dict]:
    products = []
    for i in range(id_offset + 1, id_offset + n + 1):
        marka = random.choice(ALKOHOL_MARKI)
        prefix = random.choice(ALKOHOL_NAZWY_PREFIXES)
        rocznik = None
        kat_id = random.choice(kat_alkohol_ids)
        if kat_id in range(2, 6) or kat_id in (8, 9):
            rocznik = random.randint(1990, 2025)
        products.append({
            "produkt_id": i,
            "nazwa": f"{marka} {prefix} #{i}",
            "marka": marka,
            "kategoria_id": kat_id,
            "producent_id": random.choice(producent_ids),
            "kraj_pochodzenia": random.choice(KRAJE),
            "rocznik": rocznik,
            "procent_alkoholu": round(random.uniform(0.0, 70.0), 1),
            "pojemnosc_ml": random.choice([250, 330, 500, 700, 750, 1000, 1500, 1750]),
            "typ_opakowania": random.choice(TYP_OPAKOWANIA_ALKOHOL),
            "data_wprowadzenia_do_sprzedazy": str(
                fake.date_between(start_date="-5y", end_date="today")),
            "data_wycofania": None,
            "kod_kreskowy": str(random.randint(1_000_000_000_000, 9_999_999_999_999)),
            "cena_producenta": round(random.uniform(2.0, 800.0), 2),
            "stawka_vat": random.choice([23, 8, 5]),
        })
    return products


def gen_tyton(n: int, kat_tyton_ids: List[int],
              producent_ids: List[int], id_offset: int = 0) -> List[Dict]:
    products = []
    for i in range(id_offset + 1, id_offset + n + 1):
        marka = random.choice(TYTON_MARKI)
        nazwa = random.choice(TYTON_NAZWY)
        products.append({
            "produkt_id": i,
            "nazwa": f"{marka} {nazwa} #{i}",
            "marka": marka,
            "kategoria_id": random.choice(kat_tyton_ids),
            "producent_id": random.choice(producent_ids),
            "kraj_pochodzenia": random.choice(KRAJE),
            "liczba_sztuk_w_opakowaniu": random.choice([10, 20, 25, 40, 50, 100]),
            "typ_opakowania": random.choice(TYP_OPAKOWANIA_TYTON),
            "data_wprowadzenia_do_sprzedazy": str(
                fake.date_between(start_date="-5y", end_date="today")),
            "data_wycofania": None,
            "kod_kreskowy": str(random.randint(1_000_000_000_000, 9_999_999_999_999)),
            "cena_producenta": round(random.uniform(8.0, 120.0), 2),
            "stawka_vat": 23,
        })
    return products


def gen_paragony(n: int, pracownik_ids: List[int],
                 klient_ids: List[int], id_offset: int = 0) -> List[Dict]:
    paragony = []
    start_dt = datetime(2022, 1, 1)
    end_dt = datetime(2025, 12, 31)
    delta_secs = int((end_dt - start_dt).total_seconds())

    for i in range(id_offset + 1, id_offset + n + 1):
        dt = start_dt + timedelta(seconds=random.randint(0, delta_secs))
        paragony.append({
            "paragon_id": i,
            "numer_paragonu": f"PAR/{dt.year}/{i:08d}",
            "data_sprzedazy": dt.strftime("%Y-%m-%d"),
            "godzina_sprzedazy": dt.strftime("%H:%M:%S"),
            "pracownik_id": random.choice(pracownik_ids),
            "klient_id": random.choice(klient_ids),
            "forma_platnosci": random.choice(FORMY_PLATNOSCI),
            "wartosc_netto": 0.0,
            "wartosc_brutto": 0.0,
            "status_transakcji": random.choice(STATUSY_TRANSAKCJI),
        })
    return paragony


def gen_pozycje_paragonu(paragony: List[Dict],
                         pozycje_per_paragon: float,
                         alkohol_products: List[Dict],
                         tyton_products: List[Dict],
                         pozycja_id_offset: int = 0) -> List[Dict]:
    """Generuje pozycje paragonów. Aktualizuje wartosc_netto/brutto w paragony."""
    pozycje = []
    pid = pozycja_id_offset + 1

    alk_ids = [p["produkt_id"] for p in alkohol_products]
    tyt_ids = [p["produkt_id"] for p in tyton_products]
    alk_ceny = {p["produkt_id"]: (p["cena_producenta"], p["stawka_vat"])
                for p in alkohol_products}
    tyt_ceny = {p["produkt_id"]: (p["cena_producenta"], p["stawka_vat"])
                for p in tyton_products}

    max_items = int(pozycje_per_paragon * 2)

    for paragon in paragony:
        count = random.randint(1, max(max_items, 2))
        total_netto = 0.0
        total_brutto = 0.0

        for _ in range(count):
            if random.random() < 0.7 and alk_ids:
                prod_id = random.choice(alk_ids)
                typ = "A"
                cena_netto, vat = alk_ceny.get(prod_id, (20.0, 23))
            elif tyt_ids:
                prod_id = random.choice(tyt_ids)
                typ = "T"
                cena_netto, vat = tyt_ceny.get(prod_id, (15.0, 23))
            else:
                prod_id = random.choice(alk_ids)
                typ = "A"
                cena_netto, vat = alk_ceny.get(prod_id, (20.0, 23))

            marza = random.uniform(1.1, 1.5)
            w_netto = round(cena_netto * marza, 2)
            w_brutto = round(w_netto * (1 + vat / 100), 2)

            pozycje.append({
                "pozycja_id": pid,
                "paragon_id": paragon["paragon_id"],
                "produkt_id": prod_id,
                "typ_produktu": typ,
                "wartosc_netto": w_netto,
                "wartosc_brutto": w_brutto,
            })
            total_netto += w_netto
            total_brutto += w_brutto
            pid += 1

        paragon["wartosc_netto"] = round(total_netto, 2)
        paragon["wartosc_brutto"] = round(total_brutto, 2)

    return pozycje


def gen_faktury(paragony: List[Dict], n: int,
                id_offset: int = 0) -> List[Dict]:
    selected = random.sample(paragony, min(n, len(paragony)))
    faktury = []
    for i, par in enumerate(selected, start=id_offset + 1):
        data_spr = par["data_sprzedazy"]
        try:
            dt = datetime.strptime(data_spr, "%Y-%m-%d")
            dt_wyst = dt + timedelta(days=random.randint(0, 7))
        except ValueError:
            dt_wyst = datetime.now()

        w_netto = par["wartosc_netto"]
        w_brutto = par["wartosc_brutto"]
        w_vat = round(w_brutto - w_netto, 2)

        faktury.append({
            "faktura_id": i,
            "numer_faktury": f"FV/{dt_wyst.year}/{i:08d}",
            "sprzedaz_id": par["paragon_id"],
            "data_wystawienia": dt_wyst.strftime("%Y-%m-%d"),
            "data_sprzedazy": data_spr,
            "nabywca_nazwa": fake.company(),
            "nabywca_nip": (f"{random.randint(100, 999)}-{random.randint(100, 999)}-"
                            f"{random.randint(10, 99)}-{random.randint(10, 99)}"),
            "nabywca_adres": f"{fake.city()}, {fake.street_address()}",
            "wartosc_netto": w_netto,
            "wartosc_vat": w_vat,
            "wartosc_brutto": w_brutto,
            "status_faktury": random.choice(STATUSY_FAKTURY),
        })
    return faktury


def gen_dostawy(n: int, producent_ids: List[int], id_offset: int = 0) -> List[Dict]:
    dostawy = []
    start_dt = datetime(2022, 1, 1)
    end_dt = datetime(2025, 12, 31)
    delta_secs = int((end_dt - start_dt).total_seconds())

    for i in range(id_offset + 1, id_offset + n + 1):
        dt_zamow = start_dt + timedelta(seconds=random.randint(0, delta_secs))
        dt_dost = dt_zamow + timedelta(days=random.randint(1, 30))
        w_netto = round(random.uniform(500.0, 50_000.0), 2)
        vat = random.choice([23, 8])
        w_brutto = round(w_netto * (1 + vat / 100), 2)

        dostawy.append({
            "dostawa_id": i,
            "numer_dostawy": f"DOS/{dt_zamow.year}/{i:08d}",
            "producent_id": random.choice(producent_ids),
            "data_zamowienia": dt_zamow.strftime("%Y-%m-%d"),
            "data_dostawy": dt_dost.strftime("%Y-%m-%d"),
            "wartosc_netto": w_netto,
            "wartosc_brutto": w_brutto,
            "status_dostawy": random.choice(STATUSY_DOSTAWY),
            "numer_faktury_zakupu": f"FZ/{dt_zamow.year}/{i:08d}",
        })
    return dostawy


# ────────────────────────────────────────────────────────────────
# Główna funkcja generująca (pełny zbiór)
# ────────────────────────────────────────────────────────────────

def generate_dataset(size_config: Dict, progress_callback=None) -> Dict[str, List[Dict]]:
    """
    Generuje kompletny zbiór danych wg TABELE.txt.
    Wszystkie ID zaczynają się od 1.
    """
    def _cb(name: str):
        if progress_callback:
            progress_callback(name)

    kategorie = gen_kategorie_produktow(size_config["kategorie"])
    _cb("kategorie_produktow")

    kat_alk_ids = [k["kategoria_id"] for k in kategorie if k["czy_alkohol"]]
    kat_tyt_ids = [k["kategoria_id"] for k in kategorie if k["czy_tyton"]]

    producenci = gen_producenci(size_config["producenci"])
    _cb("producenci")
    prod_ids = [p["producent_id"] for p in producenci]

    pracownicy = gen_pracownicy(size_config["pracownicy"])
    _cb("pracownicy")

    klienci = gen_klienci(size_config["klienci"])
    _cb("klienci")

    alkohol = gen_alkohol(size_config["alkohol"], kat_alk_ids, prod_ids)
    _cb("alkohol")

    tyton = gen_tyton(size_config["tyton"], kat_tyt_ids, prod_ids)
    _cb("tyton")

    pracownik_ids = [p["pracownik_id"] for p in pracownicy]
    klient_ids = [k["klient_id"] for k in klienci]
    paragony = gen_paragony(size_config["paragony"], pracownik_ids, klient_ids)
    _cb("paragony")

    pozycje = gen_pozycje_paragonu(paragony, size_config["pozycje_per_paragon"], alkohol, tyton)
    _cb("pozycje_paragonu")

    faktury = gen_faktury(paragony, size_config["faktury"])
    _cb("faktury")

    dostawy = gen_dostawy(size_config["dostawy"], prod_ids)
    _cb("dostawy")

    return {
        "kategorie_produktow": kategorie,
        "producenci": producenci,
        "pracownicy": pracownicy,
        "klienci": klienci,
        "alkohol": alkohol,
        "tyton": tyton,
        "paragony": paragony,
        "pozycje_paragonu": pozycje,
        "faktury": faktury,
        "dostawy": dostawy,
    }


# ────────────────────────────────────────────────────────────────
# Generowanie DELTY między dwoma rozmiarami (przyrostowe seedowanie)
# ────────────────────────────────────────────────────────────────

def generate_delta(prev_cfg: Dict, new_cfg: Dict,
                   prev_pozycje_count: int,
                   progress_callback=None) -> Dict[str, List[Dict]]:
    """
    Generuje tylko NOWE rekordy potrzebne do przejścia z prev_cfg do new_cfg.
    Kategorie są stałe (30) – nie generujemy delty.
    Producenci/pracownicy/klienci: generujemy brakującą liczbę z offsetem ID.

    :param prev_cfg:           DATA_SIZES[poprzedni_rozmiar]
    :param new_cfg:            DATA_SIZES[nowy_rozmiar]
    :param prev_pozycje_count: liczba pozycji_paragonu wygenerowanych w poprzedniej fazie
    :param progress_callback:  fn(stage_name)
    """
    def _cb(name: str):
        if progress_callback:
            progress_callback(name)

    # Kategorie bez zmian
    kategorie = gen_kategorie_produktow(new_cfg["kategorie"])
    kat_alk_ids = [k["kategoria_id"] for k in kategorie if k["czy_alkohol"]]
    kat_tyt_ids = [k["kategoria_id"] for k in kategorie if k["czy_tyton"]]

    # ── Nowi producenci ──
    delta_prod = new_cfg["producenci"] - prev_cfg["producenci"]
    new_producenci = (
        gen_producenci(delta_prod, id_offset=prev_cfg["producenci"]) if delta_prod > 0 else []
    )
    _cb("producenci")
    # Pełna lista ID producentów (stare + nowe) dla FK
    all_prod_ids = list(range(1, new_cfg["producenci"] + 1))

    # ── Nowi pracownicy ──
    delta_prac = new_cfg["pracownicy"] - prev_cfg["pracownicy"]
    new_pracownicy = (
        gen_pracownicy(delta_prac, id_offset=prev_cfg["pracownicy"]) if delta_prac > 0 else []
    )
    _cb("pracownicy")
    all_prac_ids = list(range(1, new_cfg["pracownicy"] + 1))

    # ── Nowi klienci ──
    delta_kli = new_cfg["klienci"] - prev_cfg["klienci"]
    new_klienci = gen_klienci(delta_kli, id_offset=prev_cfg["klienci"]) if delta_kli > 0 else []
    _cb("klienci")
    all_kli_ids = list(range(1, new_cfg["klienci"] + 1))

    # ── Nowy alkohol ──
    delta_alk = new_cfg["alkohol"] - prev_cfg["alkohol"]
    new_alkohol = gen_alkohol(delta_alk, kat_alk_ids, all_prod_ids,
                              id_offset=prev_cfg["alkohol"]) if delta_alk > 0 else []
    _cb("alkohol")
    # Pełna lista do pozycji
    all_alk = [{"produkt_id": i, "cena_producenta": random.uniform(2, 800), "stawka_vat": 23}
               for i in range(1, new_cfg["alkohol"] + 1)]
    all_alk_for_poz = new_alkohol if new_alkohol else all_alk[:prev_cfg["alkohol"]]

    # ── Nowy tytoń ──
    delta_tyt = new_cfg["tyton"] - prev_cfg["tyton"]
    new_tyton = gen_tyton(delta_tyt, kat_tyt_ids, all_prod_ids,
                          id_offset=prev_cfg["tyton"]) if delta_tyt > 0 else []
    _cb("tyton")
    all_tyt_for_poz = new_tyton if new_tyton else []

    # ── Nowe paragony ──
    delta_par = new_cfg["paragony"] - prev_cfg["paragony"]
    new_paragony = gen_paragony(delta_par, all_prac_ids, all_kli_ids,
                                id_offset=prev_cfg["paragony"]) if delta_par > 0 else []
    _cb("paragony")

    # ── Nowe pozycje paragonu ──
    all_alk_prod = [
        {"produkt_id": i, "cena_producenta": round(random.uniform(2, 800), 2), "stawka_vat": 23}
        for i in range(1, new_cfg["alkohol"] + 1)
    ]
    all_tyt_prod = [
        {"produkt_id": i, "cena_producenta": round(random.uniform(8, 120), 2), "stawka_vat": 23}
        for i in range(1, new_cfg["tyton"] + 1)
    ]

    new_pozycje = gen_pozycje_paragonu(
        new_paragony,
        new_cfg["pozycje_per_paragon"],
        all_alk_prod,
        all_tyt_prod,
        pozycja_id_offset=prev_pozycje_count,
    ) if new_paragony else []
    _cb("pozycje_paragonu")

    # ── Nowe faktury ──
    delta_fak = new_cfg["faktury"] - prev_cfg["faktury"]
    new_faktury = (
        gen_faktury(new_paragony, delta_fak, id_offset=prev_cfg["faktury"])
        if new_paragony and delta_fak > 0 else []
    )
    _cb("faktury")

    # ── Nowe dostawy ──
    delta_dos = new_cfg["dostawy"] - prev_cfg["dostawy"]
    new_dostawy = gen_dostawy(delta_dos, all_prod_ids,
                              id_offset=prev_cfg["dostawy"]) if delta_dos > 0 else []
    _cb("dostawy")

    return {
        "kategorie_produktow": [],    # brak zmian w kategoriach
        "producenci": new_producenci,
        "pracownicy": new_pracownicy,
        "klienci": new_klienci,
        "alkohol": new_alkohol,
        "tyton": new_tyton,
        "paragony": new_paragony,
        "pozycje_paragonu": new_pozycje,
        "faktury": new_faktury,
        "dostawy": new_dostawy,
    }


def count_dataset(size_config: Dict) -> int:
    """Szybkie oszacowanie łącznej liczby rekordów bez generowania danych."""
    s = size_config
    n_par = s["paragony"]
    est_pozycje = int(n_par * s["pozycje_per_paragon"])
    return (s["kategorie"] + s["producenci"] + s["pracownicy"] + s["klienci"]
            + s["alkohol"] + s["tyton"] + n_par + est_pozycje
            + s["faktury"] + s["dostawy"])


def count_delta(prev_cfg: Dict, new_cfg: Dict) -> int:
    """Oszacowanie liczby nowych rekordów w delcie."""
    def d(key):
        return max(0, new_cfg[key] - prev_cfg[key])
    n_par = d("paragony")
    est_poz = int(n_par * new_cfg["pozycje_per_paragon"])
    return (d("producenci") + d("pracownicy") + d("klienci")
            + d("alkohol") + d("tyton") + n_par + est_poz
            + d("faktury") + d("dostawy"))
