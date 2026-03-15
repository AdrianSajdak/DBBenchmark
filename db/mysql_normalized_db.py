"""
MySQL Normalized – implementacja CRUD dla modelu TABELE.txt w 3NF.

Różnice względem MySQLDB (wersja podstawowa):
  - Tabele słownikowe: kraje, miasta, stanowiska, formy_platnosci,
    statusy_transakcji, statusy_klienta, statusy_pracownika,
    statusy_dostawy, statusy_faktury, typy_opakowan
  - Kolumny VARCHAR zastąpione kluczami obcymi do tabel słownikowych
  - Więcej JOIN-ów w zapytaniach READ
  - Pełne FK + ON DELETE ograniczenia

Konsekwencja dla benchmarku:
  - CREATE/UPDATE: mapowanie string → ID słownikowy
  - READ: JOINy do tabel słownikowych
  - Lepsze spójność danych, większy narzut dla operacji write
"""
from typing import Any, Dict, List

import mysql.connector
from mysql.connector import Error

from db.base_db import BaseDB

BATCH = 1000

# Stałe słownikowe (te same co w generatorze)
_KRAJE = [
    "Polska", "Francja", "Włochy", "Hiszpania", "Niemcy",
    "Wielka Brytania", "Szkocja", "Irlandia", "USA", "Meksyk",
    "Kuba", "Brazylia", "Argentyna", "Chile", "Australia",
    "Japonia", "Rosja", "Szwecja", "Finlandia", "Holandia",
]
_FORMY_PLATNOSCI = ["gotówka", "karta_debetowa", "karta_kredytowa", "BLIK", "przelew"]
_STATUSY_TRANSAKCJI = ["zakonczona", "anulowana", "zwrot", "w_trakcie"]
_STATUSY_FAKTURY = ["wystawiona", "oplacona", "anulowana", "korekta"]
_STATUSY_DOSTAWY = ["zamowiona", "w_transporcie", "dostarczona", "anulowana", "reklamacja"]
_STANOWISKA = ["kasjer", "kierownik_zmiany", "magazynier", "sprzedawca",
               "kierownik_sklepu", "zastepca_kierownika", "specjalista_IT"]
_STATUSY_KLIENTA = ["aktywny", "nieaktywny", "zablokowany", "VIP"]
_STATUSY_PRACOWNIKA = ["aktywny", "na_urlopie", "zwolniony", "zawieszony"]
_TYPY_OPK_ALK = ["butelka", "puszka", "karton", "bag-in-box", "beczka"]
_TYPY_OPK_TYT = ["paczka", "karton", "folia", "puszka", "worek"]


class MySQLNormalizedDB(BaseDB):
    name = "MySQL-Norm"

    def __init__(self, config):
        self.cfg = config
        self.conn = None
        self.cursor = None
        # Mapy string → ID (wypełniane po setup_schema)
        self._kraje: Dict[str, int] = {}
        self._formy_platnosci: Dict[str, int] = {}
        self._statusy_transakcji: Dict[str, int] = {}
        self._statusy_faktury: Dict[str, int] = {}
        self._statusy_dostawy: Dict[str, int] = {}
        self._stanowiska: Dict[str, int] = {}
        self._statusy_klienta: Dict[str, int] = {}
        self._statusy_pracownika: Dict[str, int] = {}
        self._typy_opk_alk: Dict[str, int] = {}
        self._typy_opk_tyt: Dict[str, int] = {}

    # ────────── Lifecycle ──────────

    def connect(self) -> bool:
        try:
            self.conn = mysql.connector.connect(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
            )
            self.cursor = self.conn.cursor(dictionary=True, buffered=True)
            self.cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{self.cfg.database}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            self.cursor.execute(f"USE `{self.cfg.database}`")
            self.conn.commit()
            return True
        except Error as e:
            print(f"[MySQL-Norm] Błąd połączenia: {e}")
            return False

    def disconnect(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    # ────────── Schema ──────────

    def setup_schema(self) -> None:
        stmts = [
            # ── Tabele słownikowe ──
            """CREATE TABLE IF NOT EXISTS kraje (
                kraj_id   INT AUTO_INCREMENT PRIMARY KEY,
                nazwa     VARCHAR(80) NOT NULL UNIQUE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS stanowiska (
                stanowisko_id INT AUTO_INCREMENT PRIMARY KEY,
                nazwa         VARCHAR(60) NOT NULL UNIQUE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS formy_platnosci (
                forma_id INT AUTO_INCREMENT PRIMARY KEY,
                nazwa    VARCHAR(40) NOT NULL UNIQUE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS statusy_transakcji (
                status_id INT AUTO_INCREMENT PRIMARY KEY,
                nazwa     VARCHAR(30) NOT NULL UNIQUE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS statusy_klienta (
                status_id INT AUTO_INCREMENT PRIMARY KEY,
                nazwa     VARCHAR(30) NOT NULL UNIQUE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS statusy_pracownika (
                status_id INT AUTO_INCREMENT PRIMARY KEY,
                nazwa     VARCHAR(30) NOT NULL UNIQUE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS statusy_dostawy (
                status_id INT AUTO_INCREMENT PRIMARY KEY,
                nazwa     VARCHAR(40) NOT NULL UNIQUE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS statusy_faktury (
                status_id INT AUTO_INCREMENT PRIMARY KEY,
                nazwa     VARCHAR(30) NOT NULL UNIQUE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS typy_opakowan (
                typ_id    INT AUTO_INCREMENT PRIMARY KEY,
                nazwa     VARCHAR(30) NOT NULL,
                kategoria VARCHAR(10) NOT NULL,
                UNIQUE KEY uq_typ_kat (nazwa, kategoria)
            ) ENGINE=InnoDB""",
            # ── Tabele główne ──
            """CREATE TABLE IF NOT EXISTS kategorie_produktow (
                kategoria_id INT PRIMARY KEY,
                nazwa        VARCHAR(100) NOT NULL,
                opis         VARCHAR(255),
                czy_alkohol  TINYINT DEFAULT 0,
                czy_tyton    TINYINT DEFAULT 0
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS producenci (
                producent_id  INT PRIMARY KEY,
                nazwa         VARCHAR(150) NOT NULL,
                kraj_id       INT,
                miasto        VARCHAR(80),
                rok_zalozenia INT,
                strona_www    VARCHAR(200),
                czy_aktywny   TINYINT DEFAULT 1,
                FOREIGN KEY (kraj_id) REFERENCES kraje(kraj_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS pracownicy (
                pracownik_id      INT PRIMARY KEY,
                imie              VARCHAR(60) NOT NULL,
                nazwisko          VARCHAR(60) NOT NULL,
                stanowisko_id     INT,
                data_zatrudnienia DATE,
                data_zwolnienia   DATE,
                pensja            DECIMAL(10,2),
                telefon           VARCHAR(30),
                email             VARCHAR(100),
                status_id         INT,
                FOREIGN KEY (stanowisko_id)
                    REFERENCES stanowiska(stanowisko_id) ON DELETE SET NULL,
                FOREIGN KEY (status_id)
                    REFERENCES statusy_pracownika(status_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS klienci (
                klient_id            INT PRIMARY KEY,
                imie                 VARCHAR(60) NOT NULL,
                nazwisko             VARCHAR(60) NOT NULL,
                data_urodzenia       DATE,
                plec                 CHAR(1),
                miasto               VARCHAR(80),
                kraj_id              INT,
                telefon              VARCHAR(30),
                email                VARCHAR(120),
                data_rejestracji     DATE,
                status_id            INT,
                punkty_lojalnosciowe INT DEFAULT 0,
                FOREIGN KEY (kraj_id)   REFERENCES kraje(kraj_id) ON DELETE SET NULL,
                FOREIGN KEY (status_id) REFERENCES statusy_klienta(status_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS alkohol (
                produkt_id                    INT PRIMARY KEY,
                nazwa                         VARCHAR(200) NOT NULL,
                marka                         VARCHAR(100),
                kategoria_id                  INT,
                producent_id                  INT,
                kraj_id                       INT,
                rocznik                       INT,
                procent_alkoholu              DECIMAL(5,1),
                pojemnosc_ml                  INT,
                typ_opakowania_id             INT,
                data_wprowadzenia_do_sprzedazy DATE,
                data_wycofania                DATE,
                kod_kreskowy                  VARCHAR(20),
                cena_producenta               DECIMAL(10,2),
                stawka_vat                    INT,
                FOREIGN KEY (kategoria_id)
                    REFERENCES kategorie_produktow(kategoria_id) ON DELETE SET NULL,
                FOREIGN KEY (producent_id)
                    REFERENCES producenci(producent_id) ON DELETE SET NULL,
                FOREIGN KEY (kraj_id) REFERENCES kraje(kraj_id) ON DELETE SET NULL,
                FOREIGN KEY (typ_opakowania_id)
                    REFERENCES typy_opakowan(typ_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS tyton (
                produkt_id                    INT PRIMARY KEY,
                nazwa                         VARCHAR(200) NOT NULL,
                marka                         VARCHAR(100),
                kategoria_id                  INT,
                producent_id                  INT,
                kraj_id                       INT,
                liczba_sztuk_w_opakowaniu     INT,
                typ_opakowania_id             INT,
                data_wprowadzenia_do_sprzedazy DATE,
                data_wycofania                DATE,
                kod_kreskowy                  VARCHAR(20),
                cena_producenta               DECIMAL(10,2),
                stawka_vat                    INT,
                FOREIGN KEY (kategoria_id)
                    REFERENCES kategorie_produktow(kategoria_id) ON DELETE SET NULL,
                FOREIGN KEY (producent_id)
                    REFERENCES producenci(producent_id) ON DELETE SET NULL,
                FOREIGN KEY (kraj_id) REFERENCES kraje(kraj_id) ON DELETE SET NULL,
                FOREIGN KEY (typ_opakowania_id)
                    REFERENCES typy_opakowan(typ_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS paragony (
                paragon_id        INT PRIMARY KEY,
                numer_paragonu    VARCHAR(30),
                data_sprzedazy    DATE,
                godzina_sprzedazy TIME,
                pracownik_id      INT,
                klient_id         INT,
                forma_platnosci_id INT,
                wartosc_netto     DECIMAL(12,2),
                wartosc_brutto    DECIMAL(12,2),
                status_id         INT,
                FOREIGN KEY (pracownik_id)
                    REFERENCES pracownicy(pracownik_id) ON DELETE SET NULL,
                FOREIGN KEY (klient_id) REFERENCES klienci(klient_id) ON DELETE SET NULL,
                FOREIGN KEY (forma_platnosci_id)
                    REFERENCES formy_platnosci(forma_id) ON DELETE SET NULL,
                FOREIGN KEY (status_id)
                    REFERENCES statusy_transakcji(status_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS pozycje_paragonu (
                pozycja_id     INT PRIMARY KEY,
                paragon_id     INT,
                produkt_id     INT,
                typ_produktu   CHAR(1),
                wartosc_netto  DECIMAL(10,2),
                wartosc_brutto DECIMAL(10,2),
                FOREIGN KEY (paragon_id) REFERENCES paragony(paragon_id) ON DELETE CASCADE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS faktury (
                faktura_id       INT PRIMARY KEY,
                numer_faktury    VARCHAR(30),
                sprzedaz_id      INT,
                data_wystawienia DATE,
                data_sprzedazy   DATE,
                nabywca_nazwa    VARCHAR(200),
                nabywca_nip      VARCHAR(20),
                nabywca_adres    VARCHAR(300),
                wartosc_netto    DECIMAL(12,2),
                wartosc_vat      DECIMAL(12,2),
                wartosc_brutto   DECIMAL(12,2),
                status_id        INT,
                FOREIGN KEY (sprzedaz_id) REFERENCES paragony(paragon_id) ON DELETE SET NULL,
                FOREIGN KEY (status_id)   REFERENCES statusy_faktury(status_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS dostawy (
                dostawa_id           INT PRIMARY KEY,
                numer_dostawy        VARCHAR(30),
                producent_id         INT,
                data_zamowienia      DATE,
                data_dostawy         DATE,
                wartosc_netto        DECIMAL(12,2),
                wartosc_brutto       DECIMAL(12,2),
                status_id            INT,
                numer_faktury_zakupu VARCHAR(30),
                FOREIGN KEY (producent_id)
                    REFERENCES producenci(producent_id) ON DELETE SET NULL,
                FOREIGN KEY (status_id)
                    REFERENCES statusy_dostawy(status_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
        ]
        for stmt in stmts:
            self.cursor.execute(stmt)
        self.conn.commit()
        self._populate_lookup_tables()
        self._load_lookup_maps()

    def _populate_lookup_tables(self) -> None:
        """Wypełnia tabele słownikowe stałymi wartościami."""
        def _insert_ignore(table: str, col: str, values: List[str]):
            sql = f"INSERT IGNORE INTO `{table}` (`{col}`) VALUES (%s)"
            for v in values:
                self.cursor.execute(sql, (v,))
            self.conn.commit()

        _insert_ignore("kraje", "nazwa", _KRAJE)
        _insert_ignore("stanowiska", "nazwa", _STANOWISKA)
        _insert_ignore("formy_platnosci", "nazwa", _FORMY_PLATNOSCI)
        _insert_ignore("statusy_transakcji", "nazwa", _STATUSY_TRANSAKCJI)
        _insert_ignore("statusy_klienta", "nazwa", _STATUSY_KLIENTA)
        _insert_ignore("statusy_pracownika", "nazwa", _STATUSY_PRACOWNIKA)
        _insert_ignore("statusy_dostawy", "nazwa", _STATUSY_DOSTAWY)
        _insert_ignore("statusy_faktury", "nazwa", _STATUSY_FAKTURY)

        for t in _TYPY_OPK_ALK:
            self.cursor.execute(
                "INSERT IGNORE INTO typy_opakowan (nazwa, kategoria) VALUES (%s, 'alk')", (t,))
        for t in _TYPY_OPK_TYT:
            self.cursor.execute(
                "INSERT IGNORE INTO typy_opakowan (nazwa, kategoria) VALUES (%s, 'tyt')", (t,))
        self.conn.commit()

    def _load_lookup_maps(self) -> None:
        """Ładuje tabele słownikowe do słowników Python (string → ID)."""
        def _load(table: str, key_col: str, id_col: str) -> Dict[str, int]:
            self.cursor.execute(f"SELECT `{id_col}`, `{key_col}` FROM `{table}`")
            return {row[key_col]: row[id_col] for row in self.cursor.fetchall()}

        self._kraje = _load("kraje", "nazwa", "kraj_id")
        self._stanowiska = _load("stanowiska", "nazwa", "stanowisko_id")
        self._formy_platnosci = _load("formy_platnosci", "nazwa", "forma_id")
        self._statusy_transakcji = _load("statusy_transakcji", "nazwa", "status_id")
        self._statusy_klienta = _load("statusy_klienta", "nazwa", "status_id")
        self._statusy_pracownika = _load("statusy_pracownika", "nazwa", "status_id")
        self._statusy_dostawy = _load("statusy_dostawy", "nazwa", "status_id")
        self._statusy_faktury = _load("statusy_faktury", "nazwa", "status_id")

        self.cursor.execute("SELECT typ_id, nazwa, kategoria FROM typy_opakowan")
        self._typy_opk_alk = {}
        self._typy_opk_tyt = {}
        for row in self.cursor.fetchall():
            if row["kategoria"] == "alk":
                self._typy_opk_alk[row["nazwa"]] = row["typ_id"]
            else:
                self._typy_opk_tyt[row["nazwa"]] = row["typ_id"]

    def clear_data(self) -> None:
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        for t in ("pozycje_paragonu", "faktury", "paragony", "dostawy",
                  "alkohol", "tyton", "klienci", "pracownicy",
                  "producenci", "kategorie_produktow"):
            self.cursor.execute(f"TRUNCATE TABLE `{t}`")
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        self.conn.commit()

    def drop_schema(self) -> None:
        self.cursor.execute(f"DROP DATABASE IF EXISTS `{self.cfg.database}`")
        self.conn.commit()

    # ────────── Indeksy ──────────

    def create_indexes(self) -> None:
        idxs = [
            "CREATE INDEX idx_alk_kategoria ON alkohol(kategoria_id)",
            "CREATE INDEX idx_alk_producent ON alkohol(producent_id)",
            "CREATE INDEX idx_alk_cena ON alkohol(cena_producenta)",
            "CREATE INDEX idx_alk_kraj ON alkohol(kraj_id)",
            "CREATE INDEX idx_tyt_kategoria ON tyton(kategoria_id)",
            "CREATE INDEX idx_tyt_producent ON tyton(producent_id)",
            "CREATE INDEX idx_par_klient ON paragony(klient_id)",
            "CREATE INDEX idx_par_pracownik ON paragony(pracownik_id)",
            "CREATE INDEX idx_par_data ON paragony(data_sprzedazy)",
            "CREATE INDEX idx_par_status ON paragony(status_id)",
            "CREATE INDEX idx_poz_paragon ON pozycje_paragonu(paragon_id)",
            "CREATE INDEX idx_poz_produkt ON pozycje_paragonu(produkt_id, typ_produktu)",
            "CREATE INDEX idx_fak_sprzedaz ON faktury(sprzedaz_id)",
            "CREATE INDEX idx_fak_data ON faktury(data_wystawienia)",
            "CREATE INDEX idx_dos_producent ON dostawy(producent_id)",
            "CREATE INDEX idx_dos_status ON dostawy(status_id)",
            "CREATE INDEX idx_kli_status ON klienci(status_id)",
            "CREATE INDEX idx_prod_kraj ON producenci(kraj_id)",
        ]
        for sql in idxs:
            try:
                self.cursor.execute(sql)
            except Exception:
                pass
        self.conn.commit()

    def drop_indexes(self) -> None:
        pairs = [
            ("alkohol",          "idx_alk_kategoria"),
            ("alkohol",          "idx_alk_producent"),
            ("alkohol",          "idx_alk_cena"),
            ("alkohol",          "idx_alk_kraj"),
            ("tyton",            "idx_tyt_kategoria"),
            ("tyton",            "idx_tyt_producent"),
            ("paragony",         "idx_par_klient"),
            ("paragony",         "idx_par_pracownik"),
            ("paragony",         "idx_par_data"),
            ("paragony",         "idx_par_status"),
            ("pozycje_paragonu", "idx_poz_paragon"),
            ("pozycje_paragonu", "idx_poz_produkt"),
            ("faktury",          "idx_fak_sprzedaz"),
            ("faktury",          "idx_fak_data"),
            ("dostawy",          "idx_dos_producent"),
            ("dostawy",          "idx_dos_status"),
            ("klienci",          "idx_kli_status"),
            ("producenci",       "idx_prod_kraj"),
        ]
        for table, idx in pairs:
            try:
                self.cursor.execute(f"DROP INDEX `{idx}` ON `{table}`")
            except Exception:
                pass
        self.conn.commit()

    # ────────── Helpers ──────────

    def _bulk_insert(self, table: str, rows: List[Dict]):
        if not rows:
            return
        keys = list(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = (f"INSERT IGNORE INTO `{table}` "
               f"({', '.join(f'`{k}`' for k in keys)}) "
               f"VALUES ({placeholders})")
        for i in range(0, len(rows), BATCH):
            batch = rows[i: i + BATCH]
            values = [tuple(r[k] for k in keys) for r in batch]
            self.cursor.executemany(sql, values)
            self.conn.commit()

    def _norm_producenci(self, rows: List[Dict]) -> List[Dict]:
        """Konwertuje kraj string → kraj_id FK."""
        result = []
        for r in rows:
            result.append({
                "producent_id": r["producent_id"],
                "nazwa": r["nazwa"],
                "kraj_id": self._kraje.get(r.get("kraj"), None),
                "miasto": r.get("miasto"),
                "rok_zalozenia": r.get("rok_zalozenia"),
                "strona_www": r.get("strona_www"),
                "czy_aktywny": int(r.get("czy_aktywny", True)),
            })
        return result

    def _norm_pracownicy(self, rows: List[Dict]) -> List[Dict]:
        result = []
        for r in rows:
            result.append({
                "pracownik_id": r["pracownik_id"],
                "imie": r["imie"],
                "nazwisko": r["nazwisko"],
                "stanowisko_id": self._stanowiska.get(r.get("stanowisko"), None),
                "data_zatrudnienia": r.get("data_zatrudnienia"),
                "data_zwolnienia": r.get("data_zwolnienia"),
                "pensja": r.get("pensja"),
                "telefon": r.get("telefon"),
                "email": r.get("email"),
                "status_id": self._statusy_pracownika.get(r.get("status_pracownika"), None),
            })
        return result

    def _norm_klienci(self, rows: List[Dict]) -> List[Dict]:
        result = []
        for r in rows:
            result.append({
                "klient_id": r["klient_id"],
                "imie": r["imie"],
                "nazwisko": r["nazwisko"],
                "data_urodzenia": r.get("data_urodzenia"),
                "plec": r.get("plec"),
                "miasto": r.get("miasto"),
                "kraj_id": None,
                "telefon": r.get("telefon"),
                "email": r.get("email"),
                "data_rejestracji": r.get("data_rejestracji"),
                "status_id": self._statusy_klienta.get(r.get("status_klienta"), None),
                "punkty_lojalnosciowe": r.get("punkty_lojalnosciowe", 0),
            })
        return result

    def _norm_alkohol(self, rows: List[Dict]) -> List[Dict]:
        result = []
        for r in rows:
            result.append({
                "produkt_id": r["produkt_id"],
                "nazwa": r["nazwa"],
                "marka": r.get("marka"),
                "kategoria_id": r.get("kategoria_id"),
                "producent_id": r.get("producent_id"),
                "kraj_id": self._kraje.get(r.get("kraj_pochodzenia"), None),
                "rocznik": r.get("rocznik"),
                "procent_alkoholu": r.get("procent_alkoholu"),
                "pojemnosc_ml": r.get("pojemnosc_ml"),
                "typ_opakowania_id": self._typy_opk_alk.get(r.get("typ_opakowania"), None),
                "data_wprowadzenia_do_sprzedazy": r.get("data_wprowadzenia_do_sprzedazy"),
                "data_wycofania": r.get("data_wycofania"),
                "kod_kreskowy": r.get("kod_kreskowy"),
                "cena_producenta": r.get("cena_producenta"),
                "stawka_vat": r.get("stawka_vat"),
            })
        return result

    def _norm_tyton(self, rows: List[Dict]) -> List[Dict]:
        result = []
        for r in rows:
            result.append({
                "produkt_id": r["produkt_id"],
                "nazwa": r["nazwa"],
                "marka": r.get("marka"),
                "kategoria_id": r.get("kategoria_id"),
                "producent_id": r.get("producent_id"),
                "kraj_id": self._kraje.get(r.get("kraj_pochodzenia"), None),
                "liczba_sztuk_w_opakowaniu": r.get("liczba_sztuk_w_opakowaniu"),
                "typ_opakowania_id": self._typy_opk_tyt.get(r.get("typ_opakowania"), None),
                "data_wprowadzenia_do_sprzedazy": r.get("data_wprowadzenia_do_sprzedazy"),
                "data_wycofania": r.get("data_wycofania"),
                "kod_kreskowy": r.get("kod_kreskowy"),
                "cena_producenta": r.get("cena_producenta"),
                "stawka_vat": r.get("stawka_vat"),
            })
        return result

    def _norm_paragony(self, rows: List[Dict]) -> List[Dict]:
        result = []
        for r in rows:
            result.append({
                "paragon_id": r["paragon_id"],
                "numer_paragonu": r.get("numer_paragonu"),
                "data_sprzedazy": r.get("data_sprzedazy"),
                "godzina_sprzedazy": r.get("godzina_sprzedazy"),
                "pracownik_id": r.get("pracownik_id"),
                "klient_id": r.get("klient_id"),
                "forma_platnosci_id": self._formy_platnosci.get(r.get("forma_platnosci"), None),
                "wartosc_netto": r.get("wartosc_netto"),
                "wartosc_brutto": r.get("wartosc_brutto"),
                "status_id": self._statusy_transakcji.get(r.get("status_transakcji"), None),
            })
        return result

    def _norm_faktury(self, rows: List[Dict]) -> List[Dict]:
        result = []
        for r in rows:
            result.append({
                "faktura_id": r["faktura_id"],
                "numer_faktury": r.get("numer_faktury"),
                "sprzedaz_id": r.get("sprzedaz_id"),
                "data_wystawienia": r.get("data_wystawienia"),
                "data_sprzedazy": r.get("data_sprzedazy"),
                "nabywca_nazwa": r.get("nabywca_nazwa"),
                "nabywca_nip": r.get("nabywca_nip"),
                "nabywca_adres": r.get("nabywca_adres"),
                "wartosc_netto": r.get("wartosc_netto"),
                "wartosc_vat": r.get("wartosc_vat"),
                "wartosc_brutto": r.get("wartosc_brutto"),
                "status_id": self._statusy_faktury.get(r.get("status_faktury"), None),
            })
        return result

    def _norm_dostawy(self, rows: List[Dict]) -> List[Dict]:
        result = []
        for r in rows:
            result.append({
                "dostawa_id": r["dostawa_id"],
                "numer_dostawy": r.get("numer_dostawy"),
                "producent_id": r.get("producent_id"),
                "data_zamowienia": r.get("data_zamowienia"),
                "data_dostawy": r.get("data_dostawy"),
                "wartosc_netto": r.get("wartosc_netto"),
                "wartosc_brutto": r.get("wartosc_brutto"),
                "status_id": self._statusy_dostawy.get(r.get("status_dostawy"), None),
                "numer_faktury_zakupu": r.get("numer_faktury_zakupu"),
            })
        return result

    # ────────── Seeding ──────────

    def seed(self, data: Dict[str, List[Dict]], progress_callback=None) -> None:
        from config import SEED_ORDER
        cb = progress_callback or (lambda t, d, tot: None)
        total = sum(len(v) for v in data.values())

        mapping = {
            "kategorie_produktow": ("kategorie_produktow", lambda x: x),
            "producenci":          ("producenci",          self._norm_producenci),
            "pracownicy":          ("pracownicy",          self._norm_pracownicy),
            "klienci":             ("klienci",             self._norm_klienci),
            "alkohol":             ("alkohol",             self._norm_alkohol),
            "tyton":               ("tyton",               self._norm_tyton),
            "paragony":            ("paragony",            self._norm_paragony),
            "pozycje_paragonu":    ("pozycje_paragonu",    lambda x: x),
            "faktury":             ("faktury",             self._norm_faktury),
            "dostawy":             ("dostawy",             self._norm_dostawy),
        }

        for table in SEED_ORDER:
            rows = data.get(table, [])
            if not rows:
                continue
            target_table, transform = mapping[table]
            cb(table, len(rows), total)
            self._bulk_insert(target_table, transform(rows))

    # ==================== CREATE ====================

    def C1_add_single_alkohol(self, product: Dict) -> None:
        norm = self._norm_alkohol([product])[0]
        keys = list(norm.keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = (f"INSERT INTO alkohol ({', '.join(f'`{k}`' for k in keys)}) "
               f"VALUES ({placeholders})")
        self.cursor.execute(sql, tuple(norm[k] for k in keys))
        self.conn.commit()

    def C2_add_bulk_alkohol(self, products: List[Dict]) -> None:
        self._bulk_insert("alkohol", self._norm_alkohol(products))

    def C3_add_klient(self, klient: Dict) -> None:
        norm = self._norm_klienci([klient])[0]
        keys = list(norm.keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = (f"INSERT INTO klienci ({', '.join(f'`{k}`' for k in keys)}) "
               f"VALUES ({placeholders})")
        self.cursor.execute(sql, tuple(norm[k] for k in keys))
        self.conn.commit()

    def C4_add_paragon(self, paragon: Dict) -> None:
        norm = self._norm_paragony([paragon])[0]
        keys = list(norm.keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = (f"INSERT INTO paragony ({', '.join(f'`{k}`' for k in keys)}) "
               f"VALUES ({placeholders})")
        self.cursor.execute(sql, tuple(norm[k] for k in keys))
        self.conn.commit()

    def C5_add_pozycje_paragonu(self, pozycje: List[Dict]) -> None:
        self._bulk_insert("pozycje_paragonu", pozycje)

    def C6_add_faktura(self, faktura: Dict) -> None:
        norm = self._norm_faktury([faktura])[0]
        keys = list(norm.keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = (f"INSERT INTO faktury ({', '.join(f'`{k}`' for k in keys)}) "
               f"VALUES ({placeholders})")
        self.cursor.execute(sql, tuple(norm[k] for k in keys))
        self.conn.commit()

    # ==================== READ ====================

    def R1_get_alkohol_by_id(self, produkt_id: int) -> Any:
        self.cursor.execute(
            """SELECT a.*, k.nazwa AS kraj_nazwa, t.nazwa AS typ_opakowania
               FROM alkohol a
               LEFT JOIN kraje k ON a.kraj_id = k.kraj_id
               LEFT JOIN typy_opakowan t ON a.typ_opakowania_id = t.typ_id
               WHERE a.produkt_id = %s""",
            (produkt_id,))
        return self.cursor.fetchone()

    def R2_get_products_by_kategoria(self, kategoria_id: int) -> List:
        self.cursor.execute(
            """SELECT a.*, k.nazwa AS kraj_nazwa
               FROM alkohol a
               LEFT JOIN kraje k ON a.kraj_id = k.kraj_id
               WHERE a.kategoria_id = %s""",
            (kategoria_id,))
        return self.cursor.fetchall()

    def R3_get_paragony_klienta(self, klient_id: int) -> List:
        self.cursor.execute(
            """SELECT p.*, fp.nazwa AS forma_platnosci, st.nazwa AS status_transakcji
               FROM paragony p
               LEFT JOIN formy_platnosci fp ON p.forma_platnosci_id = fp.forma_id
               LEFT JOIN statusy_transakcji st ON p.status_id = st.status_id
               WHERE p.klient_id = %s""",
            (klient_id,))
        return self.cursor.fetchall()

    def R4_get_paragon_details(self, paragon_id: int) -> Any:
        self.cursor.execute(
            """SELECT p.*, fp.nazwa AS forma_platnosci, st.nazwa AS status,
                      pp.pozycja_id, pp.produkt_id, pp.typ_produktu,
                      pp.wartosc_netto AS poz_netto, pp.wartosc_brutto AS poz_brutto
               FROM paragony p
               LEFT JOIN formy_platnosci fp ON p.forma_platnosci_id = fp.forma_id
               LEFT JOIN statusy_transakcji st ON p.status_id = st.status_id
               LEFT JOIN pozycje_paragonu pp ON p.paragon_id = pp.paragon_id
               WHERE p.paragon_id = %s""",
            (paragon_id,))
        return self.cursor.fetchall()

    def R5_get_top_expensive_alkohol(self, limit: int = 10) -> List:
        self.cursor.execute(
            """SELECT a.*, k.nazwa AS kraj_nazwa, t.nazwa AS typ_opakowania
               FROM alkohol a
               LEFT JOIN kraje k ON a.kraj_id = k.kraj_id
               LEFT JOIN typy_opakowan t ON a.typ_opakowania_id = t.typ_id
               ORDER BY a.cena_producenta DESC LIMIT %s""",
            (limit,))
        return self.cursor.fetchall()

    def R6_get_faktury_by_okres(self, data_od: str, data_do: str) -> List:
        self.cursor.execute(
            """SELECT f.*, sf.nazwa AS status_faktury
               FROM faktury f
               LEFT JOIN statusy_faktury sf ON f.status_id = sf.status_id
               WHERE f.data_wystawienia BETWEEN %s AND %s""",
            (data_od, data_do))
        return self.cursor.fetchall()

    # ==================== UPDATE ====================

    def U1_update_cena_alkohol(self, produkt_id: int, nowa_cena: float) -> None:
        self.cursor.execute(
            "UPDATE alkohol SET cena_producenta = %s WHERE produkt_id = %s",
            (nowa_cena, produkt_id))
        self.conn.commit()

    def U2_update_klient(self, klient_id: int, data: Dict) -> None:
        self.cursor.execute(
            "UPDATE klienci SET email = %s, telefon = %s WHERE klient_id = %s",
            (data["email"], data["telefon"], klient_id))
        self.conn.commit()

    def U3_update_status_paragonu(self, paragon_id: int, status: str) -> None:
        status_id = self._statusy_transakcji.get(status, 1)
        self.cursor.execute(
            "UPDATE paragony SET status_id = %s WHERE paragon_id = %s",
            (status_id, paragon_id))
        self.conn.commit()

    def U4_bulk_update_ceny_kategoria(self, kategoria_id: int, rabat_pct: float) -> None:
        self.cursor.execute(
            "UPDATE alkohol SET cena_producenta = ROUND(cena_producenta * (1 - %s / 100), 2) "
            "WHERE kategoria_id = %s",
            (rabat_pct, kategoria_id))
        self.conn.commit()

    def U5_update_status_dostawy(self, dostawa_id: int, status: str) -> None:
        status_id = self._statusy_dostawy.get(status, 1)
        self.cursor.execute(
            "UPDATE dostawy SET status_id = %s WHERE dostawa_id = %s",
            (status_id, dostawa_id))
        self.conn.commit()

    def U6_update_producent(self, producent_id: int, data: Dict) -> None:
        self.cursor.execute(
            "UPDATE producenci SET nazwa = %s, strona_www = %s WHERE producent_id = %s",
            (data["nazwa"], data["strona_www"], producent_id))
        self.conn.commit()

    # ==================== DELETE ====================

    def D1_delete_alkohol(self, produkt_id: int) -> None:
        self.cursor.execute("DELETE FROM alkohol WHERE produkt_id = %s", (produkt_id,))
        self.conn.commit()

    def D2_delete_klient(self, klient_id: int) -> None:
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        self.cursor.execute("DELETE FROM klienci WHERE klient_id = %s", (klient_id,))
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        self.conn.commit()

    def D3_delete_paragon(self, paragon_id: int) -> None:
        self.cursor.execute("DELETE FROM paragony WHERE paragon_id = %s", (paragon_id,))
        self.conn.commit()

    def D4_delete_pozycje_paragonu(self, paragon_id: int) -> None:
        self.cursor.execute(
            "DELETE FROM pozycje_paragonu WHERE paragon_id = %s", (paragon_id,))
        self.conn.commit()

    def D5_bulk_delete_tyton(self, produkt_ids: List[int]) -> None:
        fmt = ", ".join(["%s"] * len(produkt_ids))
        self.cursor.execute(
            f"DELETE FROM tyton WHERE produkt_id IN ({fmt})", tuple(produkt_ids))
        self.conn.commit()

    def D6_delete_dostawa(self, dostawa_id: int) -> None:
        self.cursor.execute("DELETE FROM dostawy WHERE dostawa_id = %s", (dostawa_id,))
        self.conn.commit()
