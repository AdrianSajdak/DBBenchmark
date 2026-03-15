"""SQLite – implementacja CRUD dla modelu TABELE.txt."""
import os
import sqlite3
from typing import Any, Dict, List

from db.base_db import BaseDB

BATCH = 1000


def _dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class SQLiteDB(BaseDB):
    name = "SQLite"

    def __init__(self, config):
        self.cfg = config
        self.conn = None

    # ────────── Lifecycle ──────────

    def connect(self) -> bool:
        try:
            parent = os.path.dirname(os.path.abspath(self.cfg.database))
            os.makedirs(parent, exist_ok=True)
            self.conn = sqlite3.connect(self.cfg.database)
            self.conn.row_factory = _dict_factory
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA cache_size=-65536")
            self.conn.execute("PRAGMA foreign_keys=ON")
            return True
        except Exception as e:
            print(f"[SQLite] Błąd połączenia: {e}")
            return False

    def disconnect(self) -> None:
        if self.conn:
            self.conn.close()

    def setup_schema(self) -> None:
        stmts = [
            """CREATE TABLE IF NOT EXISTS kategorie_produktow (
                kategoria_id INTEGER PRIMARY KEY,
                nazwa TEXT NOT NULL,
                opis TEXT,
                czy_alkohol INTEGER DEFAULT 0,
                czy_tyton INTEGER DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS producenci (
                producent_id INTEGER PRIMARY KEY,
                nazwa TEXT NOT NULL,
                kraj TEXT,
                miasto TEXT,
                rok_zalozenia INTEGER,
                strona_www TEXT,
                czy_aktywny INTEGER DEFAULT 1
            )""",
            """CREATE TABLE IF NOT EXISTS pracownicy (
                pracownik_id INTEGER PRIMARY KEY,
                imie TEXT NOT NULL,
                nazwisko TEXT NOT NULL,
                stanowisko TEXT,
                data_zatrudnienia TEXT,
                data_zwolnienia TEXT,
                pensja REAL,
                telefon TEXT,
                email TEXT,
                status_pracownika TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS klienci (
                klient_id INTEGER PRIMARY KEY,
                imie TEXT NOT NULL,
                nazwisko TEXT NOT NULL,
                data_urodzenia TEXT,
                plec TEXT,
                miasto TEXT,
                telefon TEXT,
                email TEXT,
                data_rejestracji TEXT,
                status_klienta TEXT,
                punkty_lojalnosciowe INTEGER DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS alkohol (
                produkt_id INTEGER PRIMARY KEY,
                nazwa TEXT NOT NULL,
                marka TEXT,
                kategoria_id INTEGER REFERENCES kategorie_produktow(kategoria_id),
                producent_id INTEGER REFERENCES producenci(producent_id),
                kraj_pochodzenia TEXT,
                rocznik INTEGER,
                procent_alkoholu REAL,
                pojemnosc_ml INTEGER,
                typ_opakowania TEXT,
                data_wprowadzenia_do_sprzedazy TEXT,
                data_wycofania TEXT,
                kod_kreskowy TEXT,
                cena_producenta REAL,
                stawka_vat INTEGER
            )""",
            """CREATE TABLE IF NOT EXISTS tyton (
                produkt_id INTEGER PRIMARY KEY,
                nazwa TEXT NOT NULL,
                marka TEXT,
                kategoria_id INTEGER REFERENCES kategorie_produktow(kategoria_id),
                producent_id INTEGER REFERENCES producenci(producent_id),
                kraj_pochodzenia TEXT,
                liczba_sztuk_w_opakowaniu INTEGER,
                typ_opakowania TEXT,
                data_wprowadzenia_do_sprzedazy TEXT,
                data_wycofania TEXT,
                kod_kreskowy TEXT,
                cena_producenta REAL,
                stawka_vat INTEGER
            )""",
            """CREATE TABLE IF NOT EXISTS paragony (
                paragon_id INTEGER PRIMARY KEY,
                numer_paragonu TEXT,
                data_sprzedazy TEXT,
                godzina_sprzedazy TEXT,
                pracownik_id INTEGER REFERENCES pracownicy(pracownik_id),
                klient_id INTEGER REFERENCES klienci(klient_id),
                forma_platnosci TEXT,
                wartosc_netto REAL,
                wartosc_brutto REAL,
                status_transakcji TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS pozycje_paragonu (
                pozycja_id INTEGER PRIMARY KEY,
                paragon_id INTEGER REFERENCES paragony(paragon_id) ON DELETE CASCADE,
                produkt_id INTEGER,
                typ_produktu TEXT,
                wartosc_netto REAL,
                wartosc_brutto REAL
            )""",
            """CREATE TABLE IF NOT EXISTS faktury (
                faktura_id INTEGER PRIMARY KEY,
                numer_faktury TEXT,
                sprzedaz_id INTEGER REFERENCES paragony(paragon_id),
                data_wystawienia TEXT,
                data_sprzedazy TEXT,
                nabywca_nazwa TEXT,
                nabywca_nip TEXT,
                nabywca_adres TEXT,
                wartosc_netto REAL,
                wartosc_vat REAL,
                wartosc_brutto REAL,
                status_faktury TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS dostawy (
                dostawa_id INTEGER PRIMARY KEY,
                numer_dostawy TEXT,
                producent_id INTEGER REFERENCES producenci(producent_id),
                data_zamowienia TEXT,
                data_dostawy TEXT,
                wartosc_netto REAL,
                wartosc_brutto REAL,
                status_dostawy TEXT,
                numer_faktury_zakupu TEXT
            )""",
        ]
        with self.conn:
            for stmt in stmts:
                self.conn.execute(stmt)

    def clear_data(self) -> None:
        self.conn.execute("PRAGMA foreign_keys=OFF")
        for t in ("pozycje_paragonu", "faktury", "paragony", "dostawy",
                  "alkohol", "tyton", "klienci", "pracownicy",
                  "producenci", "kategorie_produktow"):
            self.conn.execute(f"DELETE FROM {t}")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.commit()

    def drop_schema(self) -> None:
        self.conn.execute("PRAGMA foreign_keys=OFF")
        for t in ("pozycje_paragonu", "faktury", "paragony", "dostawy",
                  "alkohol", "tyton", "klienci", "pracownicy",
                  "producenci", "kategorie_produktow"):
            self.conn.execute(f"DROP TABLE IF EXISTS {t}")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.commit()

    # ────────── Indeksy (ocena 4.0) ──────────

    def create_indexes(self) -> None:
        idxs = [
            "CREATE INDEX IF NOT EXISTS idx_alk_kategoria ON alkohol(kategoria_id)",
            "CREATE INDEX IF NOT EXISTS idx_alk_producent ON alkohol(producent_id)",
            "CREATE INDEX IF NOT EXISTS idx_alk_cena ON alkohol(cena_producenta)",
            "CREATE INDEX IF NOT EXISTS idx_alk_marka ON alkohol(marka)",
            "CREATE INDEX IF NOT EXISTS idx_tyt_kategoria ON tyton(kategoria_id)",
            "CREATE INDEX IF NOT EXISTS idx_tyt_producent ON tyton(producent_id)",
            "CREATE INDEX IF NOT EXISTS idx_par_klient ON paragony(klient_id)",
            "CREATE INDEX IF NOT EXISTS idx_par_pracownik ON paragony(pracownik_id)",
            "CREATE INDEX IF NOT EXISTS idx_par_data ON paragony(data_sprzedazy)",
            "CREATE INDEX IF NOT EXISTS idx_poz_paragon ON pozycje_paragonu(paragon_id)",
            "CREATE INDEX IF NOT EXISTS idx_poz_produkt"
            " ON pozycje_paragonu(produkt_id, typ_produktu)",
            "CREATE INDEX IF NOT EXISTS idx_fak_sprzedaz ON faktury(sprzedaz_id)",
            "CREATE INDEX IF NOT EXISTS idx_fak_data ON faktury(data_wystawienia)",
            "CREATE INDEX IF NOT EXISTS idx_dos_producent ON dostawy(producent_id)",
            "CREATE INDEX IF NOT EXISTS idx_dos_data ON dostawy(data_zamowienia)",
            "CREATE INDEX IF NOT EXISTS idx_kli_miasto ON klienci(miasto)",
            "CREATE INDEX IF NOT EXISTS idx_kli_status ON klienci(status_klienta)",
        ]
        with self.conn:
            for sql in idxs:
                self.conn.execute(sql)

    def drop_indexes(self) -> None:
        idxs = [
            "idx_alk_kategoria", "idx_alk_producent", "idx_alk_cena", "idx_alk_marka",
            "idx_tyt_kategoria", "idx_tyt_producent",
            "idx_par_klient", "idx_par_pracownik", "idx_par_data",
            "idx_poz_paragon", "idx_poz_produkt",
            "idx_fak_sprzedaz", "idx_fak_data",
            "idx_dos_producent", "idx_dos_data",
            "idx_kli_miasto", "idx_kli_status",
        ]
        with self.conn:
            for idx in idxs:
                self.conn.execute(f"DROP INDEX IF EXISTS {idx}")

    # ────────── Helpers ──────────

    def _bulk_insert(self, table: str, rows: List[Dict]):
        if not rows:
            return
        keys = list(rows[0].keys())
        placeholders = ", ".join(["?"] * len(keys))
        sql = (f"INSERT OR IGNORE INTO {table} ({', '.join(keys)}) "
               f"VALUES ({placeholders})")
        with self.conn:
            for i in range(0, len(rows), BATCH):
                batch = rows[i: i + BATCH]
                self.conn.executemany(
                    sql, [tuple(r[k] for k in keys) for r in batch])

    # ────────── Seeding ──────────

    def seed(self, data: Dict[str, List[Dict]], progress_callback=None) -> None:
        from config import SEED_ORDER
        cb = progress_callback or (lambda t, d, tot: None)
        total = sum(len(v) for v in data.values())
        for table in SEED_ORDER:
            rows = data.get(table, [])
            cb(table, len(rows), total)
            self._bulk_insert(table, rows)

    # ==================== CREATE ====================

    def C1_add_single_alkohol(self, product: Dict) -> None:
        cols = list(product.keys())
        vals = ", ".join(["?"] * len(cols))
        with self.conn:
            self.conn.execute(
                f"INSERT INTO alkohol ({', '.join(cols)}) VALUES ({vals})",
                tuple(product[c] for c in cols),
            )

    def C2_add_bulk_alkohol(self, products: List[Dict]) -> None:
        self._bulk_insert("alkohol", products)

    def C3_add_klient(self, klient: Dict) -> None:
        cols = list(klient.keys())
        vals = ", ".join(["?"] * len(cols))
        with self.conn:
            self.conn.execute(
                f"INSERT INTO klienci ({', '.join(cols)}) VALUES ({vals})",
                tuple(klient[c] for c in cols),
            )

    def C4_add_paragon(self, paragon: Dict) -> None:
        cols = list(paragon.keys())
        vals = ", ".join(["?"] * len(cols))
        with self.conn:
            self.conn.execute(
                f"INSERT INTO paragony ({', '.join(cols)}) VALUES ({vals})",
                tuple(paragon[c] for c in cols),
            )

    def C5_add_pozycje_paragonu(self, pozycje: List[Dict]) -> None:
        self._bulk_insert("pozycje_paragonu", pozycje)

    def C6_add_faktura(self, faktura: Dict) -> None:
        cols = list(faktura.keys())
        vals = ", ".join(["?"] * len(cols))
        with self.conn:
            self.conn.execute(
                f"INSERT INTO faktury ({', '.join(cols)}) VALUES ({vals})",
                tuple(faktura[c] for c in cols),
            )

    # ==================== READ ====================

    def R1_get_alkohol_by_id(self, produkt_id: int) -> Any:
        cur = self.conn.execute(
            "SELECT * FROM alkohol WHERE produkt_id = ?", (produkt_id,))
        return cur.fetchone()

    def R2_get_products_by_kategoria(self, kategoria_id: int) -> List:
        cur = self.conn.execute(
            "SELECT * FROM alkohol WHERE kategoria_id = ?", (kategoria_id,))
        return cur.fetchall()

    def R3_get_paragony_klienta(self, klient_id: int) -> List:
        cur = self.conn.execute(
            "SELECT * FROM paragony WHERE klient_id = ?", (klient_id,))
        return cur.fetchall()

    def R4_get_paragon_details(self, paragon_id: int) -> Any:
        cur = self.conn.execute(
            """SELECT p.*, pp.pozycja_id, pp.produkt_id, pp.typ_produktu,
                      pp.wartosc_netto AS poz_netto, pp.wartosc_brutto AS poz_brutto
               FROM paragony p
               LEFT JOIN pozycje_paragonu pp ON p.paragon_id = pp.paragon_id
               WHERE p.paragon_id = ?""",
            (paragon_id,),
        )
        return cur.fetchall()

    def R5_get_top_expensive_alkohol(self, limit: int = 10) -> List:
        cur = self.conn.execute(
            "SELECT * FROM alkohol ORDER BY cena_producenta DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()

    def R6_get_faktury_by_okres(self, data_od: str, data_do: str) -> List:
        cur = self.conn.execute(
            "SELECT * FROM faktury WHERE data_wystawienia BETWEEN ? AND ?",
            (data_od, data_do),
        )
        return cur.fetchall()

    # ==================== UPDATE ====================

    def U1_update_cena_alkohol(self, produkt_id: int, nowa_cena: float) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE alkohol SET cena_producenta = ? WHERE produkt_id = ?",
                (nowa_cena, produkt_id),
            )

    def U2_update_klient(self, klient_id: int, data: Dict) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE klienci SET email = ?, telefon = ? WHERE klient_id = ?",
                (data["email"], data["telefon"], klient_id),
            )

    def U3_update_status_paragonu(self, paragon_id: int, status: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE paragony SET status_transakcji = ? WHERE paragon_id = ?",
                (status, paragon_id),
            )

    def U4_bulk_update_ceny_kategoria(self, kategoria_id: int,
                                       rabat_pct: float) -> None:
        factor = round(1 - rabat_pct / 100, 4)
        with self.conn:
            self.conn.execute(
                "UPDATE alkohol SET cena_producenta = ROUND(cena_producenta * ?, 2) "
                "WHERE kategoria_id = ?",
                (factor, kategoria_id),
            )

    def U5_update_status_dostawy(self, dostawa_id: int, status: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE dostawy SET status_dostawy = ? WHERE dostawa_id = ?",
                (status, dostawa_id),
            )

    def U6_update_producent(self, producent_id: int, data: Dict) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE producenci SET nazwa = ?, strona_www = ? WHERE producent_id = ?",
                (data["nazwa"], data["strona_www"], producent_id),
            )

    # ==================== DELETE ====================

    def D1_delete_alkohol(self, produkt_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM alkohol WHERE produkt_id = ?", (produkt_id,))

    def D2_delete_klient(self, klient_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM klienci WHERE klient_id = ?", (klient_id,))

    def D3_delete_paragon(self, paragon_id: int) -> None:
        with self.conn:
            # CASCADE usunie pozycje_paragonu
            self.conn.execute(
                "DELETE FROM paragony WHERE paragon_id = ?", (paragon_id,))

    def D4_delete_pozycje_paragonu(self, paragon_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM pozycje_paragonu WHERE paragon_id = ?",
                (paragon_id,))

    def D5_bulk_delete_tyton(self, produkt_ids: List[int]) -> None:
        with self.conn:
            self.conn.executemany(
                "DELETE FROM tyton WHERE produkt_id = ?",
                [(pid,) for pid in produkt_ids],
            )

    def D6_delete_dostawa(self, dostawa_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM dostawy WHERE dostawa_id = ?", (dostawa_id,))
