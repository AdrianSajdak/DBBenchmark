"""MySQL – implementacja CRUD dla modelu TABELE.txt."""
from typing import Any, Dict, List

import mysql.connector
from mysql.connector import Error

from db.base_db import BaseDB

BATCH = 1000


class MySQLDB(BaseDB):
    name = "MySQL"

    def __init__(self, config):
        self.cfg = config
        self.conn = None
        self.cursor = None

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
            print(f"[MySQL] Błąd połączenia: {e}")
            return False

    def disconnect(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def setup_schema(self) -> None:
        stmts = [
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
                kraj          VARCHAR(80),
                miasto        VARCHAR(80),
                rok_zalozenia INT,
                strona_www    VARCHAR(200),
                czy_aktywny   TINYINT DEFAULT 1
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS pracownicy (
                pracownik_id      INT PRIMARY KEY,
                imie              VARCHAR(60) NOT NULL,
                nazwisko          VARCHAR(60) NOT NULL,
                stanowisko        VARCHAR(60),
                data_zatrudnienia DATE,
                data_zwolnienia   DATE,
                pensja            DECIMAL(10,2),
                telefon           VARCHAR(30),
                email             VARCHAR(100),
                status_pracownika VARCHAR(30)
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS klienci (
                klient_id            INT PRIMARY KEY,
                imie                 VARCHAR(60) NOT NULL,
                nazwisko             VARCHAR(60) NOT NULL,
                data_urodzenia       DATE,
                plec                 CHAR(1),
                miasto               VARCHAR(80),
                telefon              VARCHAR(30),
                email                VARCHAR(120),
                data_rejestracji     DATE,
                status_klienta       VARCHAR(30),
                punkty_lojalnosciowe INT DEFAULT 0
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS alkohol (
                produkt_id                    INT PRIMARY KEY,
                nazwa                         VARCHAR(200) NOT NULL,
                marka                         VARCHAR(100),
                kategoria_id                  INT,
                producent_id                  INT,
                kraj_pochodzenia              VARCHAR(80),
                rocznik                       INT,
                procent_alkoholu              DECIMAL(5,1),
                pojemnosc_ml                  INT,
                typ_opakowania                VARCHAR(30),
                data_wprowadzenia_do_sprzedazy DATE,
                data_wycofania                DATE,
                kod_kreskowy                  VARCHAR(20),
                cena_producenta               DECIMAL(10,2),
                stawka_vat                    INT,
                FOREIGN KEY (kategoria_id)
                    REFERENCES kategorie_produktow(kategoria_id) ON DELETE SET NULL,
                FOREIGN KEY (producent_id)
                    REFERENCES producenci(producent_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS tyton (
                produkt_id                    INT PRIMARY KEY,
                nazwa                         VARCHAR(200) NOT NULL,
                marka                         VARCHAR(100),
                kategoria_id                  INT,
                producent_id                  INT,
                kraj_pochodzenia              VARCHAR(80),
                liczba_sztuk_w_opakowaniu     INT,
                typ_opakowania                VARCHAR(30),
                data_wprowadzenia_do_sprzedazy DATE,
                data_wycofania                DATE,
                kod_kreskowy                  VARCHAR(20),
                cena_producenta               DECIMAL(10,2),
                stawka_vat                    INT,
                FOREIGN KEY (kategoria_id)
                    REFERENCES kategorie_produktow(kategoria_id) ON DELETE SET NULL,
                FOREIGN KEY (producent_id)
                    REFERENCES producenci(producent_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS paragony (
                paragon_id        INT PRIMARY KEY,
                numer_paragonu    VARCHAR(30),
                data_sprzedazy    DATE,
                godzina_sprzedazy TIME,
                pracownik_id      INT,
                klient_id         INT,
                forma_platnosci   VARCHAR(30),
                wartosc_netto     DECIMAL(12,2),
                wartosc_brutto    DECIMAL(12,2),
                status_transakcji VARCHAR(30),
                FOREIGN KEY (pracownik_id) REFERENCES pracownicy(pracownik_id) ON DELETE SET NULL,
                FOREIGN KEY (klient_id)    REFERENCES klienci(klient_id) ON DELETE SET NULL
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
                faktura_id      INT PRIMARY KEY,
                numer_faktury   VARCHAR(30),
                sprzedaz_id     INT,
                data_wystawienia DATE,
                data_sprzedazy  DATE,
                nabywca_nazwa   VARCHAR(200),
                nabywca_nip     VARCHAR(20),
                nabywca_adres   VARCHAR(300),
                wartosc_netto   DECIMAL(12,2),
                wartosc_vat     DECIMAL(12,2),
                wartosc_brutto  DECIMAL(12,2),
                status_faktury  VARCHAR(30),
                FOREIGN KEY (sprzedaz_id) REFERENCES paragony(paragon_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS dostawy (
                dostawa_id            INT PRIMARY KEY,
                numer_dostawy         VARCHAR(30),
                producent_id          INT,
                data_zamowienia       DATE,
                data_dostawy          DATE,
                wartosc_netto         DECIMAL(12,2),
                wartosc_brutto        DECIMAL(12,2),
                status_dostawy        VARCHAR(30),
                numer_faktury_zakupu  VARCHAR(30),
                FOREIGN KEY (producent_id) REFERENCES producenci(producent_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
        ]
        for stmt in stmts:
            self.cursor.execute(stmt)
        self.conn.commit()

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

    # ────────── Indeksy (ocena 4.0) ──────────

    def create_indexes(self) -> None:
        idxs = [
            "CREATE INDEX idx_alk_kategoria ON alkohol(kategoria_id)",
            "CREATE INDEX idx_alk_producent ON alkohol(producent_id)",
            "CREATE INDEX idx_alk_cena ON alkohol(cena_producenta)",
            "CREATE INDEX idx_alk_marka ON alkohol(marka)",
            "CREATE INDEX idx_tyt_kategoria ON tyton(kategoria_id)",
            "CREATE INDEX idx_tyt_producent ON tyton(producent_id)",
            "CREATE INDEX idx_par_klient ON paragony(klient_id)",
            "CREATE INDEX idx_par_pracownik ON paragony(pracownik_id)",
            "CREATE INDEX idx_par_data ON paragony(data_sprzedazy)",
            "CREATE INDEX idx_poz_paragon ON pozycje_paragonu(paragon_id)",
            "CREATE INDEX idx_poz_produkt ON pozycje_paragonu(produkt_id, typ_produktu)",
            "CREATE INDEX idx_fak_sprzedaz ON faktury(sprzedaz_id)",
            "CREATE INDEX idx_fak_data ON faktury(data_wystawienia)",
            "CREATE INDEX idx_dos_producent ON dostawy(producent_id)",
            "CREATE INDEX idx_dos_data ON dostawy(data_zamowienia)",
            "CREATE INDEX idx_kli_miasto ON klienci(miasto)",
            "CREATE INDEX idx_kli_status ON klienci(status_klienta)",
        ]
        for sql in idxs:
            try:
                self.cursor.execute(sql)
            except Exception:
                pass  # indeks może już istnieć
        self.conn.commit()

    def drop_indexes(self) -> None:
        pairs = [
            ("alkohol",           "idx_alk_kategoria"),
            ("alkohol",           "idx_alk_producent"),
            ("alkohol",           "idx_alk_cena"),
            ("alkohol",           "idx_alk_marka"),
            ("tyton",             "idx_tyt_kategoria"),
            ("tyton",             "idx_tyt_producent"),
            ("paragony",          "idx_par_klient"),
            ("paragony",          "idx_par_pracownik"),
            ("paragony",          "idx_par_data"),
            ("pozycje_paragonu",  "idx_poz_paragon"),
            ("pozycje_paragonu",  "idx_poz_produkt"),
            ("faktury",           "idx_fak_sprzedaz"),
            ("faktury",           "idx_fak_data"),
            ("dostawy",           "idx_dos_producent"),
            ("dostawy",           "idx_dos_data"),
            ("klienci",           "idx_kli_miasto"),
            ("klienci",           "idx_kli_status"),
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
        keys = list(product.keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = (f"INSERT INTO alkohol ({', '.join(f'`{k}`' for k in keys)}) "
               f"VALUES ({placeholders})")
        self.cursor.execute(sql, tuple(product[k] for k in keys))
        self.conn.commit()

    def C2_add_bulk_alkohol(self, products: List[Dict]) -> None:
        self._bulk_insert("alkohol", products)

    def C3_add_klient(self, klient: Dict) -> None:
        keys = list(klient.keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = (f"INSERT INTO klienci ({', '.join(f'`{k}`' for k in keys)}) "
               f"VALUES ({placeholders})")
        self.cursor.execute(sql, tuple(klient[k] for k in keys))
        self.conn.commit()

    def C4_add_paragon(self, paragon: Dict) -> None:
        keys = list(paragon.keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = (f"INSERT INTO paragony ({', '.join(f'`{k}`' for k in keys)}) "
               f"VALUES ({placeholders})")
        self.cursor.execute(sql, tuple(paragon[k] for k in keys))
        self.conn.commit()

    def C5_add_pozycje_paragonu(self, pozycje: List[Dict]) -> None:
        self._bulk_insert("pozycje_paragonu", pozycje)

    def C6_add_faktura(self, faktura: Dict) -> None:
        keys = list(faktura.keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = (f"INSERT INTO faktury ({', '.join(f'`{k}`' for k in keys)}) "
               f"VALUES ({placeholders})")
        self.cursor.execute(sql, tuple(faktura[k] for k in keys))
        self.conn.commit()

    # ==================== READ ====================

    def R1_get_alkohol_by_id(self, produkt_id: int) -> Any:
        self.cursor.execute(
            "SELECT * FROM alkohol WHERE produkt_id = %s", (produkt_id,))
        return self.cursor.fetchone()

    def R2_get_products_by_kategoria(self, kategoria_id: int) -> List:
        self.cursor.execute(
            "SELECT * FROM alkohol WHERE kategoria_id = %s", (kategoria_id,))
        return self.cursor.fetchall()

    def R3_get_paragony_klienta(self, klient_id: int) -> List:
        self.cursor.execute(
            "SELECT * FROM paragony WHERE klient_id = %s", (klient_id,))
        return self.cursor.fetchall()

    def R4_get_paragon_details(self, paragon_id: int) -> Any:
        self.cursor.execute(
            """SELECT p.*, pp.pozycja_id, pp.produkt_id, pp.typ_produktu,
                      pp.wartosc_netto AS poz_netto, pp.wartosc_brutto AS poz_brutto
               FROM paragony p
               LEFT JOIN pozycje_paragonu pp ON p.paragon_id = pp.paragon_id
               WHERE p.paragon_id = %s""",
            (paragon_id,),
        )
        return self.cursor.fetchall()

    def R5_get_top_expensive_alkohol(self, limit: int = 10) -> List:
        self.cursor.execute(
            "SELECT * FROM alkohol ORDER BY cena_producenta DESC LIMIT %s",
            (limit,),
        )
        return self.cursor.fetchall()

    def R6_get_faktury_by_okres(self, data_od: str, data_do: str) -> List:
        self.cursor.execute(
            "SELECT * FROM faktury WHERE data_wystawienia BETWEEN %s AND %s",
            (data_od, data_do),
        )
        return self.cursor.fetchall()

    # ==================== UPDATE ====================

    def U1_update_cena_alkohol(self, produkt_id: int, nowa_cena: float) -> None:
        self.cursor.execute(
            "UPDATE alkohol SET cena_producenta = %s WHERE produkt_id = %s",
            (nowa_cena, produkt_id),
        )
        self.conn.commit()

    def U2_update_klient(self, klient_id: int, data: Dict) -> None:
        self.cursor.execute(
            "UPDATE klienci SET email = %s, telefon = %s WHERE klient_id = %s",
            (data["email"], data["telefon"], klient_id),
        )
        self.conn.commit()

    def U3_update_status_paragonu(self, paragon_id: int, status: str) -> None:
        self.cursor.execute(
            "UPDATE paragony SET status_transakcji = %s WHERE paragon_id = %s",
            (status, paragon_id),
        )
        self.conn.commit()

    def U4_bulk_update_ceny_kategoria(self, kategoria_id: int,
                                       rabat_pct: float) -> None:
        self.cursor.execute(
            "UPDATE alkohol SET cena_producenta = ROUND(cena_producenta * (1 - %s / 100), 2) "
            "WHERE kategoria_id = %s",
            (rabat_pct, kategoria_id),
        )
        self.conn.commit()

    def U5_update_status_dostawy(self, dostawa_id: int, status: str) -> None:
        self.cursor.execute(
            "UPDATE dostawy SET status_dostawy = %s WHERE dostawa_id = %s",
            (status, dostawa_id),
        )
        self.conn.commit()

    def U6_update_producent(self, producent_id: int, data: Dict) -> None:
        self.cursor.execute(
            "UPDATE producenci SET nazwa = %s, strona_www = %s WHERE producent_id = %s",
            (data["nazwa"], data["strona_www"], producent_id),
        )
        self.conn.commit()

    # ==================== DELETE ====================

    def D1_delete_alkohol(self, produkt_id: int) -> None:
        self.cursor.execute(
            "DELETE FROM alkohol WHERE produkt_id = %s", (produkt_id,))
        self.conn.commit()

    def D2_delete_klient(self, klient_id: int) -> None:
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        self.cursor.execute(
            "DELETE FROM klienci WHERE klient_id = %s", (klient_id,))
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        self.conn.commit()

    def D3_delete_paragon(self, paragon_id: int) -> None:
        self.cursor.execute(
            "DELETE FROM paragony WHERE paragon_id = %s", (paragon_id,))
        self.conn.commit()

    def D4_delete_pozycje_paragonu(self, paragon_id: int) -> None:
        self.cursor.execute(
            "DELETE FROM pozycje_paragonu WHERE paragon_id = %s",
            (paragon_id,))
        self.conn.commit()

    def D5_bulk_delete_tyton(self, produkt_ids: List[int]) -> None:
        fmt = ", ".join(["%s"] * len(produkt_ids))
        self.cursor.execute(
            f"DELETE FROM tyton WHERE produkt_id IN ({fmt})", tuple(produkt_ids))
        self.conn.commit()

    def D6_delete_dostawa(self, dostawa_id: int) -> None:
        self.cursor.execute(
            "DELETE FROM dostawy WHERE dostawa_id = %s", (dostawa_id,))
        self.conn.commit()
