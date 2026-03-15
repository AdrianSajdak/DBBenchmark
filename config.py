"""
Konfiguracja połączeń z bazami danych oraz rozmiarów zbiorów danych.
Model danych zgodny z TABELE.txt (10 tabel).

Architektura wielobazowa:
  Każda technologia obsługuje 3 rozmiary w JEDNYM kontenerze/procesie:
    MySQL      → bazy: sklep_small / sklep_medium / sklep_large  (port 3306)
    MySQL-Norm → bazy: sklep_norm_small / _medium / _large       (port 3307)
    CouchDB    → bazy: sklep_small / sklep_medium / sklep_large  (port 5984)
    Redis      → db=0 (small) / db=1 (medium) / db=2 (large)    (port 6379)
    SQLite     → pliki: sklep_small.db / sklep_medium.db / sklep_large.db
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class MySQLConfig:
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = "benchmark123"
    database: str = "sklep_small"   # zmieniane per-rozmiar


@dataclass
class MySQLNormalizedConfig:
    """MySQL z pełną normalizacją (3NF) – tabele słownikowe."""
    host: str = "localhost"
    port: int = 3307
    user: str = "root"
    password: str = "benchmark123"
    database: str = "sklep_norm_small"   # zmieniane per-rozmiar


@dataclass
class SQLiteConfig:
    database: str = "data/sqlite/sklep_benchmark.db"   # zmieniane per-rozmiar


@dataclass
class CouchDBConfig:
    url: str = "http://localhost:5984"
    username: str = "admin"
    password: str = "benchmark123"
    database: str = "sklep_small"   # zmieniane per-rozmiar


@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    db: int = 0           # 0=small, 1=medium, 2=large
    password: Optional[str] = None


# ────────────────────────────────────────────────────────────────
# Mapowanie rozmiar → konfiguracje baz (baza/db/plik per-rozmiar)
# ────────────────────────────────────────────────────────────────

SIZE_DB_CONFIGS = {
    "small": {
        "mysql":      MySQLConfig(database="sklep_small"),
        "mysql_norm": MySQLNormalizedConfig(database="sklep_norm_small"),
        "sqlite":     SQLiteConfig(database="data/sqlite/sklep_small.db"),
        "couchdb":    CouchDBConfig(database="sklep_small"),
        "redis":      RedisConfig(db=0),
    },
    "medium": {
        "mysql":      MySQLConfig(database="sklep_medium"),
        "mysql_norm": MySQLNormalizedConfig(database="sklep_norm_medium"),
        "sqlite":     SQLiteConfig(database="data/sqlite/sklep_medium.db"),
        "couchdb":    CouchDBConfig(database="sklep_medium"),
        "redis":      RedisConfig(db=1),
    },
    "large": {
        "mysql":      MySQLConfig(database="sklep_large"),
        "mysql_norm": MySQLNormalizedConfig(database="sklep_norm_large"),
        "sqlite":     SQLiteConfig(database="data/sqlite/sklep_large.db"),
        "couchdb":    CouchDBConfig(database="sklep_large"),
        "redis":      RedisConfig(db=2),
    },
}

# ────────────────────────────────────────────────────────────────
# Rozmiary zbiorów danych
# ────────────────────────────────────────────────────────────────

DATA_SIZES = {
    "small": {
        "label": "Mały   (~500 000 rekordów)",
        "kategorie": 30,
        "producenci": 100,
        "pracownicy": 50,
        "klienci": 50_000,
        "alkohol": 5_000,
        "tyton": 2_000,
        "paragony": 120_000,
        "pozycje_per_paragon": 2.2,
        "faktury": 25_000,
        "dostawy": 15_000,
    },
    "medium": {
        "label": "Średni (~1 000 000 rekordów)",
        "kategorie": 30,
        "producenci": 300,
        "pracownicy": 80,
        "klienci": 100_000,
        "alkohol": 10_000,
        "tyton": 4_000,
        "paragony": 250_000,
        "pozycje_per_paragon": 2.2,
        "faktury": 50_000,
        "dostawy": 40_000,
    },
    "large": {
        "label": "Duży  (~10 000 000 rekordów)",
        "kategorie": 30,
        "producenci": 500,
        "pracownicy": 100,
        "klienci": 1_000_000,
        "alkohol": 50_000,
        "tyton": 20_000,
        "paragony": 2_500_000,
        "pozycje_per_paragon": 2.2,
        "faktury": 500_000,
        "dostawy": 400_000,
    },
}

# Kolejność seedowania (zależności FK)
SEED_ORDER = [
    "kategorie_produktow",
    "producenci",
    "pracownicy",
    "klienci",
    "alkohol",
    "tyton",
    "paragony",
    "pozycje_paragonu",
    "faktury",
    "dostawy",
]

# Domyślne instancje (backwards-compat dla smoke_test / main.py)
mysql_cfg = MySQLConfig()
mysql_normalized_cfg = MySQLNormalizedConfig()
sqlite_cfg = SQLiteConfig()
couchdb_cfg = CouchDBConfig()
redis_cfg = RedisConfig()
