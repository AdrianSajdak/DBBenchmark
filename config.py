"""
Konfiguracja połączeń z bazami danych oraz rozmiarów zbiorów danych.
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MySQLConfig:
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = "password"
    database: str = "liquor_store_benchmark"


@dataclass
class SQLiteConfig:
    database: str = "liquor_store.db"


@dataclass
class CouchDBConfig:
    url: str = "http://localhost:5984"
    username: str = "admin"
    password: str = "password"
    database: str = "liquor_store_benchmark"


@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None


# ------- Rozmiary zbiorów danych -------
# Zgodnie z dokumentem: 500 000 / 1 000 000 / 9 000 000 rekordów łącznie.
# Rekordy są rozkładane proporcjonalnie pomiędzy tabele.

DATA_SIZES = {
    "small": {
        "label": "Mały   (~500 000 rekordów)",
        "stores": 5,
        "categories": 10,
        "suppliers": 50,
        "employees": 50,
        "customers": 50_000,
        "products": 10_000,
        "orders": 100_000,
        "order_items_per_order": 3,   # średnio 3 pozycje → ~300 000 order_items
        "inventory_per_product": 1,   # 1 wpis / produkt → 10 000
        # Płatności = Zamówienia → 100 000
        # Łącznie ≈ 570 000
    },
    "medium": {
        "label": "Średni (~1 000 000 rekordów)",
        "stores": 5,
        "categories": 10,
        "suppliers": 100,
        "employees": 100,
        "customers": 100_000,
        "products": 20_000,
        "orders": 200_000,
        "order_items_per_order": 3,
        "inventory_per_product": 1,
    },
    "large": {
        "label": "Duży   (~9 000 000 rekordów)",
        "stores": 10,
        "categories": 20,
        "suppliers": 200,
        "employees": 500,
        "customers": 900_000,
        "products": 180_000,
        "orders": 1_800_000,
        "order_items_per_order": 3,
        "inventory_per_product": 1,
    },
}

# Konfiguracja domyślna (może być zmieniona przez użytkownika w main.py)
mysql_cfg = MySQLConfig()
sqlite_cfg = SQLiteConfig()
couchdb_cfg = CouchDBConfig()
redis_cfg = RedisConfig()
