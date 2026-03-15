"""
measure_volumes.py – Mierzy faktyczny rozmiar danych każdej bazy (nie cały wolumen Docker)
i aktualizuje results/data_seeding.csv (kolumna volume_size_bytes).

Dla każdej bazy mierzy rozmiar konkretnych plików/katalogów, nie całego wolumenu:
  SQLite     – rozmiar pliku data/sqlite/sklep_{size}.db
  MySQL      – du -sb /var/lib/mysql/{db_name}  w kontenerze bench_mysql
  MySQL-Norm – du -sb /var/lib/mysql/{db_name}  w kontenerze bench_mysql_norm
  CouchDB    – sizes.file z API GET /{db_name}  (faktyczny rozmiar pliku .couch na dysku)
  Redis      – pomiar przyrostu used_memory podczas seedowania (w _bench_runner.py)

Skrypt można uruchomić samodzielnie lub jest wywoływany automatycznie przez seed_all.py.

Użycie:
  python measure_volumes.py                        # wszystkie bazy i rozmiary
  python measure_volumes.py --sizes small medium   # tylko small i medium
  python measure_volumes.py --dbs sqlite mysql     # tylko wybrane bazy
"""
import argparse
import base64
import csv
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.table import Table

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import SIZE_DB_CONFIGS

console = Console()

ROOT = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(ROOT, "results")
SEEDING_CSV = os.path.join(RESULTS_DIR, "data_seeding.csv")
CSV_HEADER = ["data_set", "database", "seeding_time_s", "seeding_amount",
              "volume_size_bytes", "seeding_timestamp"]

# Kontenery Docker per baza
_CONTAINERS = {
    "mysql":      "bench_mysql",
    "mysql_norm": "bench_mysql_norm",
    "couchdb":    "bench_couchdb",
    "redis":      "bench_redis",
}

# Klucze → etykiety w CSV (zgodne z istniejącym formatem)
DB_CSV_LABEL = {
    "sqlite":     "sqlite",
    "mysql":      "mysql",
    "mysql_norm": "mysql-norm",
    "couchdb":    "couchdb",
    "redis":      "redis",
}

ALL_DB_KEYS_DISK = ["sqlite", "mysql", "mysql_norm", "couchdb"]
ALL_SIZES = ["small", "medium", "large"]

SIZE_ORDER = {"small": 0, "medium": 1, "large": 2}


# ────────────────────────────────────────────────────────────────
# Pomiar rozmiarów
# ────────────────────────────────────────────────────────────────

def measure_db_size(db_key: str, size_name: str) -> Optional[int]:
    """
    Mierzy faktyczny rozmiar danych danej bazy dla podanego rozmiaru (w bajtach).

    Zwraca None gdy baza niedostępna, kontener nie działa lub brak danych.
    """
    try:
        if db_key == "sqlite":
            path = SIZE_DB_CONFIGS[size_name]["sqlite"].database
            # Ścieżka może być względna – rozwiąż od ROOT
            if not os.path.isabs(path):
                path = os.path.join(ROOT, path)
            return int(os.path.getsize(path)) if os.path.exists(path) else None

        elif db_key in ("mysql", "mysql_norm"):
            cfg = SIZE_DB_CONFIGS[size_name][db_key]
            container = _CONTAINERS[db_key]
            db_path = f"/var/lib/mysql/{cfg.database}"
            result = subprocess.run(
                ["docker", "exec", container, "du", "-sb", db_path],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0 and result.stdout.strip():
                return int(result.stdout.split()[0])
            # katalog może nie istnieć jeszcze – baza nie seedowana
            return None

        elif db_key == "couchdb":
            cfg = SIZE_DB_CONFIGS[size_name]["couchdb"]
            url = f"{cfg.url}/{cfg.database}"
            req = urllib.request.Request(url)
            creds = base64.b64encode(
                f"{cfg.username}:{cfg.password}".encode()
            ).decode()
            req.add_header("Authorization", f"Basic {creds}")
            resp = urllib.request.urlopen(req, timeout=8)
            info = json.loads(resp.read())
            # CouchDB ≥ 2 zwraca {'sizes': {'file': <bytes>, 'external': ..., 'active': ...}}
            # sizes.file = faktyczny rozmiar pliku .couch na dysku
            return (info.get("sizes", {}).get("file")
                    or info.get("disk_size"))

        # Redis pomijany tutaj – mierzony w _bench_runner.py (load-on-demand)
    except (subprocess.TimeoutExpired, urllib.error.URLError,
            FileNotFoundError, KeyError) as e:
        console.print(f"  [dim yellow]Pomiar {db_key}[{size_name}]: {e}[/dim yellow]")
    except Exception as e:
        console.print(f"  [dim yellow]Pomiar {db_key}[{size_name}]: {type(e).__name__}: {e}[/dim yellow]")
    return None


# ────────────────────────────────────────────────────────────────
# Zapis do CSV
# ────────────────────────────────────────────────────────────────

def update_seeding_row(
    size_name: str,
    db_key: str,
    *,
    seeding_time: Optional[float] = None,
    seeding_amount: Optional[int] = None,
    volume_bytes: Optional[int] = None,
    timestamp: Optional[str] = None,
) -> None:
    """
    Aktualizuje lub wstawia wiersz w data_seeding.csv.

    Przekazuj tylko pola, które mają być nadpisane – pozostałe są zachowane.
    Wywołuj po seedowaniu (seeding_time/amount/timestamp) oraz po pomiarze
    wolumenu (volume_bytes) – mogą to być dwa oddzielne wywołania.
    """
    os.makedirs(RESULTS_DIR, exist_ok=True)
    db_label = DB_CSV_LABEL.get(db_key, db_key)

    rows: list = []
    existing_fields: list = []

    if os.path.exists(SEEDING_CSV):
        with open(SEEDING_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_fields = reader.fieldnames or []
            rows = list(reader)

    # Nagłówek = CSV_HEADER + ewentualne dodatkowe kolumny z istniejącego pliku
    final_fields = CSV_HEADER[:]
    for col in existing_fields:
        if col not in final_fields:
            final_fields.append(col)

    # Zaktualizuj istniejący wiersz lub wstaw nowy
    target = None
    for row in rows:
        if row.get("data_set") == size_name and row.get("database") == db_label:
            target = row
            break
    if target is None:
        target = {"data_set": size_name, "database": db_label}
        rows.append(target)

    if seeding_time is not None:
        target["seeding_time_s"] = f"{seeding_time:.1f}"
    if seeding_amount is not None:
        target["seeding_amount"] = str(seeding_amount)
    if volume_bytes is not None:
        target["volume_size_bytes"] = str(volume_bytes)
    if timestamp is not None:
        target["seeding_timestamp"] = timestamp

    # Sortuj: small → medium → large, w obrębie rozmiaru alfabetycznie po nazwie bazy
    rows.sort(key=lambda r: (
        SIZE_ORDER.get(r.get("data_set", ""), 9),
        r.get("database", ""),
    ))

    with open(SEEDING_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=final_fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in final_fields})


# ────────────────────────────────────────────────────────────────
# Zbiorcze uruchomienie
# ────────────────────────────────────────────────────────────────

def collect_volumes(sizes: Optional[list] = None,
                    dbs: Optional[list] = None) -> None:
    """
    Mierzy rozmiary danych dla wszystkich podanych baz i rozmiarów,
    aktualizuje kolumnę volume_size_bytes w data_seeding.csv.

    Redis jest pomijany (jego rozmiar mierzony jest w _bench_runner.py).
    """
    if sizes is None:
        sizes = ALL_SIZES
    if dbs is None:
        dbs = ALL_DB_KEYS_DISK

    console.print("\n[bold]══ Pomiar rozmiarów baz danych ══[/bold]")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Baza",    width=12)
    table.add_column("Rozmiar", width=8)
    table.add_column("Bytes",   justify="right", width=16)
    table.add_column("MB",      justify="right", width=10)
    table.add_column("Status",  width=12)

    any_ok = False
    for size_name in sizes:
        for db_key in dbs:
            if db_key == "redis":
                continue
            vol = measure_db_size(db_key, size_name)
            if vol is not None:
                update_seeding_row(size_name, db_key, volume_bytes=vol)
                table.add_row(
                    db_key, size_name,
                    f"{vol:,}", f"{vol / 1024 / 1024:.1f}",
                    "[green]OK[/green]",
                )
                any_ok = True
            else:
                table.add_row(
                    db_key, size_name, "-", "-",
                    "[yellow]brak danych[/yellow]",
                )

    console.print(table)
    if any_ok:
        console.print(f"[dim]→ zaktualizowano: {SEEDING_CSV}[/dim]\n")
    else:
        console.print("[yellow]Żadna baza nie zwróciła danych – czy kontenery działają?[/yellow]\n")


# ────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Pomiar rozmiarów danych w bazach – aktualizuje results/data_seeding.csv.\n"
            "Mierzy katalog/plik konkretnej bazy, nie cały wolumen Docker.\n"
            "Redis jest pominięty (mierzony podczas seedowania przez _bench_runner.py)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--sizes", nargs="+", choices=ALL_SIZES, default=ALL_SIZES,
        help=f"Rozmiary do sprawdzenia (domyślnie: wszystkie = {ALL_SIZES})",
    )
    parser.add_argument(
        "--dbs", nargs="+", choices=ALL_DB_KEYS_DISK, default=ALL_DB_KEYS_DISK,
        help=(
            f"Bazy do sprawdzenia (domyślnie: {ALL_DB_KEYS_DISK}). "
            "Redis pominięty – mierzony przez _bench_runner.py."
        ),
    )
    args = parser.parse_args()
    collect_volumes(args.sizes, args.dbs)


if __name__ == "__main__":
    main()
