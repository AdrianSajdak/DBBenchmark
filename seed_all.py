"""
seed_all.py – Jednorazowe wypełnienie baz danych danymi.

Uruchamiasz RAZ po postawieniu kontenerów Dockera.

WAŻNE – Redis NIE jest seedowany tutaj (domyślnie):
  Redis trzyma dane w RAM permanentnie → zamiast tego każdy run/run_*.py
  ładuje Redis tuż przed benchmarkiem i zwalnia RAM po nim.
  Dane do Redis też są brane z cache .pkl (generowanego tutaj).

  Jeśli chcesz mimo to pre-seedować Redis: --include-redis

Użycie:
  python seed_all.py                          # seed SQLite + MySQL + CouchDB (wszystkie rozmiary)
  python seed_all.py --sizes small            # tylko small
  python seed_all.py --dbs sqlite mysql       # tylko wybrane bazy
  python seed_all.py --include-redis          # dodaj też Redis (zajmie ~13 GB RAM!)
  python seed_all.py --regen                  # wymuś ponowne generowanie danych
  python seed_all.py --no-volumes             # pomiń pomiar rozmiarów po seedowaniu

Argumenty:
  --sizes   {small,medium,large} [...]   Które rozmiary seedować (domyślnie: wszystkie)
  --dbs     {sqlite,mysql,mysql_norm,couchdb} [...]
                                         Które bazy seedować (domyślnie: wszystkie dyskowe)
  --include-redis                        Dodaj Redis do seedowania (wymaga ~13 GB RAM)
  --regen                                Wymuś ponowne generowanie danych (ignoruj cache)
  --no-volumes                           Pomiń pomiar rozmiarów baz po seedowaniu

Dane generowane raz, zapisywane do pliku cache (data/cache/):
  data/cache/dataset_small.pkl
  data/cache/dataset_medium.pkl
  data/cache/dataset_large.pkl
Jeśli pliki istnieją, dane NIE są generowane ponownie (--regen wymusza).

Po seedowaniu skrypt automatycznie mierzy rozmiary baz i zapisuje do:
  results/data_seeding.csv  (czasy seedowania + rozmiary + timestamp)
"""
import argparse
import os
import pickle
import sys
import time
from datetime import datetime

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DATA_SIZES, SIZE_DB_CONFIGS
from data.generator import generate_dataset, count_dataset
from measure_volumes import collect_volumes, update_seeding_row

console = Console()

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Redis domyślnie wyłączony z seedowania (load-on-demand w run/*.py)
ALL_DB_KEYS_DISK = ["sqlite", "mysql", "mysql_norm", "couchdb"]
ALL_DB_KEYS_ALL  = ["sqlite", "mysql", "mysql_norm", "couchdb", "redis"]
ALL_SIZES        = ["small", "medium", "large"]


# ────────────────────────────────────────────────────────────────
# Cache danych
# ────────────────────────────────────────────────────────────────

def _cache_path(size_name: str) -> str:
    return os.path.join(CACHE_DIR, f"dataset_{size_name}.pkl")


def _load_or_generate(size_name: str, regen: bool = False):
    """Ładuje dataset z cache lub generuje go i zapisuje."""
    path = _cache_path(size_name)

    if not regen and os.path.exists(path):
        size_mb = os.path.getsize(path) / 1024 / 1024
        console.print(f"  [dim]Ładowanie z cache: {path} ({size_mb:.1f} MB)[/dim]")
        t0 = time.perf_counter()
        with open(path, "rb") as f:
            data = pickle.load(f)
        console.print(f"  [green]✓ Dane załadowane z cache ({time.perf_counter() - t0:.1f}s)[/green]")
        return data

    # Generowanie
    size_cfg = DATA_SIZES[size_name]
    est = count_dataset(size_cfg)
    console.print(f"  [cyan]Generowanie ~{est:,} rekordów dla [{size_name}]...[/cyan]")

    stages_done = []
    with Progress(SpinnerColumn(), TextColumn("  {task.description}"),
                  TimeElapsedColumn(), console=console) as pbar:
        task = pbar.add_task("Generowanie...", total=None)

        def cb(stage):
            stages_done.append(stage)
            pbar.update(task, description=f"  Generowanie: {stage} ({len(stages_done)}/10)")

        t0 = time.perf_counter()
        data = generate_dataset(size_cfg, progress_callback=cb)

    total = sum(len(v) for v in data.values())
    console.print(f"  [green]✓ Wygenerowano {total:,} rekordów ({time.perf_counter() - t0:.1f}s)[/green]")

    console.print(f"  Zapisuję cache → {path}...")
    t0 = time.perf_counter()
    with open(path, "wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    size_mb = os.path.getsize(path) / 1024 / 1024
    console.print(f"  [green]✓ Cache zapisany ({size_mb:.1f} MB, {time.perf_counter() - t0:.1f}s)[/green]")

    return data


# ────────────────────────────────────────────────────────────────
# Tworzenie instancji baz
# ────────────────────────────────────────────────────────────────

def _make_db(db_key: str, size_name: str):
    from db import MySQLDB, MySQLNormalizedDB, SQLiteDB, CouchDBDB, RedisDB
    cfg = SIZE_DB_CONFIGS[size_name][db_key]
    return {
        "sqlite":     lambda: SQLiteDB(cfg),
        "mysql":      lambda: MySQLDB(cfg),
        "mysql_norm": lambda: MySQLNormalizedDB(cfg),
        "couchdb":    lambda: CouchDBDB(cfg),
        "redis":      lambda: RedisDB(cfg),
    }[db_key]()


def _db_label(db_key: str, size_name: str) -> str:
    labels = {
        "sqlite":     "SQLite",
        "mysql":      "MySQL",
        "mysql_norm": "MySQL-Norm",
        "couchdb":    "CouchDB",
        "redis":      "Redis",
    }
    return f"{labels[db_key]} [{size_name}]"


# ────────────────────────────────────────────────────────────────
# Seed jednej bazy
# ────────────────────────────────────────────────────────────────

def _seed_db(db_key: str, size_name: str, data: dict) -> tuple:
    """
    Seeduje jedną bazę.
    Zwraca (sukces: bool, elapsed_s: float).
    """
    label = _db_label(db_key, size_name)
    db = _make_db(db_key, size_name)

    if not db.connect():
        console.print(f"    [red]✗ {label}: nie można połączyć[/red]")
        return False, 0.0

    total_records = sum(len(v) for v in data.values())

    try:
        console.print(f"    [yellow]▶ {label}[/yellow] – setup schema...")
        db.setup_schema()
        db.clear_data()

        with Progress(
            SpinnerColumn(),
            TextColumn("      {task.description}"),
            BarColumn(),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as pbar:
            task = pbar.add_task(f"{label}", total=total_records)
            inserted = [0]

            def cb(table, count, tot):
                pbar.update(task, description=f"      {label} › {table}")
                pbar.advance(task, count)
                inserted[0] += count

            t0 = time.perf_counter()
            db.seed(data, progress_callback=cb)
            elapsed = time.perf_counter() - t0

        console.print(
            f"    [green]✓ {label} – {total_records:,} rekordów w {elapsed:.1f}s[/green]"
        )
        return True, elapsed

    except Exception as e:
        console.print(f"    [red]✗ {label}: {e}[/red]")
        return False, 0.0
    finally:
        db.disconnect()


# ────────────────────────────────────────────────────────────────
# Główna sekwencja
# ────────────────────────────────────────────────────────────────

def seed_all(sizes: list, db_keys: list, regen: bool = False,
             measure_vols: bool = True) -> None:
    redis_included = "redis" in db_keys
    disk_keys = [k for k in db_keys if k != "redis"]

    console.print(Panel.fit(
        "[bold magenta]seed_all.py – Jednorazowe wypełnienie baz[/bold magenta]\n"
        f"Rozmiary: {', '.join(sizes)}\n"
        f"Bazy dyskowe: {', '.join(disk_keys) or '(brak)'}\n"
        + ("[yellow]Redis: BĘDZIE seedowany (zajmie dużo RAM!)[/yellow]\n"
           if redis_included else
           "[dim]Redis: POMINIĘTY – dane ładowane on-demand przez run/*.py[/dim]\n")
        + f"Regen cache: {'TAK' if regen else 'NIE'}\n"
        + f"Pomiar wolumenów: {'TAK' if measure_vols else 'NIE (--no-volumes)'}",
        title="DBBenchmark Seeder",
    ))

    results = {}   # {(db_key, size): bool}
    t_start = time.perf_counter()

    for size_name in sizes:
        size_cfg = DATA_SIZES[size_name]
        console.print(f"\n[bold blue]══ Rozmiar: {size_cfg['label']} ══[/bold blue]")

        data = _load_or_generate(size_name, regen=regen)
        total_records = sum(len(v) for v in data.values())

        if not disk_keys and not redis_included:
            console.print("  [dim]Brak baz do seedowania dla tego rozmiaru (tylko cache).[/dim]")
            continue

        console.print(f"\n  [bold]Seedowanie baz dyskowych:[/bold]")
        for db_key in disk_keys:
            ok, elapsed = _seed_db(db_key, size_name, data)
            results[(db_key, size_name)] = ok
            if ok:
                update_seeding_row(
                    size_name, db_key,
                    seeding_time=elapsed,
                    seeding_amount=total_records,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )

        if redis_included:
            console.print(f"\n  [bold yellow]Seedowanie Redis [{size_name}]:[/bold yellow]")
            ok, elapsed = _seed_db("redis", size_name, data)
            results[("redis", size_name)] = ok
            if ok:
                update_seeding_row(
                    size_name, "redis",
                    seeding_time=elapsed,
                    seeding_amount=total_records,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )

    # Podsumowanie seedowania
    elapsed_total = time.perf_counter() - t_start
    console.print(f"\n[bold]══ Podsumowanie seeda ══[/bold]")
    ok_count   = sum(1 for v in results.values() if v)
    fail_count = len(results) - ok_count

    for (db_key, size_name), ok in results.items():
        label  = _db_label(db_key, size_name)
        status = "[green]✓ OK[/green]" if ok else "[red]✗ BŁĄD[/red]"
        console.print(f"  {label:35s} {status}")

    console.print(f"\n  Łącznie: [green]{ok_count} OK[/green] / [red]{fail_count} BŁĄD[/red]")
    console.print(f"  Czas całkowity: {elapsed_total:.1f}s\n")

    if fail_count > 0:
        console.print("[red]UWAGA: Niektóre bazy nie zostały wypełnione![/red]")
        sys.exit(1)

    # Pomiar rozmiarów baz (bez Redis – ten jest mierzony w _bench_runner.py)
    if measure_vols and disk_keys:
        seeded_disk = [k for k in disk_keys if k != "redis"]
        seeded_sizes = [s for s in sizes if any(results.get((k, s)) for k in seeded_disk)]
        if seeded_disk and seeded_sizes:
            collect_volumes(seeded_sizes, seeded_disk)

    from measure_volumes import SEEDING_CSV
    console.print(Panel.fit(
        "[bold green]✓ Seed zakończony sukcesem![/bold green]\n"
        f"Czasy i rozmiary zapisane → {SEEDING_CSV}\n\n"
        "Możesz teraz uruchamiać benchmarki:\n"
        "  python run/run_small.py\n"
        "  python run/run_medium.py\n"
        "  python run/run_large.py\n"
        "  python run_all.py",
        style="green",
    ))


# ────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Jednorazowe wypełnienie baz danych danymi benchmarkowymi",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Przykłady:\n"
            "  python seed_all.py\n"
            "  python seed_all.py --sizes small\n"
            "  python seed_all.py --dbs sqlite mysql\n"
            "  python seed_all.py --include-redis\n"
            "  python seed_all.py --regen --no-volumes\n"
        ),
    )
    parser.add_argument(
        "--sizes", nargs="+", choices=ALL_SIZES, default=ALL_SIZES,
        metavar="SIZE",
        help=f"Rozmiary do seedowania (domyślnie: wszystkie = {ALL_SIZES})",
    )
    parser.add_argument(
        "--dbs", nargs="+", choices=ALL_DB_KEYS_DISK, default=ALL_DB_KEYS_DISK,
        metavar="DB",
        help=(
            f"Bazy dyskowe do seedowania (domyślnie: {ALL_DB_KEYS_DISK}). "
            "Redis pominięty – użyj --include-redis."
        ),
    )
    parser.add_argument(
        "--include-redis", action="store_true",
        help="Dodaj Redis do seedowania (zajmie ~13 GB RAM!)",
    )
    parser.add_argument(
        "--regen", action="store_true",
        help="Wymuś ponowne generowanie danych (ignoruj cache .pkl)",
    )
    parser.add_argument(
        "--no-volumes", action="store_true",
        help="Pomiń pomiar rozmiarów baz po seedowaniu",
    )
    args = parser.parse_args()

    db_keys = args.dbs + (["redis"] if args.include_redis else [])
    seed_all(args.sizes, db_keys, regen=args.regen,
             measure_vols=not args.no_volumes)


if __name__ == "__main__":
    main()
