"""
_bench_runner.py – Wspólna logika dla run_small/medium/large.py

NIE uruchamiaj bezpośrednio.

Kluczowa różnica między bazami:
  - MySQL / MySQL-Norm / CouchDB / SQLite:
      dane pre-seedowane przez seed_all.py, trwałe na dysku/wolumenie Docker
      → benchmark łączy się i od razu testuje

  - Redis:
      dane w RAM → permanentnie zajmują pamięć jeśli pre-seedowane
      → strategia: FLUSH → załaduj z cache → benchmark → FLUSH po zakończeniu
      → Redis zajmuje RAM tylko podczas własnego benchmarku (max 1 rozmiar naraz)
"""
import argparse
import csv
import os
import pickle
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

sys.path.insert(0, os.path.dirname(__file__))

from config import DATA_SIZES, SIZE_DB_CONFIGS
from scenarios.runner import run_benchmarks, SCENARIO_DESCRIPTIONS, ScenarioResult

try:
    from measure_volumes import update_seeding_row as _csv_update_row
    _HAS_CSV = True
except ImportError:
    _HAS_CSV = False
    def _csv_update_row(*args, **kwargs): pass

console = Console()

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
CACHE_DIR = os.path.join(os.path.dirname(__file__), "data", "cache")
os.makedirs(RESULTS_DIR, exist_ok=True)

ALL_DB_KEYS = ["sqlite", "mysql", "mysql_norm", "couchdb", "redis"]
# Redis zawsze OSTATNI – żeby inne bazy testowały bez presji RAM od Redisa
DB_KEYS_ORDERED = ["sqlite", "mysql", "mysql_norm", "couchdb", "redis"]

DB_LABELS = {
    "sqlite":     "SQLite",
    "mysql":      "MySQL",
    "mysql_norm": "MySQL-Norm",
    "couchdb":    "CouchDB",
    "redis":      "Redis",
}


# ────────────────────────────────────────────────────────────────
# Tworzenie instancji bazy
# ────────────────────────────────────────────────────────────────

def _make_db(db_key: str, size_name: str):
    from db import MySQLDB, MySQLNormalizedDB, SQLiteDB, CouchDBDB, RedisDB
    cfg = SIZE_DB_CONFIGS[size_name][db_key]
    factories = {
        "sqlite":     lambda: SQLiteDB(cfg),
        "mysql":      lambda: MySQLDB(cfg),
        "mysql_norm": lambda: MySQLNormalizedDB(cfg),
        "couchdb":    lambda: CouchDBDB(cfg),
        "redis":      lambda: RedisDB(cfg),
    }
    return factories[db_key]()


# ────────────────────────────────────────────────────────────────
# Redis – specjalna obsługa RAM
# ────────────────────────────────────────────────────────────────

def _load_cache(size_name: str) -> Optional[dict]:
    """Ładuje dataset z pliku cache .pkl"""
    path = os.path.join(CACHE_DIR, f"dataset_{size_name}.pkl")
    if not os.path.exists(path):
        return None
    console.print(f"    Ładowanie cache Redis [{size_name}]...", end="")
    t0 = time.perf_counter()
    with open(path, "rb") as f:
        data = pickle.load(f)
    console.print(f" [{time.perf_counter() - t0:.1f}s]")
    return data


def _redis_prepare(size_name: str) -> bool:
    """
    Czyści Redis db= dla tego rozmiaru, ładuje dane z cache.
    Zwraca True jeśli sukces.
    Mierzy czas seedowania i przyrost pamięci RAM, zapisuje do data_seeding.csv.
    """
    db = _make_db("redis", size_name)
    if not db.connect():
        console.print("    [red]Redis: brak połączenia[/red]")
        return False

    try:
        # Wyczyść stare dane
        console.print("    Redis: FLUSHDB (czyszczenie starego datasetu)...", end="")
        db.clear_data()
        console.print(" [green]✓[/green]")

        # Załaduj z cache
        data = _load_cache(size_name)
        if data is None:
            console.print(
                f"    [red]Redis: brak cache {size_name} – uruchom seed_all.py[/red]"
            )
            return False

        total = sum(len(v) for v in data.values())

        # Pomiar pamięci przed seedowaniem
        mem_before = None
        try:
            import redis as _redis_lib
            cfg = SIZE_DB_CONFIGS[size_name]["redis"]
            _r = _redis_lib.Redis(host=cfg.host, port=cfg.port,
                                  password=cfg.password, db=cfg.db,
                                  socket_connect_timeout=3)
            mem_before = _r.info("memory")["used_memory"]
        except Exception:
            pass

        with Progress(SpinnerColumn(), TextColumn("      {task.description}"),
                      BarColumn(), TimeElapsedColumn(), console=console) as pbar:
            task = pbar.add_task("Redis seed", total=total)

            def cb(table, count, tot):
                pbar.update(task, description=f"      Redis › {table}")
                pbar.advance(task, count)

            t0 = time.perf_counter()
            db.seed(data, progress_callback=cb)
            elapsed = time.perf_counter() - t0

        console.print(f"    [green]✓ Redis załadowany ({elapsed:.1f}s)[/green]")

        # Pomiar pamięci po seedowaniu i zapis do CSV
        mem_delta = None
        try:
            mem_after = _r.info("memory")["used_memory"]
            if mem_before is not None:
                mem_delta = mem_after - mem_before
        except Exception:
            pass

        from datetime import datetime as _dt
        _csv_update_row(
            size_name, "redis",
            seeding_time=elapsed,
            seeding_amount=total,
            volume_bytes=mem_delta if mem_delta and mem_delta > 0 else None,
            timestamp=_dt.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        return True

    except Exception as e:
        console.print(f"    [red]Redis prepare error: {e}[/red]")
        return False
    finally:
        db.disconnect()


def _redis_flush(size_name: str) -> None:
    """Zwalnia RAM po benchmarku Redis – czyści db=."""
    db = _make_db("redis", size_name)
    if db.connect():
        try:
            db.clear_data()
            console.print(f"    [dim]Redis db={SIZE_DB_CONFIGS[size_name]['redis'].db}"
                          f" wyczyszczony – RAM zwolniony[/dim]")
        except Exception:
            pass
        finally:
            db.disconnect()


# ────────────────────────────────────────────────────────────────
# Eksport CSV
# ────────────────────────────────────────────────────────────────

def _export_csv(all_results: Dict[str, List[ScenarioResult]],
                size_name: str, with_indexes: bool) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    idx_tag = "idx" if with_indexes else "noidx"
    filename = os.path.join(RESULTS_DIR, f"results_{size_name}_{idx_tag}_{ts}.csv")

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["scenario_id", "description", "operacja"]
        for db_label in all_results:
            header.extend([f"{db_label}_avg_s", f"{db_label}_min_s", f"{db_label}_max_s"])
        writer.writerow(header)

        first = list(all_results.values())[0]
        for sr in first:
            row = [sr.scenario_id, sr.description, sr.scenario_id[0]]
            for db_label in all_results:
                matching = [r for r in all_results[db_label]
                            if r.scenario_id == sr.scenario_id]
                if matching:
                    m = matching[0]
                    row.extend([
                        f"{m.avg:.6f}" if m.avg >= 0 else "BŁĄD",
                        f"{m.min_t:.6f}" if m.min_t >= 0 else "BŁĄD",
                        f"{m.max_t:.6f}" if m.max_t >= 0 else "BŁĄD",
                    ])
                else:
                    row.extend(["-", "-", "-"])
            writer.writerow(row)

    return filename


# ────────────────────────────────────────────────────────────────
# Wyświetlanie wyników
# ────────────────────────────────────────────────────────────────

def _print_results(all_results: Dict[str, List[ScenarioResult]], title: str):
    table = Table(title=title, show_lines=True, expand=True)
    table.add_column("ID", style="bold cyan", width=5)
    table.add_column("Opis", width=46)
    for db_label in all_results:
        table.add_column(db_label, justify="right", width=14)

    op_colors = {"C": "green", "R": "blue", "U": "yellow", "D": "red"}
    first = list(all_results.values())[0]

    for sr in first:
        color = op_colors.get(sr.scenario_id[0], "white")
        row = [f"[{color}]{sr.scenario_id}[/{color}]", sr.description]
        for db_label in all_results:
            matching = [r for r in all_results[db_label]
                        if r.scenario_id == sr.scenario_id]
            if matching and matching[0].avg >= 0:
                row.append(f"{matching[0].avg:.4f}s")
            else:
                row.append("[red]BŁĄD[/red]")
        table.add_row(*row)

    console.print(table)


def _print_speedup(no_idx: Dict, with_idx: Dict):
    table = Table(
        title="Speedup indeksów (>1.0 = indeks przyspiesza)",
        show_lines=True, expand=True)
    table.add_column("ID", style="bold", width=5)
    for db_label in no_idx:
        table.add_column(db_label, justify="right", width=14)

    first = list(no_idx.values())[0]
    for sr in first:
        row = [sr.scenario_id]
        for db_label in no_idx:
            r_no = [r for r in no_idx[db_label] if r.scenario_id == sr.scenario_id]
            r_yes = [r for r in with_idx.get(db_label, []) if r.scenario_id == sr.scenario_id]
            if r_no and r_yes and r_no[0].avg > 0 and r_yes[0].avg > 0:
                speedup = r_no[0].avg / r_yes[0].avg
                color = "green" if speedup >= 1.0 else "red"
                row.append(f"[{color}]{speedup:.2f}x[/{color}]")
            else:
                row.append("-")
        table.add_row(*row)

    console.print(table)


# ────────────────────────────────────────────────────────────────
# Benchmark dla jednej bazy (nie-Redis)
# ────────────────────────────────────────────────────────────────

def _bench_one_db(db_key: str, size_name: str,
                  repetitions: int, test_indexes: bool):
    """
    Uruchamia benchmarki na jednej bazie.
    Zwraca (results_no_idx, results_with_idx) lub (None, None) przy błędzie.
    """
    label = DB_LABELS[db_key]
    console.print(f"\n  [cyan]● {label} [{size_name}][/cyan]")

    db = _make_db(db_key, size_name)
    if not db.connect():
        console.print(f"    [red]Brak połączenia z {label}. Pomijam.[/red]")
        return None, None

    try:
        results_no = None
        results_idx = None

        # ── Bez indeksów ──
        if test_indexes:
            db.drop_indexes()
        console.print(f"    BEZ indeksów...", end="")
        t0 = time.perf_counter()
        results_no = run_benchmarks(db, repetitions=repetitions)
        console.print(f" [{time.perf_counter() - t0:.1f}s] [green]✓[/green]")

        # ── Z indeksami ──
        if test_indexes:
            db.create_indexes()
            console.print(f"    Z indeksami...", end="")
            t0 = time.perf_counter()
            results_idx = run_benchmarks(db, repetitions=repetitions)
            console.print(f" [{time.perf_counter() - t0:.1f}s] [green]✓[/green]")

        return results_no, results_idx

    except Exception as e:
        console.print(f"    [red]Błąd: {e}[/red]")
        return None, None
    finally:
        db.disconnect()


# ────────────────────────────────────────────────────────────────
# Benchmark Redis (specjalna obsługa: load → bench → flush)
# ────────────────────────────────────────────────────────────────

def _bench_redis(size_name: str, repetitions: int, test_indexes: bool,
                 flush_after: bool = True):
    """
    Sekwencja Redis:
      1. FLUSHDB  – zwolnij pamięć po poprzednim rozmiarze
      2. Załaduj dane z cache  (jedyny raz gdy Redis coś zajmuje)
      3. Benchmark bez indeksów
      4. Benchmark z indeksami
      5. FLUSHDB  – zwolnij RAM (opcjonalnie)
    """
    console.print(f"\n  [cyan]● Redis [{size_name}] (load-on-demand)[/cyan]")
    console.print(f"  [dim]Redis ładuje dane tuż przed benchmarkiem i zwalnia RAM po nim[/dim]")

    # Przygotuj dane w Redis
    ok = _redis_prepare(size_name)
    if not ok:
        return None, None

    db = _make_db("redis", size_name)
    if not db.connect():
        return None, None

    try:
        results_no = None
        results_idx = None

        # Redis nie wspiera indeksów w tradycyjnym sensie, ale zachowujemy
        # interfejs dla spójności (drop/create to no-op lub indeksy pomocnicze)
        if test_indexes:
            db.drop_indexes()
        console.print(f"    BEZ indeksów...", end="")
        t0 = time.perf_counter()
        results_no = run_benchmarks(db, repetitions=repetitions)
        console.print(f" [{time.perf_counter() - t0:.1f}s] [green]✓[/green]")

        if test_indexes:
            db.create_indexes()
            console.print(f"    Z indeksami...", end="")
            t0 = time.perf_counter()
            results_idx = run_benchmarks(db, repetitions=repetitions)
            console.print(f" [{time.perf_counter() - t0:.1f}s] [green]✓[/green]")

        return results_no, results_idx

    except Exception as e:
        console.print(f"    [red]Błąd Redis: {e}[/red]")
        return None, None
    finally:
        db.disconnect()
        if flush_after:
            _redis_flush(size_name)


# ────────────────────────────────────────────────────────────────
# Główna funkcja (wywoływana przez run_small/medium/large.py)
# ────────────────────────────────────────────────────────────────

def run_size_benchmark(size_name: str) -> None:

    # ── Parsuj argumenty CLI ──
    parser = argparse.ArgumentParser(
        description=f"Benchmark [{size_name.upper()}] – 5 baz, 24 scenariusze × 3 powtórzenia")
    parser.add_argument("--dbs", nargs="+", choices=ALL_DB_KEYS,
                        default=ALL_DB_KEYS, help="Bazy do testowania")
    parser.add_argument("--reps", type=int, default=3,
                        help="Powtórzenia na scenariusz (domyślnie 3)")
    parser.add_argument("--no-index", action="store_true",
                        help="Pomiń analizę z/bez indeksów")
    parser.add_argument("--keep-redis", action="store_true",
                        help="NIE czyść Redis po benchmarku (zostaw dane w RAM)")
    args = parser.parse_args()

    db_keys = args.dbs
    repetitions = args.reps
    test_indexes = not args.no_index
    flush_redis_after = not args.keep_redis

    size_cfg = DATA_SIZES[size_name]

    # Sortuj: Redis zawsze ostatni (żeby nie blokował RAM innym bazom)
    non_redis = [k for k in DB_KEYS_ORDERED if k in db_keys and k != "redis"]
    has_redis = "redis" in db_keys
    ordered_keys = non_redis + (["redis"] if has_redis else [])

    console.print(Panel.fit(
        f"[bold magenta]Benchmark [{size_name.upper()}][/bold magenta]\n"
        f"{size_cfg['label']}\n"
        f"Kolejność baz: {' → '.join(DB_LABELS[k] for k in ordered_keys)}\n"
        f"Powtórzenia: {repetitions} | Indeksy: {'TAK' if test_indexes else 'NIE'}\n"
        + ("[yellow]Redis: ładowany tuż przed benchmarkiem, zwalniany po[/yellow]"
           if has_redis else "[dim]Redis: pominięty[/dim]"),
        title=f"run_{size_name}.py",
    ))

    console.print(
        "\n[dim]Wymaganie: seed_all.py --dbs sqlite mysql mysql_norm couchdb"
        " już uruchomiony.[/dim]"
    )
    if has_redis:
        console.print(f"[dim]Redis: dane załadowane z data/cache/dataset_{size_name}.pkl[/dim]\n")

    t_start = time.perf_counter()
    all_no_idx: Dict[str, List[ScenarioResult]] = {}
    all_with_idx: Dict[str, List[ScenarioResult]] = {}

    # ── Bazy dyskowe (SQLite, MySQL, CouchDB) ──
    for db_key in non_redis:
        res_no, res_idx = _bench_one_db(db_key, size_name, repetitions, test_indexes)
        if res_no is not None:
            all_no_idx[DB_LABELS[db_key]] = res_no
        if res_idx is not None:
            all_with_idx[DB_LABELS[db_key]] = res_idx

    # ── Redis (load-on-demand) ──
    if has_redis:
        console.print(f"\n  [bold yellow]▶ Redis – ładowanie danych do RAM[/bold yellow]")
        res_no, res_idx = _bench_redis(size_name, repetitions, test_indexes,
                                        flush_after=flush_redis_after)
        if res_no is not None:
            all_no_idx[DB_LABELS["redis"]] = res_no
        if res_idx is not None:
            all_with_idx[DB_LABELS["redis"]] = res_idx

    # ── Wyniki ──
    console.print()

    if all_no_idx:
        _print_results(all_no_idx,
                       f"[{size_name.upper()}] BEZ indeksów – {size_cfg['label']}")
        path = _export_csv(all_no_idx, size_name, False)
        console.print(f"\n[green]→ CSV: {path}[/green]")

    if all_with_idx:
        console.print()
        _print_results(all_with_idx,
                       f"[{size_name.upper()}] Z indeksami – {size_cfg['label']}")
        path = _export_csv(all_with_idx, size_name, True)
        console.print(f"\n[green]→ CSV: {path}[/green]")

    if all_no_idx and all_with_idx:
        console.print()
        _print_speedup(all_no_idx, all_with_idx)

    elapsed = time.perf_counter() - t_start
    console.print(Panel.fit(
        f"[bold green]✓ Benchmark [{size_name.upper()}] zakończony[/bold green]\n"
        f"Czas: {elapsed:.1f}s | Wyniki: {RESULTS_DIR}/",
        style="green",
    ))
