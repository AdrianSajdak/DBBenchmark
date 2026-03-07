"""
DB Benchmark – Sklep Monopolowy
Konsola do porównywania wydajności MySQL, SQLite, CouchDB i Redis.

Uruchomienie:
    pip install -r requirements.txt
    python main.py
"""
import csv
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

# Sprawdź dostępność biblioteki rich
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
    from rich.prompt import Prompt, Confirm
    from rich.text import Text
    from rich import box
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

import config as cfg
from benchmark_runner import SCENARIOS, ScenarioResult, run_benchmarks
from data_generator import count_dataset, generate_dataset
from db import MySQLDB, SQLiteDB, CouchDBDB, RedisDB
from db.base_db import BaseDB

console = Console() if HAS_RICH else None

# ================================================================
# Colory / drukowanie
# ================================================================

def cprint(text: str, style: str = "") -> None:
    if HAS_RICH:
        console.print(text, style=style)
    else:
        print(text)


def rule(title: str = "") -> None:
    if HAS_RICH:
        console.rule(title)
    else:
        print(f"\n{'─' * 60}  {title}")


def header() -> None:
    if HAS_RICH:
        console.print(Panel.fit(
            "[bold cyan]DB BENCHMARK[/bold cyan]  –  Sklep Monopolowy\n"
            "[dim]MySQL · SQLite · CouchDB · Redis[/dim]",
            border_style="cyan",
        ))
    else:
        print("=" * 60)
        print("  DB BENCHMARK – Sklep Monopolowy")
        print("  MySQL / SQLite / CouchDB / Redis")
        print("=" * 60)


# ================================================================
# Tworzenie instancji baz danych
# ================================================================

DB_LIST = [
    ("MySQL",   lambda: MySQLDB(cfg.mysql_cfg)),
    ("SQLite",  lambda: SQLiteDB(cfg.sqlite_cfg)),
    ("CouchDB", lambda: CouchDBDB(cfg.couchdb_cfg)),
    ("Redis",   lambda: RedisDB(cfg.redis_cfg)),
]


def select_databases() -> List[BaseDB]:
    rule("Wybór baz danych")
    for i, (name, _) in enumerate(DB_LIST, 1):
        cprint(f"  [bold]{i}[/bold]. {name}" if HAS_RICH else f"  {i}. {name}")
    cprint("")
    raw = input("Które bazy testować? (np. 1,2,3,4  |  Enter = wszystkie): ").strip()
    if not raw:
        indices = list(range(len(DB_LIST)))
    else:
        try:
            indices = [int(x.strip()) - 1 for x in raw.split(",")]
        except ValueError:
            cprint("[red]Nieprawidłowy wybór. Wybieram wszystkie.[/red]" if HAS_RICH else "Nieprawidłowy wybór.")
            indices = list(range(len(DB_LIST)))

    selected = []
    for idx in indices:
        if 0 <= idx < len(DB_LIST):
            name, factory = DB_LIST[idx]
            db = factory()
            selected.append(db)
    return selected


# ================================================================
# Wybór rozmiaru danych
# ================================================================

def select_data_size() -> tuple:
    rule("Rozmiar zbioru danych")
    keys = list(cfg.DATA_SIZES.keys())
    for i, k in enumerate(keys, 1):
        info = cfg.DATA_SIZES[k]
        est = count_dataset(info)
        cprint(f"  [bold]{i}[/bold]. {info['label']}  (~{est:,} łącznie)"
               if HAS_RICH else f"  {i}. {info['label']}  (~{est:,} łącznie)")
    cprint("")
    raw = input("Wybierz rozmiar (1/2/3  |  Enter = mały): ").strip()
    try:
        idx = int(raw) - 1 if raw else 0
        key = keys[max(0, min(idx, len(keys) - 1))]
    except ValueError:
        key = "small"
    return key, cfg.DATA_SIZES[key]


# ================================================================
# Wybór scenariuszy
# ================================================================

def select_scenarios() -> List[str]:
    rule("Wybór scenariuszy")
    groups = {
        "C": "CREATE (C1–C6)",
        "R": "READ   (R1–R6)",
        "U": "UPDATE (U1–U6)",
        "D": "DELETE (D1–D6)",
    }
    for g, desc in groups.items():
        ids = [sid for sid in SCENARIOS if sid.startswith(g)]
        cprint(f"  [cyan]{g}[/cyan]: {desc}  →  {', '.join(ids)}"
               if HAS_RICH else f"  {g}: {desc}  →  {', '.join(ids)}")
    cprint("")
    raw = input(
        "Podaj ID scenariuszy (np. C1,R1,R2  |  Enter = wszystkie  |  C/R/U/D = grupa): "
    ).strip().upper()

    if not raw:
        return list(SCENARIOS.keys())

    selected = []
    for token in raw.split(","):
        token = token.strip()
        if token in ("C", "R", "U", "D"):
            selected.extend(sid for sid in SCENARIOS if sid.startswith(token))
        elif token in SCENARIOS:
            selected.append(token)
        else:
            cprint(f"  [yellow]Nieznany scenariusz: {token}[/yellow]" if HAS_RICH
                   else f"  Nieznany scenariusz: {token}")
    return selected if selected else list(SCENARIOS.keys())


# ================================================================
# Generowanie i załadowanie danych
# ================================================================

def seed_database(db: BaseDB, data: Dict, verbose: bool = True) -> bool:
    if verbose:
        cprint(f"\n[bold]Załaduj dane do {db.name}…[/bold]" if HAS_RICH
               else f"\nZaładuj dane do {db.name}…")

    if HAS_RICH and verbose:
        tables = ["stores", "categories", "suppliers", "employees",
                  "customers", "products", "orders", "order_items",
                  "inventory", "payments"]
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f"  {db.name} – seed", total=len(tables))

            def cb(t, done, tot):
                progress.update(task, advance=1,
                                description=f"  [{db.name}] {t}")

            db.seed(data, progress_callback=cb)
    else:
        def cb(t, done, tot):
            if verbose:
                print(f"  [{db.name}] tabela: {t} ({done:,} rekordów)")
        db.seed(data, progress_callback=cb)

    return True


# ================================================================
# Wyświetlanie wyników
# ================================================================

def _fmt_time(ms: Optional[float]) -> str:
    if ms is None:
        return "—"
    if ms >= 1000:
        return f"{ms / 1000:.3f} s"
    return f"{ms:.2f} ms"


def print_results(results: List[ScenarioResult]) -> None:
    # Zgrupuj po db_name→scenario_id
    from collections import defaultdict
    by_db: Dict[str, Dict[str, ScenarioResult]] = defaultdict(dict)
    for r in results:
        by_db[r.db_name][r.scenario_id] = r

    db_names = list(by_db.keys())

    if HAS_RICH:
        t = Table(title="Wyniki benchmarku (średni czas)", box=box.ROUNDED,
                  show_header=True, header_style="bold magenta")
        t.add_column("ID",          style="cyan",  width=5)
        t.add_column("Scenariusz",  style="white", width=35)
        for db_name in db_names:
            t.add_column(db_name, justify="right", width=14)

        prev_group = None
        for sid, desc in SCENARIOS.items():
            group = sid[0]
            if group != prev_group and prev_group is not None:
                t.add_section()
            prev_group = group

            row_cells = [sid, desc]
            for db_name in db_names:
                r = by_db[db_name].get(sid)
                if r is None:
                    row_cells.append("[dim]—[/dim]")
                elif r.error:
                    row_cells.append(f"[red]ERR[/red]")
                else:
                    row_cells.append(f"[green]{_fmt_time(r.avg_ms)}[/green]")
            t.add_row(*row_cells)

        console.print(t)
    else:
        # Tabela tekstowa
        header_row = f"{'ID':<5} {'Scenariusz':<35}" + "".join(f" {n:>14}" for n in db_names)
        print("\n" + "─" * len(header_row))
        print(header_row)
        print("─" * len(header_row))
        for sid, desc in SCENARIOS.items():
            row = f"{sid:<5} {desc:<35}"
            for db_name in db_names:
                r = by_db[db_name].get(sid)
                if r is None:
                    cell = "—"
                elif r.error:
                    cell = "ERR"
                else:
                    cell = _fmt_time(r.avg_ms)
                row += f" {cell:>14}"
            print(row)
        print("─" * len(header_row))


def print_detail_results(results: List[ScenarioResult]) -> None:
    """Szczegółowe wyniki z min/avg/max dla każdej bazy."""
    if HAS_RICH:
        for db_name in sorted({r.db_name for r in results}):
            db_results = [r for r in results if r.db_name == db_name]
            t = Table(title=f"Szczegóły – {db_name}", box=box.SIMPLE,
                      show_header=True, header_style="bold blue")
            t.add_column("ID",            style="cyan",    width=5)
            t.add_column("Opis",          style="white",   width=38)
            t.add_column("Min",           justify="right", width=12)
            t.add_column("Śr.",           justify="right", width=12)
            t.add_column("Max",           justify="right", width=12)
            t.add_column("Próby",         justify="right", width=6)

            for r in sorted(db_results, key=lambda x: list(SCENARIOS.keys()).index(x.scenario_id)
                            if x.scenario_id in SCENARIOS else 999):
                if r.error:
                    t.add_row(r.scenario_id, r.description,
                              "[red]ERR[/red]", "[red]ERR[/red]", "[red]ERR[/red]",
                              str(len(r.times_ms)))
                else:
                    t.add_row(r.scenario_id, r.description,
                              _fmt_time(r.min_ms),
                              _fmt_time(r.avg_ms),
                              _fmt_time(r.max_ms),
                              str(len(r.times_ms)))
            console.print(t)
    else:
        for db_name in sorted({r.db_name for r in results}):
            print(f"\n=== {db_name} ===")
            print(f"{'ID':<5} {'Opis':<38} {'Min':>12} {'Śr.':>12} {'Max':>12} {'Próby':>6}")
            print("─" * 75)
            for r in [x for x in results if x.db_name == db_name]:
                if r.error:
                    print(f"{r.scenario_id:<5} {r.description:<38} {'ERR':>12}")
                else:
                    print(f"{r.scenario_id:<5} {r.description:<38} "
                          f"{_fmt_time(r.min_ms):>12} {_fmt_time(r.avg_ms):>12} "
                          f"{_fmt_time(r.max_ms):>12} {len(r.times_ms):>6}")


# ================================================================
# Eksport wyników do CSV
# ================================================================

def export_csv(results: List[ScenarioResult], size_key: str) -> str:
    os.makedirs("results", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join("results", f"benchmark_{size_key}_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["db", "scenario_id", "description",
                         "min_ms", "avg_ms", "max_ms", "repetitions", "error"])
        for r in results:
            writer.writerow([
                r.db_name, r.scenario_id, r.description,
                f"{r.min_ms:.4f}" if r.min_ms else "",
                f"{r.avg_ms:.4f}" if r.avg_ms else "",
                f"{r.max_ms:.4f}" if r.max_ms else "",
                len(r.times_ms),
                r.error or "",
            ])
    return path


# ================================================================
# Konfiguracja połączeń
# ================================================================

def menu_configure() -> None:
    rule("Konfiguracja połączeń")
    cprint("Aktualna konfiguracja:\n")
    cprint(f"  MySQL  : {cfg.mysql_cfg.host}:{cfg.mysql_cfg.port}  "
           f"user={cfg.mysql_cfg.user}  db={cfg.mysql_cfg.database}")
    cprint(f"  SQLite : {cfg.sqlite_cfg.database}")
    cprint(f"  CouchDB: {cfg.couchdb_cfg.url}  user={cfg.couchdb_cfg.username}  "
           f"db={cfg.couchdb_cfg.database}")
    cprint(f"  Redis  : {cfg.redis_cfg.host}:{cfg.redis_cfg.port}  "
           f"db={cfg.redis_cfg.db}")
    cprint("")

    def ask(prompt: str, current) -> str:
        val = input(f"{prompt} [{current}]: ").strip()
        return val if val else str(current)

    cprint("[bold]MySQL[/bold]" if HAS_RICH else "MySQL:")
    cfg.mysql_cfg.host     = ask("  Host",     cfg.mysql_cfg.host)
    cfg.mysql_cfg.port     = int(ask("  Port", cfg.mysql_cfg.port))
    cfg.mysql_cfg.user     = ask("  User",     cfg.mysql_cfg.user)
    cfg.mysql_cfg.password = ask("  Password", cfg.mysql_cfg.password)
    cfg.mysql_cfg.database = ask("  Database", cfg.mysql_cfg.database)

    cprint("\n[bold]SQLite[/bold]" if HAS_RICH else "\nSQLite:")
    cfg.sqlite_cfg.database = ask("  Plik DB", cfg.sqlite_cfg.database)

    cprint("\n[bold]CouchDB[/bold]" if HAS_RICH else "\nCouchDB:")
    cfg.couchdb_cfg.url      = ask("  URL",      cfg.couchdb_cfg.url)
    cfg.couchdb_cfg.username = ask("  Username", cfg.couchdb_cfg.username)
    cfg.couchdb_cfg.password = ask("  Password", cfg.couchdb_cfg.password)
    cfg.couchdb_cfg.database = ask("  Database", cfg.couchdb_cfg.database)

    cprint("\n[bold]Redis[/bold]" if HAS_RICH else "\nRedis:")
    cfg.redis_cfg.host     = ask("  Host",     cfg.redis_cfg.host)
    cfg.redis_cfg.port     = int(ask("  Port", cfg.redis_cfg.port))
    cfg.redis_cfg.db       = int(ask("  DB",   cfg.redis_cfg.db))
    pwd = ask("  Password (puste = brak)", cfg.redis_cfg.password or "")
    cfg.redis_cfg.password = pwd if pwd and pwd != "None" else None

    cprint("\n[green]✓ Konfiguracja zapisana.[/green]" if HAS_RICH
           else "\nKonfiguracja zapisana.")


# ================================================================
# Test połączenia
# ================================================================

def menu_test_connections() -> None:
    rule("Test połączeń")
    dbs = [
        ("MySQL",   MySQLDB(cfg.mysql_cfg)),
        ("SQLite",  SQLiteDB(cfg.sqlite_cfg)),
        ("CouchDB", CouchDBDB(cfg.couchdb_cfg)),
        ("Redis",   RedisDB(cfg.redis_cfg)),
    ]
    for name, db in dbs:
        ok = db.connect()
        status = ("[green]✓ OK[/green]" if ok else "[red]✗ BŁĄD[/red]") if HAS_RICH \
                 else ("✓ OK" if ok else "✗ BŁĄD")
        cprint(f"  {name:10s}: {status}")
        db.disconnect()


# ================================================================
# Wyczyść dane w bazie
# ================================================================

def menu_clear() -> None:
    rule("Wyczyszczenie danych")
    dbs = select_databases()
    if HAS_RICH:
        confirm = Confirm.ask("[yellow]Czy na pewno chcesz usunąć WSZYSTKIE dane?[/yellow]")
    else:
        confirm = input("Czy na pewno? (t/N): ").strip().lower() == "t"

    if not confirm:
        cprint("Anulowano.")
        return

    for db in dbs:
        ok = db.connect()
        if not ok:
            cprint(f"[red]Błąd połączenia: {db.name}[/red]" if HAS_RICH
                   else f"Błąd połączenia: {db.name}")
            continue
        try:
            db.clear_data()
            cprint(f"  [green]✓ {db.name} – dane usunięte.[/green]" if HAS_RICH
                   else f"  ✓ {db.name} – dane usunięte.")
        except Exception as e:
            cprint(f"  [red]✗ {db.name} – błąd: {e}[/red]" if HAS_RICH
                   else f"  ✗ {db.name} – błąd: {e}")
        finally:
            db.disconnect()


# ================================================================
# Główny przepływ benchmarku
# ================================================================

def menu_run_benchmark() -> None:
    rule("Benchmark CRUD")

    # --- Wybory użytkownika ---
    selected_dbs    = select_databases()
    size_key, size_cfg = select_data_size()
    selected_scens  = select_scenarios()

    cprint(f"\nGotowe do uruchomienia: {len(selected_dbs)} baz, "
           f"{len(selected_scens)} scenariuszy, rozmiar: {size_cfg['label']}\n")

    # --- Generowanie danych ---
    rule("Generowanie danych")
    total_est = count_dataset(size_cfg)
    cprint(f"Generuję ~{total_est:,} rekordów…")

    gen_start = time.time()
    if HAS_RICH:
        with console.status("[bold green]Generowanie danych…"):
            data = generate_dataset(size_cfg)
    else:
        data = generate_dataset(size_cfg)

    cprint(f"Wygenerowano w {time.time() - gen_start:.1f}s.\n")

    all_results: List[ScenarioResult] = []

    for db in selected_dbs:
        cprint(f"\n[bold cyan]=== {db.name} ===[/bold cyan]\n" if HAS_RICH
               else f"\n=== {db.name} ===\n")

        # Połączenie
        ok = db.connect()
        if not ok:
            cprint(f"[red]✗ Pominięto – brak połączenia.[/red]" if HAS_RICH
                   else f"✗ Pominięto – brak połączenia.")
            continue

        # Schemat + seed
        try:
            db.setup_schema()
            db.clear_data()
            seed_database(db, data, verbose=True)
        except Exception as e:
            cprint(f"[red]✗ Błąd przygotowania bazy: {e}[/red]" if HAS_RICH
                   else f"✗ Błąd przygotowania bazy: {e}")
            db.disconnect()
            continue

        # Benchmark
        cprint(f"\nUruchamiam testy dla {db.name}…")

        def pb(sid, desc):
            cprint(f"  [cyan]{sid}[/cyan] {desc}" if HAS_RICH else f"  {sid} {desc}")

        try:
            results = run_benchmarks(db, data, selected=selected_scens, progress_cb=pb)
            all_results.extend(results)
        except Exception as e:
            cprint(f"[red]Błąd podczas benchmark: {e}[/red]" if HAS_RICH
                   else f"Błąd podczas benchmark: {e}")
        finally:
            db.disconnect()

    # --- Wyniki ---
    if not all_results:
        cprint("[yellow]Brak wyników.[/yellow]" if HAS_RICH else "Brak wyników.")
        return

    rule("Wyniki")
    print_results(all_results)

    # Szczegóły?
    show_det = input("\nCzy pokazać szczegółowe wyniki (min/avg/max)? (t/N): ").strip().lower()
    if show_det == "t":
        print_detail_results(all_results)

    # Eksport CSV?
    save = input("\nZapisać wyniki do pliku CSV? (t/N): ").strip().lower()
    if save == "t":
        path = export_csv(all_results, size_key)
        cprint(f"[green]✓ Wyniki zapisane: {path}[/green]" if HAS_RICH
               else f"✓ Wyniki zapisane: {path}")


# ================================================================
# Wyświetl zapisane wyniki
# ================================================================

def menu_show_saved() -> None:
    rule("Zapisane wyniki")
    os.makedirs("results", exist_ok=True)
    files = sorted([f for f in os.listdir("results") if f.endswith(".csv")])
    if not files:
        cprint("Brak zapisanych wyników.")
        return
    for i, f in enumerate(files, 1):
        cprint(f"  {i}. {f}")
    raw = input("\nWybierz plik (numer lub Enter = ostatni): ").strip()
    try:
        idx = int(raw) - 1 if raw else len(files) - 1
        fname = files[idx]
    except (ValueError, IndexError):
        cprint("Nieprawidłowy wybór.")
        return

    path = os.path.join("results", fname)
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if HAS_RICH:
        db_names = sorted({r["db"] for r in rows})
        t = Table(title=f"Wyniki z pliku: {fname}", box=box.ROUNDED,
                  header_style="bold magenta")
        t.add_column("ID",          style="cyan",  width=5)
        t.add_column("Scenariusz",  style="white", width=35)
        for db_name in db_names:
            t.add_column(db_name, justify="right", width=14)

        scenario_ids = list(dict.fromkeys(r["scenario_id"] for r in rows))
        for sid in scenario_ids:
            cells = [sid, SCENARIOS.get(sid, "")]
            for db_name in db_names:
                match = next((r for r in rows
                              if r["db"] == db_name and r["scenario_id"] == sid), None)
                if match:
                    cells.append(f"[green]{_fmt_time(float(match['avg_ms']))}"
                                 f"[/green]" if match["avg_ms"] else "[dim]—[/dim]")
                else:
                    cells.append("[dim]—[/dim]")
            t.add_row(*cells)
        console.print(t)
    else:
        print(f"\nPlik: {fname}")
        for r in rows:
            avg = f"{float(r['avg_ms']):.2f} ms" if r.get("avg_ms") else "—"
            print(f"  {r['db']:10} {r['scenario_id']:5} {r.get('description', ''):35} {avg}")


# ================================================================
# Menu główne
# ================================================================

MENU_ITEMS = [
    ("1", "Uruchom benchmark CRUD",          menu_run_benchmark),
    ("2", "Konfiguracja połączeń",            menu_configure),
    ("3", "Test połączeń z bazami",           menu_test_connections),
    ("4", "Wyczyść dane w bazach",            menu_clear),
    ("5", "Przeglądaj zapisane wyniki (CSV)", menu_show_saved),
    ("0", "Wyjdź",                            None),
]


def main_menu() -> None:
    while True:
        header()
        cprint("")
        for key, label, _ in MENU_ITEMS:
            cprint(f"  [bold]{key}[/bold]. {label}" if HAS_RICH else f"  {key}. {label}")
        cprint("")
        choice = input("Wybierz opcję: ").strip()
        for key, _, fn in MENU_ITEMS:
            if choice == key:
                if fn is None:
                    cprint("\nDo widzenia!")
                    sys.exit(0)
                print()
                fn()
                print()
                break
        else:
            cprint("[yellow]Nieznana opcja.[/yellow]" if HAS_RICH else "Nieznana opcja.")


if __name__ == "__main__":
    main_menu()
