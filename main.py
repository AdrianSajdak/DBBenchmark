"""
Główny interfejs CLI benchmarku bazodanowego.
Model danych TABELE.txt · 4 DBMS · 24 scenariuszy CRUD · 3 rozmiary danych.
"""
import csv
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.table import Table
from rich.prompt import Prompt, IntPrompt, Confirm

from config import (
    DATA_SIZES, SEED_ORDER,
    mysql_cfg, mysql_normalized_cfg, sqlite_cfg, couchdb_cfg, redis_cfg,
)
from data.generator import generate_dataset, count_dataset
from scenarios.runner import run_benchmarks, SCENARIO_DESCRIPTIONS, ScenarioResult

console = Console()


# ────────────────────────────────────────────────────────────────
# Inicjalizacja baz
# ────────────────────────────────────────────────────────────────

def _get_db_instances() -> Dict:
    """Tworzy słownik {nazwa: instancja} wszystkich 5 baz."""
    from db import MySQLDB, MySQLNormalizedDB, SQLiteDB, CouchDBDB, RedisDB
    return {
        "SQLite":     SQLiteDB(sqlite_cfg),
        "MySQL":      MySQLDB(mysql_cfg),
        "MySQL-Norm": MySQLNormalizedDB(mysql_normalized_cfg),
        "CouchDB":    CouchDBDB(couchdb_cfg),
        "Redis":      RedisDB(redis_cfg),
    }


def _select_dbs(available: Dict) -> Dict:
    """Pozwala wybrać podzbiór baz do benchmarku."""
    console.print("\n[bold]Dostępne bazy danych:[/bold]")
    names = list(available.keys())
    for i, name in enumerate(names, 1):
        console.print(f"  {i}. {name}")
    console.print(f"  {len(names)+1}. Wszystkie")

    choice = Prompt.ask(
        "Wybierz bazy (numery oddzielone przecinkami)",
        default=str(len(names)+1),
    )

    if choice.strip() == str(len(names)+1):
        return available

    selected = {}
    for c in choice.split(","):
        c = c.strip()
        if c.isdigit():
            idx = int(c) - 1
            if 0 <= idx < len(names):
                selected[names[idx]] = available[names[idx]]
    return selected if selected else available


def _select_size() -> tuple:
    """Pozwala wybrać rozmiar danych."""
    console.print("\n[bold]Rozmiary zbiorów danych:[/bold]")
    keys = list(DATA_SIZES.keys())
    for i, key in enumerate(keys, 1):
        cfg = DATA_SIZES[key]
        est = count_dataset(cfg)
        console.print(f"  {i}. {cfg['label']}  (~{est:,} rekordów)")
    choice = IntPrompt.ask("Wybierz rozmiar", default=1)
    idx = max(0, min(choice - 1, len(keys) - 1))
    return keys[idx], DATA_SIZES[keys[idx]]


# ────────────────────────────────────────────────────────────────
# Wyświetlanie wyników
# ────────────────────────────────────────────────────────────────

def _display_results(all_results: Dict[str, List[ScenarioResult]]):
    """Wyświetla tabelę porównawczą wyników CRUD."""
    table = Table(title="Wyniki benchmarku (czas w sekundach)",
                  show_lines=True, expand=True)
    table.add_column("CRUD", style="bold", width=6)
    table.add_column("Scenariusz", width=50)
    for db_name in all_results:
        table.add_column(db_name, justify="right", width=14)

    # Zbierz scenariusze z pierwszej bazy
    first = list(all_results.values())[0]
    for sr in first:
        row = [sr.scenario_id, sr.description]
        for db_name in all_results:
            results = all_results[db_name]
            matching = [r for r in results if r.scenario_id == sr.scenario_id]
            if matching:
                avg = matching[0].avg
                row.append(f"{avg:.6f}" if avg >= 0 else "BŁĄD")
            else:
                row.append("-")
        table.add_row(*row)

    console.print(table)


def _export_csv(all_results: Dict[str, List[ScenarioResult]],
                size_name: str, with_indexes: bool):
    """Eksportuje wyniki do pliku CSV."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    idx_tag = "idx" if with_indexes else "noidx"
    filename = f"results_{size_name}_{idx_tag}_{ts}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["scenario_id", "description"]
        for db_name in all_results:
            header.extend([f"{db_name}_avg", f"{db_name}_min", f"{db_name}_max"])
        writer.writerow(header)

        first = list(all_results.values())[0]
        for sr in first:
            row = [sr.scenario_id, sr.description]
            for db_name in all_results:
                results = all_results[db_name]
                matching = [r for r in results if r.scenario_id == sr.scenario_id]
                if matching:
                    m = matching[0]
                    row.extend([f"{m.avg:.6f}", f"{m.min_t:.6f}", f"{m.max_t:.6f}"])
                else:
                    row.extend(["-", "-", "-"])
            writer.writerow(row)

    console.print(f"\n[green]Wyniki zapisane do:[/green] {filename}")


# ────────────────────────────────────────────────────────────────
# Główne akcje menu
# ────────────────────────────────────────────────────────────────

def action_run_benchmark():
    """Pełny przebieg benchmarku."""
    all_dbs = _get_db_instances()
    selected_dbs = _select_dbs(all_dbs)
    size_name, size_cfg = _select_size()
    repetitions = IntPrompt.ask("Liczba powtórzeń na scenariusz", default=3)
    test_indexes = Confirm.ask("Testować porównanie z/bez indeksów?", default=True)

    # 1. Generuj dane
    console.print(Panel.fit("[bold cyan]Generowanie danych testowych...[/bold cyan]"))
    with Progress(SpinnerColumn(), TextColumn("{task.description}"),
                  BarColumn(), console=console) as pbar:
        task = pbar.add_task("Generowanie...", total=10)
        def gen_cb(stage):
            pbar.advance(task)
        data = generate_dataset(size_cfg, progress_callback=gen_cb)

    total_records = sum(len(v) for v in data.values())
    console.print(f"[green]Wygenerowano {total_records:,} rekordów.[/green]\n")

    all_results_no_idx = {}
    all_results_idx = {}

    for db_name, db in selected_dbs.items():
        console.print(Panel.fit(f"[bold yellow]{db_name}[/bold yellow]"))

        # Połączenie
        if not db.connect():
            console.print(f"[red]Nie udało się połączyć z {db_name}. Pomijam.[/red]")
            continue

        try:
            # Schema + seed
            db.setup_schema()
            db.clear_data()

            console.print(f"  Ładowanie danych do {db_name}...")
            with Progress(SpinnerColumn(), TextColumn("{task.description}"),
                          BarColumn(), console=console) as pbar:
                task = pbar.add_task("Seeding...", total=total_records)
                def seed_cb(table, count, total):
                    pbar.update(task, description=f"{table} ({count:,})")
                    pbar.advance(task, count)
                db.seed(data, progress_callback=seed_cb)

            # ── Bez indeksów ──
            if test_indexes:
                db.drop_indexes()
            console.print(f"  Benchmark BEZ indeksów...")
            results_no = run_benchmarks(
                db, repetitions=repetitions,
                progress_callback=lambda sid, r, t:
                    console.print(f"    {sid} [{r}/{t}]", end="\r"),
            )
            all_results_no_idx[db_name] = results_no
            console.print()

            # ── Z indeksami ──
            if test_indexes:
                db.create_indexes()
                console.print(f"  Benchmark Z indeksami...")
                results_idx = run_benchmarks(
                    db, repetitions=repetitions,
                    progress_callback=lambda sid, r, t:
                        console.print(f"    {sid} [{r}/{t}]", end="\r"),
                )
                all_results_idx[db_name] = results_idx
                console.print()

        except Exception as e:
            console.print(f"[red]Błąd {db_name}: {e}[/red]")
        finally:
            db.disconnect()

    # Wyświetlanie
    if all_results_no_idx:
        console.print(Panel.fit(
            "[bold]Wyniki BEZ indeksów[/bold]"))
        _display_results(all_results_no_idx)
        _export_csv(all_results_no_idx, size_name, False)

    if all_results_idx:
        console.print(Panel.fit(
            "[bold]Wyniki Z indeksami[/bold]"))
        _display_results(all_results_idx)
        _export_csv(all_results_idx, size_name, True)

    # Porównanie indeksów
    if all_results_no_idx and all_results_idx:
        _display_index_comparison(all_results_no_idx, all_results_idx)


def _display_index_comparison(no_idx: Dict, with_idx: Dict):
    """Zestawienie przyspieszenia po dodaniu indeksów."""
    table = Table(title="Porównanie: indeksy vs brak indeksów (speedup)",
                  show_lines=True, expand=True)
    table.add_column("Scenariusz", style="bold", width=8)
    for db_name in no_idx:
        table.add_column(db_name, justify="right", width=14)

    first = list(no_idx.values())[0]
    for sr in first:
        row = [sr.scenario_id]
        for db_name in no_idx:
            res_no = [r for r in no_idx[db_name] if r.scenario_id == sr.scenario_id]
            res_yes = [r for r in with_idx.get(db_name, [])
                       if r.scenario_id == sr.scenario_id]
            if res_no and res_yes and res_yes[0].avg > 0:
                speedup = res_no[0].avg / res_yes[0].avg
                row.append(f"{speedup:.2f}x")
            else:
                row.append("-")
        table.add_row(*row)

    console.print(table)


def action_test_connections():
    """Test połączenia z każdą bazą."""
    all_dbs = _get_db_instances()
    for name, db in all_dbs.items():
        ok = db.connect()
        status = "[green]OK[/green]" if ok else "[red]BŁĄD[/red]"
        console.print(f"  {name}: {status}")
        if ok:
            db.disconnect()


def action_clear_data():
    """Czyści dane we wszystkich bazach."""
    if not Confirm.ask("Czy na pewno wyczyścić dane we wszystkich bazach?"):
        return
    all_dbs = _get_db_instances()
    for name, db in all_dbs.items():
        if db.connect():
            try:
                db.setup_schema()
                db.clear_data()
                console.print(f"  {name}: [green]wyczyszczono[/green]")
            except Exception as e:
                console.print(f"  {name}: [red]{e}[/red]")
            finally:
                db.disconnect()


def action_quick_test():
    """Szybki test SQLite z małym zbiorem danych."""
    from db import SQLiteDB
    cfg_small = DATA_SIZES["small"]

    console.print("[cyan]Szybki test SQLite (mały zbiór)...[/cyan]")

    # Mniejszy zbiór na szybki test
    mini_cfg = {k: max(v // 100, 2) if isinstance(v, int) else v
                for k, v in cfg_small.items()}
    mini_cfg["label"] = "Mini test"
    mini_cfg["kategorie"] = 30
    mini_cfg["pozycje_per_paragon"] = 2.0

    data = generate_dataset(mini_cfg)
    total = sum(len(v) for v in data.values())
    console.print(f"  Wygenerowano {total:,} rekordów")

    db = SQLiteDB(sqlite_cfg)
    if not db.connect():
        console.print("[red]Nie udało się połączyć z SQLite[/red]")
        return
    try:
        db.setup_schema()
        db.clear_data()
        db.seed(data)
        console.print("  Dane załadowane")

        results = run_benchmarks(db, repetitions=1)
        for r in results:
            status = f"{r.avg:.4f}s" if r.avg >= 0 else "BŁĄD"
            console.print(f"    {r.scenario_id}: {status}")
        console.print("[green]Test zakończony pomyślnie.[/green]")
    except Exception as e:
        console.print(f"[red]Błąd: {e}[/red]")
    finally:
        db.disconnect()


# ────────────────────────────────────────────────────────────────
# Menu główne
# ────────────────────────────────────────────────────────────────

def main():
    console.print(Panel.fit(
        "[bold magenta]Benchmark Baz Danych[/bold magenta]\n"
        "Model: sklep alkoholowo-tytoniowy (TABELE.txt)\n"
        "Bazy: SQLite · MySQL · MySQL-Norm · CouchDB · Redis\n"
        "Scenariusze CRUD: 24 (po 6 na operację)\n"
        "[dim]Pełna sekwencja: python run_all.py[/dim]",
        title="DBBenchmark",
    ))

    while True:
        console.print("\n[bold]Menu główne:[/bold]")
        console.print("  1. Uruchom benchmark")
        console.print("  2. Test połączeń")
        console.print("  3. Szybki test (SQLite)")
        console.print("  4. Wyczyść dane")
        console.print("  5. Wyjście")

        choice = Prompt.ask("Wybierz opcję", default="1")

        if choice == "1":
            action_run_benchmark()
        elif choice == "2":
            action_test_connections()
        elif choice == "3":
            action_quick_test()
        elif choice == "4":
            action_clear_data()
        elif choice == "5":
            console.print("[dim]Do widzenia![/dim]")
            break
        else:
            console.print("[red]Nieznana opcja.[/red]")


if __name__ == "__main__":
    main()
