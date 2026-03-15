"""
run_all.py – Odpala CAŁĄ sekwencję benchmarków dla wszystkich rozmiarów.

Wymaga wcześniejszego uruchomienia:  python seed_all.py

Kolejność:
  1. Benchmark SMALL  (~500K)
  2. Benchmark MEDIUM (~1M)
  3. Benchmark LARGE  (~10M)

Dla każdego rozmiaru: 24 scenariusze × 3 powtórzenia, z i bez indeksów.

Użycie:
  python run_all.py
  python run_all.py --dbs sqlite mysql
  python run_all.py --sizes small medium
  python run_all.py --reps 1 --no-index

Argumenty:
  --dbs     {sqlite,mysql,mysql_norm,couchdb,redis} [...]
                          Bazy do testowania (domyślnie: wszystkie)
  --sizes   {small,medium,large} [...]
                          Rozmiary do przetestowania (domyślnie: wszystkie)
  --reps    INT           Powtórzenia na scenariusz (domyślnie: 3)
  --no-index              Pomiń analizę z/bez indeksów (szybszy test)
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from rich.console import Console
from rich.panel import Panel

from config import DATA_SIZES

console = Console()

ALL_DB_KEYS = ["sqlite", "mysql", "mysql_norm", "couchdb", "redis"]
ALL_SIZES = ["small", "medium", "large"]


def main():
    parser = argparse.ArgumentParser(
        description="DBBenchmark – pełna sekwencja (small → medium → large)")
    parser.add_argument("--dbs", nargs="+", choices=ALL_DB_KEYS, default=ALL_DB_KEYS)
    parser.add_argument("--sizes", nargs="+",
                        choices=ALL_SIZES, default=ALL_SIZES)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--no-index", action="store_true")
    args = parser.parse_args()

    console.print(Panel.fit(
        "[bold magenta]DBBenchmark – Pełna sekwencja[/bold magenta]\n"
        "5 baz: SQLite · MySQL · MySQL-Norm · CouchDB · Redis\n"
        f"Rozmiary: {' → '.join(args.sizes)}\n"
        f"Powtórzenia: {args.reps} | Indeksy: {'NIE' if args.no_index else 'TAK'}\n\n"
        "[dim]Dane muszą być załadowane przez seed_all.py[/dim]",
        title="run_all.py",
    ))

    t_total = time.perf_counter()

    # Import po parsowaniu, żeby ewentualne błędy importu były widoczne
    from _bench_runner import run_size_benchmark, DB_LABELS

    for size_name in args.sizes:
        size_cfg = DATA_SIZES[size_name]
        console.print(Panel.fit(
            f"[bold white]▶ Rozmiar: {size_cfg['label']}[/bold white]",
            style="blue",
        ))

        # Uruchom bezpośrednio (bez sub-procesu, żeby zachować kontekst)
        from _bench_runner import run_size_benchmark as _bench
        _bench.__globals__['sys'] = sys

        # Tymczasowo podmieniamy argv żeby _bench_runner parsował własne argumenty
        old_argv = sys.argv
        sys.argv = [f"run_{size_name}.py"]
        if args.dbs != ALL_DB_KEYS:
            sys.argv += ["--dbs"] + args.dbs
        sys.argv += ["--reps", str(args.reps)]
        if args.no_index:
            sys.argv += ["--no-index"]

        try:
            run_size_benchmark(size_name)
        finally:
            sys.argv = old_argv

        console.print()

    elapsed = time.perf_counter() - t_total
    console.print(Panel.fit(
        f"[bold green]✓ WSZYSTKIE BENCHMARKI ZAKOŃCZONE[/bold green]\n"
        f"Czas całkowity: {elapsed / 60:.1f} min\n"
        f"Wyniki: results/",
        style="green",
    ))


if __name__ == "__main__":
    main()
