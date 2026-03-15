"""
Szybki smoke test – sprawdza czy SQLite działa poprawnie z minimalnym zbiorem danych.
Uruchom: python smoke_test.py
"""
import sys
from config import DATA_SIZES, sqlite_cfg
from data.generator import generate_dataset
from scenarios.runner import run_benchmarks


def main():
    print("=== Smoke test (SQLite, mini zbiór) ===\n")

    # Miniaturowa konfiguracja
    base = DATA_SIZES["small"]
    mini_cfg = {
        "label": "smoke",
        "kategorie": 30,
        "producenci": 10,
        "pracownicy": 5,
        "klienci": 50,
        "alkohol": 100,
        "tyton": 40,
        "paragony": 200,
        "pozycje_per_paragon": 2.0,
        "faktury": 30,
        "dostawy": 20,
    }

    print("Generowanie danych...")
    data = generate_dataset(mini_cfg)
    total = sum(len(v) for v in data.values())
    print(f"  Rekordów: {total:,}\n")

    from db.sqlite_db import SQLiteDB
    db = SQLiteDB(sqlite_cfg)

    if not db.connect():
        print("BŁĄD: nie udało się połączyć z SQLite")
        sys.exit(1)

    try:
        db.setup_schema()
        db.clear_data()
        db.seed(data)
        print("Dane załadowane do SQLite.\n")

        print("Uruchamianie scenariuszy (1 powtórzenie)...\n")
        results = run_benchmarks(db, repetitions=1)

        ok = 0
        fail = 0
        for r in results:
            if r.avg >= 0:
                print(f"  [OK]   {r.scenario_id}: {r.avg:.4f}s")
                ok += 1
            else:
                print(f"  [FAIL] {r.scenario_id}: BŁĄD")
                fail += 1

        print(f"\nWynik: {ok} OK, {fail} FAIL")
        if fail > 0:
            sys.exit(1)
        print("\n=== Smoke test PASSED ===")
    except Exception as e:
        print(f"\nBŁĄD: {e}")
        sys.exit(1)
    finally:
        db.disconnect()


if __name__ == "__main__":
    main()
