"""
run/run_large.py – Benchmark dla zbioru LARGE (~10 000 000 rekordów).

Wymaga wcześniejszego uruchomienia:  python seed_all.py --sizes large

Użycie:
  python run/run_large.py
  python run/run_large.py --dbs sqlite mysql
  python run/run_large.py --reps 1 --no-index
  python run/run_large.py --keep-redis

Argumenty:
  --dbs     {sqlite,mysql,mysql_norm,couchdb,redis} [...]
                          Bazy do testowania (domyślnie: wszystkie)
  --reps    INT           Powtórzenia na scenariusz (domyślnie: 3)
  --no-index              Pomiń analizę z/bez indeksów (szybszy test)
  --keep-redis            NIE czyść Redis po benchmarku (zostaw dane w RAM)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _bench_runner import run_size_benchmark

if __name__ == "__main__":
    run_size_benchmark("large")
