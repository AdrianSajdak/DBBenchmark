"""Smoke test – SQLite bez serwera."""
import os
import random

from config import DATA_SIZES, SQLiteConfig
from data_generator import generate_dataset
from db.sqlite_db import SQLiteDB
from benchmark_runner import run_benchmarks

mini = {
    **DATA_SIZES["small"],
    "orders": 200,
    "customers": 1000,
    "products": 500,
    "order_items_per_order": 2,
    "employees": 20,
    "suppliers": 20,
    "stores": 3,
    "inventory_per_product": 1,
}

print("Generowanie danych...")
data = generate_dataset(mini)
print({k: len(v) for k, v in data.items()})

cfg = SQLiteConfig(database="/tmp/bench_test.db")
db = SQLiteDB(cfg)
db.connect()
db.setup_schema()
db.clear_data()

print("Seedowanie...")
db.seed(data)

print("Uruchamiam scenariusze (C1, R1, R2, R4, R5, U1, U4, D1, D5)...")
results = run_benchmarks(db, data,
                         selected=["C1", "C2", "R1", "R2", "R4", "R5", "U1", "U4", "D1", "D5"])
for r in results:
    err = f" [BLĄD: {r.error}]" if r.error else ""
    avg = f"{r.avg_ms:.3f} ms" if r.avg_ms is not None else "—"
    print(f"  {r.scenario_id}: avg={avg}  próby={len(r.times_ms)}{err}")

db.disconnect()
if os.path.exists("/tmp/bench_test.db"):
    os.remove("/tmp/bench_test.db")
print("\nTest zakończony pomyślnie!")
