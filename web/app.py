"""
web/app.py – Aplikacja Flask do interaktywnego uruchamiania scenariuszy benchmarku.

Uruchomienie:
  python web/app.py

Dostępne endpointy:
  GET  /                          – strona główna (HTML)
  GET  /api/scenarios             – lista 24 scenariuszy
  GET  /api/databases             – lista dostępnych baz
  POST /api/run                   – uruchomienie scenariusza
        body: { "db": "SQLite", "scenario": "R1", "reps": 3 }
  GET  /api/results               – lista plików CSV w results/
"""
import json
import os
import sys
import time

from flask import Flask, jsonify, request, render_template
from flask_cors import CORS

# Ścieżka do katalogu głównego projektu
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from config import SIZE_DB_CONFIGS
from scenarios.runner import run_benchmarks, SCENARIO_DESCRIPTIONS, ScenarioResult

app = Flask(__name__, template_folder="templates")
CORS(app)

RESULTS_DIR = os.path.join(ROOT, "results")
os.makedirs(RESULTS_DIR, exist_ok=True)


# ────────────────────────────────────────────────────────────────
# Pomocnicze
# ────────────────────────────────────────────────────────────────

def _get_db(db_key: str, size_name: str = "small"):
    """Tworzy instancję bazy dla danej bazy i rozmiaru danych."""
    from db import MySQLDB, MySQLNormalizedDB, SQLiteDB, CouchDBDB, RedisDB
    key = db_key.lower().strip()
    if size_name not in SIZE_DB_CONFIGS or key not in SIZE_DB_CONFIGS[size_name]:
        return None
    cfg = SIZE_DB_CONFIGS[size_name][key]
    factories = {
        "sqlite":     lambda: SQLiteDB(cfg),
        "mysql":      lambda: MySQLDB(cfg),
        "mysql_norm": lambda: MySQLNormalizedDB(cfg),
        "couchdb":    lambda: CouchDBDB(cfg),
        "redis":      lambda: RedisDB(cfg),
    }
    if key not in factories:
        return None
    return factories[key]()


# ────────────────────────────────────────────────────────────────
# Trasy
# ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/scenarios")
def api_scenarios():
    """Zwraca listę scenariuszy z opisami."""
    scenarios = []
    for sid, desc in SCENARIO_DESCRIPTIONS.items():
        scenarios.append({
            "id": sid,
            "description": desc,
            "operation": sid[0],   # C / R / U / D
        })
    return jsonify({"scenarios": scenarios})


@app.route("/api/databases")
def api_databases():
    """Zwraca listę baz z informacją o dostępności."""
    databases = [
        {"key": "sqlite",     "label": "SQLite"},
        {"key": "mysql",      "label": "MySQL (podstawowy)"},
        {"key": "mysql_norm", "label": "MySQL (znormalizowany)"},
        {"key": "couchdb",    "label": "CouchDB"},
        {"key": "redis",      "label": "Redis"},
    ]
    sizes = [
        {"key": "small",  "label": "~500K rekordów"},
        {"key": "medium", "label": "~1M rekordów"},
        {"key": "large",  "label": "~10M rekordów"},
    ]
    return jsonify({"databases": databases, "sizes": sizes})


@app.route("/api/run", methods=["POST"])
def api_run():
    """
    Uruchamia wybrany scenariusz na wybranej bazie.
    Body JSON: { "db": "sqlite", "scenario": "R1", "reps": 3 }
    """
    body = request.get_json(force=True, silent=True) or {}
    db_name = body.get("db", "sqlite")
    size_name = body.get("size", "small")
    scenario_id = body.get("scenario", "R1").upper()
    reps = max(1, min(int(body.get("reps", 3)), 10))  # 1–10

    if scenario_id not in SCENARIO_DESCRIPTIONS:
        return jsonify({"error": f"Nieznany scenariusz: {scenario_id}"}), 400

    db = _get_db(db_name, size_name)
    if db is None:
        return jsonify({"error": f"Nieznana baza danych: {db_name} / rozmiar: {size_name}"}), 400

    # Połącz
    if not db.connect():
        return jsonify({"error": f"Nie można połączyć się z bazą: {db_name}"}), 503

    try:
        # Sprawdź czy są dane (minimalny setup)
        db.setup_schema()

        # Uruchom scenariusz
        results = run_benchmarks(db, repetitions=reps, scenarios=[scenario_id])
        r = results[0] if results else None

        if r is None:
            return jsonify({"error": "Nie udało się uruchomić scenariusza"}), 500

        return jsonify({
            "scenario_id": r.scenario_id,
            "description": r.description,
            "db": db_name,
            "repetitions": reps,
            "times_s": [round(t, 6) for t in r.times],
            "avg_s": round(r.avg, 6) if r.avg >= 0 else None,
            "min_s": round(r.min_t, 6) if r.min_t >= 0 else None,
            "max_s": round(r.max_t, 6) if r.max_t >= 0 else None,
            "status": "ok" if r.avg >= 0 else "error",
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.disconnect()


@app.route("/api/run_multi", methods=["POST"])
def api_run_multi():
    """
    Uruchamia scenariusz na wielu bazach naraz.
    Body JSON: { "dbs": ["sqlite", "mysql"], "scenario": "R1", "reps": 3 }
    """
    body = request.get_json(force=True, silent=True) or {}
    db_names = body.get("dbs", ["sqlite"])
    size_name = body.get("size", "small")
    scenario_id = body.get("scenario", "R1").upper()
    reps = max(1, min(int(body.get("reps", 3)), 10))

    if scenario_id not in SCENARIO_DESCRIPTIONS:
        return jsonify({"error": f"Nieznany scenariusz: {scenario_id}"}), 400

    results_out = []
    for db_name in db_names:
        db = _get_db(db_name, size_name)
        if db is None:
            results_out.append({"db": db_name, "status": "error",
                                 "error": "nieznana baza"})
            continue

        if not db.connect():
            results_out.append({"db": db_name, "status": "error",
                                 "error": "brak połączenia"})
            continue

        try:
            db.setup_schema()
            results = run_benchmarks(db, repetitions=reps, scenarios=[scenario_id])
            r = results[0] if results else None
            if r:
                results_out.append({
                    "db": db_name,
                    "status": "ok" if r.avg >= 0 else "error",
                    "avg_s": round(r.avg, 6) if r.avg >= 0 else None,
                    "min_s": round(r.min_t, 6) if r.min_t >= 0 else None,
                    "max_s": round(r.max_t, 6) if r.max_t >= 0 else None,
                    "times_s": [round(t, 6) for t in r.times],
                })
            else:
                results_out.append({"db": db_name, "status": "error",
                                     "error": "brak wyników"})
        except Exception as e:
            results_out.append({"db": db_name, "status": "error", "error": str(e)})
        finally:
            db.disconnect()

    return jsonify({
        "scenario_id": scenario_id,
        "description": SCENARIO_DESCRIPTIONS[scenario_id],
        "repetitions": reps,
        "results": results_out,
    })


@app.route("/api/results")
def api_results():
    """Lista plików CSV z wynikami benchmarku."""
    files = []
    if os.path.isdir(RESULTS_DIR):
        for fn in sorted(os.listdir(RESULTS_DIR), reverse=True):
            if fn.endswith(".csv"):
                path = os.path.join(RESULTS_DIR, fn)
                files.append({
                    "name": fn,
                    "size_kb": round(os.path.getsize(path) / 1024, 1),
                })
    return jsonify({"files": files})


# ────────────────────────────────────────────────────────────────
# Start
# ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("DBBenchmark Web App")
    print("Otwórz: http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5000)
