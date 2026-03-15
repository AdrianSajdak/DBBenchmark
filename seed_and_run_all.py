"""
seed_and_run_all.py – uruchamia seed_all.py, a następnie run_all.py.

Domyślnie:
  1) seed_all.py
  2) run_all.py

Użycie:
  python seed_and_run_all.py
  python seed_and_run_all.py --seed-args --sizes small --dbs sqlite mysql -- --sizes small --dbs sqlite mysql --reps 1

Wszystko po `--seed-args` aż do `--` trafia do seed_all.py.
Wszystko po `--` trafia do run_all.py.
Jeśli pominiesz `--seed-args`, argumenty po `--` trafią tylko do run_all.py.
"""

from __future__ import annotations

import os
import subprocess
import sys


def _split_args(argv: list[str]) -> tuple[list[str], list[str]]:
    seed_args: list[str] = []
    run_args: list[str] = []

    if "--seed-args" in argv:
        seed_start = argv.index("--seed-args") + 1
        if "--" in argv[seed_start:]:
            sep = argv.index("--", seed_start)
            seed_args = argv[seed_start:sep]
            run_args = argv[sep + 1 :]
        else:
            seed_args = argv[seed_start:]
            run_args = []
    else:
        if "--" in argv:
            sep = argv.index("--")
            run_args = argv[sep + 1 :]
        else:
            run_args = argv

    return seed_args, run_args


def _run_script(script_name: str, script_args: list[str]) -> int:
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    cmd = [sys.executable, script_path, *script_args]
    print(f"\n=== Uruchamiam: {' '.join(cmd)} ===")
    completed = subprocess.run(cmd, check=False)
    return completed.returncode


def main() -> int:
    seed_args, run_args = _split_args(sys.argv[1:])

    seed_rc = _run_script("seed_all.py", seed_args)
    if seed_rc != 0:
        print("\n[ERROR] seed_all.py zakończył się błędem. Przerywam.")
        return seed_rc

    run_rc = _run_script("run_all.py", run_args)
    if run_rc != 0:
        print("\n[ERROR] run_all.py zakończył się błędem.")
    return run_rc


if __name__ == "__main__":
    raise SystemExit(main())
