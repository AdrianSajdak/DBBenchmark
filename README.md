# DBBenchmark – Benchmark Baz Danych

Projekt porównuje wydajność 5 systemów zarządzania bazami danych na modelu danych
sklepu alkoholowo-tytoniowego (10 tabel wg `descript/TABELE.txt`).

## Bazy danych

| Baza | Kontener Docker | Port |
|------|----------------|------|
| SQLite | – (plik lokalny) | – |
| MySQL (podstawowy) | `bench_mysql` | 3306 |
| MySQL (znormalizowany 3NF) | `bench_mysql_norm` | 3307 |
| CouchDB | `bench_couchdb` | 5984 |
| Redis | `bench_redis` | 6379 |

---

## Szybki start

```bash
# 1. Uruchom kontenery
docker compose up -d

# 2. Zainstaluj zależności
pip install -r requirements.txt

# 3. Wypełnij bazy danymi (raz)
python seed_all.py

# 4. Uruchom benchmarki
python run_all.py
```

---

## Wymagania

- Python 3.10+
- Docker + Docker Compose
- RAM: min. 4 GB (small/medium), ~14 GB dla large z Redis

```bash
# Wirtualne środowisko (opcjonalnie)
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

---

## Skrypty – opis i argumenty

### `seed_all.py` – wypełnienie baz danymi

Uruchamiasz **raz** po `docker compose up -d`. Generuje dane i ładuje do wszystkich baz.
Dane cachowane w `data/cache/*.pkl` – kolejne uruchomienia nie regenerują danych.
Po seedowaniu automatycznie mierzy rozmiary baz i zapisuje do `results/data_seeding.csv`.

```
python seed_all.py [opcje]

  --sizes   {small,medium,large} [...]
              Rozmiary do seedowania.
              Domyślnie: small medium large

  --dbs     {sqlite,mysql,mysql_norm,couchdb} [...]
              Bazy dyskowe do seedowania.
              Domyślnie: sqlite mysql mysql_norm couchdb

  --include-redis
              Dodaj Redis do seedowania (wymagane ~13 GB RAM dla all sizes!).
              Domyślnie: Redis pomijany – ładowany on-demand przez run/*.py.

  --regen     Wymuś ponowne generowanie danych (ignoruj cache .pkl).

  --no-volumes
              Pomiń pomiar rozmiarów baz po seedowaniu.
```

**Przykłady:**
```bash
python seed_all.py                              # wszystkie bazy i rozmiary
python seed_all.py --sizes small               # tylko small
python seed_all.py --dbs sqlite mysql          # tylko SQLite i MySQL
python seed_all.py --include-redis             # z Redisem (dużo RAM!)
python seed_all.py --regen                     # wymuś regenerację danych
python seed_all.py --sizes small --no-volumes  # bez pomiaru rozmiarów
```

---

### `run_all.py` – pełna sekwencja benchmarków

Uruchamia benchmarki kolejno dla small → medium → large.
Wymaga wcześniejszego `python seed_all.py`.

```
python run_all.py [opcje]

  --dbs     {sqlite,mysql,mysql_norm,couchdb,redis} [...]
              Bazy do testowania.
              Domyślnie: sqlite mysql mysql_norm couchdb redis

  --sizes   {small,medium,large} [...]
              Rozmiary do przetestowania.
              Domyślnie: small medium large

  --reps    INT
              Liczba powtórzeń każdego scenariusza.
              Domyślnie: 3

  --no-index  Pomiń porównanie z/bez indeksów (szybszy test).
```

**Przykłady:**
```bash
python run_all.py                              # pełna sekwencja
python run_all.py --dbs sqlite mysql           # tylko SQLite i MySQL
python run_all.py --sizes small medium         # tylko small i medium
python run_all.py --reps 1 --no-index          # szybki test (1 powtórzenie, bez indeksów)
```

---

### `run/run_small.py` / `run/run_medium.py` / `run/run_large.py` – benchmark jednego rozmiaru

Benchmark dla jednego rozmiaru danych.
Redis ładuje dane z cache tuż przed benchmarkiem i zwalnia RAM po nim.

```
python run/run_small.py  [opcje]   # ~500 000 rekordów
python run/run_medium.py [opcje]   # ~1 000 000 rekordów
python run/run_large.py  [opcje]   # ~10 000 000 rekordów

  --dbs     {sqlite,mysql,mysql_norm,couchdb,redis} [...]
              Bazy do testowania.
              Domyślnie: sqlite mysql mysql_norm couchdb redis

  --reps    INT
              Liczba powtórzeń każdego scenariusza.
              Domyślnie: 3

  --no-index  Pomiń porównanie z/bez indeksów (szybszy test).

  --keep-redis
              NIE czyść Redis po benchmarku (zostaw dane w RAM).
              Domyślnie: Redis jest czyszczony po benchmarku.
```

**Przykłady:**
```bash
python run/run_small.py                        # wszystkie bazy, 3 powtórzenia
python run/run_small.py --dbs sqlite mysql     # tylko SQLite i MySQL
python run/run_medium.py --reps 1 --no-index   # szybki test
python run/run_large.py --dbs redis            # tylko Redis (large)
python run/run_small.py --keep-redis           # zostaw dane w Redis po teście
```

---

### `seed_and_run_all.py` – seed + benchmark w jednym wywołaniu

Uruchamia `seed_all.py`, a następnie `run_all.py`.

```
python seed_and_run_all.py [opcje]

  --seed-args ARG [ARG ...]
              Argumenty przekazywane do seed_all.py (przed separatorem --).

  -- ARG [ARG ...]
              Argumenty przekazywane do run_all.py (po separatorze --).
```

**Przykłady:**
```bash
python seed_and_run_all.py
# Equivalent to: python seed_all.py && python run_all.py

python seed_and_run_all.py --seed-args --sizes small -- --sizes small --reps 1
# seed tylko small, a potem benchmark tylko small z 1 powtórzeniem

python seed_and_run_all.py -- --dbs sqlite mysql --no-index
# seed wszystkich, benchmark tylko SQLite+MySQL bez indeksów
```

---

### `measure_volumes.py` – pomiar rozmiarów baz

Mierzy faktyczny rozmiar danych każdej bazy (nie cały wolumen Docker) i aktualizuje
`results/data_seeding.csv`. Wywoływany automatycznie przez `seed_all.py`, ale można
też uruchomić samodzielnie (np. po ręcznym seedowaniu lub żeby odświeżyć dane).

```
python measure_volumes.py [opcje]

  --sizes   {small,medium,large} [...]
              Rozmiary do sprawdzenia.
              Domyślnie: small medium large

  --dbs     {sqlite,mysql,mysql_norm,couchdb} [...]
              Bazy do sprawdzenia (Redis pominięty – mierzony przez run/*.py).
              Domyślnie: sqlite mysql mysql_norm couchdb
```

**Co mierzy:**
- **SQLite** – rozmiar pliku `data/sqlite/sklep_{size}.db`
- **MySQL** – `du -sb /var/lib/mysql/{db_name}` w kontenerze `bench_mysql`
- **MySQL-Norm** – `du -sb /var/lib/mysql/{db_name}` w kontenerze `bench_mysql_norm`
- **CouchDB** – `sizes.file` z API `GET /{db_name}` (faktyczny rozmiar pliku `.couch`)
- **Redis** – przyrost `used_memory` podczas seedowania (zapisywany przez `run/*.py`)

**Przykłady:**
```bash
python measure_volumes.py                      # wszystkie bazy i rozmiary
python measure_volumes.py --sizes small        # tylko small
python measure_volumes.py --dbs sqlite couchdb # tylko SQLite i CouchDB
```

---

### `smoke_test.py` – szybki test (bez Dockera)

Sprawdza czy SQLite działa poprawnie z minimalnym zbiorem danych.
Nie wymaga Dockera ani wcześniejszego seedowania.

```bash
python smoke_test.py
```

---

### `main.py` – interaktywne CLI (legacy)

Interaktywne menu Rich do uruchamiania benchmarków. Opcja dla jednego rozmiaru
na raz, z pełnym seeding+benchmark w jednej sesji.

```bash
python main.py
```

---

### `web/app.py` – aplikacja webowa Flask

REST API + HTML UI do interaktywnego uruchamiania pojedynczych scenariuszy.

```bash
python web/app.py
```

Otwórz: **http://localhost:5000**

Endpointy:
- `GET /` – UI HTML
- `GET /api/scenarios` – 24 scenariusze z opisami
- `GET /api/databases` – lista baz i rozmiarów
- `POST /api/run` – uruchom scenariusz na jednej bazie (`{db, size, scenario, reps}`)
- `POST /api/run_multi` – uruchom scenariusz na wielu bazach (`{dbs, size, scenario, reps}`)
- `GET /api/results` – lista plików CSV w `results/`

---

## Architektura wielobazowa

Każda technologia obsługuje **3 rozmiary w jednym kontenerze**:

| Baza | small | medium | large |
|------|-------|--------|-------|
| MySQL (3306) | `sklep_small` | `sklep_medium` | `sklep_large` |
| MySQL-Norm (3307) | `sklep_norm_small` | `sklep_norm_medium` | `sklep_norm_large` |
| CouchDB (5984) | `sklep_small` | `sklep_medium` | `sklep_large` |
| Redis (6379) | `db=0` | `db=1` | `db=2` |
| SQLite (plik) | `data/sqlite/sklep_small.db` | `data/sqlite/sklep_medium.db` | `data/sqlite/sklep_large.db` |

**Redis i pamięć RAM:**
Redis przechowuje dane wyłącznie w RAM. Szacowane zapotrzebowanie:
- small (~500K rek.) → ~1 GB
- medium (~1M rek.) → ~2 GB
- large (~10M rek.) → ~10 GB

Strategia `run/*.py`: FLUSH → ładuj z cache → benchmark → FLUSH (RAM zwalniany po każdym teście).

---

## Struktura projektu

```
DBBenchmark/
├── docker-compose.yml           # 4 kontenery (MySQL 3306, MySQL-Norm 3307, CouchDB 5984, Redis 6379)
├── config.py                    # Konfiguracje połączeń + DATA_SIZES + SIZE_DB_CONFIGS
├── requirements.txt             # Zależności Python
│
├── seed_all.py                  # JEDNORAZOWY SEEDER – uruchom raz po docker compose up
├── run_all.py                   # Pełna sekwencja benchmarków (small → medium → large)
├── seed_and_run_all.py          # Seed + benchmark w jednym wywołaniu
├── measure_volumes.py           # Pomiar rozmiarów baz – aktualizuje data_seeding.csv
├── _bench_runner.py             # Wspólna logika run/*.py (nie uruchamiaj bezpośrednio)
│
├── run/                         # Benchmarki pojedynczych rozmiarów
│   ├── run_small.py             # Benchmark SMALL (~500K rekordów)
│   ├── run_medium.py            # Benchmark MEDIUM (~1M rekordów)
│   └── run_large.py             # Benchmark LARGE (~10M rekordów)
│
├── main.py                      # Interaktywne CLI (legacy, Rich)
├── smoke_test.py                # Szybki test SQLite bez Dockera
│
├── data/
│   ├── __init__.py
│   ├── generator.py             # generate_dataset(), count_dataset()
│   ├── cache/                   # Pliki .pkl z danymi (tworzone przez seed_all.py)
│   │   ├── dataset_small.pkl
│   │   ├── dataset_medium.pkl
│   │   └── dataset_large.pkl
│   └── sqlite/                  # Pliki bazy SQLite
│       ├── sklep_small.db
│       ├── sklep_medium.db
│       └── sklep_large.db
│
├── db/
│   ├── __init__.py
│   ├── base_db.py               # Abstrakcyjna klasa BaseDB (24 metody CRUD)
│   ├── sqlite_db.py             # SQLite
│   ├── mysql_db.py              # MySQL podstawowy
│   ├── mysql_normalized_db.py   # MySQL 3NF (tabele słownikowe)
│   ├── couchdb_db.py            # CouchDB (dokumenty JSON)
│   └── redis_db.py              # Redis (HSET, db=0/1/2)
│
├── scenarios/
│   ├── __init__.py
│   └── runner.py                # run_benchmarks(), ScenarioResult, SCENARIO_DESCRIPTIONS
│
├── web/
│   ├── app.py                   # Flask API
│   └── templates/
│       └── index.html
│
├── results/
│   ├── data_seeding.csv         # Czasy i rozmiary seedowania (auto-aktualizowany)
│   └── results_{size}_{noidx|idx}_{timestamp}.csv
│
└── descript/
    ├── TABELE.txt               # Schemat 10 tabel modelu danych
    ├── project_req.md
    └── teoria.md
```

---

## 24 scenariusze CRUD

| ID | Opis |
|----|------|
| **C1** | Dodanie pojedynczego produktu alkoholowego |
| **C2** | Dodanie hurtowe 1000 produktów alkoholowych |
| **C3** | Dodanie nowego klienta |
| **C4** | Dodanie nowego paragonu |
| **C5** | Dodanie pozycji do paragonu |
| **C6** | Dodanie nowej faktury |
| **R1** | Pobranie produktu alkoholowego po ID |
| **R2** | Lista produktów z danej kategorii |
| **R3** | Historia paragonów klienta |
| **R4** | Szczegóły paragonu z pozycjami (JOIN) |
| **R5** | Top 10 najdroższych produktów alkoholowych |
| **R6** | Wyszukanie faktur z danego okresu |
| **U1** | Aktualizacja ceny produktu alkoholowego |
| **U2** | Aktualizacja danych klienta |
| **U3** | Zmiana statusu transakcji paragonu |
| **U4** | Masowa aktualizacja cen w kategorii (promocja) |
| **U5** | Aktualizacja statusu dostawy |
| **U6** | Aktualizacja danych producenta |
| **D1** | Usunięcie pojedynczego produktu alkoholowego |
| **D2** | Usunięcie klienta |
| **D3** | Usunięcie paragonu (kaskadowe z pozycjami) |
| **D4** | Usunięcie pozycji paragonu |
| **D5** | Masowe usunięcie 100 produktów tytoniowych |
| **D6** | Usunięcie dostawy |

---

## Format pliku `results/data_seeding.csv`

| Kolumna | Opis |
|---------|------|
| `data_set` | Rozmiar danych: `small` / `medium` / `large` |
| `database` | Nazwa bazy: `sqlite` / `mysql` / `mysql-norm` / `couchdb` / `redis` |
| `seeding_time_s` | Czas seedowania w sekundach |
| `seeding_amount` | Łączna liczba zaimportowanych rekordów |
| `volume_size_bytes` | Faktyczny rozmiar danych na dysku/w RAM (bajty) |
| `seeding_timestamp` | Data i godzina seedowania (`YYYY-MM-DD HH:MM:SS`) |

Plik jest automatycznie aktualizowany przez:
- `seed_all.py` – czasy i rozmiary wszystkich baz dyskowych
- `run/*.py` – czas i przyrost pamięci Redis (load-on-demand)

## Format pliku `results/results_{size}_{idx|noidx}_{timestamp}.csv`

| Kolumna | Opis |
|---------|------|
| `scenario_id` | np. `R4` |
| `description` | Opis słowny scenariusza |
| `operacja` | Jedna z: C / R / U / D |
| `{DB}_avg_s` | Średni czas wykonania [s] |
| `{DB}_min_s` | Minimalny czas [s] |
| `{DB}_max_s` | Maksymalny czas [s] |

Każda faza (small/medium/large) × (noidx/idx) generuje osobny plik CSV.

---

## Domyślne dane dostępowe

| Baza | Użytkownik | Hasło | Host:Port |
|------|-----------|-------|-----------|
| MySQL | root | benchmark123 | localhost:3306 |
| MySQL-Norm | root | benchmark123 | localhost:3307 |
| CouchDB | admin | benchmark123 | localhost:5984 |
| Redis | – | – | localhost:6379 |
| SQLite | – | – | `data/sqlite/sklep_{size}.db` |
