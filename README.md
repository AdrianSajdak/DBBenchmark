# Status implementacji – DB Benchmark Sklep Monopolowy

## ✅ Zrealizowane

### Struktura projektu
- [x] Modularny podział na pliki (`config.py`, `data_generator.py`, `benchmark_runner.py`, `main.py`, `db/`)
- [x] Abstrakcyjna klasa bazowa `BaseDB` – jednolity interfejs dla każdej bazy
- [x] `requirements.txt` z zależnościami

### Bazy danych
- [x] **MySQL** – pełna implementacja CRUD (`db/mysql_db.py`), batch insert, FK, transakcje
- [x] **SQLite** – pełna implementacja CRUD (`db/sqlite_db.py`), WAL mode, PRAGMA optymalizacje
- [x] **CouchDB** – implementacja CRUD przez REST API (`db/couchdb_db.py`), Mango indeksy, bulk_docs
- [x] **Redis** – implementacja CRUD key-value (`db/redis_db.py`), pipeline, indeksy SET/ZSET

### Model danych (10 tabel zgodnie z dokumentem)
- [x] Products, Categories, Suppliers, Customers, Employees
- [x] Orders, Order_Items, Inventory, Payments, Stores
- [x] Relacje FK (MySQL, SQLite) / struktury dokumentów (CouchDB) / klucze + indeksy (Redis)

### Scenariusze CRUD (wszystkie 24 z dokumentu)
| ID  | Opis                                      | Status |
|-----|-------------------------------------------|--------|
| C1  | Dodanie pojedynczego produktu             | ✅     |
| C2  | Dodanie 1000 produktów naraz              | ✅     |
| C3  | Dodanie nowego klienta                    | ✅     |
| C4  | Dodanie nowego zamówienia                 | ✅     |
| C5  | Dodanie kilku pozycji zamówienia          | ✅     |
| C6  | Dodanie rekordów magazynowych             | ✅     |
| R1  | Pobranie produktu po ID                   | ✅     |
| R2  | Lista produktów z kategorii               | ✅     |
| R3  | Historia zamówień klienta                 | ✅     |
| R4  | Szczegóły zamówienia (JOIN)               | ✅     |
| R5  | Top 10 najdroższych produktów             | ✅     |
| R6  | Pełny stan magazynowy                     | ✅     |
| U1  | Aktualizacja ceny produktu                | ✅     |
| U2  | Aktualizacja danych klienta               | ✅     |
| U3  | Aktualizacja stanu magazynowego           | ✅     |
| U4  | Masowa aktualizacja cen (promocja)        | ✅     |
| U5  | Zmiana statusu zamówienia                 | ✅     |
| U6  | Aktualizacja danych dostawcy              | ✅     |
| D1  | Usunięcie pojedynczego produktu           | ✅     |
| D2  | Usunięcie klienta                         | ✅     |
| D3  | Usunięcie zamówienia (CASCADE)            | ✅     |
| D4  | Usunięcie pozycji zamówienia              | ✅     |
| D5  | Masowe usunięcie 100 produktów            | ✅     |
| D6  | Usunięcie rekordów magazynowych           | ✅     |

### Rozmiary zbiorów danych (zgodnie z dokumentem)
- [x] Mały  – ~500 000 rekordów
- [x] Średni – ~1 000 000 rekordów
- [x] Duży  – ~9 000 000 rekordów

### Pomiar wydajności
- [x] Każdy scenariusz uruchamiany **3 razy** (zgodnie z dokumentem)
- [x] Zapis min / avg / max czasu w ms
- [x] Eksport wyników do **CSV** (`results/`)

### Interfejs konsolowy
- [x] Menu główne (opcje 0–5)
- [x] Wybór baz do testowania (1–4 lub dowolna kombinacja)
- [x] Wybór rozmiaru danych (mały/średni/duży)
- [x] Wybór scenariuszy (pojedyncze ID, grupy C/R/U/D, wszystkie)
- [x] Wyświetlanie wyników w tabeli (rich lub plain-text fallback)
- [x] Widok szczegółowy min/avg/max per baza
- [x] Konfiguracja połączeń z poziomu menu (bez edycji kodu)
- [x] Test połączeń ping do każdej bazy
- [x] Czyszczenie danych z poziomu menu
- [x] Przeglądanie zapisanych CSV

### Generator danych
- [x] Realistyczne dane po polsku (Faker pl_PL)
- [x] Produkty z dziedziny sklepu monopolowego (wódki, wina, piwa, przekąski itp.)
- [x] Spójne klucze obce między tabelami

---

## ❌ Niezrealizowane / do zrobienia

### Pomiar i raportowanie
- [ ] **Wykresy** porównawcze (np. bar chart z matplotlib/plotly) – dokument sugeruje analizę wizualną wyników
- [ ] Automatyczne obliczenie **statystyk zbiorczych** (np. która baza wygrywa w każdej kategorii)
- [ ] Eksport wyników do **HTML/PDF** raportu

### Bazy danych
- [ ] **Indeksy na tabelach MySQL/SQLite** nie są jawnie tworzone poza PK/FK – przy dużych zbiorach mogą przyspieszyć READ
- [ ] CouchDB: scenariusz **R6 (pełny magazyn)** przy dużym zbiorze może być wolny bez dedykowanego widoku MapReduce (`_design/`)
- [ ] Redis: **R6** iteruje po `lq:inv:*` kluczach – przy dużym zbiorze lepiej byłoby trzymać indeks SET z ID wszystkich inventory

### Infrastruktura / uruchomienie
- [ ] Brak **docker-compose.yml** – MySQL, CouchDB i Redis wymagają ręcznego uruchomienia serwerów
- [ ] Brak skryptu `setup.sh` / instrukcji krok po kroku dla środowiska

### Testy
- [ ] Brak testów jednostkowych (pytest) dla generatora danych i poszczególnych implementacji DB
- [ ] `smoke_test.py` pokrywa tylko SQLite i wybrane scenariusze

---

## Szybki start

```bash
# Instalacja zależności
pip install -r requirements.txt

# Uruchomienie benchmarku
python main.py

# Szybki test SQLite (bez serwera)
python smoke_test.py
```

Serwery wymagane dla pełnego testu:
- **MySQL** na `localhost:3306` (user: root, pass: password)
- **CouchDB** na `localhost:5984` (admin/password)
- **Redis** na `localhost:6379`

Konfigurację można zmienić przez opcję **2. Konfiguracja połączeń** w menu lub edytując `config.py`.
