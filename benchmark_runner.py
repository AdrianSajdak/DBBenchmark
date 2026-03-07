"""
Silnik benchmarku – mierzy czas wykonania każdego scenariusza CRUD.

Każdy test uruchamia się REPETITIONS razy, wynik to średnia i minimum.
"""
import random
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from data_generator import (
    gen_customers, gen_order_items, gen_orders, gen_products,
    gen_inventory, fake,
)
from db.base_db import BaseDB

REPETITIONS = 3     # liczba powtórzeń każdego scenariusza
BULK_PRODUCTS = 1000  # scenariusz C2 – ile produktów na raz
BULK_DELETE = 100     # scenariusz D5 – ile produktów usuniemy

# ----------------------------------------------------------------
# Wynik pojedynczego testu
# ----------------------------------------------------------------

@dataclass
class ScenarioResult:
    scenario_id: str              # np. "C1", "R3"
    description: str
    db_name: str
    times_ms: List[float] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def avg_ms(self) -> Optional[float]:
        return sum(self.times_ms) / len(self.times_ms) if self.times_ms else None

    @property
    def min_ms(self) -> Optional[float]:
        return min(self.times_ms) if self.times_ms else None

    @property
    def max_ms(self) -> Optional[float]:
        return max(self.times_ms) if self.times_ms else None


# ----------------------------------------------------------------
# Opis wszystkich scenariuszy
# ----------------------------------------------------------------

SCENARIOS = {
    # CREATE
    "C1": "Dodanie pojedynczego produktu",
    "C2": f"Dodanie {BULK_PRODUCTS} produktów naraz",
    "C3": "Dodanie nowego klienta",
    "C4": "Dodanie nowego zamówienia",
    "C5": "Dodanie kilku pozycji zamówienia",
    "C6": "Dodanie rekordów magazynowych",
    # READ
    "R1": "Pobranie produktu po ID",
    "R2": "Lista produktów z kategorii",
    "R3": "Historia zamówień klienta",
    "R4": "Szczegóły zamówienia (JOIN)",
    "R5": "Top 10 najdroższych produktów",
    "R6": "Pełny stan magazynowy",
    # UPDATE
    "U1": "Aktualizacja ceny produktu",
    "U2": "Aktualizacja danych klienta",
    "U3": "Aktualizacja stanu magazynowego",
    "U4": "Masowa aktualizacja cen (promocja)",
    "U5": "Zmiana statusu zamówienia",
    "U6": "Aktualizacja danych dostawcy",
    # DELETE
    "D1": "Usunięcie pojedynczego produktu",
    "D2": "Usunięcie klienta",
    "D3": "Usunięcie zamówienia (CASCADE)",
    "D4": "Usunięcie pozycji zamówienia",
    "D5": f"Masowe usunięcie {BULK_DELETE} produktów",
    "D6": "Usunięcie rekordów magazynowych",
}


def _measure(fn: Callable, reps: int = REPETITIONS) -> Tuple[List[float], Optional[str]]:
    """Uruchamia fn() reps razy i zwraca listę czasów w ms."""
    times = []
    err = None
    for _ in range(reps):
        try:
            t0 = time.perf_counter()
            fn()
            times.append((time.perf_counter() - t0) * 1000)
        except Exception as e:
            err = str(e)
            break
    return times, err


def run_benchmarks(
    db: BaseDB,
    data: Dict,
    selected: Optional[List[str]] = None,
    progress_cb: Callable[[str, str], None] = None,
) -> List[ScenarioResult]:
    """
    Uruchamia wybrane (lub wszystkie) scenariusze CRUD dla danej bazy.

    :param db:         Instancja BaseDB (już połączona i z wgranym sedem).
    :param data:       Zbiór danych (słownik tabela → rekordy).
    :param selected:   Lista ID scenariuszy do uruchomienia (None = wszystkie).
    :param progress_cb: Callback (scenario_id, description) wywoływany przed każdym testem.
    :return: Lista ScenarioResult.
    """
    to_run = set(selected) if selected else set(SCENARIOS.keys())
    results: List[ScenarioResult] = []

    # -------------------------------------------------------
    # Przygotuj pomocnicze ID z istniejącego zbioru danych
    # -------------------------------------------------------
    products     = data.get("products", [])
    customers    = data.get("customers", [])
    orders       = data.get("orders", [])
    order_items  = data.get("order_items", [])
    suppliers    = data.get("suppliers", [])
    categories   = data.get("categories", [])
    stores       = data.get("stores", [])
    inventory    = data.get("inventory", [])

    def pick(lst, key, default=1):
        if lst:
            return random.choice(lst)[key]
        return default

    existing_product_id   = pick(products,    "product_id")
    existing_customer_id  = pick(customers,   "customer_id")
    existing_order_id     = pick(orders,       "order_id")
    existing_category_id  = pick(categories,  "category_id")
    existing_supplier_id  = pick(suppliers,   "supplier_id")
    existing_store_id     = pick(stores,       "store_id")

    # -------------------------------------------------------
    # Generuj "świeże" rekordy TYLKO do testów INSERT/DELETE
    # -------------------------------------------------------
    max_pid = max((p["product_id"] for p in products), default=0)
    max_cid = max((c["customer_id"] for c in customers), default=0)
    max_oid = max((o["order_id"] for o in orders), default=0)
    max_iid = max((i["order_item_id"] for i in order_items), default=0)
    max_invid = max((i["inventory_id"] for i in inventory), default=0)

    # Pula produktów dla C2 (1000 szt.)
    c2_products = gen_products(
        BULK_PRODUCTS,
        [c["category_id"] for c in categories],
        [s["supplier_id"] for s in suppliers],
    )
    # Przesuń ID żeby nie kolidowały z sedem
    offset = max_pid + 10_000_000
    for i, p in enumerate(c2_products):
        p["product_id"] = offset + i

    # C1 – jeden produkt
    c1_product = {
        "product_id": offset + BULK_PRODUCTS + 1,
        "name": "TestProduct C1",
        "category_id": existing_category_id,
        "supplier_id": existing_supplier_id,
        "price": 49.99,
        "barcode": "0000000000001",
    }

    # C3 – jeden klient
    c3_customer = {
        "customer_id": max_cid + 10_000_000,
        "first_name": "Jan",
        "last_name": "Testowy",
        "email": "test@benchmark.pl",
        "phone": "+48 000 000 000",
        "birth_date": "1990-01-01",
    }

    # C4 – jedno zamówienie
    c4_order = {
        "order_id": max_oid + 10_000_000,
        "customer_id": existing_customer_id,
        "employee_id": pick(data.get("employees", [{"employee_id": 1}]), "employee_id"),
        "order_date": "2025-01-01 12:00:00",
        "total_amount": 99.99,
        "status": "nowe",
    }

    # C5 – pozycje zamówienia (dla c4_order)
    c5_items = [
        {
            "order_item_id": max_iid + 10_000_000 + j,
            "order_id": c4_order["order_id"],
            "product_id": existing_product_id,
            "quantity": random.randint(1, 3),
            "price": round(random.uniform(5.0, 100.0), 2),
        }
        for j in range(5)
    ]

    # C6 – rekordy magazynowe
    c6_records = [
        {
            "inventory_id": max_invid + 10_000_000 + j,
            "product_id": existing_product_id,
            "store_id": existing_store_id,
            "quantity": 10 + j,
            "last_update": "2025-01-01",
        }
        for j in range(3)
    ]

    # Rekordy do DELETE (wstawiamy przed testem, poza pomiarem)
    d1_product_id  = offset + BULK_PRODUCTS + 100
    d2_customer_id = max_cid + 10_000_000 + 100
    d3_order_id    = max_oid + 10_000_000 + 100
    d5_ids         = list(range(offset + BULK_PRODUCTS + 200,
                                offset + BULK_PRODUCTS + 200 + BULK_DELETE))

    # -------------------------------------------------------
    # Definicje scenariuszy jako lambdy
    # -------------------------------------------------------

    def prepare_d1():
        try:
            db.C1_add_single_product({
                "product_id": d1_product_id, "name": "D1 TestProduct",
                "category_id": existing_category_id, "supplier_id": existing_supplier_id,
                "price": 9.99, "barcode": "0000000000D1",
            })
        except Exception:
            pass

    def prepare_d2():
        try:
            db.C3_add_customer({
                "customer_id": d2_customer_id, "first_name": "D2", "last_name": "TestCustomer",
                "email": "d2@test.pl", "phone": "123", "birth_date": "2000-01-01",
            })
        except Exception:
            pass

    def prepare_d3():
        try:
            db.C4_add_order({
                "order_id": d3_order_id, "customer_id": existing_customer_id,
                "employee_id": pick(data.get("employees", [{"employee_id": 1}]), "employee_id"),
                "order_date": "2025-06-01 10:00:00", "total_amount": 0.0, "status": "nowe",
            })
        except Exception:
            pass

    def prepare_d5():
        prods = [
            {"product_id": pid, "name": f"D5 Product {pid}",
             "category_id": existing_category_id, "supplier_id": existing_supplier_id,
             "price": 5.0, "barcode": f"D5{pid}"}
            for pid in d5_ids
        ]
        try:
            db.C2_add_bulk_products(prods)
        except Exception:
            pass

    scenario_fns: Dict[str, Tuple[Optional[Callable], Callable]] = {
        # (setup, test)
        "C1": (None,    lambda: db.C1_add_single_product({
            **c1_product, "product_id": random.randint(10**8, 2*10**8)})),
        "C2": (None,    lambda: db.C2_add_bulk_products([
            {**p, "product_id": p["product_id"] + random.randint(10**8, 2*10**8)}
            for p in c2_products])),
        "C3": (None,    lambda: db.C3_add_customer(
            {**c3_customer, "customer_id": random.randint(10**8, 2*10**8)})),
        "C4": (None,    lambda: db.C4_add_order(
            {**c4_order, "order_id": random.randint(10**8, 2*10**8)})),
        "C5": (lambda: db.C4_add_order(
            {**c4_order, "order_id": max_oid + 5_000_000}),
               lambda: db.C5_add_order_items([
                   {**item, "order_id": max_oid + 5_000_000,
                    "order_item_id": random.randint(10**8, 2*10**8)}
                   for item in c5_items])),
        "C6": (None,    lambda: db.C6_add_inventory_records([
            {**r, "inventory_id": random.randint(10**8, 2*10**8)} for r in c6_records])),

        "R1": (None, lambda: db.R1_get_product_by_id(existing_product_id)),
        "R2": (None, lambda: db.R2_get_products_by_category(existing_category_id)),
        "R3": (None, lambda: db.R3_get_customer_orders(existing_customer_id)),
        "R4": (None, lambda: db.R4_get_order_details(existing_order_id)),
        "R5": (None, lambda: db.R5_get_top_expensive_products(10)),
        "R6": (None, lambda: db.R6_get_inventory()),

        "U1": (None, lambda: db.U1_update_product_price(
            existing_product_id, round(random.uniform(5.0, 200.0), 2))),
        "U2": (None, lambda: db.U2_update_customer(
            existing_customer_id, {"email": fake.email(), "phone": fake.phone_number()})),
        "U3": (None, lambda: db.U3_update_inventory(
            existing_product_id, existing_store_id, random.randint(0, 100))),
        "U4": (None, lambda: db.U4_bulk_update_product_prices(existing_category_id, 5.0)),
        "U5": (None, lambda: db.U5_update_order_status(
            existing_order_id, random.choice(["nowe", "zrealizowane", "anulowane"]))),
        "U6": (None, lambda: db.U6_update_supplier(
            existing_supplier_id, {"email": fake.email(), "phone": fake.phone_number()})),

        "D1": (prepare_d1, lambda: db.D1_delete_product(d1_product_id)),
        "D2": (prepare_d2, lambda: db.D2_delete_customer(d2_customer_id)),
        "D3": (prepare_d3, lambda: db.D3_delete_order(d3_order_id)),
        "D4": (lambda: db.C4_add_order(
            {**c4_order, "order_id": max_oid + 6_000_000}),
               lambda: db.D4_delete_order_items(max_oid + 6_000_000)),
        "D5": (prepare_d5, lambda: db.D5_bulk_delete_products(d5_ids)),
        "D6": (None, lambda: db.D6_delete_inventory_records(existing_product_id)),
    }

    # -------------------------------------------------------
    # Uruchom testy
    # -------------------------------------------------------
    for sid, desc in SCENARIOS.items():
        if sid not in to_run:
            continue
        if progress_cb:
            progress_cb(sid, desc)
        result = ScenarioResult(scenario_id=sid, description=desc, db_name=db.name)
        setup_fn, test_fn = scenario_fns.get(sid, (None, None))
        if test_fn is None:
            result.error = "Brak implementacji scenariusza"
            results.append(result)
            continue

        times = []
        err = None
        for rep in range(REPETITIONS):
            # Przygotowanie przed każdym powtórzeniem (poza pomiarem)
            if setup_fn:
                try:
                    setup_fn()
                except Exception:
                    pass
            try:
                t0 = time.perf_counter()
                test_fn()
                elapsed_ms = (time.perf_counter() - t0) * 1000
                times.append(elapsed_ms)
            except Exception as e:
                err = str(e)
                break

        result.times_ms = times
        result.error = err
        results.append(result)

    return results
