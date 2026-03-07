"""
Generator danych testowych dla sklepu monopolowego.
Używa biblioteki Faker do generowania realistycznych danych.
"""
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List

from faker import Faker

fake = Faker("pl_PL")
random.seed(42)

# -------- Stałe słowniki dziedzinowe --------

PRODUCT_NAMES = [
    "Żubrówka Bison Grass", "Wyborowa", "Absolut Vodka", "Belvedere",
    "Jack Daniel's", "Jim Beam", "Johnnie Walker Black", "Jameson",
    "Hennessy VS", "Rémy Martin", "Martell VS", "Courvoisier",
    "Heineken 500ml", "Tyskie Gronie", "Żywiec", "Lech Premium",
    "Corona Extra", "Budweiser", "Desperados", "Peroni",
    "Prosecco DOC", "Chardonnay Pays d'Oc", "Cabernet Sauvignon",
    "Merlot Reserve", "Pinot Grigio", "Sauvignon Blanc",
    "Ballantine's Finest", "Grant's Family Reserve", "Famous Grouse",
    "Aperol", "Campari", "Martini Bianco", "Cinzano Rosso",
    "Soplica Brzoskwiniowa", "Soplica Wiśniowa", "Lubelska Czarna",
    "Nalewka Babuni", "Starka", "Goldwasser",
    "Red Bull 250ml", "Monster Energy", "Coca-Cola 1L",
    "Sprite 1.5L", "Pepsi Max", "Fanta Pomarańczowa",
    "Marlboro Red", "Philip Morris", "L&M Red", "Winston Blue",
    "Snickers", "Twix", "KitKat", "Pringles Original",
    "Lay's Papryka", "Doritos Nacho", "Orzeszki Solone", "Migdały",
    "Smirnoff 700ml", "Finlandia", "Grey Goose", "Ketel One",
    "Diplomatico Reserva", "Havana Club 7", "Captain Morgan",
    "Sambuca Molinari", "Baileys Original", "Kahlúa",
    "Strongbow Gold", "Cider Warka", "Desperados Lime",
    "Wino Musujące Charmat", "Lambrusco Rosso", "Sangria",
    "Luksusowa", "Pan Tadeusz", "Stolichnaya",
    "Tyskie 1.0 bezalkoholowe", "Księżycowe 0%",
]

CATEGORY_NAMES = [
    ("Wódka", "Wódki czyste i smakowe"),
    ("Whisky", "Whisky szkocka, irlandzka, bourbon"),
    ("Koniak i Brandy", "Koniaki i brandy"),
    ("Gin", "Giny różnych gatunków"),
    ("Rum", "Rumy jasne i ciemne"),
    ("Wino", "Wina białe, czerwone i musujące"),
    ("Piwo", "Piwa krajowe i zagraniczne"),
    ("Napoje bezalkoholowe", "Wody, soki, napoje energetyczne"),
    ("Przekąski", "Chipsy, orzechy, słodycze"),
    ("Papierosy i tytoń", "Papierosy, cygara, tytoń"),
    ("Likiery i nalewki", "Likiery smakowe i nalewki"),
    ("Alkohole mocne inne", "Tequila, absynt, inne"),
]

PAYMENT_METHODS = ["gotówka", "karta kredytowa", "karta debetowa", "BLIK", "przelew"]
ORDER_STATUSES = ["nowe", "w_realizacji", "zrealizowane", "anulowane"]
POSITIONS = ["kasjer", "kierownik", "magazynier", "sprzedawca", "asystent kierownika"]


# ========================================================
# Generatory poszczególnych tabel
# ========================================================

def gen_stores(n: int) -> List[Dict]:
    cities = ["Warszawa", "Kraków", "Wrocław", "Gdańsk", "Poznań",
              "Łódź", "Katowice", "Szczecin", "Lublin", "Białystok"]
    stores = []
    for i in range(1, n + 1):
        city = cities[(i - 1) % len(cities)]
        stores.append({
            "store_id": i,
            "name": f"Monopolowy {city} {i}",
            "city": city,
            "address": fake.street_address(),
            "phone": fake.phone_number(),
        })
    return stores


def gen_categories(n: int) -> List[Dict]:
    cats = []
    for i in range(1, n + 1):
        base = CATEGORY_NAMES[(i - 1) % len(CATEGORY_NAMES)]
        cats.append({
            "category_id": i,
            "name": base[0],
            "description": base[1],
        })
    return cats


def gen_suppliers(n: int) -> List[Dict]:
    suppliers = []
    for i in range(1, n + 1):
        suppliers.append({
            "supplier_id": i,
            "name": fake.company(),
            "phone": fake.phone_number(),
            "email": fake.company_email(),
            "address": fake.address().replace("\n", ", "),
        })
    return suppliers


def gen_employees(n: int, store_ids: List[int]) -> List[Dict]:
    employees = []
    for i in range(1, n + 1):
        hire = fake.date_between(start_date="-10y", end_date="today")
        employees.append({
            "employee_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "position": random.choice(POSITIONS),
            "hire_date": str(hire),
            "salary": round(random.uniform(2800.0, 6500.0), 2),
            "store_id": random.choice(store_ids),
        })
    return employees


def gen_customers(n: int) -> List[Dict]:
    customers = []
    for i in range(1, n + 1):
        bday = fake.date_of_birth(minimum_age=18, maximum_age=80)
        customers.append({
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "birth_date": str(bday),
        })
    return customers


def gen_products(n: int, category_ids: List[int], supplier_ids: List[int]) -> List[Dict]:
    products = []
    prod_pool = PRODUCT_NAMES * ((n // len(PRODUCT_NAMES)) + 1)
    for i in range(1, n + 1):
        products.append({
            "product_id": i,
            "name": f"{prod_pool[i - 1]} #{i}",
            "category_id": random.choice(category_ids),
            "supplier_id": random.choice(supplier_ids),
            "price": round(random.uniform(2.5, 500.0), 2),
            "barcode": str(random.randint(1_000_000_000_000, 9_999_999_999_999)),
        })
    return products


def gen_orders(n: int, customer_ids: List[int], employee_ids: List[int]) -> List[Dict]:
    orders = []
    start = datetime(2022, 1, 1)
    end = datetime(2025, 12, 31)
    delta_secs = int((end - start).total_seconds())
    for i in range(1, n + 1):
        order_dt = start + timedelta(seconds=random.randint(0, delta_secs))
        orders.append({
            "order_id": i,
            "customer_id": random.choice(customer_ids),
            "employee_id": random.choice(employee_ids),
            "order_date": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "total_amount": 0.0,          # zostanie zaktualizowane po order_items
            "status": random.choice(ORDER_STATUSES),
        })
    return orders


def gen_order_items(orders: List[Dict],
                    items_per_order: int,
                    product_ids: List[int],
                    products_map: Dict[int, float]) -> List[Dict]:
    """Generuje pozycje zamówień; aktualizuje total_amount w orders."""
    items = []
    oid = 1
    for order in orders:
        count = random.randint(1, items_per_order * 2)  # losowo 1..2*avg
        total = 0.0
        for _ in range(count):
            pid = random.choice(product_ids)
            qty = random.randint(1, 5)
            price = products_map.get(pid, round(random.uniform(5.0, 200.0), 2))
            total += qty * price
            items.append({
                "order_item_id": oid,
                "order_id": order["order_id"],
                "product_id": pid,
                "quantity": qty,
                "price": price,
            })
            oid += 1
        order["total_amount"] = round(total, 2)
    return items


def gen_inventory(products: List[Dict], store_ids: List[int]) -> List[Dict]:
    inv = []
    iid = 1
    for p in products:
        for sid in store_ids:
            inv.append({
                "inventory_id": iid,
                "product_id": p["product_id"],
                "store_id": sid,
                "quantity": random.randint(0, 200),
                "last_update": str(fake.date_between(start_date="-1y", end_date="today")),
            })
            iid += 1
    return inv


def gen_payments(orders: List[Dict]) -> List[Dict]:
    payments = []
    for i, order in enumerate(orders, start=1):
        pay_dt = order["order_date"]
        payments.append({
            "payment_id": i,
            "order_id": order["order_id"],
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_date": pay_dt,
            "amount": order["total_amount"],
        })
    return payments


# ========================================================
# Główna funkcja generująca cały zbiór danych
# ========================================================

def generate_dataset(size_config: Dict, progress_callback=None) -> Dict[str, List[Dict]]:
    """
    Generuje kompletny zbiór danych dla sklepu monopolowego.

    :param size_config: słownik z kluczami z config.DATA_SIZES[key]
    :param progress_callback: opcjonalna funkcja(etap: str) wywoływana po każdym etapie
    :return: słownik {nazwa_tabeli: [rekordy]}
    """
    def _cb(name: str):
        if progress_callback:
            progress_callback(name)

    stores = gen_stores(size_config["stores"])
    _cb("stores")

    categories = gen_categories(size_config["categories"])
    _cb("categories")

    suppliers = gen_suppliers(size_config["suppliers"])
    _cb("suppliers")

    employees = gen_employees(size_config["employees"],
                              [s["store_id"] for s in stores])
    _cb("employees")

    customers = gen_customers(size_config["customers"])
    _cb("customers")

    products = gen_products(size_config["products"],
                            [c["category_id"] for c in categories],
                            [s["supplier_id"] for s in suppliers])
    _cb("products")

    products_map = {p["product_id"]: p["price"] for p in products}

    orders = gen_orders(size_config["orders"],
                        [c["customer_id"] for c in customers],
                        [e["employee_id"] for e in employees])
    _cb("orders")

    order_items = gen_order_items(
        orders,
        size_config["order_items_per_order"],
        [p["product_id"] for p in products],
        products_map,
    )
    _cb("order_items")

    inventory = gen_inventory(products, [s["store_id"] for s in stores])
    _cb("inventory")

    payments = gen_payments(orders)
    _cb("payments")

    return {
        "stores": stores,
        "categories": categories,
        "suppliers": suppliers,
        "employees": employees,
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "inventory": inventory,
        "payments": payments,
    }


def count_dataset(size_config: Dict) -> int:
    """Szybkie wyliczenie łącznej liczby rekordów bez generowania danych."""
    s = size_config
    n_orders = s["orders"]
    n_products = s["products"]
    n_stores = s["stores"]
    est_items = n_orders * s["order_items_per_order"]          # przybliżenie
    est_inv   = n_products * s["inventory_per_product"] * n_stores
    return (s["stores"] + s["categories"] + s["suppliers"] + s["employees"] +
            s["customers"] + n_products + n_orders +
            est_items + est_inv + n_orders)   # +payments = orders
