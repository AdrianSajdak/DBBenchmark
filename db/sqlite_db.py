"""SQLite – implementacja scenariuszy CRUD."""
import sqlite3
from typing import Any, Dict, List

from db.base_db import BaseDB

BATCH = 1000


def _dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class SQLiteDB(BaseDB):
    name = "SQLite"

    def __init__(self, config):
        self.cfg = config
        self.conn = None

    # -------- Lifecycle --------

    def connect(self) -> bool:
        try:
            self.conn = sqlite3.connect(self.cfg.database)
            self.conn.row_factory = _dict_factory
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA cache_size=-65536")   # 64 MB
            self.conn.execute("PRAGMA foreign_keys=ON")
            return True
        except Exception as e:
            print(f"[SQLite] Błąd połączenia: {e}")
            return False

    def disconnect(self) -> None:
        if self.conn:
            self.conn.close()

    def setup_schema(self) -> None:
        stmts = [
            """CREATE TABLE IF NOT EXISTS stores (
                store_id INTEGER PRIMARY KEY,
                name TEXT, city TEXT, address TEXT, phone TEXT)""",
            """CREATE TABLE IF NOT EXISTS categories (
                category_id INTEGER PRIMARY KEY,
                name TEXT, description TEXT)""",
            """CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id INTEGER PRIMARY KEY,
                name TEXT, phone TEXT, email TEXT, address TEXT)""",
            """CREATE TABLE IF NOT EXISTS employees (
                employee_id INTEGER PRIMARY KEY,
                first_name TEXT, last_name TEXT, position TEXT,
                hire_date TEXT, salary REAL,
                store_id INTEGER REFERENCES stores(store_id) ON DELETE SET NULL)""",
            """CREATE TABLE IF NOT EXISTS customers (
                customer_id INTEGER PRIMARY KEY,
                first_name TEXT, last_name TEXT, email TEXT,
                phone TEXT, birth_date TEXT)""",
            """CREATE TABLE IF NOT EXISTS products (
                product_id INTEGER PRIMARY KEY,
                name TEXT, category_id INTEGER, supplier_id INTEGER,
                price REAL, barcode TEXT,
                FOREIGN KEY (category_id) REFERENCES categories(category_id) ON DELETE SET NULL,
                FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id) ON DELETE SET NULL)""",
            """CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER, employee_id INTEGER,
                order_date TEXT, total_amount REAL, status TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL,
                FOREIGN KEY (employee_id) REFERENCES employees(employee_id) ON DELETE SET NULL)""",
            """CREATE TABLE IF NOT EXISTS order_items (
                order_item_id INTEGER PRIMARY KEY,
                order_id INTEGER, product_id INTEGER,
                quantity INTEGER, price REAL,
                FOREIGN KEY (order_id)   REFERENCES orders(order_id)   ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE SET NULL)""",
            """CREATE TABLE IF NOT EXISTS inventory (
                inventory_id INTEGER PRIMARY KEY,
                product_id INTEGER, store_id INTEGER,
                quantity INTEGER, last_update TEXT,
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
                FOREIGN KEY (store_id)   REFERENCES stores(store_id)   ON DELETE CASCADE)""",
            """CREATE TABLE IF NOT EXISTS payments (
                payment_id INTEGER PRIMARY KEY,
                order_id INTEGER, payment_method TEXT,
                payment_date TEXT, amount REAL,
                FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE)""",
        ]
        with self.conn:
            for stmt in stmts:
                self.conn.execute(stmt)

    def clear_data(self) -> None:
        self.conn.execute("PRAGMA foreign_keys=OFF")
        for t in ("payments", "order_items", "orders", "inventory",
                  "products", "employees", "customers",
                  "suppliers", "categories", "stores"):
            self.conn.execute(f"DELETE FROM {t}")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.commit()

    def drop_schema(self) -> None:
        self.conn.execute("PRAGMA foreign_keys=OFF")
        for t in ("payments", "order_items", "orders", "inventory",
                  "products", "employees", "customers",
                  "suppliers", "categories", "stores"):
            self.conn.execute(f"DROP TABLE IF EXISTS {t}")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.commit()

    # -------- Helper --------

    def _bulk_insert(self, table: str, rows: List[Dict]):
        if not rows:
            return
        keys = list(rows[0].keys())
        placeholders = ", ".join(["?"] * len(keys))
        sql = f"INSERT OR IGNORE INTO {table} ({', '.join(keys)}) VALUES ({placeholders})"
        with self.conn:
            for i in range(0, len(rows), BATCH):
                batch = rows[i: i + BATCH]
                self.conn.executemany(sql, [tuple(r[k] for k in keys) for r in batch])

    # -------- Seeding --------

    def seed(self, data: Dict[str, List[Dict]], progress_callback=None) -> None:
        cb = progress_callback or (lambda t, d, tot: None)
        order = ["stores", "categories", "suppliers", "employees",
                 "customers", "products", "orders", "order_items",
                 "inventory", "payments"]
        for table in order:
            rows = data.get(table, [])
            cb(table, len(rows), sum(len(v) for v in data.values()))
            self._bulk_insert(table, rows)

    # ===================== CREATE =====================

    def C1_add_single_product(self, product: Dict) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO products (product_id, name, category_id, supplier_id, price, barcode) "
                "VALUES (:product_id, :name, :category_id, :supplier_id, :price, :barcode)",
                product,
            )

    def C2_add_bulk_products(self, products: List[Dict]) -> None:
        self._bulk_insert("products", products)

    def C3_add_customer(self, customer: Dict) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO customers (customer_id, first_name, last_name, email, phone, birth_date) "
                "VALUES (:customer_id, :first_name, :last_name, :email, :phone, :birth_date)",
                customer,
            )

    def C4_add_order(self, order: Dict) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO orders (order_id, customer_id, employee_id, order_date, total_amount, status) "
                "VALUES (:order_id, :customer_id, :employee_id, :order_date, :total_amount, :status)",
                order,
            )

    def C5_add_order_items(self, items: List[Dict]) -> None:
        self._bulk_insert("order_items", items)

    def C6_add_inventory_records(self, records: List[Dict]) -> None:
        self._bulk_insert("inventory", records)

    # ===================== READ =====================

    def R1_get_product_by_id(self, product_id: int) -> Any:
        cur = self.conn.execute("SELECT * FROM products WHERE product_id = ?", (product_id,))
        return cur.fetchone()

    def R2_get_products_by_category(self, category_id: int) -> List:
        cur = self.conn.execute("SELECT * FROM products WHERE category_id = ?", (category_id,))
        return cur.fetchall()

    def R3_get_customer_orders(self, customer_id: int) -> List:
        cur = self.conn.execute("SELECT * FROM orders WHERE customer_id = ?", (customer_id,))
        return cur.fetchall()

    def R4_get_order_details(self, order_id: int) -> Any:
        cur = self.conn.execute(
            """SELECT o.*, oi.order_item_id, oi.product_id, oi.quantity, oi.price AS item_price,
                      p.name AS product_name
               FROM orders o
               LEFT JOIN order_items oi ON o.order_id = oi.order_id
               LEFT JOIN products p    ON oi.product_id = p.product_id
               WHERE o.order_id = ?""",
            (order_id,),
        )
        return cur.fetchall()

    def R5_get_top_expensive_products(self, limit: int = 10) -> List:
        cur = self.conn.execute(
            "SELECT * FROM products ORDER BY price DESC LIMIT ?", (limit,)
        )
        return cur.fetchall()

    def R6_get_inventory(self) -> List:
        cur = self.conn.execute("SELECT * FROM inventory")
        return cur.fetchall()

    # ===================== UPDATE =====================

    def U1_update_product_price(self, product_id: int, new_price: float) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE products SET price = ? WHERE product_id = ?", (new_price, product_id)
            )

    def U2_update_customer(self, customer_id: int, data: Dict) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE customers SET email = ?, phone = ? WHERE customer_id = ?",
                (data["email"], data["phone"], customer_id),
            )

    def U3_update_inventory(self, product_id: int, store_id: int, qty: int) -> None:
        from datetime import date
        with self.conn:
            self.conn.execute(
                "UPDATE inventory SET quantity = ?, last_update = ? "
                "WHERE product_id = ? AND store_id = ?",
                (qty, str(date.today()), product_id, store_id),
            )

    def U4_bulk_update_product_prices(self, category_id: int, discount_pct: float) -> None:
        factor = round(1 - discount_pct / 100, 4)
        with self.conn:
            self.conn.execute(
                "UPDATE products SET price = ROUND(price * ?, 2) WHERE category_id = ?",
                (factor, category_id),
            )

    def U5_update_order_status(self, order_id: int, status: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE orders SET status = ? WHERE order_id = ?", (status, order_id)
            )

    def U6_update_supplier(self, supplier_id: int, data: Dict) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE suppliers SET email = ?, phone = ? WHERE supplier_id = ?",
                (data["email"], data["phone"], supplier_id),
            )

    # ===================== DELETE =====================

    def D1_delete_product(self, product_id: int) -> None:
        self.conn.execute("PRAGMA foreign_keys=OFF")
        with self.conn:
            self.conn.execute("DELETE FROM products WHERE product_id = ?", (product_id,))
        self.conn.execute("PRAGMA foreign_keys=ON")

    def D2_delete_customer(self, customer_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM customers WHERE customer_id = ?", (customer_id,))

    def D3_delete_order(self, order_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM orders WHERE order_id = ?", (order_id,))

    def D4_delete_order_items(self, order_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM order_items WHERE order_id = ?", (order_id,))

    def D5_bulk_delete_products(self, product_ids: List[int]) -> None:
        self.conn.execute("PRAGMA foreign_keys=OFF")
        with self.conn:
            self.conn.executemany(
                "DELETE FROM products WHERE product_id = ?",
                [(pid,) for pid in product_ids],
            )
        self.conn.execute("PRAGMA foreign_keys=ON")

    def D6_delete_inventory_records(self, product_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM inventory WHERE product_id = ?", (product_id,))
