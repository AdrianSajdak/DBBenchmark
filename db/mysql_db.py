"""MySQL – implementacja scenariuszy CRUD."""
import random
from typing import Any, Dict, List

import mysql.connector
from mysql.connector import Error

from db.base_db import BaseDB

BATCH = 1000   # rozmiar wsadowej wstawki


class MySQLDB(BaseDB):
    name = "MySQL"

    def __init__(self, config):
        self.cfg = config
        self.conn = None
        self.cursor = None

    # -------- Lifecycle --------

    def connect(self) -> bool:
        try:
            self.conn = mysql.connector.connect(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
            )
            self.cursor = self.conn.cursor(dictionary=True, buffered=True)
            self.cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{self.cfg.database}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            self.cursor.execute(f"USE `{self.cfg.database}`")
            self.conn.commit()
            return True
        except Error as e:
            print(f"[MySQL] Błąd połączenia: {e}")
            return False

    def disconnect(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def setup_schema(self) -> None:
        stmts = [
            """CREATE TABLE IF NOT EXISTS stores (
                store_id   INT PRIMARY KEY,
                name       VARCHAR(100),
                city       VARCHAR(80),
                address    VARCHAR(200),
                phone      VARCHAR(30)
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS categories (
                category_id INT PRIMARY KEY,
                name        VARCHAR(80),
                description VARCHAR(255)
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id INT PRIMARY KEY,
                name        VARCHAR(120),
                phone       VARCHAR(30),
                email       VARCHAR(100),
                address     VARCHAR(200)
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS employees (
                employee_id INT PRIMARY KEY,
                first_name  VARCHAR(60),
                last_name   VARCHAR(60),
                position    VARCHAR(60),
                hire_date   DATE,
                salary      DECIMAL(10,2),
                store_id    INT,
                FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY,
                first_name  VARCHAR(60),
                last_name   VARCHAR(60),
                email       VARCHAR(100),
                phone       VARCHAR(30),
                birth_date  DATE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS products (
                product_id  INT PRIMARY KEY,
                name        VARCHAR(150),
                category_id INT,
                supplier_id INT,
                price       DECIMAL(10,2),
                barcode     VARCHAR(30),
                FOREIGN KEY (category_id) REFERENCES categories(category_id) ON DELETE SET NULL,
                FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS orders (
                order_id     INT PRIMARY KEY,
                customer_id  INT,
                employee_id  INT,
                order_date   DATETIME,
                total_amount DECIMAL(12,2),
                status       VARCHAR(30),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL,
                FOREIGN KEY (employee_id) REFERENCES employees(employee_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS order_items (
                order_item_id INT PRIMARY KEY,
                order_id      INT,
                product_id    INT,
                quantity      INT,
                price         DECIMAL(10,2),
                FOREIGN KEY (order_id)   REFERENCES orders(order_id)   ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE SET NULL
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS inventory (
                inventory_id INT PRIMARY KEY,
                product_id   INT,
                store_id     INT,
                quantity     INT,
                last_update  DATE,
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
                FOREIGN KEY (store_id)   REFERENCES stores(store_id)    ON DELETE CASCADE
            ) ENGINE=InnoDB""",
            """CREATE TABLE IF NOT EXISTS payments (
                payment_id     INT PRIMARY KEY,
                order_id       INT,
                payment_method VARCHAR(40),
                payment_date   DATETIME,
                amount         DECIMAL(12,2),
                FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
            ) ENGINE=InnoDB""",
        ]
        for stmt in stmts:
            self.cursor.execute(stmt)
        self.conn.commit()

    def clear_data(self) -> None:
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        for t in ("payments", "order_items", "orders", "inventory",
                  "products", "employees", "customers",
                  "suppliers", "categories", "stores"):
            self.cursor.execute(f"TRUNCATE TABLE `{t}`")
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        self.conn.commit()

    def drop_schema(self) -> None:
        self.cursor.execute(f"DROP DATABASE IF EXISTS `{self.cfg.database}`")
        self.conn.commit()

    # -------- Helper --------

    def _bulk_insert(self, table: str, rows: List[Dict]):
        if not rows:
            return
        keys = list(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(keys))
        sql = f"INSERT IGNORE INTO `{table}` ({', '.join(f'`{k}`' for k in keys)}) VALUES ({placeholders})"
        for i in range(0, len(rows), BATCH):
            batch = rows[i: i + BATCH]
            values = [tuple(r[k] for k in keys) for r in batch]
            self.cursor.executemany(sql, values)
            self.conn.commit()

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
        self.cursor.execute(
            "INSERT INTO products (product_id, name, category_id, supplier_id, price, barcode) "
            "VALUES (%(product_id)s, %(name)s, %(category_id)s, %(supplier_id)s, %(price)s, %(barcode)s)",
            product,
        )
        self.conn.commit()

    def C2_add_bulk_products(self, products: List[Dict]) -> None:
        self._bulk_insert("products", products)

    def C3_add_customer(self, customer: Dict) -> None:
        self.cursor.execute(
            "INSERT INTO customers (customer_id, first_name, last_name, email, phone, birth_date) "
            "VALUES (%(customer_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s, %(birth_date)s)",
            customer,
        )
        self.conn.commit()

    def C4_add_order(self, order: Dict) -> None:
        self.cursor.execute(
            "INSERT INTO orders (order_id, customer_id, employee_id, order_date, total_amount, status) "
            "VALUES (%(order_id)s, %(customer_id)s, %(employee_id)s, %(order_date)s, %(total_amount)s, %(status)s)",
            order,
        )
        self.conn.commit()

    def C5_add_order_items(self, items: List[Dict]) -> None:
        self._bulk_insert("order_items", items)

    def C6_add_inventory_records(self, records: List[Dict]) -> None:
        self._bulk_insert("inventory", records)

    # ===================== READ =====================

    def R1_get_product_by_id(self, product_id: int) -> Any:
        self.cursor.execute("SELECT * FROM products WHERE product_id = %s", (product_id,))
        return self.cursor.fetchone()

    def R2_get_products_by_category(self, category_id: int) -> List:
        self.cursor.execute("SELECT * FROM products WHERE category_id = %s", (category_id,))
        return self.cursor.fetchall()

    def R3_get_customer_orders(self, customer_id: int) -> List:
        self.cursor.execute("SELECT * FROM orders WHERE customer_id = %s", (customer_id,))
        return self.cursor.fetchall()

    def R4_get_order_details(self, order_id: int) -> Any:
        self.cursor.execute(
            """SELECT o.*, oi.order_item_id, oi.product_id, oi.quantity, oi.price AS item_price,
                      p.name AS product_name
               FROM orders o
               LEFT JOIN order_items oi ON o.order_id = oi.order_id
               LEFT JOIN products p    ON oi.product_id = p.product_id
               WHERE o.order_id = %s""",
            (order_id,),
        )
        return self.cursor.fetchall()

    def R5_get_top_expensive_products(self, limit: int = 10) -> List:
        self.cursor.execute(
            "SELECT * FROM products ORDER BY price DESC LIMIT %s", (limit,)
        )
        return self.cursor.fetchall()

    def R6_get_inventory(self) -> List:
        self.cursor.execute("SELECT * FROM inventory")
        return self.cursor.fetchall()

    # ===================== UPDATE =====================

    def U1_update_product_price(self, product_id: int, new_price: float) -> None:
        self.cursor.execute(
            "UPDATE products SET price = %s WHERE product_id = %s",
            (new_price, product_id),
        )
        self.conn.commit()

    def U2_update_customer(self, customer_id: int, data: Dict) -> None:
        self.cursor.execute(
            "UPDATE customers SET email = %(email)s, phone = %(phone)s WHERE customer_id = %(customer_id)s",
            {**data, "customer_id": customer_id},
        )
        self.conn.commit()

    def U3_update_inventory(self, product_id: int, store_id: int, qty: int) -> None:
        self.cursor.execute(
            "UPDATE inventory SET quantity = %s, last_update = CURDATE() "
            "WHERE product_id = %s AND store_id = %s",
            (qty, product_id, store_id),
        )
        self.conn.commit()

    def U4_bulk_update_product_prices(self, category_id: int, discount_pct: float) -> None:
        self.cursor.execute(
            "UPDATE products SET price = ROUND(price * (1 - %s / 100), 2) WHERE category_id = %s",
            (discount_pct, category_id),
        )
        self.conn.commit()

    def U5_update_order_status(self, order_id: int, status: str) -> None:
        self.cursor.execute(
            "UPDATE orders SET status = %s WHERE order_id = %s", (status, order_id)
        )
        self.conn.commit()

    def U6_update_supplier(self, supplier_id: int, data: Dict) -> None:
        self.cursor.execute(
            "UPDATE suppliers SET email = %(email)s, phone = %(phone)s WHERE supplier_id = %(supplier_id)s",
            {**data, "supplier_id": supplier_id},
        )
        self.conn.commit()

    # ===================== DELETE =====================

    def D1_delete_product(self, product_id: int) -> None:
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        self.cursor.execute("DELETE FROM products WHERE product_id = %s", (product_id,))
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        self.conn.commit()

    def D2_delete_customer(self, customer_id: int) -> None:
        self.cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
        self.conn.commit()

    def D3_delete_order(self, order_id: int) -> None:
        # CASCADE usuwa order_items i payments
        self.cursor.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
        self.conn.commit()

    def D4_delete_order_items(self, order_id: int) -> None:
        self.cursor.execute("DELETE FROM order_items WHERE order_id = %s", (order_id,))
        self.conn.commit()

    def D5_bulk_delete_products(self, product_ids: List[int]) -> None:
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        fmt = ", ".join(["%s"] * len(product_ids))
        self.cursor.execute(f"DELETE FROM products WHERE product_id IN ({fmt})", product_ids)
        self.cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        self.conn.commit()

    def D6_delete_inventory_records(self, product_id: int) -> None:
        self.cursor.execute("DELETE FROM inventory WHERE product_id = %s", (product_id,))
        self.conn.commit()
