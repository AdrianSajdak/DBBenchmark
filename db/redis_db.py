"""Redis – implementacja scenariuszy CRUD (key-value + indeksy pomocnicze)."""
import json
import random
from typing import Any, Dict, List, Optional

import redis as redis_lib

from db.base_db import BaseDB

PIPELINE_BATCH = 500   # ile komend w jednym pipeline

# Prefiks przestrzeni nazw
NS = "lq:"   # liquor_store


class RedisDB(BaseDB):
    name = "Redis"

    def __init__(self, config):
        self.cfg = config
        self.r: Optional[redis_lib.Redis] = None

    # -------- Lifecycle --------

    def connect(self) -> bool:
        try:
            self.r = redis_lib.Redis(
                host=self.cfg.host,
                port=self.cfg.port,
                db=self.cfg.db,
                password=self.cfg.password,
                decode_responses=True,
            )
            self.r.ping()
            return True
        except Exception as e:
            print(f"[Redis] Błąd połączenia: {e}")
            return False

    def disconnect(self) -> None:
        if self.r:
            self.r.close()

    def setup_schema(self) -> None:
        # Redis nie wymaga schematu – nie robimy nic
        pass

    def clear_data(self) -> None:
        """Usuwa wszystkie klucze z prefiksu NS."""
        keys = self.r.keys(f"{NS}*")
        if keys:
            pipe = self.r.pipeline(transaction=False)
            for k in keys:
                pipe.delete(k)
            pipe.execute()

    def drop_schema(self) -> None:
        self.clear_data()

    # -------- Konwencje kluczy --------
    # Dane:
    #   {NS}product:{id}              → HSET (wszystkie pola)
    #   {NS}customer:{id}             → HSET
    #   {NS}order:{id}                → HSET
    #   {NS}order_item:{id}           → HSET
    #   {NS}inventory:{prod_id}:{store_id} → HSET
    #   {NS}supplier:{id}             → HSET
    #   {NS}category:{id}             → HSET
    #   {NS}employee:{id}             → HSET
    #   {NS}store:{id}                → HSET
    #   {NS}payment:{id}              → HSET
    # Indeksy:
    #   {NS}idx:products              → SET  (product IDs)
    #   {NS}idx:products:cat:{cat_id} → SET  (product IDs per category)
    #   {NS}idx:products:price        → ZSET (score=price, member=product_id)
    #   {NS}idx:customers             → SET  (customer IDs)
    #   {NS}idx:orders                → SET  (order IDs)
    #   {NS}idx:orders:cust:{cust_id} → SET  (order IDs per customer)
    #   {NS}idx:order_items:order:{order_id} → SET (item IDs)
    #   {NS}idx:inventory             → SET  ("{prod_id}:{store_id}")
    #   {NS}idx:suppliers             → SET  (supplier IDs)
    # Liczniki:
    #   {NS}cnt:product, cnt:customer, cnt:order, cnt:order_item, cnt:inventory

    def _pk(self, entity: str, eid) -> str:
        return f"{NS}{entity}:{eid}"

    def _set_entity(self, pipe, entity: str, eid, data: Dict) -> None:
        key = self._pk(entity, eid)
        # Przechowuj wszystkie pola jako JSON w jednym polu "data"
        # (HSET z wieloma polami jest wydajniejszy, ale wymaga str konwersji)
        pipe.hset(key, mapping={k: str(v) if v is not None else "" for k, v in data.items()})

    def _get_entity(self, entity: str, eid) -> Optional[Dict]:
        key = self._pk(entity, eid)
        data = self.r.hgetall(key)
        return data if data else None

    # -------- Seeding --------

    def _seed_table(self, table: str, rows: List[Dict]) -> None:
        pipe = self.r.pipeline(transaction=False)
        chunk = 0
        for row in rows:
            pid_map = {
                "stores":      ("store",      "store_id"),
                "categories":  ("category",   "category_id"),
                "suppliers":   ("supplier",   "supplier_id"),
                "employees":   ("employee",   "employee_id"),
                "customers":   ("customer",   "customer_id"),
                "products":    ("product",    "product_id"),
                "orders":      ("order",      "order_id"),
                "order_items": ("order_item", "order_item_id"),
                "inventory":   ("inv",        "inventory_id"),
                "payments":    ("payment",    "payment_id"),
            }
            entity, pk_field = pid_map[table]
            eid = row[pk_field]

            self._set_entity(pipe, entity, eid, row)

            # Indeksy
            if table == "products":
                pipe.sadd(f"{NS}idx:products", eid)
                pipe.sadd(f"{NS}idx:products:cat:{row['category_id']}", eid)
                pipe.zadd(f"{NS}idx:products:price", {str(eid): float(row["price"])})
            elif table == "customers":
                pipe.sadd(f"{NS}idx:customers", eid)
            elif table == "orders":
                pipe.sadd(f"{NS}idx:orders", eid)
                pipe.sadd(f"{NS}idx:orders:cust:{row['customer_id']}", eid)
            elif table == "order_items":
                pipe.sadd(f"{NS}idx:order_items:order:{row['order_id']}", eid)
            elif table == "inventory":
                inv_key = f"{row['product_id']}:{row['store_id']}"
                pipe.sadd(f"{NS}idx:inventory", inv_key)
            elif table == "suppliers":
                pipe.sadd(f"{NS}idx:suppliers", eid)

            chunk += 1
            if chunk >= PIPELINE_BATCH:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)
                chunk = 0
        if chunk:
            pipe.execute()

    def seed(self, data: Dict[str, List[Dict]], progress_callback=None) -> None:
        cb = progress_callback or (lambda t, d, tot: None)
        order = ["stores", "categories", "suppliers", "employees",
                 "customers", "products", "orders", "order_items",
                 "inventory", "payments"]
        total = sum(len(v) for v in data.values())
        for table in order:
            rows = data.get(table, [])
            cb(table, len(rows), total)
            self._seed_table(table, rows)

    # ===================== CREATE =====================

    def C1_add_single_product(self, product: Dict) -> None:
        pipe = self.r.pipeline()
        eid = product["product_id"]
        self._set_entity(pipe, "product", eid, product)
        pipe.sadd(f"{NS}idx:products", eid)
        pipe.sadd(f"{NS}idx:products:cat:{product['category_id']}", eid)
        pipe.zadd(f"{NS}idx:products:price", {str(eid): float(product["price"])})
        pipe.execute()

    def C2_add_bulk_products(self, products: List[Dict]) -> None:
        pipe = self.r.pipeline(transaction=False)
        for i, product in enumerate(products):
            eid = product["product_id"]
            self._set_entity(pipe, "product", eid, product)
            pipe.sadd(f"{NS}idx:products", eid)
            pipe.sadd(f"{NS}idx:products:cat:{product['category_id']}", eid)
            pipe.zadd(f"{NS}idx:products:price", {str(eid): float(product["price"])})
            if (i + 1) % PIPELINE_BATCH == 0:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)
        pipe.execute()

    def C3_add_customer(self, customer: Dict) -> None:
        pipe = self.r.pipeline()
        eid = customer["customer_id"]
        self._set_entity(pipe, "customer", eid, customer)
        pipe.sadd(f"{NS}idx:customers", eid)
        pipe.execute()

    def C4_add_order(self, order: Dict) -> None:
        pipe = self.r.pipeline()
        eid = order["order_id"]
        self._set_entity(pipe, "order", eid, order)
        pipe.sadd(f"{NS}idx:orders", eid)
        pipe.sadd(f"{NS}idx:orders:cust:{order['customer_id']}", eid)
        pipe.execute()

    def C5_add_order_items(self, items: List[Dict]) -> None:
        pipe = self.r.pipeline(transaction=False)
        for i, item in enumerate(items):
            eid = item["order_item_id"]
            self._set_entity(pipe, "order_item", eid, item)
            pipe.sadd(f"{NS}idx:order_items:order:{item['order_id']}", eid)
            if (i + 1) % PIPELINE_BATCH == 0:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)
        pipe.execute()

    def C6_add_inventory_records(self, records: List[Dict]) -> None:
        pipe = self.r.pipeline(transaction=False)
        for i, rec in enumerate(records):
            eid = rec["inventory_id"]
            self._set_entity(pipe, "inv", eid, rec)
            inv_key = f"{rec['product_id']}:{rec['store_id']}"
            pipe.sadd(f"{NS}idx:inventory", inv_key)
            if (i + 1) % PIPELINE_BATCH == 0:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)
        pipe.execute()

    # ===================== READ =====================

    def R1_get_product_by_id(self, product_id: int) -> Any:
        return self._get_entity("product", product_id)

    def R2_get_products_by_category(self, category_id: int) -> List:
        ids = self.r.smembers(f"{NS}idx:products:cat:{category_id}")
        pipe = self.r.pipeline(transaction=False)
        for pid in ids:
            pipe.hgetall(f"{NS}product:{pid}")
        return [d for d in pipe.execute() if d]

    def R3_get_customer_orders(self, customer_id: int) -> List:
        ids = self.r.smembers(f"{NS}idx:orders:cust:{customer_id}")
        pipe = self.r.pipeline(transaction=False)
        for oid in ids:
            pipe.hgetall(f"{NS}order:{oid}")
        return [d for d in pipe.execute() if d]

    def R4_get_order_details(self, order_id: int) -> Any:
        order = self._get_entity("order", order_id)
        item_ids = self.r.smembers(f"{NS}idx:order_items:order:{order_id}")
        pipe = self.r.pipeline(transaction=False)
        for iid in item_ids:
            pipe.hgetall(f"{NS}order_item:{iid}")
        items = [d for d in pipe.execute() if d]
        # Dołącz nazwę produktu (aplikacyjny JOIN)
        pipe2 = self.r.pipeline(transaction=False)
        for item in items:
            pid = item.get("product_id", "")
            pipe2.hget(f"{NS}product:{pid}", "name")
        names = pipe2.execute()
        for item, name in zip(items, names):
            item["product_name"] = name
        return {"order": order, "items": items}

    def R5_get_top_expensive_products(self, limit: int = 10) -> List:
        # ZREVRANGE zwraca produkty od najdroższego
        entries = self.r.zrevrange(
            f"{NS}idx:products:price", 0, limit - 1, withscores=True
        )
        pipe = self.r.pipeline(transaction=False)
        for pid, _ in entries:
            pipe.hgetall(f"{NS}product:{pid}")
        return [d for d in pipe.execute() if d]

    def R6_get_inventory(self) -> List:
        keys = self.r.smembers(f"{NS}idx:inventory")
        results = []
        # Wyszukaj przez inventory_id (stored w HSET)
        # Iteruj po wszystkich kluczach inv:*
        all_keys = self.r.keys(f"{NS}inv:*")
        pipe = self.r.pipeline(transaction=False)
        for k in all_keys:
            pipe.hgetall(k)
        return [d for d in pipe.execute() if d]

    # ===================== UPDATE =====================

    def U1_update_product_price(self, product_id: int, new_price: float) -> None:
        key = self._pk("product", product_id)
        pipe = self.r.pipeline()
        pipe.hset(key, "price", str(new_price))
        pipe.zadd(f"{NS}idx:products:price", {str(product_id): new_price})
        pipe.execute()

    def U2_update_customer(self, customer_id: int, data: Dict) -> None:
        key = self._pk("customer", customer_id)
        self.r.hset(key, mapping={k: str(v) for k, v in data.items()})

    def U3_update_inventory(self, product_id: int, store_id: int, qty: int) -> None:
        from datetime import date
        # Znajdź klucz inventory dla tego produktu i sklepu
        all_keys = self.r.keys(f"{NS}inv:*")
        pipe = self.r.pipeline(transaction=False)
        for k in all_keys:
            pipe.hgetall(k)
        all_inv = pipe.execute()
        for inv_data, k in zip(all_inv, all_keys):
            if (inv_data.get("product_id") == str(product_id) and
                    inv_data.get("store_id") == str(store_id)):
                self.r.hset(k, mapping={"quantity": str(qty),
                                        "last_update": str(date.today())})
                break

    def U4_bulk_update_product_prices(self, category_id: int, discount_pct: float) -> None:
        ids = self.r.smembers(f"{NS}idx:products:cat:{category_id}")
        factor = 1 - discount_pct / 100
        pipe = self.r.pipeline(transaction=False)
        for pid in ids:
            key = self._pk("product", pid)
            old_price = self.r.hget(key, "price")
            if old_price:
                new_price = round(float(old_price) * factor, 2)
                pipe.hset(key, "price", str(new_price))
                pipe.zadd(f"{NS}idx:products:price", {str(pid): new_price})
        pipe.execute()

    def U5_update_order_status(self, order_id: int, status: str) -> None:
        self.r.hset(self._pk("order", order_id), "status", status)

    def U6_update_supplier(self, supplier_id: int, data: Dict) -> None:
        key = self._pk("supplier", supplier_id)
        self.r.hset(key, mapping={k: str(v) for k, v in data.items()})

    # ===================== DELETE =====================

    def D1_delete_product(self, product_id: int) -> None:
        cat = self.r.hget(self._pk("product", product_id), "category_id")
        price = self.r.hget(self._pk("product", product_id), "price")
        pipe = self.r.pipeline()
        pipe.delete(self._pk("product", product_id))
        pipe.srem(f"{NS}idx:products", product_id)
        if cat:
            pipe.srem(f"{NS}idx:products:cat:{cat}", product_id)
        if price:
            pipe.zrem(f"{NS}idx:products:price", str(product_id))
        pipe.execute()

    def D2_delete_customer(self, customer_id: int) -> None:
        pipe = self.r.pipeline()
        pipe.delete(self._pk("customer", customer_id))
        pipe.srem(f"{NS}idx:customers", customer_id)
        pipe.execute()

    def D3_delete_order(self, order_id: int) -> None:
        pipe = self.r.pipeline()
        pipe.delete(self._pk("order", order_id))
        pipe.srem(f"{NS}idx:orders", order_id)
        # Usuń powiązane pozycje
        item_ids = self.r.smembers(f"{NS}idx:order_items:order:{order_id}")
        for iid in item_ids:
            pipe.delete(self._pk("order_item", iid))
        pipe.delete(f"{NS}idx:order_items:order:{order_id}")
        pipe.execute()

    def D4_delete_order_items(self, order_id: int) -> None:
        item_ids = self.r.smembers(f"{NS}idx:order_items:order:{order_id}")
        pipe = self.r.pipeline()
        for iid in item_ids:
            pipe.delete(self._pk("order_item", iid))
        pipe.delete(f"{NS}idx:order_items:order:{order_id}")
        pipe.execute()

    def D5_bulk_delete_products(self, product_ids: List[int]) -> None:
        pipe = self.r.pipeline(transaction=False)
        for pid in product_ids:
            cat = self.r.hget(self._pk("product", pid), "category_id")
            pipe.delete(self._pk("product", pid))
            pipe.srem(f"{NS}idx:products", pid)
            if cat:
                pipe.srem(f"{NS}idx:products:cat:{cat}", pid)
            pipe.zrem(f"{NS}idx:products:price", str(pid))
        pipe.execute()

    def D6_delete_inventory_records(self, product_id: int) -> None:
        all_keys = self.r.keys(f"{NS}inv:*")
        pipe = self.r.pipeline(transaction=False)
        for k in all_keys:
            inv = self.r.hgetall(k)
            if inv.get("product_id") == str(product_id):
                pipe.delete(k)
                store_id = inv.get("store_id", "")
                pipe.srem(f"{NS}idx:inventory", f"{product_id}:{store_id}")
        pipe.execute()
