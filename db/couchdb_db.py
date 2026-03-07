"""CouchDB – implementacja scenariuszy CRUD (via REST API/requests)."""
import random
from typing import Any, Dict, List, Optional

import requests
from requests.auth import HTTPBasicAuth

from db.base_db import BaseDB

BULK_SIZE = 500   # CouchDB bulk_docs batch size


class CouchDBDB(BaseDB):
    name = "CouchDB"

    def __init__(self, config):
        self.cfg = config
        self.base = config.url
        self.db_url = f"{config.url}/{config.database}"
        self.auth = HTTPBasicAuth(config.username, config.password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Content-Type": "application/json"})

    # -------- Lifecycle --------

    def connect(self) -> bool:
        try:
            r = self.session.get(f"{self.base}/_up", timeout=5)
            return r.ok
        except Exception as e:
            print(f"[CouchDB] Błąd połączenia: {e}")
            return False

    def disconnect(self) -> None:
        self.session.close()

    def setup_schema(self) -> None:
        # Utwórz bazę danych jeśli nie istnieje
        r = self.session.put(self.db_url)
        if r.status_code not in (201, 412):
            r.raise_for_status()

        # Utwórz indeksy Mango dla wydajnych zapytań
        indexes = [
            {"index": {"fields": ["type"]},            "name": "idx_type"},
            {"index": {"fields": ["type", "category_id"]}, "name": "idx_product_cat"},
            {"index": {"fields": ["type", "customer_id"]}, "name": "idx_order_cust"},
            {"index": {"fields": ["type", "order_id"]},    "name": "idx_item_order"},
            {"index": {"fields": ["type", "product_id"]},  "name": "idx_inv_product"},
            {"index": {"fields": ["type", "price"]},       "name": "idx_product_price"},
        ]
        for idx in indexes:
            self.session.post(f"{self.db_url}/_index", json=idx)

    def clear_data(self) -> None:
        """Usuwa i odtwarza bazę danych."""
        self.session.delete(self.db_url)
        self.session.put(self.db_url)
        self.setup_schema()

    def drop_schema(self) -> None:
        self.session.delete(self.db_url)

    # -------- Helpers --------

    def _doc_id(self, doc_type: str, numeric_id: int) -> str:
        return f"{doc_type}-{numeric_id}"

    def _bulk_save(self, docs: List[Dict]) -> None:
        for i in range(0, len(docs), BULK_SIZE):
            batch = docs[i: i + BULK_SIZE]
            r = self.session.post(f"{self.db_url}/_bulk_docs", json={"docs": batch})
            if not r.ok:
                r.raise_for_status()

    def _find(self, selector: Dict, fields: Optional[List] = None,
              sort: Optional[List] = None, limit: int = 25) -> List:
        body: Dict = {"selector": selector, "limit": limit}
        if fields:
            body["fields"] = fields
        if sort:
            body["sort"] = sort
        r = self.session.post(f"{self.db_url}/_find", json=body)
        r.raise_for_status()
        return r.json().get("docs", [])

    def _find_all(self, selector: Dict) -> List:
        """Pobiera wszystkie dopasowania przez stronicowanie."""
        results, bookmark = [], None
        while True:
            body: Dict = {"selector": selector, "limit": 1000}
            if bookmark:
                body["bookmark"] = bookmark
            r = self.session.post(f"{self.db_url}/_find", json=body)
            r.raise_for_status()
            data = r.json()
            docs = data.get("docs", [])
            results.extend(docs)
            if len(docs) < 1000:
                break
            new_bm = data.get("bookmark")
            if not new_bm or new_bm == bookmark:
                break
            bookmark = new_bm
        return results

    def _get(self, doc_id: str) -> Optional[Dict]:
        r = self.session.get(f"{self.db_url}/{doc_id}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def _save(self, doc: Dict) -> Dict:
        if "_id" in doc:
            r = self.session.put(f"{self.db_url}/{doc['_id']}", json=doc)
        else:
            r = self.session.post(self.db_url, json=doc)
        r.raise_for_status()
        return r.json()

    def _delete_doc(self, doc_id: str) -> None:
        doc = self._get(doc_id)
        if doc:
            self.session.delete(
                f"{self.db_url}/{doc_id}",
                params={"rev": doc["_rev"]},
            )

    # -------- Seeding --------

    def _table_to_docs(self, table: str, rows: List[Dict]) -> List[Dict]:
        docs = []
        for row in rows:
            doc = {"type": table, **row}
            # Generuj _id z pola klucza głównego
            pk = f"{table[:-1]}_id" if table.endswith("s") else f"{table}_id"
            # Obsłuż wyjątki w nazewnictwie
            pk_map = {
                "order_items": "order_item_id",
                "stores": "store_id",
                "categories": "category_id",
                "suppliers": "supplier_id",
                "employees": "employee_id",
                "customers": "customer_id",
                "products": "product_id",
                "orders": "order_id",
                "inventory": "inventory_id",
                "payments": "payment_id",
            }
            primary_key = pk_map.get(table)
            if primary_key and primary_key in row:
                doc["_id"] = f"{table}-{row[primary_key]}"
            docs.append(doc)
        return docs

    def seed(self, data: Dict[str, List[Dict]], progress_callback=None) -> None:
        cb = progress_callback or (lambda t, d, tot: None)
        order = ["stores", "categories", "suppliers", "employees",
                 "customers", "products", "orders", "order_items",
                 "inventory", "payments"]
        total = sum(len(v) for v in data.values())
        for table in order:
            rows = data.get(table, [])
            cb(table, len(rows), total)
            docs = self._table_to_docs(table, rows)
            self._bulk_save(docs)

    # ===================== CREATE =====================

    def C1_add_single_product(self, product: Dict) -> None:
        doc = {"type": "products", "_id": f"products-{product['product_id']}", **product}
        self._save(doc)

    def C2_add_bulk_products(self, products: List[Dict]) -> None:
        docs = [{"type": "products", "_id": f"products-{p['product_id']}", **p}
                for p in products]
        self._bulk_save(docs)

    def C3_add_customer(self, customer: Dict) -> None:
        doc = {"type": "customers", "_id": f"customers-{customer['customer_id']}", **customer}
        self._save(doc)

    def C4_add_order(self, order: Dict) -> None:
        doc = {"type": "orders", "_id": f"orders-{order['order_id']}", **order}
        self._save(doc)

    def C5_add_order_items(self, items: List[Dict]) -> None:
        docs = [{"type": "order_items", "_id": f"order_items-{i['order_item_id']}", **i}
                for i in items]
        self._bulk_save(docs)

    def C6_add_inventory_records(self, records: List[Dict]) -> None:
        docs = [{"type": "inventory", "_id": f"inventory-{r['inventory_id']}", **r}
                for r in records]
        self._bulk_save(docs)

    # ===================== READ =====================

    def R1_get_product_by_id(self, product_id: int) -> Any:
        return self._get(f"products-{product_id}")

    def R2_get_products_by_category(self, category_id: int) -> List:
        return self._find({"type": "products", "category_id": category_id}, limit=10000)

    def R3_get_customer_orders(self, customer_id: int) -> List:
        return self._find({"type": "orders", "customer_id": customer_id}, limit=10000)

    def R4_get_order_details(self, order_id: int) -> Any:
        order = self._get(f"orders-{order_id}")
        items = self._find({"type": "order_items", "order_id": order_id}, limit=1000)
        result = {"order": order, "items": []}
        for item in items:
            product = self._get(f"products-{item.get('product_id')}")
            result["items"].append({**item, "product_name": product.get("name") if product else None})
        return result

    def R5_get_top_expensive_products(self, limit: int = 10) -> List:
        return self._find(
            {"type": "products", "price": {"$gt": 0}},
            sort=[{"price": "desc"}],
            limit=limit,
        )

    def R6_get_inventory(self) -> List:
        return self._find_all({"type": "inventory"})

    # ===================== UPDATE =====================

    def U1_update_product_price(self, product_id: int, new_price: float) -> None:
        doc = self._get(f"products-{product_id}")
        if doc:
            doc["price"] = new_price
            self.session.put(f"{self.db_url}/products-{product_id}", json=doc).raise_for_status()

    def U2_update_customer(self, customer_id: int, data: Dict) -> None:
        doc = self._get(f"customers-{customer_id}")
        if doc:
            doc.update(data)
            self.session.put(f"{self.db_url}/customers-{customer_id}", json=doc).raise_for_status()

    def U3_update_inventory(self, product_id: int, store_id: int, qty: int) -> None:
        docs = self._find({"type": "inventory", "product_id": product_id,
                           "store_id": store_id}, limit=1)
        if docs:
            doc = docs[0]
            doc["quantity"] = qty
            self.session.put(f"{self.db_url}/{doc['_id']}", json=doc).raise_for_status()

    def U4_bulk_update_product_prices(self, category_id: int, discount_pct: float) -> None:
        docs = self._find({"type": "products", "category_id": category_id}, limit=100000)
        factor = 1 - discount_pct / 100
        for doc in docs:
            doc["price"] = round(doc["price"] * factor, 2)
        self._bulk_save(docs)

    def U5_update_order_status(self, order_id: int, status: str) -> None:
        doc = self._get(f"orders-{order_id}")
        if doc:
            doc["status"] = status
            self.session.put(f"{self.db_url}/orders-{order_id}", json=doc).raise_for_status()

    def U6_update_supplier(self, supplier_id: int, data: Dict) -> None:
        doc = self._get(f"suppliers-{supplier_id}")
        if doc:
            doc.update(data)
            self.session.put(f"{self.db_url}/suppliers-{supplier_id}", json=doc).raise_for_status()

    # ===================== DELETE =====================

    def D1_delete_product(self, product_id: int) -> None:
        self._delete_doc(f"products-{product_id}")

    def D2_delete_customer(self, customer_id: int) -> None:
        self._delete_doc(f"customers-{customer_id}")

    def D3_delete_order(self, order_id: int) -> None:
        self._delete_doc(f"orders-{order_id}")
        # Usuń też powiązane pozycje i płatność
        items = self._find({"type": "order_items", "order_id": order_id}, limit=1000)
        payms = self._find({"type": "payments",    "order_id": order_id}, limit=100)
        to_del = [{"_id": d["_id"], "_rev": d["_rev"], "_deleted": True}
                  for d in items + payms]
        if to_del:
            self._bulk_save(to_del)

    def D4_delete_order_items(self, order_id: int) -> None:
        docs = self._find({"type": "order_items", "order_id": order_id}, limit=1000)
        to_del = [{"_id": d["_id"], "_rev": d["_rev"], "_deleted": True} for d in docs]
        if to_del:
            self._bulk_save(to_del)

    def D5_bulk_delete_products(self, product_ids: List[int]) -> None:
        to_del = []
        for pid in product_ids:
            doc = self._get(f"products-{pid}")
            if doc:
                to_del.append({"_id": doc["_id"], "_rev": doc["_rev"], "_deleted": True})
        if to_del:
            self._bulk_save(to_del)

    def D6_delete_inventory_records(self, product_id: int) -> None:
        docs = self._find({"type": "inventory", "product_id": product_id}, limit=10000)
        to_del = [{"_id": d["_id"], "_rev": d["_rev"], "_deleted": True} for d in docs]
        if to_del:
            self._bulk_save(to_del)
