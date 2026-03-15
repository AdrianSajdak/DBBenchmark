"""CouchDB – implementacja CRUD dla modelu TABELE.txt (dokument NoSQL)."""
import json
from typing import Any, Dict, List, Optional

import requests
from requests.auth import HTTPBasicAuth

from db.base_db import BaseDB

BULK_BATCH = 500


class CouchDBDB(BaseDB):
    name = "CouchDB"

    def __init__(self, config):
        self.cfg = config
        self.base_url = config.url.rstrip("/")
        self.db_url = f"{self.base_url}/{config.database}"
        self.auth = HTTPBasicAuth(config.username, config.password)
        self.session = None

    # ────────── Lifecycle ──────────

    def connect(self) -> bool:
        try:
            self.session = requests.Session()
            self.session.auth = self.auth
            self.session.headers.update({"Content-Type": "application/json"})
            r = self.session.get(self.base_url)
            r.raise_for_status()
            return True
        except Exception as e:
            print(f"[CouchDB] Błąd połączenia: {e}")
            return False

    def disconnect(self) -> None:
        if self.session:
            self.session.close()

    def setup_schema(self) -> None:
        r = self.session.put(self.db_url)
        # 412 = already exists – OK
        if r.status_code not in (201, 202, 412):
            r.raise_for_status()

    def clear_data(self) -> None:
        self.session.delete(self.db_url)
        self.session.put(self.db_url)
        self._ensure_indexes()

    def drop_schema(self) -> None:
        self.session.delete(self.db_url)

    # ────────── Indeksy ──────────

    def _ensure_indexes(self) -> None:
        """Tworzy indeksy Mango potrzebne w scenariuszach."""
        indexes = [
            {"index": {"fields": ["type"]}, "name": "idx_type"},
            {"index": {"fields": ["type", "kategoria_id"]}, "name": "idx_type_kat"},
            {"index": {"fields": ["type", "klient_id"]}, "name": "idx_type_klient"},
            {"index": {"fields": ["type", "paragon_id"]}, "name": "idx_type_paragon"},
            {"index": {"fields": ["type", "cena_producenta"]}, "name": "idx_type_cena"},
            {"index": {"fields": ["type", "data_wystawienia"]}, "name": "idx_type_data_fak"},
            {"index": {"fields": ["type", "data_zamowienia"]}, "name": "idx_type_data_dos"},
            {"index": {"fields": ["type", "producent_id"]}, "name": "idx_type_producent"},
            {"index": {"fields": ["type", "miasto"]}, "name": "idx_type_miasto"},
            {"index": {"fields": ["type", "status_klienta"]}, "name": "idx_type_status"},
        ]
        for idx in indexes:
            self.session.post(f"{self.db_url}/_index", json=idx)

    def create_indexes(self) -> None:
        self._ensure_indexes()

    def drop_indexes(self) -> None:
        r = self.session.get(f"{self.db_url}/_index")
        if r.status_code == 200:
            for idx in r.json().get("indexes", []):
                ddoc = idx.get("ddoc")
                name = idx.get("name")
                if ddoc and name and name != "_all_docs":
                    self.session.delete(
                        f"{self.db_url}/_index/{ddoc}/json/{name}")

    # ────────── Helpers ──────────

    def _doc_id(self, doc_type: str, eid) -> str:
        return f"{doc_type}:{eid}"

    def _put_doc(self, doc: Dict) -> Dict:
        doc_id = doc["_id"]
        r = self.session.put(f"{self.db_url}/{doc_id}", json=doc)
        return r.json()

    def _get_doc(self, doc_id: str) -> Optional[Dict]:
        r = self.session.get(f"{self.db_url}/{doc_id}")
        if r.status_code == 200:
            return r.json()
        return None

    def _delete_doc(self, doc_id: str) -> bool:
        doc = self._get_doc(doc_id)
        if doc:
            r = self.session.delete(
                f"{self.db_url}/{doc_id}", params={"rev": doc["_rev"]})
            return r.status_code in (200, 202)
        return False

    def _bulk_docs(self, docs: List[Dict]) -> None:
        for i in range(0, len(docs), BULK_BATCH):
            batch = docs[i: i + BULK_BATCH]
            self.session.post(
                f"{self.db_url}/_bulk_docs", json={"docs": batch})

    def _mango_find(self, selector: Dict, fields: List[str] = None,
                    sort: List = None, limit: int = None) -> List[Dict]:
        body: Dict[str, Any] = {"selector": selector}
        if fields:
            body["fields"] = fields
        if sort:
            body["sort"] = sort
        if limit:
            body["limit"] = limit
        else:
            body["limit"] = 200
        r = self.session.post(f"{self.db_url}/_find", json=body)
        if r.status_code == 200:
            return r.json().get("docs", [])
        return []

    def _table_to_type(self, table: str) -> str:
        return table

    def _row_to_doc(self, table: str, row: Dict, pk_field: str) -> Dict:
        doc = dict(row)
        doc["type"] = table
        doc["_id"] = self._doc_id(table, row[pk_field])
        return doc

    # ────────── Seeding ──────────

    _PK_MAP = {
        "kategorie_produktow": "kategoria_id",
        "producenci": "producent_id",
        "pracownicy": "pracownik_id",
        "klienci": "klient_id",
        "alkohol": "produkt_id",
        "tyton": "produkt_id",
        "paragony": "paragon_id",
        "pozycje_paragonu": "pozycja_id",
        "faktury": "faktura_id",
        "dostawy": "dostawa_id",
    }

    def seed(self, data: Dict[str, List[Dict]], progress_callback=None) -> None:
        from config import SEED_ORDER
        cb = progress_callback or (lambda t, d, tot: None)
        total = sum(len(v) for v in data.values())
        for table in SEED_ORDER:
            rows = data.get(table, [])
            cb(table, len(rows), total)
            pk = self._PK_MAP[table]
            docs = [self._row_to_doc(table, r, pk) for r in rows]
            self._bulk_docs(docs)

    # ==================== CREATE ====================

    def C1_add_single_alkohol(self, product: Dict) -> None:
        doc = self._row_to_doc("alkohol", product, "produkt_id")
        self._put_doc(doc)

    def C2_add_bulk_alkohol(self, products: List[Dict]) -> None:
        docs = [self._row_to_doc("alkohol", p, "produkt_id") for p in products]
        self._bulk_docs(docs)

    def C3_add_klient(self, klient: Dict) -> None:
        doc = self._row_to_doc("klienci", klient, "klient_id")
        self._put_doc(doc)

    def C4_add_paragon(self, paragon: Dict) -> None:
        doc = self._row_to_doc("paragony", paragon, "paragon_id")
        self._put_doc(doc)

    def C5_add_pozycje_paragonu(self, pozycje: List[Dict]) -> None:
        docs = [self._row_to_doc("pozycje_paragonu", p, "pozycja_id")
                for p in pozycje]
        self._bulk_docs(docs)

    def C6_add_faktura(self, faktura: Dict) -> None:
        doc = self._row_to_doc("faktury", faktura, "faktura_id")
        self._put_doc(doc)

    # ==================== READ ====================

    def R1_get_alkohol_by_id(self, produkt_id: int) -> Any:
        return self._get_doc(self._doc_id("alkohol", produkt_id))

    def R2_get_products_by_kategoria(self, kategoria_id: int) -> List:
        return self._mango_find(
            {"type": "alkohol", "kategoria_id": kategoria_id})

    def R3_get_paragony_klienta(self, klient_id: int) -> List:
        return self._mango_find(
            {"type": "paragony", "klient_id": klient_id})

    def R4_get_paragon_details(self, paragon_id: int) -> Any:
        paragon = self._get_doc(self._doc_id("paragony", paragon_id))
        pozycje = self._mango_find(
            {"type": "pozycje_paragonu", "paragon_id": paragon_id},
            limit=50)
        return {"paragon": paragon, "pozycje": pozycje}

    def R5_get_top_expensive_alkohol(self, limit: int = 10) -> List:
        return self._mango_find(
            {"type": "alkohol", "cena_producenta": {"$gt": 0}},
            sort=[{"cena_producenta": "desc"}],
            limit=limit)

    def R6_get_faktury_by_okres(self, data_od: str, data_do: str) -> List:
        return self._mango_find({
            "type": "faktury",
            "data_wystawienia": {"$gte": data_od, "$lte": data_do},
        })

    # ==================== UPDATE ====================

    def _update_doc(self, doc_id: str, updates: Dict) -> None:
        doc = self._get_doc(doc_id)
        if doc:
            doc.update(updates)
            self.session.put(f"{self.db_url}/{doc_id}", json=doc)

    def U1_update_cena_alkohol(self, produkt_id: int, nowa_cena: float) -> None:
        self._update_doc(
            self._doc_id("alkohol", produkt_id),
            {"cena_producenta": nowa_cena})

    def U2_update_klient(self, klient_id: int, data: Dict) -> None:
        self._update_doc(
            self._doc_id("klienci", klient_id),
            {"email": data["email"], "telefon": data["telefon"]})

    def U3_update_status_paragonu(self, paragon_id: int, status: str) -> None:
        self._update_doc(
            self._doc_id("paragony", paragon_id),
            {"status_transakcji": status})

    def U4_bulk_update_ceny_kategoria(self, kategoria_id: int,
                                       rabat_pct: float) -> None:
        docs = self._mango_find(
            {"type": "alkohol", "kategoria_id": kategoria_id},
            limit=100_000)
        factor = 1 - rabat_pct / 100
        updated = []
        for doc in docs:
            doc["cena_producenta"] = round(doc["cena_producenta"] * factor, 2)
            updated.append(doc)
        if updated:
            self._bulk_docs(updated)

    def U5_update_status_dostawy(self, dostawa_id: int, status: str) -> None:
        self._update_doc(
            self._doc_id("dostawy", dostawa_id),
            {"status_dostawy": status})

    def U6_update_producent(self, producent_id: int, data: Dict) -> None:
        self._update_doc(
            self._doc_id("producenci", producent_id),
            {"nazwa": data["nazwa"], "strona_www": data["strona_www"]})

    # ==================== DELETE ====================

    def D1_delete_alkohol(self, produkt_id: int) -> None:
        self._delete_doc(self._doc_id("alkohol", produkt_id))

    def D2_delete_klient(self, klient_id: int) -> None:
        self._delete_doc(self._doc_id("klienci", klient_id))

    def D3_delete_paragon(self, paragon_id: int) -> None:
        # Kaskadowe usunięcie pozycji
        pozycje = self._mango_find(
            {"type": "pozycje_paragonu", "paragon_id": paragon_id},
            limit=50)
        for poz in pozycje:
            if "_id" in poz and "_rev" in poz:
                self.session.delete(
                    f"{self.db_url}/{poz['_id']}",
                    params={"rev": poz["_rev"]})
        self._delete_doc(self._doc_id("paragony", paragon_id))

    def D4_delete_pozycje_paragonu(self, paragon_id: int) -> None:
        pozycje = self._mango_find(
            {"type": "pozycje_paragonu", "paragon_id": paragon_id},
            limit=50)
        for poz in pozycje:
            if "_id" in poz and "_rev" in poz:
                self.session.delete(
                    f"{self.db_url}/{poz['_id']}",
                    params={"rev": poz["_rev"]})

    def D5_bulk_delete_tyton(self, produkt_ids: List[int]) -> None:
        for pid in produkt_ids:
            self._delete_doc(self._doc_id("tyton", pid))

    def D6_delete_dostawa(self, dostawa_id: int) -> None:
        self._delete_doc(self._doc_id("dostawy", dostawa_id))
