"""Abstrakcyjna klasa bazowa dla wszystkich implementacji baz danych."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseDB(ABC):
    name: str = "BaseDB"

    # ---------- Lifecycle ----------

    @abstractmethod
    def connect(self) -> bool:
        """Nawiązuje połączenie. Zwraca True jeśli sukces."""

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def setup_schema(self) -> None:
        """Tworzy tabele / kolekcje / klucze. Idempotentne."""

    @abstractmethod
    def clear_data(self) -> None:
        """Usuwa wszystkie dane (nie strukturę)."""

    @abstractmethod
    def drop_schema(self) -> None:
        """Usuwa tabele / kolekcje / klucze."""

    # ---------- Seeding ----------

    @abstractmethod
    def seed(self, data: Dict[str, List[Dict]],
             progress_callback=None) -> None:
        """Wstawia wygenerowany zbiór danych do bazy."""

    # ===================== CREATE =====================

    @abstractmethod
    def C1_add_single_product(self, product: Dict) -> None:
        """C1 – Dodanie pojedynczego produktu."""

    @abstractmethod
    def C2_add_bulk_products(self, products: List[Dict]) -> None:
        """C2 – Dodanie 1000 produktów jednocześnie."""

    @abstractmethod
    def C3_add_customer(self, customer: Dict) -> None:
        """C3 – Dodanie nowego klienta."""

    @abstractmethod
    def C4_add_order(self, order: Dict) -> None:
        """C4 – Dodanie nowego zamówienia."""

    @abstractmethod
    def C5_add_order_items(self, items: List[Dict]) -> None:
        """C5 – Dodanie kilku pozycji zamówienia."""

    @abstractmethod
    def C6_add_inventory_records(self, records: List[Dict]) -> None:
        """C6 – Dodanie rekordów magazynowych."""

    # ===================== READ =====================

    @abstractmethod
    def R1_get_product_by_id(self, product_id: int) -> Any:
        """R1 – Pobranie produktu po ID."""

    @abstractmethod
    def R2_get_products_by_category(self, category_id: int) -> List:
        """R2 – Lista produktów z danej kategorii."""

    @abstractmethod
    def R3_get_customer_orders(self, customer_id: int) -> List:
        """R3 – Historia zamówień klienta."""

    @abstractmethod
    def R4_get_order_details(self, order_id: int) -> Any:
        """R4 – Szczegóły zamówienia (JOIN z order_items i products)."""

    @abstractmethod
    def R5_get_top_expensive_products(self, limit: int = 10) -> List:
        """R5 – Najdroższe produkty (sortowanie malejące po cenie)."""

    @abstractmethod
    def R6_get_inventory(self) -> List:
        """R6 – Pełny stan magazynowy."""

    # ===================== UPDATE =====================

    @abstractmethod
    def U1_update_product_price(self, product_id: int, new_price: float) -> None:
        """U1 – Aktualizacja ceny produktu."""

    @abstractmethod
    def U2_update_customer(self, customer_id: int, data: Dict) -> None:
        """U2 – Aktualizacja danych klienta."""

    @abstractmethod
    def U3_update_inventory(self, product_id: int, store_id: int, qty: int) -> None:
        """U3 – Aktualizacja stanu magazynowego."""

    @abstractmethod
    def U4_bulk_update_product_prices(self, category_id: int,
                                      discount_pct: float) -> None:
        """U4 – Masowa promocja cen produktów z kategorii."""

    @abstractmethod
    def U5_update_order_status(self, order_id: int, status: str) -> None:
        """U5 – Zmiana statusu zamówienia."""

    @abstractmethod
    def U6_update_supplier(self, supplier_id: int, data: Dict) -> None:
        """U6 – Aktualizacja danych dostawcy."""

    # ===================== DELETE =====================

    @abstractmethod
    def D1_delete_product(self, product_id: int) -> None:
        """D1 – Usunięcie pojedynczego produktu."""

    @abstractmethod
    def D2_delete_customer(self, customer_id: int) -> None:
        """D2 – Usunięcie klienta."""

    @abstractmethod
    def D3_delete_order(self, order_id: int) -> None:
        """D3 – Usunięcie zamówienia."""

    @abstractmethod
    def D4_delete_order_items(self, order_id: int) -> None:
        """D4 – Usunięcie pozycji zamówienia."""

    @abstractmethod
    def D5_bulk_delete_products(self, product_ids: List[int]) -> None:
        """D5 – Masowe usunięcie grupy produktów."""

    @abstractmethod
    def D6_delete_inventory_records(self, product_id: int) -> None:
        """D6 – Usunięcie wpisów magazynowych dla produktu."""
