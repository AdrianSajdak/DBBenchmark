"""Abstrakcyjna klasa bazowa dla wszystkich implementacji baz danych.
Model danych zgodny z TABELE.txt (10 tabel).

Scenariusze CRUD (24 szt., po 6 na operację):
  CREATE: C1–C6
  READ:   R1–R6
  UPDATE: U1–U6
  DELETE: D1–D6
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseDB(ABC):
    name: str = "BaseDB"

    # ────────── Lifecycle ──────────

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

    # ────────── Indeksy (ocena 4.0) ──────────

    @abstractmethod
    def create_indexes(self) -> None:
        """Tworzy indeksy dodatkowe (poza PK/FK)."""

    @abstractmethod
    def drop_indexes(self) -> None:
        """Usuwa indeksy dodatkowe."""

    # ────────── Seeding ──────────

    @abstractmethod
    def seed(self, data: Dict[str, List[Dict]],
             progress_callback=None) -> None:
        """Wstawia wygenerowany zbiór danych do bazy."""

    # ==================== CREATE ====================

    @abstractmethod
    def C1_add_single_alkohol(self, product: Dict) -> None:
        """C1 – Dodanie pojedynczego produktu alkoholowego."""

    @abstractmethod
    def C2_add_bulk_alkohol(self, products: List[Dict]) -> None:
        """C2 – Dodanie hurtowe 1000 produktów alkoholowych."""

    @abstractmethod
    def C3_add_klient(self, klient: Dict) -> None:
        """C3 – Dodanie nowego klienta."""

    @abstractmethod
    def C4_add_paragon(self, paragon: Dict) -> None:
        """C4 – Dodanie nowego paragonu."""

    @abstractmethod
    def C5_add_pozycje_paragonu(self, pozycje: List[Dict]) -> None:
        """C5 – Dodanie pozycji do paragonu."""

    @abstractmethod
    def C6_add_faktura(self, faktura: Dict) -> None:
        """C6 – Dodanie nowej faktury."""

    # ==================== READ ====================

    @abstractmethod
    def R1_get_alkohol_by_id(self, produkt_id: int) -> Any:
        """R1 – Pobranie produktu alkoholowego po ID."""

    @abstractmethod
    def R2_get_products_by_kategoria(self, kategoria_id: int) -> List:
        """R2 – Lista produktów alkoholowych z danej kategorii."""

    @abstractmethod
    def R3_get_paragony_klienta(self, klient_id: int) -> List:
        """R3 – Historia paragonów klienta."""

    @abstractmethod
    def R4_get_paragon_details(self, paragon_id: int) -> Any:
        """R4 – Szczegóły paragonu z pozycjami (JOIN)."""

    @abstractmethod
    def R5_get_top_expensive_alkohol(self, limit: int = 10) -> List:
        """R5 – Top 10 najdroższych produktów alkoholowych."""

    @abstractmethod
    def R6_get_faktury_by_okres(self, data_od: str, data_do: str) -> List:
        """R6 – Wyszukanie faktur z danego okresu."""

    # ==================== UPDATE ====================

    @abstractmethod
    def U1_update_cena_alkohol(self, produkt_id: int, nowa_cena: float) -> None:
        """U1 – Aktualizacja ceny produktu alkoholowego."""

    @abstractmethod
    def U2_update_klient(self, klient_id: int, data: Dict) -> None:
        """U2 – Aktualizacja danych klienta."""

    @abstractmethod
    def U3_update_status_paragonu(self, paragon_id: int, status: str) -> None:
        """U3 – Zmiana statusu transakcji paragonu."""

    @abstractmethod
    def U4_bulk_update_ceny_kategoria(self, kategoria_id: int,
                                       rabat_pct: float) -> None:
        """U4 – Masowa aktualizacja cen produktów w kategorii (promocja)."""

    @abstractmethod
    def U5_update_status_dostawy(self, dostawa_id: int, status: str) -> None:
        """U5 – Aktualizacja statusu dostawy."""

    @abstractmethod
    def U6_update_producent(self, producent_id: int, data: Dict) -> None:
        """U6 – Aktualizacja danych producenta."""

    # ==================== DELETE ====================

    @abstractmethod
    def D1_delete_alkohol(self, produkt_id: int) -> None:
        """D1 – Usunięcie pojedynczego produktu alkoholowego."""

    @abstractmethod
    def D2_delete_klient(self, klient_id: int) -> None:
        """D2 – Usunięcie klienta."""

    @abstractmethod
    def D3_delete_paragon(self, paragon_id: int) -> None:
        """D3 – Usunięcie paragonu (kaskadowe usunięcie pozycji)."""

    @abstractmethod
    def D4_delete_pozycje_paragonu(self, paragon_id: int) -> None:
        """D4 – Usunięcie pozycji paragonu."""

    @abstractmethod
    def D5_bulk_delete_tyton(self, produkt_ids: List[int]) -> None:
        """D5 – Masowe usunięcie grupy produktów tytoniowych."""

    @abstractmethod
    def D6_delete_dostawa(self, dostawa_id: int) -> None:
        """D6 – Usunięcie dostawy."""
