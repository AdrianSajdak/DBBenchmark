"""Redis – implementacja CRUD dla modelu TABELE.txt (key-value + indeksy pomocnicze)."""
from typing import Any, Dict, List, Optional

import redis as redis_lib

from db.base_db import BaseDB

PIPELINE_BATCH = 500
NS = "sb:"  # sklep_benchmark


class RedisDB(BaseDB):
    name = "Redis"

    def __init__(self, config):
        self.cfg = config
        self.r: Optional[redis_lib.Redis] = None

    # ────────── Lifecycle ──────────

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
        pass  # Redis nie wymaga schematu

    def clear_data(self) -> None:
        cursor = "0"
        while cursor != 0:
            cursor, keys = self.r.scan(cursor=cursor, match=f"{NS}*", count=1000)
            if keys:
                self.r.delete(*keys)

    def drop_schema(self) -> None:
        self.clear_data()

    # ────────── Konwencje kluczy ──────────
    # {NS}alkohol:{id}            HSET
    # {NS}tyton:{id}              HSET
    # {NS}kategoria:{id}          HSET
    # {NS}producent:{id}          HSET
    # {NS}pracownik:{id}          HSET
    # {NS}klient:{id}             HSET
    # {NS}paragon:{id}            HSET
    # {NS}pozycja:{id}            HSET
    # {NS}faktura:{id}            HSET
    # {NS}dostawa:{id}            HSET
    #
    # Indeksy:
    # {NS}idx:alk:kat:{kat_id}    SET (produkt_id)
    # {NS}idx:alk:cena            ZSET (score=cena, member=produkt_id)
    # {NS}idx:tyt:kat:{kat_id}    SET (produkt_id)
    # {NS}idx:par:klient:{kl_id}  SET (paragon_id)
    # {NS}idx:poz:paragon:{p_id}  SET (pozycja_id)
    # {NS}idx:fak:data             ZSET (score=data_yyyymmdd, member=faktura_id)
    # {NS}idx:dos:producent:{p_id} SET (dostawa_id)

    def _pk(self, entity: str, eid) -> str:
        return f"{NS}{entity}:{eid}"

    def _hset(self, pipe, entity: str, eid, data: Dict) -> None:
        key = self._pk(entity, eid)
        pipe.hset(key, mapping={k: str(v) if v is not None else "" for k, v in data.items()})

    def _hgetall(self, entity: str, eid) -> Optional[Dict]:
        data = self.r.hgetall(self._pk(entity, eid))
        return data if data else None

    # ────────── Indeksy ──────────

    def create_indexes(self) -> None:
        # Indeksy budowane są przy seedzie, ta metoda jest no-op dla Redis
        pass

    def drop_indexes(self) -> None:
        # Usuwanie zbiorów indeksowych
        cursor = "0"
        while cursor != 0:
            cursor, keys = self.r.scan(cursor=cursor, match=f"{NS}idx:*", count=1000)
            if keys:
                self.r.delete(*keys)

    # ────────── Seeding ──────────

    _TABLE_MAP = {
        "kategorie_produktow": ("kategoria", "kategoria_id"),
        "producenci":          ("producent",  "producent_id"),
        "pracownicy":          ("pracownik",  "pracownik_id"),
        "klienci":             ("klient",     "klient_id"),
        "alkohol":             ("alkohol",    "produkt_id"),
        "tyton":               ("tyton",      "produkt_id"),
        "paragony":            ("paragon",    "paragon_id"),
        "pozycje_paragonu":    ("pozycja",    "pozycja_id"),
        "faktury":             ("faktura",    "faktura_id"),
        "dostawy":             ("dostawa",    "dostawa_id"),
    }

    def _seed_table(self, table: str, rows: List[Dict]) -> None:
        entity, pk = self._TABLE_MAP[table]
        pipe = self.r.pipeline(transaction=False)
        chunk = 0
        for row in rows:
            eid = row[pk]
            self._hset(pipe, entity, eid, row)

            # Buduj indeksy pomocnicze
            if table == "alkohol":
                pipe.sadd(f"{NS}idx:alk:kat:{row['kategoria_id']}", eid)
                pipe.zadd(f"{NS}idx:alk:cena", {str(eid): float(row["cena_producenta"])})
            elif table == "tyton":
                pipe.sadd(f"{NS}idx:tyt:kat:{row['kategoria_id']}", eid)
            elif table == "paragony":
                pipe.sadd(f"{NS}idx:par:klient:{row['klient_id']}", eid)
            elif table == "pozycje_paragonu":
                pipe.sadd(f"{NS}idx:poz:paragon:{row['paragon_id']}", eid)
            elif table == "faktury":
                dw = str(row.get("data_wystawienia", "2024-01-01")).replace("-", "")
                try:
                    score = int(dw)
                except ValueError:
                    score = 20240101
                pipe.zadd(f"{NS}idx:fak:data", {str(eid): score})
            elif table == "dostawy":
                pipe.sadd(f"{NS}idx:dos:producent:{row['producent_id']}", eid)

            chunk += 1
            if chunk >= PIPELINE_BATCH:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)
                chunk = 0
        if chunk:
            pipe.execute()

    def seed(self, data: Dict[str, List[Dict]], progress_callback=None) -> None:
        from config import SEED_ORDER
        cb = progress_callback or (lambda t, d, tot: None)
        total = sum(len(v) for v in data.values())
        for table in SEED_ORDER:
            rows = data.get(table, [])
            cb(table, len(rows), total)
            self._seed_table(table, rows)

    # ==================== CREATE ====================

    def C1_add_single_alkohol(self, product: Dict) -> None:
        pipe = self.r.pipeline()
        eid = product["produkt_id"]
        self._hset(pipe, "alkohol", eid, product)
        pipe.sadd(f"{NS}idx:alk:kat:{product['kategoria_id']}", eid)
        pipe.zadd(f"{NS}idx:alk:cena", {str(eid): float(product["cena_producenta"])})
        pipe.execute()

    def C2_add_bulk_alkohol(self, products: List[Dict]) -> None:
        pipe = self.r.pipeline(transaction=False)
        for i, p in enumerate(products):
            eid = p["produkt_id"]
            self._hset(pipe, "alkohol", eid, p)
            pipe.sadd(f"{NS}idx:alk:kat:{p['kategoria_id']}", eid)
            pipe.zadd(f"{NS}idx:alk:cena", {str(eid): float(p["cena_producenta"])})
            if (i + 1) % PIPELINE_BATCH == 0:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)
        pipe.execute()

    def C3_add_klient(self, klient: Dict) -> None:
        pipe = self.r.pipeline()
        eid = klient["klient_id"]
        self._hset(pipe, "klient", eid, klient)
        pipe.execute()

    def C4_add_paragon(self, paragon: Dict) -> None:
        pipe = self.r.pipeline()
        eid = paragon["paragon_id"]
        self._hset(pipe, "paragon", eid, paragon)
        pipe.sadd(f"{NS}idx:par:klient:{paragon['klient_id']}", eid)
        pipe.execute()

    def C5_add_pozycje_paragonu(self, pozycje: List[Dict]) -> None:
        pipe = self.r.pipeline(transaction=False)
        for i, poz in enumerate(pozycje):
            eid = poz["pozycja_id"]
            self._hset(pipe, "pozycja", eid, poz)
            pipe.sadd(f"{NS}idx:poz:paragon:{poz['paragon_id']}", eid)
            if (i + 1) % PIPELINE_BATCH == 0:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)
        pipe.execute()

    def C6_add_faktura(self, faktura: Dict) -> None:
        pipe = self.r.pipeline()
        eid = faktura["faktura_id"]
        self._hset(pipe, "faktura", eid, faktura)
        dw = str(faktura.get("data_wystawienia", "2024-01-01")).replace("-", "")
        try:
            score = int(dw)
        except ValueError:
            score = 20240101
        pipe.zadd(f"{NS}idx:fak:data", {str(eid): score})
        pipe.execute()

    # ==================== READ ====================

    def R1_get_alkohol_by_id(self, produkt_id: int) -> Any:
        return self._hgetall("alkohol", produkt_id)

    def R2_get_products_by_kategoria(self, kategoria_id: int) -> List:
        ids = self.r.smembers(f"{NS}idx:alk:kat:{kategoria_id}")
        if not ids:
            return []
        pipe = self.r.pipeline(transaction=False)
        for pid in ids:
            pipe.hgetall(f"{NS}alkohol:{pid}")
        return [d for d in pipe.execute() if d]

    def R3_get_paragony_klienta(self, klient_id: int) -> List:
        ids = self.r.smembers(f"{NS}idx:par:klient:{klient_id}")
        if not ids:
            return []
        pipe = self.r.pipeline(transaction=False)
        for pid in ids:
            pipe.hgetall(f"{NS}paragon:{pid}")
        return [d for d in pipe.execute() if d]

    def R4_get_paragon_details(self, paragon_id: int) -> Any:
        paragon = self._hgetall("paragon", paragon_id)
        poz_ids = self.r.smembers(f"{NS}idx:poz:paragon:{paragon_id}")
        pozycje = []
        if poz_ids:
            pipe = self.r.pipeline(transaction=False)
            for pid in poz_ids:
                pipe.hgetall(f"{NS}pozycja:{pid}")
            pozycje = [d for d in pipe.execute() if d]
        return {"paragon": paragon, "pozycje": pozycje}

    def R5_get_top_expensive_alkohol(self, limit: int = 10) -> List:
        entries = self.r.zrevrange(
            f"{NS}idx:alk:cena", 0, limit - 1, withscores=True)
        if not entries:
            return []
        pipe = self.r.pipeline(transaction=False)
        for pid, _ in entries:
            pipe.hgetall(f"{NS}alkohol:{pid}")
        return [d for d in pipe.execute() if d]

    def R6_get_faktury_by_okres(self, data_od: str, data_do: str) -> List:
        score_od = int(data_od.replace("-", ""))
        score_do = int(data_do.replace("-", ""))
        ids = self.r.zrangebyscore(f"{NS}idx:fak:data", score_od, score_do)
        if not ids:
            return []
        pipe = self.r.pipeline(transaction=False)
        for fid in ids:
            pipe.hgetall(f"{NS}faktura:{fid}")
        return [d for d in pipe.execute() if d]

    # ==================== UPDATE ====================

    def U1_update_cena_alkohol(self, produkt_id: int, nowa_cena: float) -> None:
        pipe = self.r.pipeline()
        pipe.hset(self._pk("alkohol", produkt_id), "cena_producenta", str(nowa_cena))
        pipe.zadd(f"{NS}idx:alk:cena", {str(produkt_id): nowa_cena})
        pipe.execute()

    def U2_update_klient(self, klient_id: int, data: Dict) -> None:
        key = self._pk("klient", klient_id)
        self.r.hset(key, mapping={
            "email": data["email"],
            "telefon": data["telefon"],
        })

    def U3_update_status_paragonu(self, paragon_id: int, status: str) -> None:
        self.r.hset(self._pk("paragon", paragon_id), "status_transakcji", status)

    def U4_bulk_update_ceny_kategoria(self, kategoria_id: int,
                                       rabat_pct: float) -> None:
        ids = self.r.smembers(f"{NS}idx:alk:kat:{kategoria_id}")
        factor = 1 - rabat_pct / 100
        pipe = self.r.pipeline(transaction=False)
        for pid in ids:
            key = self._pk("alkohol", pid)
            old = self.r.hget(key, "cena_producenta")
            if old:
                new_price = round(float(old) * factor, 2)
                pipe.hset(key, "cena_producenta", str(new_price))
                pipe.zadd(f"{NS}idx:alk:cena", {str(pid): new_price})
        pipe.execute()

    def U5_update_status_dostawy(self, dostawa_id: int, status: str) -> None:
        self.r.hset(self._pk("dostawa", dostawa_id), "status_dostawy", status)

    def U6_update_producent(self, producent_id: int, data: Dict) -> None:
        key = self._pk("producent", producent_id)
        self.r.hset(key, mapping={
            "nazwa": data["nazwa"],
            "strona_www": data["strona_www"],
        })

    # ==================== DELETE ====================

    def D1_delete_alkohol(self, produkt_id: int) -> None:
        key = self._pk("alkohol", produkt_id)
        kat = self.r.hget(key, "kategoria_id")
        pipe = self.r.pipeline()
        pipe.delete(key)
        if kat:
            pipe.srem(f"{NS}idx:alk:kat:{kat}", produkt_id)
        pipe.zrem(f"{NS}idx:alk:cena", str(produkt_id))
        pipe.execute()

    def D2_delete_klient(self, klient_id: int) -> None:
        self.r.delete(self._pk("klient", klient_id))

    def D3_delete_paragon(self, paragon_id: int) -> None:
        # Kaskadowe usunięcie pozycji
        poz_ids = self.r.smembers(f"{NS}idx:poz:paragon:{paragon_id}")
        pipe = self.r.pipeline()
        for pid in poz_ids:
            pipe.delete(self._pk("pozycja", pid))
        pipe.delete(f"{NS}idx:poz:paragon:{paragon_id}")
        pipe.delete(self._pk("paragon", paragon_id))
        pipe.execute()

    def D4_delete_pozycje_paragonu(self, paragon_id: int) -> None:
        poz_ids = self.r.smembers(f"{NS}idx:poz:paragon:{paragon_id}")
        pipe = self.r.pipeline()
        for pid in poz_ids:
            pipe.delete(self._pk("pozycja", pid))
        pipe.delete(f"{NS}idx:poz:paragon:{paragon_id}")
        pipe.execute()

    def D5_bulk_delete_tyton(self, produkt_ids: List[int]) -> None:
        pipe = self.r.pipeline(transaction=False)
        for pid in produkt_ids:
            key = self._pk("tyton", pid)
            kat = self.r.hget(key, "kategoria_id")
            pipe.delete(key)
            if kat:
                pipe.srem(f"{NS}idx:tyt:kat:{kat}", pid)
        pipe.execute()

    def D6_delete_dostawa(self, dostawa_id: int) -> None:
        self.r.delete(self._pk("dostawa", dostawa_id))
