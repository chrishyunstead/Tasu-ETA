from __future__ import annotations

from typing import Iterable, Union


class ApartmentFlagProcessor:
    def __init__(self, db_handler):
        """PostgreSQL에서 address_id 기준 apartment_flag를 조회한다."""
        self.db_handler = db_handler

    @staticmethod
    def _quote(value: object) -> str:
        text = str(value).strip().replace("'", "''")
        return f"'{text}'"

    def apartmentflag_data(self, address_id: Union[str, int, Iterable[object]]):
        """
        address_id 1개 또는 리스트를 받아 apartment_flag를 조회한다.
        app.py에서는 itemdata에서 얻은 address_id 목록을 batch로 넘긴다.
        """
        if isinstance(address_id, (str, int)):
            ids = [address_id]
        else:
            ids = list(address_id or [])

        ids = [str(x).strip() for x in ids if str(x).strip()]
        ids = list(dict.fromkeys(ids))
        if not ids:
            return None

        id_list = ", ".join(self._quote(x) for x in ids)
        query = f"""
        SELECT
            a.address_id,
            b.apartment_flag
        FROM sample_address_master a
        JOIN sample_building_master b
          ON a.building_management_number = b.building_management_number
        WHERE a.address_id IN ({id_list})
        """
        return self.db_handler.fetch_data(query)