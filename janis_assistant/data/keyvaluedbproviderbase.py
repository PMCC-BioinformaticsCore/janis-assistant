from typing import Type, Dict, Optional, Set, List, Tuple
from sqlite3 import Connection

from janis_core import Logger

from janis_assistant.data.dbproviderbase import DbBase
from janis_assistant.data.models.base import KVDatabaseObject, deserialize_inner


class KvDB(DbBase):

    attributes_to_persist = {}

    def __init__(
        self,
        db: Connection,
        readonly: bool,
        tablename: str,
        scopes: Dict[str, str],
        scopes_keys: Optional[Set] = None,
    ):

        super().__init__(db=db, tablename=tablename, readonly=readonly)

        self._scopes = scopes or {}
        self._scopes_keys = sorted(scopes_keys or self._scopes.keys())

        schema = self.schema()
        with self.with_cursor() as cursor:
            cursor.execute(schema)

    def schema(self):
        scopes = ",\n".join(f"    {k} STRING NOT NULL" for k in self._scopes_keys)
        return f"""
CREATE TABLE IF NOT EXISTS {self._tablename} (
    id STRING NOT NULL,
{scopes},
    value BINARY,
    PRIMARY KEY (id, {', '.join(self._scopes_keys)})
);
"""

    def save_encoded_rows(self, rows: List[Tuple[str, str]]):
        if len(rows) == 0:
            return Logger.debug(
                f"Skipping save of {self.__class__.__name__} for empty set of changes"
            )

        keys = ["id", *self._scopes_keys, "value"]
        scopes = list(self._scopes[s] for s in self._scopes_keys)

        fvalues = []
        inner_qs = ", ".join("?" for _ in range(len(keys)))
        replacer = ",\n    ".join(f"({inner_qs})" for _ in range(len(rows)))
        for row in rows:
            fvalues.extend([row[0], *scopes, row[1]])

        if not self._readonly:

            query = f"""
    REPLACE INTO {self._tablename}
        ({', '.join(keys)})
    VALUES
        {replacer}
    """
            with self.with_cursor() as cursor:
                try:
                    cursor.execute(query, fvalues)
                except Exception as e:
                    Logger.critical("Error executing query: " + query)
                    raise e

    def get_rows(self):
        scopes = " AND ".join(f"{k} = ?" for k in self._scopes_keys)
        scope_values = [self._scopes[k] for k in self._scopes_keys]
        query = f"SELECT id, value FROM {self._tablename} WHERE {scopes}"
        with self.with_cursor() as cursor:
            rows = cursor.execute(query, scope_values).fetchall()

        return rows
