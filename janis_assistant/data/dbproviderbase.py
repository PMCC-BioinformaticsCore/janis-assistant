from typing import Type, List, Tuple, Dict, Union
from abc import abstractmethod
from sqlite3 import Connection
from contextlib import contextmanager

from janis_assistant.data.models.base import DatabaseObject


class DbProviderBase:
    def __init__(
        self,
        base_type: Type[DatabaseObject],
        db: Connection,
        tablename: str,
        scopes: Dict[str, str],
    ):

        self.db: Connection = db
        self.tablename = tablename
        self.scopes = scopes
        self.base = base_type

        schema = self.table_schema()
        with self.with_cursor() as cursor:
            cursor.execute(schema)

    @contextmanager
    def with_cursor(self):
        cursor = None
        try:
            cursor = self.db.cursor()
            yield cursor
        finally:
            # Change back up
            if cursor:
                cursor.close()

    def get(
        self, keys: Union[str, List[str]] = "*", where: Tuple[str, List[any]] = None
    ):
        jkeys = ", ".join(keys) if isinstance(keys, list) else keys
        if jkeys == "*":
            keys = [k for _, k in self.base.table_schema()]

        values = []
        whereclauses = []
        if self.scopes:
            scopes = self.scopes.items()
            whereclauses.extend(f"{k} = ?" for k, _ in scopes)
            values.extend(v for _, v in scopes)

        if where:
            whereclauses.append(where[0])
            values.extend(where[1])

        query = f"SELECT {jkeys} FROM {self.tablename}"

        if whereclauses:
            query += f" WHERE {' AND '.join(whereclauses)}"

        with self.with_cursor() as cursor:
            rows = cursor.execute(query, values)

        parsedrows = [self.base.deserialize(keys, r) for r in rows]
        return parsedrows

    def commit(self):
        return self.db.commit()

    def table_schema(self):
        return f"""\
        CREATE TABLE IF NOT EXISTS {self.tablename} (
        {self.base.table_schema()}
        )
        """
