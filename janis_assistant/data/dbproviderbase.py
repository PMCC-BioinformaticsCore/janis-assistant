from typing import (
    Type,
    List,
    Tuple,
    Dict,
    Union,
    ContextManager,
    Optional,
    TypeVar,
    Generic,
)
from abc import abstractmethod
from sqlite3 import Connection, Cursor, OperationalError
from contextlib import contextmanager

from janis_core import Logger

from janis_assistant.data.models.base import DatabaseObject

T = TypeVar("T")


class DbProviderBase(Generic[T]):
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
    def with_cursor(self) -> ContextManager[Cursor]:
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
    ) -> Optional[List[T]]:
        jkeys = ", ".join(keys) if isinstance(keys, list) else keys
        if jkeys == "*":
            keys = [t.dbalias for t in self.base.keymap()]
            jkeys = ", ".join(keys) if isinstance(keys, list) else keys

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
            try:
                rows = cursor.execute(query, values).fetchall()
            except OperationalError as e:
                if "readonly database" in str(e):
                    # mfranklin: idk, this sometimes happens. We're doing a select query, idk sqlite3 driver...
                    Logger.debug(
                        "Got readonly error when running query: '{query}', skipping for now"
                    )
                    return None
                elif "locked" in str(e):
                    Logger.debug(
                        "We hit the database at the same time the janis process wrote to it, meh"
                    )
                    return None
                raise

        parsedrows = [self.base.deserialize(keys, r) for r in rows]
        return parsedrows

    def commit(self):
        return self.db.commit()

    def get_primary_keys(self):
        pkeys = [t.dbalias for t in self.base.keymap() if t.is_primary]
        if len(pkeys) == 0:
            pkeys = ["id"]
        return pkeys

    def table_schema(self):

        tschema = self.base.table_schema()
        if tschema.strip()[-1] != ",":
            tschema += ","
        pkeys = self.get_primary_keys()

        return f"""\
        CREATE TABLE IF NOT EXISTS {self.tablename} (
            {tschema}
            PRIMARY KEY({', '.join(pkeys)})
        )
        """

    def insert_or_update_many(self, els: List[T]):
        queries: Dict[str, List[List[any]]] = {}
        update_separator = ",\n"
        tab = "\t"

        pkeys = set(self.get_primary_keys())
        pkeys_ordered = list(pkeys)
        existing_keys = set()  # (*pkeys_ordered)

        # get all primary keys
        prows = f"SELECT {', '.join(pkeys_ordered)} FROM {self.tablename}"

        with self.with_cursor() as cursor:
            rows = cursor.execute(prows).fetchall()
            for row in rows:
                existing_keys.add(row)

        dbalias_map = {t.dbalias: t.name for t in self.base.keymap()}

        for el in els:

            keys, values = el.prepare_insert()
            el_pkeys = [getattr(el, dbalias_map[_k]) for _k in pkeys_ordered]
            missing_pkeys = [
                _k for _k in pkeys_ordered if getattr(el, dbalias_map[_k]) is None
            ]
            if missing_pkeys:
                raise Exception(
                    f"An internal error occurred when updating the {self.tablename} database, "
                    f"the object {repr(el)} was missing the primary keys {', '.join(missing_pkeys)}"
                )
            obj_exists = tuple(el_pkeys) in existing_keys

            pkey_updater = " AND ".join(f"{k_} = ?" for k_ in pkeys_ordered)

            if obj_exists:
                # it exists, we'll update
                keys_np, values_np = [], []
                for k, v in zip(keys, values):
                    if k in pkeys:
                        continue

                    keys_np.append(k)
                    values_np.append(v)

                prepared_statement = f"""
                UPDATE {self.tablename}
                    SET {', '.join(f'{k} = ?' for k in keys_np)}
                WHERE
                    {pkey_updater}
                """
                vtuple = (
                    *values_np,
                    *[getattr(el, dbalias_map[pkey]) for pkey in pkeys_ordered],
                )
            else:
                prepared_statement = f"""
                INSERT INTO {self.tablename}
                    ({', '.join(keys)})
                VALUES
                    ({', '.join(f'?' for _ in keys)});
                """
                vtuple = values

            if prepared_statement in queries:
                queries[prepared_statement].append(vtuple)
            else:
                queries[prepared_statement] = [vtuple]

        with self.with_cursor() as cursor:
            for query, vvalues in queries.items():
                try:
                    Logger.debug(f"Running query: {query}")
                    cursor.executemany(query, vvalues)
                except OperationalError as e:
                    Logger.log_ex(e)

        return True
