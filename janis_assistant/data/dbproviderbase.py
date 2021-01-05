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
    Set,
)
from abc import abstractmethod
from sqlite3 import Connection, Cursor, OperationalError
from contextlib import contextmanager

from janis_assistant.utils import second_formatter

from janis_assistant.utils.dateutils import DateUtil
from janis_core import Logger

from janis_assistant.data.models.base import (
    DatabaseObject,
    DatabaseObjectField,
    prep_object_for_db,
)

T = TypeVar("T")


class DbBase:
    def __init__(self, db: Connection, tablename: str, readonly: bool):
        self._db = db
        self._tablename = tablename
        self._readonly = readonly

    def commit(self):
        if self._readonly:
            Logger.critical("Attempting to commit to readonly connection")

        self._db.commit()

    @contextmanager
    def with_cursor(self) -> ContextManager[Cursor]:
        cursor = None
        try:
            cursor = self._db.cursor()
            yield cursor
        finally:
            # Change back up
            if cursor:
                cursor.close()


class DbProviderBase(DbBase, Generic[T]):
    def __init__(
        self,
        base_type: Type[DatabaseObject],
        db: Connection,
        tablename: str,
        readonly: bool,
        scopes: Dict[str, str],
    ):
        super().__init__(db=db, tablename=tablename, readonly=readonly)
        self._scopes = scopes
        self._base = base_type

        self._id_cache: Set = None

        if not self._readonly:
            schema = self.table_schema()
            with self.with_cursor() as cursor:
                cursor.execute(schema)

    def get(
        self,
        keys: Union[str, List[str]] = "*",
        where: Tuple[str, List[any]] = None,
        allow_operational_errors=True,
    ) -> Optional[List[T]]:
        jkeys = ", ".join(keys) if isinstance(keys, list) else keys
        if jkeys == "*":
            keys = [t.dbalias for t in self._base.keymap()]
            jkeys = ", ".join(keys) if isinstance(keys, list) else keys

        values = []
        whereclauses = []
        if self._scopes:
            scopes = self._scopes.items()
            whereclauses.extend(f"{k} = ?" for k, _ in scopes)
            values.extend(v for _, v in scopes)

        if where:
            whereclauses.append(where[0])
            values.extend(where[1])

        query = f"SELECT {jkeys} FROM {self._tablename}"

        if whereclauses:
            query += f" WHERE {' AND '.join(whereclauses)}"

        with self.with_cursor() as cursor:
            try:
                rows = cursor.execute(query, values).fetchall()
            except OperationalError as e:
                if not allow_operational_errors:
                    raise e
                if "readonly database" in str(e):
                    # mfranklin: idk, this sometimes happens. We're doing a select query, idk sqlite3 driver...
                    Logger.debug(
                        f"Got readonly error when running query: '{query}', skipping for now"
                    )
                    return None
                elif "locked" in str(e):
                    Logger.debug(
                        f"We hit the janis database.{self._tablename} at the same time the janis process wrote to it, we'll skip for now "
                    )
                    return None
                raise

        parsedrows = [self._base.deserialize(keys, r) for r in rows]
        return parsedrows

    def get_primary_keys(self):
        pkeys = [t.dbalias for t in self._base.keymap() if t.is_primary]
        # if len(pkeys) == 0:
        # pkeys = ["id"]
        return pkeys

    def get_id_keys(self):
        idkeys = [t.dbalias for t in self._base.keymap() if t.is_id_key or t.is_primary]
        if len(idkeys) == 0:
            idkeys = ["id"]
        return idkeys

    def table_schema(self):

        tschema = self._base.table_schema().strip()

        pkey_schema = ""
        pkeys = self.get_primary_keys()
        has_trailing_comma = tschema[-1] == ","
        if pkeys:
            if not has_trailing_comma:
                tschema += ","
            pkey_schema = f"PRIMARY KEY({', '.join(pkeys)})"
        else:
            if has_trailing_comma:
                tschema = tschema[:-1]

        schema = f"""\
        CREATE TABLE IF NOT EXISTS {self._tablename} (
            {tschema}
            {pkey_schema}
        )
        """
        return schema

    def populate_cache_if_required(self):
        if self._id_cache is not None:
            return False
        self.populate_cache()
        return True

    def populate_cache(self):
        self._id_cache = set()
        idkeys = set(self.get_id_keys())
        idkeys_ordered = list(idkeys)
        prows = f"SELECT {', '.join(idkeys_ordered)} FROM {self._tablename}"
        with self.with_cursor() as cursor:
            Logger.log("Running query: " + str(prows))
            rows = cursor.execute(prows).fetchall()
            for row in rows:
                self._id_cache.add(row)

    def filter_updates(
        self, jobs: List[T], add_inserts_to_cache=True
    ) -> Tuple[List[T], List[T]]:
        self.populate_cache_if_required()

        updates = []
        inserts = []

        idkeys = set(self.get_id_keys())
        idkeys_ordered = list(idkeys)
        dbalias_map: Dict[str, DatabaseObjectField] = {
            t.dbalias: t for t in self._base.keymap()
        }

        prep_el_idkey = lambda job: tuple(
            [
                prep_object_for_db(
                    getattr(job, dbalias_map[_k].name), encode=dbalias_map[_k].encode,
                )
                for _k in idkeys_ordered
            ]
        )

        for job in jobs:
            el_idkey = prep_el_idkey(job)
            if el_idkey in self._id_cache:
                updates.append(job)
            else:
                inserts.append(job)

        for job in inserts:
            el_idkey = prep_el_idkey(job)
            self._id_cache.add(el_idkey)

        return updates, inserts

    def insert_or_update_many(self, els: List[T]):
        if len(els) == 0:
            return
        queries: Dict[str, List[List[any]]] = {}
        update_separator = ",\n"
        tab = "\t"

        idkeys = set(self.get_id_keys())
        idkeys_ordered = list(idkeys)
        pkeys_ordered = self.get_primary_keys()
        existing_keys = set()  # (*pkeys_ordered)

        # get all primary keys

        dbalias_map: Dict[str, DatabaseObjectField] = {
            t.dbalias: t for t in self._base.keymap()
        }

        updates, inserts = self.filter_updates(els)

        def add_query(query, values):
            if query in queries:
                queries[query].append(values)
            else:
                queries[query] = [values]

        for job in updates:
            keys, values = job.prepare_insert()
            # el_pkeys = [getattr(job, dbalias_map[_k]) for _k in idkeys_ordered]

            keys_np, values_np = [], []
            for k, v in zip(keys, values):
                if k in idkeys:
                    continue

                keys_np.append(k)
                values_np.append(v)

            # problem is we want to update matching on some fields when they are NULL, our WHERE statement
            # should be something like:
            #   WHERE id1 = ? AND id2 = ? AND id3 is null AND id4 is null

            id_keyvalues = {
                pkey: prep_object_for_db(
                    getattr(job, dbalias_map[pkey].name),
                    encode=dbalias_map[pkey].encode,
                )
                for pkey in idkeys_ordered
            }
            id_withvalues_keyvalue_ordered = [
                (idkey, idvalue)
                for idkey, idvalue in id_keyvalues.items()
                if idvalue is not None
            ]
            id_withvalues_updater_keys = [
                f"{idkey} = ?" for idkey, _ in id_withvalues_keyvalue_ordered
            ]
            id_withvalues_updater_values = [
                idvalue for _, idvalue in id_withvalues_keyvalue_ordered
            ]
            id_novalues_updater_keys = [
                f"{idkey} is NULL"
                for idkey, idvalue in id_keyvalues.items()
                if idvalue is None
            ]

            prepared_statement = f"""
            UPDATE {self._tablename}
                SET {', '.join(f'{k} = ?' for k in keys_np)}
            WHERE
                {" AND ".join([*id_withvalues_updater_keys, *id_novalues_updater_keys])}
            """
            vtuple = (
                *values_np,
                *id_withvalues_updater_values,
            )

            add_query(prepared_statement, vtuple)

        for job in inserts:
            keys, values = job.prepare_insert()
            # el_pkeys = [getattr(job, dbalias_map[_k]) for _k in idkeys_ordered]
            prepared_statement = f"""
            INSERT INTO {self._tablename}
                ({', '.join(keys)})
            VALUES
                ({', '.join(f'?' for _ in keys)});
            """
            add_query(prepared_statement, values)

        Logger.log(
            f"DB {self._tablename}: Inserting {len(inserts)} and updating {len(updates)} rows"
        )
        with self.with_cursor() as cursor:
            start = DateUtil.now()
            if len(inserts) + len(updates) > 300:
                Logger.warn(
                    f"DB '{self._tablename}' is inserting {len(inserts)} and updating {len(updates)} rows, this might take a while"
                )
            for query, vvalues in queries.items():
                try:
                    Logger.log(f"Running query: {query}\n\t: values: {vvalues}")
                    cursor.executemany(query, vvalues)
                except OperationalError as e:
                    Logger.log_ex(e)
            seconds = (DateUtil.now() - start).total_seconds()
            if seconds > 2:
                Logger.warn(
                    f"DB '{self._tablename}' took {second_formatter(seconds)} to insert {len(inserts)} and update {len(updates)} rows"
                )

        return True
