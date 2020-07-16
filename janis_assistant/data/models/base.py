import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Union

from janis_core import Logger


class DatabaseObjectField:
    def __init__(
        self, name, dbalias=None, is_primary=False, encode=False, is_id_key=False
    ):
        """
        :param is_id_key: Sometimes it can't be an primary key, but it's worth trying to match on it.
        """
        self.name = name
        self.dbalias = dbalias or name
        self.is_primary = is_primary
        self.encode = encode
        self.is_id_key = is_id_key


class DatabaseObject(ABC):
    def __repr__(self):
        fields = ", ".join(str(getattr(self, k)) for k, _ in self.keymap())
        return f"{self.__class__.__name__}({fields})"

    @classmethod
    @abstractmethod
    def keymap(cls) -> List[DatabaseObjectField]:
        pass

    @classmethod
    @abstractmethod
    def table_schema(cls):
        pass

    def prepare_insert(self):
        keys = []
        values = []

        for t in self.keymap():
            objkey, dbkey = t.name, t.dbalias
            val = getattr(self, objkey)
            if val is None:
                continue
            keys.append(dbkey)
            values.append(DatabaseObject.serialize(val))

        return keys, values

    @classmethod
    def deserialize(cls, keys: List[str], row: List, **kwargs):
        if len(keys) != len(row):
            raise Exception(
                f"Expected same number of keys ({len(keys)}) as items in the row ({len(row)}): "
                f"\n\tKeys: {str(keys)}\n\tRow: {str(row)}"
            )

        km = cls.keymap()
        rkeymap = {t.dbalias: t.name for t in cls.keymap()}
        dbalias_to_decode = {t.dbalias for t in km if t.encode}

        initdict = {
            rkeymap[dbalias]: cls.deserialize_inner(value)
            if dbalias in dbalias_to_decode
            else value
            for dbalias, value in zip(keys, row)
        }

        return cls(**initdict)

    @staticmethod
    def serialize(val):
        if val is None:
            return None
        elif isinstance(val, (str, float, bool, int)):
            return val
        elif isinstance(val, datetime):
            return str(val)
        elif isinstance(val, Enum):
            return val.value
        elif isinstance(val, list):
            return json.dumps([DatabaseObject.serialize(el) for el in val])
        elif isinstance(val, dict):
            return json.dumps({k: DatabaseObject.serialize(v) for k, v in val.items()})

        return json.dumps(val)

    @staticmethod
    def deserialize_inner(val):
        if val is None:
            return None
        try:
            return json.loads(val)
        except Exception as ex:
            Logger.debug(
                f"Couldn't deserialize value, using string representation instead (value: {repr(val)}): {repr(ex)}"
            )
            return str(val)
