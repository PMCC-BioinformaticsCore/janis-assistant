import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Union

from janis_core import Logger


class DatabaseObject(ABC):
    def __repr__(self):
        fields = ", ".join(str(getattr(self, k)) for k, _ in self.keymap())
        return f"{self.__class__.__name__}({fields})"

    @classmethod
    @abstractmethod
    def keymap(cls) -> List[Union[Tuple[str, str, bool], Tuple[str, str]]]:
        pass

    @classmethod
    @abstractmethod
    def table_schema(cls):
        pass

    def prepare_insert(self):
        keys = []
        values = []
        for t in self.keymap():
            objkey, dbkey = t[:2]
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

        rkeymap = {t[1]: t[0] for t in cls.keymap()}

        initdict = {
            rkeymap[keys[idx]]: cls.deserialize_inner(row[idx])
            for idx in range(len(keys))
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
        if isinstance(val, int):
            return val
        try:
            return json.loads(val)
        except Exception as ex:
            Logger.debug(
                f"Couldn't deserialize value, using string representation instead (value: {repr(val)}): {repr(ex)}"
            )
            return str(val)
