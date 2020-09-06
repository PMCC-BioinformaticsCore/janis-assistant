import json
import base64
import pickle
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Union, Optional, Set

from janis_core import Logger


def prep_object_for_db(val, encode: bool):
    prepped = _prepare_for_serialization(val)
    if not encode:
        return prepped
    else:
        return json.dumps(prepped)


def _prepare_for_serialization(val) -> Optional[any]:
    if val is None:
        return None
    elif isinstance(val, Enum):
        return val.value
    elif isinstance(val, datetime):
        return str(val)
    elif isinstance(val, list):
        return [_prepare_for_serialization(el) for el in val]
    elif isinstance(val, dict):
        return {k: _prepare_for_serialization(v) for k, v in val.items()}

    return val


def deserialize_inner(val):
    if val is None:
        return None
    try:
        return json.loads(val)
    except Exception as ex:
        # tbh, sometimes the mysql database converts '"myvalue"' -> 'myvalue' (dropping the quotes), we'll do
        Logger.debug(
            f"Couldn't deserialize value, using string representation instead (value: {repr(val)}): {repr(ex)}"
        )
        return str(val)


def pickle_obj(obj):
    if obj is None:
        return None
    try:
        return pickle.dumps(obj, protocol=2)
    except Exception as ex:
        Logger.warn(f"Couldn't pickle {repr(obj)} as encountered {repr(ex)}")
        return None


def unpickle_obj(obj):
    if obj is None:
        return None
    try:
        return pickle.loads(obj)
    except Exception as ex:
        Logger.warn(f"Couldn't unpickle {repr(obj)} as encountered {repr(ex)}")
        return None


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
        fields = ", ".join(str(getattr(self, k.name)) for k in self.keymap())
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
            prepared_val = prep_object_for_db(val, encode=t.encode)
            values.append(prepared_val)

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
            rkeymap[dbalias]: deserialize_inner(value)
            if dbalias in dbalias_to_decode
            else value
            for dbalias, value in zip(keys, row)
        }

        return cls(**initdict)


class KVDatabaseObject(ABC):
    def __init__(self, **kwargs):
        self._changes = {}
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if not key.startswith("_"):
            self._changes[key] = value

    def discard_changes(self):
        self._changes = {}

    def get_encoded_changes(self):
        return self.encode_field_dict(self._changes)

    def get_encoded_rows(self):
        return self.encode_field_dict(self.__dict__)

    @classmethod
    def encode_field_dict(cls, d: dict) -> (str, str):
        encode_fields = set(cls.fields_to_encode() or [])
        fields_to_ignore = set(cls.fields_to_ignore() or [])
        rows = []
        for k, v in d.items():
            if k.startswith("_") or v is None or k in fields_to_ignore:
                continue

            rows.append((k, pickle_obj(v)))

        return rows

    @classmethod
    def decode_rows_to_dict(cls, rows: List[Tuple[str, str]]):
        kwargs = {k: unpickle_obj(v) for k, v in rows}
        return cls(**kwargs)

    def update_from_rows(self, rows: List[Tuple[str, str]]):
        for k, v in rows:
            self.__setattr__(k, unpickle_obj(v))
        return self

    @classmethod
    def fields_to_encode(cls) -> Optional[Set[str]]:
        pass

    @classmethod
    def fields_to_ignore(cls) -> Optional[Set[str]]:
        pass
