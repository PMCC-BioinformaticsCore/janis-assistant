import json
from enum import Enum
from typing import Dict, Tuple, Any


class Serializable:
    parse_types = {}
    key_map = {}

    def output(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def serialize(key, value) -> Tuple[str, Any]:
        if value is None:
            return key, None
        if isinstance(value, int) or isinstance(value, str) or isinstance(value, float):
            return key, value
        elif isinstance(value, Enum):
            return key, value.value
        elif isinstance(value, dict):
            return key, Serializable.serialize_dict(value, {})
        elif isinstance(value, list):
            return key, [Serializable.serialize(None, t)[1] for t in value]
        elif isinstance(value, Serializable):
            return key, value.to_dict()

        raise Exception(
            "Unable to serialize '{key}' of type '{value}".format(
                key=key, value=type(value)
            )
        )

    @staticmethod
    def serialize_dict(d, km: Dict[str, str]):
        retval = {}
        for k, v in d.items():
            if v is None:
                continue
            if k.startswith("_"):
                continue
            k, v = Serializable.serialize(km.get(k, k), v)
            if not isinstance(v, bool) and not v:
                continue
            retval[k] = v
        return retval

    def to_dict(self):
        return self.serialize_dict(vars(self), self.key_map or {})

    @classmethod
    def from_dict(cls, d):
        import inspect

        kwargs = {}
        argspec = inspect.getfullargspec(cls.__init__)
        ptypes = cls.parse_types or {}

        for k in argspec.args:
            if k not in d:
                continue
            if k in ptypes:
                kwargs[k] = ptypes[k].from_dict(d[k])
            else:
                kwargs[k] = d[k]

        return cls.__init__(**kwargs)
