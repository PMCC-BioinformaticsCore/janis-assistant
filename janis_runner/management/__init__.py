# Components to create data
import abc
import inspect
from typing import List, Tuple


class Archivable(abc.ABC):
    @abc.abstractmethod
    def id(self):
        pass

    def db_to_kwargs(self, keys: List[str] = None):
        if not keys:
            keys = {k for k, v in vars(self).items() if not k.startswith("_")}

        return {k: str(self.__getattribute__(k)) for k in keys}

    @classmethod
    def db_from_kvargs(cls, kvargs: [Tuple[str, any]]):
        return cls.db_from_kwargs(**{k: v for k, v in kvargs})

    @classmethod
    def db_from_kwargs(cls, **kwargs):
        init_kwargs = cls.get_required_input_params_for_cls(kwargs)
        # noinspection PyArgumentList
        self = cls(**init_kwargs)

        for k, v in kwargs.items():
            if k in init_kwargs:
                continue
            self.__setattr__(k, v)
        return self

    @classmethod
    def get_required_input_params_for_cls(cls, valuesdict):
        argspec = inspect.getfullargspec(cls.__init__)
        args, defaults = argspec.args, argspec.defaults
        endidx = len(defaults) if defaults else None
        required_param_keys = set(args[1:-endidx]) if endidx else args[1:]

        inspect_ignore_keys = {"self", "args", "kwargs"}
        # Params can't shadow the built in 'id', so we'll put in a little hack
        # to guess the required param name that ends in

        id_field_names = [
            k for k in required_param_keys if k == "identifier" or k.endswith("_id")
        ]
        id_field_name = None
        id_field_value = valuesdict.get("identifier")

        if len(id_field_names) == 1:
            id_field_name = id_field_names[0]
            inspect_ignore_keys.add(id_field_name)
        elif len(id_field_names) > 1:
            print(
                "Warning, can't determine id field as there are potentially many: "
                + ", ".join(id_field_names)
            )

        required_init_kwargs = {}
        try:
            required_init_kwargs = {
                k: valuesdict[k]
                for k in required_param_keys
                if (k not in inspect_ignore_keys)
            }
        except Exception as e:
            print(e)

        if id_field_name:
            required_init_kwargs[id_field_name] = id_field_value

        return required_init_kwargs
