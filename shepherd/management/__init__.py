# Components to create data
import abc
import inspect
from typing import List, Tuple


class Archivable(abc.ABC):

    def db_to_kwargs(self, keys: List[str]=None):
        if not keys:
            keys = {k for k,v in self.__dict__.items() if not k.startswith("_") and not callable(v)}

        return {k: self.__getattribute__(k) for k in keys}

    @classmethod
    def db_from_kvargs(cls, kvargs: [Tuple[str, any]]):
        return cls.db_from_kwargs(**{k: v for k, v in kvargs})

    @classmethod
    def db_from_kwargs(cls, **kwargs):
        init_kwargs = cls.get_required_input_params_for_cls(kwargs)
        self = cls(**init_kwargs)

        for k, v in kwargs.items():
            if k in init_kwargs: continue
            self.__setattr__(k, v)
        return self


    @classmethod
    def get_required_input_params_for_cls(cls, valuesdict):
        try:
            argspec = inspect.getfullargspec(cls.__init__)
        except:
            # we're in Python 2
            argspec = inspect.getargspec(cls.__init__)

        args, defaults = argspec.args, argspec.defaults
        endidx = len(defaults) if defaults else None
        required_param_keys = set(args[1:-endidx]) if endidx else args[1:]

        inspect_ignore_keys = {"self", "args", "kwargs"}
        # Params can't shadow the built in 'id', so we'll put in a little hack
        # to guess the required param name that ends in

        id_field_names = [k for k in required_param_keys if k == "id" or k.endswith("_id")]
        id_field_name = None
        id_field_value = valuesdict.get("id")

        if len(id_field_names) == 1:
            id_field_name = id_field_names[0]
            inspect_ignore_keys.add(id_field_name)
        elif len(id_field_names) > 1:
            print("Warning, can't determine if there are multiple id fieldnames")

        required_init_kwargs = {k: valuesdict[k] for k in required_param_keys if (k not in inspect_ignore_keys)}
        if id_field_name:
            required_init_kwargs[id_field_name] = id_field_value

        return required_init_kwargs