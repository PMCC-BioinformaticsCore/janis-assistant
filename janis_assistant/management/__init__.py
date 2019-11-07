# Components to create data
import abc
import inspect
from typing import List, Tuple


class Archivable(abc.ABC):
    @abc.abstractmethod
    def id(self):
        pass

    def __getstate__(self):
        return {k: v for k, v in vars(self).items() if not k.startswith("_")}

    def __setstate__(self, state):
        self.__dict__.update(state)

        return self
