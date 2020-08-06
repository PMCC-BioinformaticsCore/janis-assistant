from abc import ABC, abstractmethod
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Callable

from janis_assistant.data.enums import TaskStatus
from janis_assistant.data.models.run import RunModel
from janis_assistant.engines.enginetypes import EngineType
from janis_assistant.management import Archivable


class Engine(Archivable, ABC):
    def __init__(
        self, identifier: str, engtype: EngineType, execution_dir: str, logfile=None
    ):
        self.identifier = identifier
        self.engtype = engtype
        self.execution_dir = execution_dir
        self.is_started = False
        self.process_id = None
        self.logfile = logfile
        self._logfp = None

        self.progress_callbacks: Dict[str, List[Callable[[RunModel], None]]] = {}

    def add_callback(
        self, engine_identifier: str, callback: Callable[[RunModel], None]
    ):
        self.progress_callbacks[engine_identifier] = self.progress_callbacks.get(
            engine_identifier, []
        ) + [callback]

    def id(self):
        return self.identifier

    def test_connection(self):
        pass

    def description(self):
        return self.engtype.value

    @abstractmethod
    def start_engine(self):
        pass

    @abstractmethod
    def stop_engine(self):
        pass

    @abstractmethod
    def start_from_paths(self, wid, source_path: str, input_path: str, deps_path: str):
        pass

    @abstractmethod
    def poll_task(self, identifier) -> TaskStatus:
        pass

    @abstractmethod
    def outputs_task(self, identifier) -> Dict[str, Any]:
        pass

    @abstractmethod
    def terminate_task(self, identifier) -> TaskStatus:
        pass

    @abstractmethod
    def metadata(self, identifier) -> RunModel:
        pass

    keys_to_ignore = {"progress_callbacks"}

    def __getstate__(self):
        d = super().__getstate__().copy()
        for k in self.keys_to_ignore:
            if k in d:
                del d[k]

        return d

    def __setstate__(self, state):
        super().__setstate__(state)
        # Don't reopen as we can't reconnect to the Cromwell instance anyway
        self._logfp = None  # open(self.logfile, "a+")
        self.progress_callbacks = {}
