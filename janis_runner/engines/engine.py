import threading
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List

from janis_runner.data.models.workflow import WorkflowModel
from janis_runner.engines.enginetypes import EngineType
from janis_runner.management import Archivable
from janis_runner.data.enums import TaskStatus


class Engine(Archivable, ABC):
    def __init__(self, identifier: str, engtype: EngineType, watch=True, logfile=None):
        self.identifier = identifier
        self.engtype = engtype
        self.is_started = False
        self.process_id = None
        self.logfile = logfile
        self.watch = watch
        self._logfp = None

    def id(self):
        return self.identifier

    @staticmethod
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
    def metadata(self, identifier) -> WorkflowModel:
        pass

    def __setstate__(self, state):
        super().__setstate__(state)
        # Don't reopen as we can't reconnect to the Cromwell instance anyway
        self._logfp = None  # open(self.logfile, "a+")
