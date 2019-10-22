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
    def start_from_task(self, task):
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


class TaskBase:
    def __init__(
        self,
        wid: str,
        engine: Engine,
        source=None,
        source_path=None,
        inputs=None,
        input_paths=None,
        dependencies=None,
        dependencies_path=None,
        identifier=None,
        status: Optional[TaskStatus] = None,
        outputs=None,
        task_start=None,
        task_finish=None,
    ):
        self.wid = wid
        self.engine = engine

        self.source = source
        self.source_path = source_path
        self.inputs = (
            (inputs if isinstance(inputs, list) else [inputs])
            if inputs is not None
            else None
        )
        self.input_paths = (
            (input_paths if isinstance(input_paths, list) else [input_paths])
            if input_paths is not None
            else None
        )
        self.dependencies = dependencies
        self.dependencies_path = dependencies_path

        self.status: Optional[TaskStatus] = status
        self.identifier = identifier
        self.status = status
        self.outputs = outputs

        self.task_start = task_start
        self.task_finish = task_finish

    def poll(self):
        self.status = self.engine.poll_task(self.identifier)
        return self.status

    def terminate(self):
        self.status = self.engine.terminate_task(self.identifier)
        return self.status

    def start(self):
        self.engine.start_from_task(self)
        return self


class AsyncTask(threading.Thread, TaskBase):
    def __init__(
        self,
        wid: str,
        engine: Engine,
        source=None,
        source_path=None,
        inputs=None,
        input_paths=None,
        dependencies=None,
        dependencies_path=None,
        identifier=None,
        handler=None,
        onerror=None,
        status: Optional[TaskStatus] = None,
        outputs=None,
        task_start=None,
        task_finish=None,
    ):
        threading.Thread.__init__(self)
        TaskBase.__init__(
            self,
            wid=wid,
            engine=engine,
            source=source,
            source_path=source_path,
            inputs=inputs,
            input_paths=input_paths,
            dependencies=dependencies,
            dependencies_path=dependencies_path,
            identifier=identifier,
            status=status,
            outputs=outputs,
            task_start=task_start,
            task_finish=task_finish,
        )

        self.handler = handler
        self.onerror = onerror

    def run(self):
        TaskBase.start(self)
        if self.status == TaskStatus.COMPLETED and self.handler:
            self.handler(self, self.status, self.outputs)
        elif self.onerror:
            self.onerror(self)


class SyncTask(TaskBase):
    def __init__(
        self,
        wid: str,
        engine: Engine,
        source=None,
        source_path=None,
        inputs=None,
        input_paths=None,
        dependencies=None,
        dependencies_path=None,
        identifier=None,
        status: Optional[TaskStatus] = None,
        outputs=None,
        task_start=None,
        task_finish=None,
        should_start=True,
    ):
        TaskBase.__init__(
            self,
            wid=wid,
            engine=engine,
            source=source,
            source_path=source_path,
            inputs=inputs,
            input_paths=input_paths,
            dependencies=dependencies,
            dependencies_path=dependencies_path,
            identifier=identifier,
            status=status,
            outputs=outputs,
            task_start=task_start,
            task_finish=task_finish,
        )

        if should_start:
            self.start()
