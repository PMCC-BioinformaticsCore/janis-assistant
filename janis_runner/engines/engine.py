import threading
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, Optional, List

from janis_runner.management import Archivable
from janis_runner.data.models.schema import TaskStatus, TaskMetadata


class Engine(Archivable, ABC):
    class EngineType(Enum):
        cromwell = "cromwell"
        cwltool = "cwltool"
        toil = "toil"

        def __str__(self):
            return self.value

    def __init__(self, identifier: str, engtype: EngineType):
        self.identifier = identifier
        self.engtype = engtype

    def db_to_kwargs(self, keys: List[str] = None):
        k = ["engtype"]
        if keys:
            k.extend(keys)
        return super(Engine, self).db_to_kwargs(k)

    def id(self):
        return self.identifier

    @abstractmethod
    def start_engine(self):
        pass

    @abstractmethod
    def stop_engine(self):
        pass

    @abstractmethod
    def start_from_paths(self, tid, source_path: str, input_path: str, deps_path: str):
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
    def metadata(self, identifier) -> TaskMetadata:
        pass


class TaskBase:
    def __init__(
        self,
        tid: str,
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
        self.tid = tid
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
        tid: str,
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
            tid=tid,
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
        tid: str,
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
            tid=tid,
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
