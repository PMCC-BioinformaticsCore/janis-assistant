import threading
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
import time
from typing import Dict, Any, Optional

from shepherd.utils.logger import Logger


class TaskStatus(Enum):
    PROCESSING = 0
    QUEUED = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4
    TERMINATED = 5

    @staticmethod
    def FINAL_STATES(): return [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TERMINATED]


class Engine(ABC):

    @abstractmethod
    def start_engine(self):
        pass

    @abstractmethod
    def stop_engine(self):
        pass

    @abstractmethod
    def start_task(self, task):
        pass

    # @abstractmethod
    # def create_task(self, source, inputs, dependencies) -> str:
    #     pass

    @abstractmethod
    def poll_task(self, identifier) -> TaskStatus:
        pass

    @abstractmethod
    def outputs_task(self, identifier) -> Dict[str, Any]:
        pass

    def terminate_task(self, identifier) -> TaskStatus:
        pass


class TaskBase:
    def __init__(self, engine: Engine,
                 source=None, source_path=None,
                 inputs=None, input_paths=None,
                 dependencies=None, dependencies_path=None,

                 identifier=None,
                 status: Optional[TaskStatus] = None, outputs=None, task_start=None, task_finish=None):
        self.engine = engine

        self.source = source
        self.source_path = source_path
        self.inputs = (inputs if isinstance(inputs, list) else [inputs]) if inputs is not None else None
        self.input_paths = (
            input_paths if isinstance(input_paths, list) else [input_paths]) if input_paths is not None else None
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
        self.engine.start_task(self)


class AsyncTask(threading.Thread, TaskBase):
    def __init__(self, engine: Engine,
                 source=None, source_path=None,
                 inputs=None, input_paths=None,
                 dependencies=None, dependencies_path=None,

                 identifier=None,
                 handler=None, onerror=None,
                 status: Optional[TaskStatus] = None, outputs=None, task_start=None, task_finish=None):
        threading.Thread.__init__(self)
        TaskBase.__init__(self, engine=engine,
                          source=source, source_path=source_path, inputs=inputs, input_paths=input_paths,
                          dependencies=dependencies, dependencies_path=dependencies_path, identifier=identifier,
                          status=status, outputs=outputs, task_start=task_start, task_finish=task_finish)

        self.handler = handler
        self.onerror = onerror

    def run(self):
        TaskBase.start(self)
        if self.status == TaskStatus.COMPLETED and self.handler:
            self.handler(self, self.status, self.outputs)
        elif self.onerror:
            self.onerror(self)


class SyncTask(TaskBase):
    def __init__(self, engine: Engine,
                 source=None, source_path=None,
                 inputs=None, input_paths=None,
                 dependencies=None, dependencies_path=None,

                 identifier=None,
                 status: Optional[TaskStatus] = None, outputs=None, task_start=None, task_finish=None,
                 should_start=True):
        TaskBase.__init__(self, engine=engine,
                          source=source, source_path=source_path, inputs=inputs, input_paths=input_paths,
                          dependencies=dependencies, dependencies_path=dependencies_path, identifier=identifier,
                          status=status, outputs=outputs, task_start=task_start, task_finish=task_finish)

        if should_start:
            self.start()
