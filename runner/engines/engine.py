import threading
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
import time
from typing import Dict, Any, Optional

from utils.logger import Logger


class TaskStatus(Enum):
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
    def create_task(self, source, inputs, dependencies) -> str:
        pass

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
        Logger.log("Creating workflow and submitting to Cromwell")

        ins, deps = [], None
        if self.inputs or self.input_paths:
            ins = self.inputs if self.inputs else [open(i) for i in self.input_paths]
        if self.dependencies or self.dependencies_path:
            deps = self.dependencies if self.dependencies else open(self.dependencies_path, 'rb')

        self.identifier = self.engine.create_task(
            source=self.source if self.source else open(self.source_path, 'rb'),
            inputs=ins,
            dependencies=deps
        )
        Logger.info("Created task with id: " + self.identifier)
        Logger.log("Task is now processing")
        self.task_start = datetime.now()
        while self.status not in TaskStatus.FINAL_STATES():
            status = self.engine.poll_task(self.identifier)
            if status != self.status:
                Logger.info("Task ('{id}') has progressed to: '{status}'".format(id=self.identifier, status=status))
            self.status = status
            time.sleep(1)

        self.task_finish = datetime.now()
        Logger.info("Task ('{id}') has finished processing: {t} seconds"
                    .format(id=self.identifier, t=str((self.task_finish - self.task_start).total_seconds())))

        if self.status == TaskStatus.COMPLETED:
            Logger.log("Collecting outputs")
            self.outputs = self.engine.outputs_task(self.identifier)


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
