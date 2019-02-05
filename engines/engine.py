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


class Task(threading.Thread):
    def __init__(self, engine: Engine, source, inputs, dependencies=None, identifier=None, handler=None,
                 status: Optional[TaskStatus] = None, outputs=None, task_start=None, task_finish=None,
                 should_start=None):
        threading.Thread.__init__(self)
        self.engine = engine

        self.source = source
        self.inputs = inputs if isinstance(inputs, list) else [inputs]
        self.dependencies = dependencies
        self.handler = handler

        self.status: Optional[TaskStatus] = status
        self.identifier = identifier
        self.status = status
        self.outputs = outputs

        self.task_start = task_start
        self.task_finish = task_finish

        if should_start:
            self.start()

    def poll(self):
        self.status = self.engine.poll_task(self.identifier)
        return self.status

    def terminate(self):
        self.status = self.engine.terminate_task(self.identifier)
        return self.status

    def start(self):
        Logger.log("Creating workflow and submitting to Cromwell")
        self.identifier = self.engine.create_task(
            source=self.source,
            inputs=self.inputs,
            dependencies=self.dependencies
        )
        Logger.info("Created task with id: " + self.identifier)
        Logger.log("Task is now processing")
        self.task_start = datetime.now()
        while self.status != TaskStatus.COMPLETED:
            status = self.engine.poll_task(self.identifier)
            if status != self.status:
                Logger.info(f"Task ('{self.identifier}') has progressed to: '{status}'")
            self.status = status
            time.sleep(1)

        self.task_finish = datetime.now()
        Logger.info("Task ('{id}') has finished processing: {t} seconds"
                    .format(id=self.identifier, t=str((self.task_finish - self.task_start).total_seconds())))
        Logger.log("Collecting outputs")
        self.outputs = self.engine.outputs_task(self.identifier)

        if self.handler:
            self.handler(self, self.status, self.outputs)
