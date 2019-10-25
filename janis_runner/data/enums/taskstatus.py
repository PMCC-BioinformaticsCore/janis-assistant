from enum import Enum


class TaskStatus(Enum):
    PROCESSING = "processing"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"

    @staticmethod
    def all():
        return [t for t in TaskStatus]

    @staticmethod
    def final_states():
        return [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.ABORTED]

    def __str__(self):
        __str = {
            TaskStatus.PROCESSING.value: "Processing",
            TaskStatus.QUEUED.value: "Queued",
            TaskStatus.RUNNING.value: "Running",
            TaskStatus.COMPLETED.value: "Completed",
            TaskStatus.FAILED.value: "Failed",
            TaskStatus.ABORTED.value: "Terminated",
        }
        return __str[self.value]

    def symbol(self):
        __str = {
            TaskStatus.PROCESSING.value: "...",
            TaskStatus.QUEUED.value: " ",
            TaskStatus.RUNNING.value: "~",
            TaskStatus.COMPLETED.value: "✓",
            TaskStatus.FAILED.value: "!",
            TaskStatus.ABORTED.value: "T",
        }
        return __str[self.value]
