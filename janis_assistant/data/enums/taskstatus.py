from enum import Enum


class TaskStatus(Enum):
    PROCESSING = "processing"
    PREPARED = "prepared"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTING = "aborting"
    ABORTED = "aborted"
    DRY_RUN = "dry-run"
    ON_HOLD = "on-hold"
    SUSPENDED = "suspended"

    @staticmethod
    def all():
        return [t for t in TaskStatus]

    @staticmethod
    def final_states():
        return [
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.ABORTED,
            TaskStatus.DRY_RUN,
            TaskStatus.SUSPENDED,
        ]

    def is_in_final_state(self):
        return self in self.final_states()

    def __str__(self):
        __str = {
            TaskStatus.PROCESSING.value: "Processing",
            TaskStatus.QUEUED.value: "Queued",
            TaskStatus.RUNNING.value: "Running",
            TaskStatus.COMPLETED.value: "Completed",
            TaskStatus.FAILED.value: "Failed",
            TaskStatus.ABORTED.value: "Terminated",
            TaskStatus.DRY_RUN.value: "Dry run",
            TaskStatus.ON_HOLD.value: "On hold",
            TaskStatus.ABORTING.value: "Aborting",
            TaskStatus.SUSPENDED.value: "Suspended",
            TaskStatus.PREPARED.value: "Prepared",
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
            TaskStatus.DRY_RUN.value: "DR",
            TaskStatus.ON_HOLD.value: "?",
            TaskStatus.ABORTING.value: "~x",
            TaskStatus.SUSPENDED.value: "II",
            TaskStatus.PREPARED.value: ":",
        }
        return __str[self.value]

    def to_hexcolor(self):
        __str = {
            TaskStatus.FAILED.value: "#a63030",
        }

        return __str.get(self.value)

    def get_exit_code(self):
        if (
            self == TaskStatus.FAILED
            or self == TaskStatus.ABORTED
            or self == TaskStatus.ABORTING
            or self == TaskStatus.SUSPENDED
        ):
            return 3

        # Queued, Completed, Dry run, On Hold, Prepared
        return 0

    @classmethod
    def collapse_states(cls, states: list):

        if len(states) == 0:
            return cls.PROCESSING

        running = {cls.RUNNING, cls.PREPARED, cls.QUEUED, cls.PROCESSING}
        if any(s in running for s in states):
            return cls.RUNNING
        if any(s == cls.ABORTING for s in states):
            return cls.ABORTING
        if any(s == cls.ABORTED for s in states):
            return cls.ABORTED
        if any(s == cls.FAILED for s in states):
            return cls.FAILED
        if all(s == cls.COMPLETED for s in states):
            return cls.COMPLETED

        return states[0]
