import unittest

from janis_assistant.data.enums import TaskStatus


class TestTaskStatus(unittest.TestCase):
    def test_in_final_state(self):
        self.assertTrue(TaskStatus.COMPLETED.is_in_final_state())
        self.assertTrue(TaskStatus.FAILED.is_in_final_state())
        self.assertTrue(TaskStatus.ABORTED.is_in_final_state())
        self.assertTrue(TaskStatus.DRY_RUN.is_in_final_state())
        self.assertTrue(TaskStatus.SUSPENDED.is_in_final_state())

        self.assertFalse(TaskStatus.PROCESSING.is_in_final_state())
        self.assertFalse(TaskStatus.QUEUED.is_in_final_state())
        self.assertFalse(TaskStatus.RUNNING.is_in_final_state())
        self.assertFalse(TaskStatus.ABORTING.is_in_final_state())
