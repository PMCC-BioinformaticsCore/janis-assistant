import tempfile
import os
from shutil import rmtree
from unittest import TestCase

from janis_runner.data.providers.task.dbmanager import TaskDbManager
from janis_runner.data.enums import InfoKeys, ProgressKeys


class TestDatabaseManager(TestCase):
    def setUp(self):
        self.tid = "a3ie8h"
        self.tmpdir = tempfile.gettempdir()
        self.path = self.tmpdir + self.tid + "/"
        if os.path.exists(self.path):
            rmtree(self.path)
        os.makedirs(self.path)
        self.db = TaskDbManager(self.path)

    def tearDown(self):
        from shutil import rmtree

        self.db.commit()
        rmtree(self.path)

    def test_add_info(self):
        self.db.add_meta_info(InfoKeys.engineId, "cromwell-identifier")
        self.assertEqual("cromwell-identifier", self.db.get_engine_identifier())

    def test_mark_completed_step(self):
        self.assertFalse(self.db.progress_has_completed(ProgressKeys.copiedOutputs))
        self.db.progress_mark_completed(ProgressKeys.copiedOutputs)
        self.assertTrue(self.db.progress_has_completed(ProgressKeys.copiedOutputs))
