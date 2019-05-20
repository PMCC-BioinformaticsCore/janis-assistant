import tempfile
import os
from unittest import TestCase

from shepherd.data.dbmanager import DatabaseManager
from shepherd.data.schema import TaskStatus


class TestDatabaseManager(TestCase):
    def setUp(self):
        self.tid = "a3ie8h"
        self.tmpdir = tempfile.gettempdir()
        self.path = self.tmpdir + self.tid + "/"
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        self.db = DatabaseManager("tid", self.path)

    def tearDown(self):
        from shutil import rmtree
        rmtree(self.path)

    def test_table_noexists(self):
        self.assertFalse(self.db.table_exists("test"))

    def test_table_exists(self):
        self.assertFalse(self.db.table_exists("info"))
        self.db.create_info_table_if_required()
        self.assertTrue(self.db.table_exists("info"))

    def test_add_info(self):
        self.db.create_info_table_if_required()
        self.db.add_meta_info(DatabaseManager.InfoKeys.engineId, "cromwell-identifier")
        self.assertEqual("cromwell-identifier", self.db.get_engine_identifier())
