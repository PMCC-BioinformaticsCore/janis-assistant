import unittest

from janis_runner.management.configmanager import ConfigManager
from janis_runner.data.models.filescheme import LocalFileScheme, SSHFileScheme


class TestTaskManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tm = ConfigManager()
        cls.tm.cursor.execute("DELETE FROM fileschemes")
        cls.tm.commit()


class TestPersistFileschemes(TestTaskManager):
    def test_persist_local_filescheme(self):
        fs = LocalFileScheme()
        self.tm.persist_filescheme(fs, should_commit=True)

        f = self.tm.get_filescheme("local")
        self.assertEqual("local", f.id())
        self.assertEqual("local", f.fstype)

    def test_persist_ssh_filescheme(self):
        fs = SSHFileScheme("pmac", "cluster")
        self.tm.persist_filescheme(fs, should_commit=True)
        f: SSHFileScheme = self.tm.get_filescheme("pmac")

        self.assertIsInstance(f, SSHFileScheme)
        self.assertEqual("pmac", f.id())
        self.assertEqual("ssh", f.fstype)
        self.assertEqual("cluster", f.connectionstring)


# class TestPersistEngines(TestTaskManager):
#     def test_persist_local_cromwell(self):
#         eng = Cromwell('local-test')
#         self.tm.persist_engine(engine=eng)
#
#         e = self.tm.get_engine()
