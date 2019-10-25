import importlib.util
from unittest import TestCase

import builtins


# class TestImports(TestCase):
#     def test_test(self):
#         file = "/Users/franklinmichael/janis-search-path/simple.py"
#         spec = importlib.util.spec_from_file_location("module.name", file)
#         foo = importlib.util.module_from_spec(spec)
#         import janis_core
#
#         stored_attributes = set()
#         for k, v in janis_core.__dict__.items():
#             if k.startswith("_"):
#                 continue
#             setattr(builtins, k, v)
#             stored_attributes.add(k)
#
#         # builtins.WorkflowBuilder = WorkflowBuilder
#         complete = False
#         while not complete:
#             try:
#                 spec.loader.exec_module(foo)
#                 complete = True
#             except NameError as e:
#                 name = str(e).split("'")[1]
#                 tool = janis_core.JanisShed.get_by_class_name(name)
#                 if tool:
#                     setattr(builtins, name, tool)
#                     stored_attributes.add(name)
#                 else:
#                     raise e
#         for k in stored_attributes:
#             delattr(builtins, k)
#
#         print({k: v for k, v in foo.__dict__.items() if not k.startswith("_")})


import unittest


class TestFMP(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # set `fmp` and `locator` onto the class through `cls`
        print("Setting up")
        cls.fmp = ""
        cls.locator = ""

    @classmethod
    def tearDownClass(cls):
        # Dispose of those resources through `cls.fmp` and `cls.locator`
        print("Disposing")

    def test_method(self):
        # access through self:
        print("Testing")
        self.assertEqual(self.fmp, self.locator)
