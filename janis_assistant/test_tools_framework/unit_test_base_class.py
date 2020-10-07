import unittest
import os
import subprocess
from abc import ABC, abstractmethod


class JanisToolUnitTestClass(unittest.TestCase, ABC):
    input_params = {}

    @classmethod
    @abstractmethod
    def tool_full_path(cls):
        pass

    @classmethod
    def setUpClass(cls):
        curr_dir = os.getcwd()
        output_dir = os.path.join(curr_dir, "tests_output", "junytest")

        input_list = []
        for key, val in cls.input_params.items():
            input_list.append(key)
            input_list.append(val)

        subprocess.run(
            [
                "janis",
                "run",
                "--engine",
                "cwltool",
                "-o",
                os.path.join(output_dir, "cwl"),
                cls.tool_full_path(),
            ]
            + input_list
        )
