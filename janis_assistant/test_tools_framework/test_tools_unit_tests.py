import unittest
import janis_core as jc
from typing import Dict, Optional, List, Set
from .helpers import run_tool_unit_tests, get_all_tools, print_test_report


class ToolsTestSuite:
    def run_test(self, modules: List):
        all_tools = get_all_tools(modules)

        failed = {}
        succeeded = set()
        # TODO: revert to full list
        # for tool_versions in all_tools:
        for tool_versions in all_tools[132:134]:
            for versioned_tool in tool_versions:
                result = run_tool_unit_tests(versioned_tool)
                count_failed = len(result.failures) + len(result.errors)

                if count_failed > 0:
                    failed[
                        versioned_tool.versioned_id()
                    ] = f"{count_failed} unit test failed"
                else:
                    succeeded.add(versioned_tool.versioned_id())

        print_test_report(failed, succeeded)

        if len(failed) > 0:
            raise Exception(
                f"There were {len(failed)} tool(s) that fail their unit test suite. "
                f"Please check to ensure your tool is in the list below"
            )
