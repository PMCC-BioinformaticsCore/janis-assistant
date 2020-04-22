import unittest

from janis_assistant.main import fromjanis
from janis_core import WorkflowBuilder

from janis_assistant.utils.batchrun import BatchRunRequirements

from janis_assistant.modifiers.batchmodifier import BatchPipelineModifier

try:
    from janis_unix import Echo

    has_janis_unix = True
except ImportError:
    has_janis_unix = False


@unittest.skipUnless(has_janis_unix, "Run these if janis_unix is available")
class TestBatchModifier(unittest.TestCase):
    def test_basic(self):
        inputs = {"inp": ["test1", "test2"]}
        modifier = BatchPipelineModifier(BatchRunRequirements(["inp"], "inp"))
        new_workflow = modifier.tool_modifier(Echo(), inputs, {})
        self.assertIsInstance(new_workflow, WorkflowBuilder)
        self.assertEqual("echo", new_workflow.id())
        # tinputs = new_workflow.tool_inputs()

    def test_output_name_and_folder(self):
        w = WorkflowBuilder("wf")
        w.input("inp", str)
        w.step("print", Echo(inp=w.inp))
        w.output("out", source=w.print, output_name=w.inp, output_folder=[w.inp])

        inputs = {"inp": ["test1", "test2"]}
        modifier = BatchPipelineModifier(BatchRunRequirements(["inp"], "inp"))
        new_workflow = modifier.tool_modifier(w, inputs, {})
        print(new_workflow)
