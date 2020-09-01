import janis_core as j

from unittest import TestCase

from janis_assistant.management.workflowmanager import WorkflowManager

ct = j.CommandToolBuilder(
    tool="TEST_EXTENSION",
    base_command=["echo", "1", ">", "file.out"],
    inputs=[],
    outputs=[j.ToolOutput("out", j.File(extension=".out"), selector="file.out")],
    container="ubuntu:latest",
    version="TEST",
)


class TestOutputEvaluation(TestCase):
    def test_basic_extension(self):
        w = j.WorkflowBuilder("wf")
        w.step("stp", ct)
        w.output("out", source=w.stp.out)

        outputs = WorkflowManager.evaluate_output_params(
            wf=w, inputs={}, submission_id="SID", run_id="RID"
        )

        self.assertEqual(".out", outputs[0].extension)

    def test_basic_extension_override(self):
        w = j.WorkflowBuilder("wf")
        w.step("stp", ct)
        w.output("out", source=w.stp.out, extension="_fastqc.txt")

        outputs = WorkflowManager.evaluate_output_params(
            wf=w, inputs={}, submission_id="SID", run_id="RID"
        )

        self.assertEqual("_fastqc.txt", outputs[0].extension)
