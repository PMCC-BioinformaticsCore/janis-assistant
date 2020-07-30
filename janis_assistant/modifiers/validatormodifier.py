from abc import ABC
from typing import Dict, List, Tuple

from janis_core import Tool, Logger, WorkflowBuilder, Workflow, WorkflowBase

from janis_assistant.validation import ValidationRequirements
from janis_assistant.modifiers.base import PipelineModifierBase
from janis_assistant.validation import ensure_outputs_are_in_workflow_and_are_compatible


class ValidatorPipelineModifier(PipelineModifierBase):
    def __init__(self, validation_requirements: ValidationRequirements):
        self.validation = validation_requirements

    def tool_modifier(self, tool: Tool, inputs: Dict, hints: Dict[str, str]) -> Tool:
        from janis_bioinformatics.data_types import FastaWithDict, Vcf, Bed
        from janis_bioinformatics.tools.illumina import HapPyValidator_0_3_9

        (
            failed_outputs,
            untyped_outputs,
        ) = ensure_outputs_are_in_workflow_and_are_compatible(
            tool, self.validation.fields, Vcf()
        )

        if len(failed_outputs) > 0:
            raise Exception(
                f"Some outputs for validation were not found in the tool '{tool.id()}': "
                f"{', '.join(failed_outputs)}"
            )

        if len(untyped_outputs) > 0:
            Logger.critical(
                f"Some outputs for validation from the tool '{tool.id()}' were not "
                f"compatible with VCF: {', '.join(untyped_outputs)}"
            )

        w = WorkflowBuilder(tool.id() + "_validated")

        w.input("validatorReference", FastaWithDict, value=self.validation.reference)
        w.input("validatorTruthVCF", Vcf, value=self.validation.truthVCF)
        w.input(
            "validatorIntervals", Bed(optional=True), value=self.validation.intervals
        )

        inpdict = {i.id(): w.input(i.id(), i.intype) for i in tool.tool_inputs()}
        toolstp = w.step(tool.id(), tool(**inpdict))

        if isinstance(tool, WorkflowBase):
            wf: Workflow = tool
            for o in wf.output_nodes.values():
                w.output(
                    identifier=o.id(),
                    source=toolstp[o.id()],
                    output_folder=o.output_folder,
                    output_name=o.output_name,
                )
        else:
            for o in tool.tool_outputs():
                w.output(identifier=o.id(), source=toolstp[o.id()])

        for o in self.validation.fields:

            sid = "validator_" + o
            valstp = w.step(
                sid,
                HapPyValidator_0_3_9(
                    compareVCF=toolstp[o],
                    reportPrefix=o,  # this will generate an input node with format validator_{o}_reportPrefix
                    reference=w.validatorReference,
                    truthVCF=w.validatorTruthVCF,
                    intervals=w.validatorIntervals,
                ),
            )

            # Connect all the inputs of the validator to an output
            for vo in valstp.tool.outputs():
                w.output(
                    f"validated_{o}_{vo.id()}",
                    source=valstp[vo.id()],
                    output_folder="validated",
                )

        return w
